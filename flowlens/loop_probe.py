"""事件循环延迟探针（P2d-3 二.3，二.3 授权的测量工具）。

**只在 FLOWLENS_DEBUG=1 时启用**：未开启时 `make_loop_probe()` 返回 `enabled=False`
的探针，`start()` 不创建任何后台任务、`record_tick()` 为空操作——零任务、零开销。
启用时：

- 每 100 ms 用 `loop.call_soon` 调度一个立即执行的小任务，记录"计划执行时刻"到
  "实际执行时刻"之差（事件循环被长任务 / gc / tick 占住的额外延迟，正是决定查询
  p99 的量）；
- 每分钟把本分钟的延迟汇总成 p50 / p99 / 最大值写日志，并列出本分钟最长的 5 次
  延迟及其发生的时刻（epoch ms）；
- `record_tick()` 记录每拍定稿（`engine.advance`）与快照（`refresh_snapshot_async`）
  各自的耗时与时刻，供与"最长几次延迟"对齐、判断延迟是不是定稿或快照造成的。

这是 P2d-3 第零节"唯一服务代码改动"之外的**第二处**改动，由二.3 明确授权（二.3 把
"仅 FLOWLENS_DEBUG=1 时开启的探针"列为可选项）；见 docs/lanes/P2-notes.md P2d-3 与
AI_LOG。生产不设 FLOWLENS_DEBUG，行为与无探针完全一致。
"""

import asyncio
import logging
import time

from .api import debug

log = logging.getLogger("flowlens.loop_probe")


def _pctl(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = (len(s) - 1) * p / 100.0
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


class LoopProbe:
    """事件循环延迟探针。`enabled=False` 时所有方法均为空操作。"""

    def __init__(self, enabled: bool, *, interval: float = 0.1,
                 report_every: float = 60.0):
        self.enabled = enabled
        self.interval = interval
        self.report_every = report_every
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        # 本分钟采集的延迟（微秒）与对应时刻（epoch ms）
        self._sample_us: list[float] = []
        self._sample_ts: list[int] = []
        # 全生命周期保留的最长 5 次（供收尾与 tick 对齐）
        self._top: list[tuple[float, int]] = []

    def start(self) -> None:
        """启用时创建后台任务；未启用时不创建任何任务。需在运行中的事件循环里调用。"""
        if not self.enabled:
            return
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def close(self) -> None:
        """停止并做最后一次汇总（未启用时为空操作）。"""
        if not self.enabled:
            return
        self._stop.set()
        if self._task is not None:
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        next_t = loop.time() + self.interval
        report_at = next_t
        while not self._stop.is_set():
            # 用 loop.call_at 把小任务安排到 next_t：若事件循环在 next_t 之前被
            # 长任务（定稿/快照/写入块）占住，回调就被推到解除阻塞之后才跑，
            # "实际执行时刻 − 计划时刻"正是那段独占延迟。
            # 不能用 wait_for/sleep 睡到点再 call_soon：定时器回调本身在阻塞
            # 期间无法返回，"计划时刻"会取在解除阻塞之后，把最长的那段漏掉
            # （P2d-3 二.3 首跑的测量缺陷）。
            done = asyncio.Event()

            def _task() -> None:
                self._sample_us.append((loop.time() - next_t) * 1e6)
                self._sample_ts.append(int(time.time() * 1000))
                done.set()

            loop.call_at(next_t, _task)
            await done.wait()
            next_t += self.interval
            if loop.time() > next_t + self.interval:
                # 落后超过一个周期（发生了长阻塞）：不追赶积压，重新锚定
                next_t = loop.time() + self.interval
            if time.monotonic() >= report_at:
                self._report()
                report_at += self.report_every
        self._report()  # 收尾最后一次汇总

    def _report(self) -> None:
        us, ts = self._sample_us, self._sample_ts
        if not us:
            return
        top = sorted(zip(us, ts), key=lambda x: -x[0])[:5]
        self._top = sorted(self._top + top, key=lambda x: -x[0])[:5]
        log.info(
            "事件循环延迟 本分钟 p50=%.1f p99=%.1f max=%.1f μs（n=%d）；本分钟最长 5 次: %s",
            _pctl(us, 50), _pctl(us, 99), max(us), len(us),
            ", ".join(f"{a:.0f}μs@{t}" for a, t in top))
        self._sample_us.clear()
        self._sample_ts.clear()

    def final_top(self) -> str:
        """全生命周期最长的几次延迟（供收尾打印、与 tick 时刻对齐）。"""
        if not self._top:
            return ""
        return ", ".join(f"{a:.0f}μs@{t}" for a, t in self._top)

    def record_tick(self, finalize_ms: float, snapshot_ms: float,
                    start_ms: int) -> None:
        """每拍定稿与快照各自的耗时（ms）与**tick 开始时刻**，仅启用时由 Ticker 调用。

        开始时刻（而非结束时刻）供与"最长几次延迟"的时刻对齐：一次延迟落在
        [start, start + 定稿 + 快照] 窗内即可归因为本拍。
        """
        if not self.enabled:
            return
        log.info("tick 分解 定稿(advance)=%.2f ms 快照=%.2f ms @%d",
                 finalize_ms, snapshot_ms, start_ms)


def make_loop_probe() -> LoopProbe:
    """按 FLOWLENS_DEBUG=1 决定是否启用探针（生产不设 → 未启用、零开销）。"""
    return LoopProbe(debug.is_enabled())
