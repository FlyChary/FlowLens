"""后台每秒任务：推进环与去重、刷新默认窗口快照。

放在独立模块而不是 ingest.py：Ticker 是长生命周期的后台驱动（每秒一拍），
ingest.py 是一次性请求的处理，两者职责不同，合在一个模块里会让"请求处理"混进
一个永不结束的任务。
"""

import asyncio
import ctypes
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable

from .engine import Engine

log = logging.getLogger(__name__)


# ---- 定期归还空闲内存（C3）：libc.malloc_trim(0) ----
#
# C2 实测：满载中每 10s 一次 trim 可回收 20~42 MiB，停发后一次回收 ~60 MiB；
# 基线（不 trim）停发后一点不回落——是 glibc 把已释放的堆顶页留住没还给 OS。
# 满载中 trim 追不上 churn、压不住峰值，但能把"负载后的常驻高水位"降下来。
# 所以放在 Ticker 一拍内做一次，代价 0.4~6.8 ms（满载下），可忽略。

# libc 句柄缓存：只加载一次；None 表示尚未尝试，False 表示加载失败（不再重试）
_LIBC: ctypes.CDLL | bool = None


def _load_libc():
    """加载 libc（glibc 才有 malloc_trim）；失败返回 None，不再重试。"""
    global _LIBC
    if _LIBC is None:
        try:
            _LIBC = ctypes.CDLL("libc.so.6")
        except OSError:
            _LIBC = False
    return _LIBC if _LIBC is not False else None


def malloc_trim() -> bool:
    """调 libc 的 malloc_trim(0)，请求 glibc 把可归还的堆顶页还给操作系统。

    非 Linux / 加载不到 libc / 该 libc 没有 malloc_trim（如 musl）时返回 False
    （什么都不做、不报错）。成功返回 True。
    """
    if os.name != "posix" or sys.platform.startswith("win"):
        return False
    lib = _load_libc()
    if lib is None:
        return False
    try:
        fn = lib.malloc_trim
    except AttributeError:
        return False
    try:
        fn(0)
        return True
    except OSError:
        return False


def _rss_anon_bytes() -> int:
    """当前进程 RssAnon（字节）：Linux 读 /proc/self/status，其他平台返回 0。"""
    if os.name != "posix" or sys.platform.startswith("win"):
        return 0
    try:
        with open("/proc/self/status", encoding="ascii") as f:
            for line in f:
                if line.startswith("RssAnon:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


@dataclass
class TrimStat:
    """定期 malloc_trim 的统计（/healthz 读取）。"""

    enabled: bool                       # 是否启用（period > 0 且平台支持）
    period_sec: float                   # 触发间隔（秒）
    call_count: int = 0                 # 累计调用次数
    total_ms: float = 0.0               # 累计调用耗时（毫秒）
    total_freed_bytes: int = 0          # 累计回收的字节数（非 Linux 恒 0）
    last_ms: float = 0.0                # 最近一次调用耗时（毫秒）
    last_freed_bytes: int = 0           # 最近一次回收的字节数（非 Linux 恒 0）


class Ticker:
    """按固定周期执行 engine.advance() + refresh_snapshot_async()。

    记录每次 tick 的耗时，以及实际开始时间相对预定时间的延迟（降级控制器 T8 要用，
    用属性对外提供）。每拍结束后调一次降级控制器 update（喂 occupancy / tick 延迟 /
    内存读数），所以控制器由 Ticker 驱动——单写者、每秒一次、无需独立任务。
    单拍抛普通异常时记日志、计入 error_count 并继续下一拍，不让后台任务因一次失败
    就永久退出。

    C3：每隔 trim_period_sec 秒（默认 10）调一次 malloc_trim(0) 归还空闲内存，
    记录每次耗时与累计次数（trim 属性对外提供，/healthz 读取）。
    """

    def __init__(self, engine: Engine, period: float = 1.0, *,
                 controller=None, admission=None, memory_reader=None,
                 trim_period: float | None = None, loop_probe=None):
        self.engine = engine
        self.period = period
        self.tick_count = 0              # 已完成的 tick 次数
        self.error_count = 0             # tick 抛异常的累计次数
        self.last_tick_duration_ms = 0.0  # 最近一次 tick（advance + 快照）耗时
        self.last_tick_delay_ms = 0.0     # 最近一次 tick 实际开始相对预定时间的延迟
        # P2d-3 二.3：事件循环延迟探针（FLOWLENS_DEBUG=1 时启用）。每拍把定稿
        # （advance）与快照（refresh_snapshot_async）各自的耗时喂给它，供与"最长
        # 几次事件循环延迟"对齐。None 或 disabled 时 record_tick 为空操作。
        self.loop_probe = loop_probe
        # 降级控制器（T8）与写入在途预留（occupancy 来源）：create_app 注入
        self.controller = controller
        self.admission = admission
        # 内存读数 reader（T8b）：返回 (memory_bytes, memory_limit_bytes)。create_app
        # 注入（默认 memstat）；测试可注入假读数，让测试与真实内存解耦。None 时不喂内存
        # （只有 create_app 之外的直接构造会用，如 test_ingest）。
        self.memory_reader: Callable[[], tuple[int, int | None]] | None = memory_reader
        # 定期 malloc_trim（C3）：默认 10 秒，可配置；0 或负数表示关闭。
        if trim_period is None:
            try:
                trim_period = float(os.environ.get("FLOWLENS_MALLOC_TRIM_SEC", "10"))
            except ValueError:
                trim_period = 10.0
        self.trim = TrimStat(enabled=trim_period > 0 and os.name == "posix",
                             period_sec=trim_period)
        self._trim_last_at: float | None = None
        self._stop = asyncio.Event()

    def stop(self) -> None:
        """请求停止。run() 会在当前一拍结束后干净退出。"""
        self._stop.set()

    async def run(self) -> None:
        """循环执行直到 stop()。被取消时干净退出（不吞 CancelledError）。

        按绝对时刻调度：每拍睡到"预定时刻"，而不是"工作完再睡 period"，这样一拍
        的耗时不会累积成越来越晚的漂移（只要单拍耗时 < period）。
        """
        loop = asyncio.get_running_loop()
        next_at = loop.time()  # 第一拍立即执行
        while not self._stop.is_set():
            start = loop.time()
            # 延迟 = 实际开始相对预定时刻（next_at）的偏移；早于预定为负
            self.last_tick_delay_ms = (start - next_at) * 1000.0
            try:
                t0 = loop.time()
                tick_start_ms = int(time.time() * 1000)  # tick 开始（供探针归因对齐）
                fa = loop.time()
                self.engine.advance()
                fin_ms = (loop.time() - fa) * 1000.0
                await asyncio.sleep(0)
                fs = loop.time()
                await self.engine.refresh_snapshot_async()
                snap_ms = (loop.time() - fs) * 1000.0
                self.last_tick_duration_ms = (loop.time() - t0) * 1000.0
                self.tick_count += 1
                self._update_degrade()
                self._maybe_trim(loop.time())
                if self.loop_probe is not None:
                    self.loop_probe.record_tick(fin_ms, snap_ms, tick_start_ms)
            except Exception:
                # 单拍失败不退出后台任务：记完整异常、计数，继续下一拍。
                # 只捕获 Exception——CancelledError（3.11+ 属 BaseException）照常向上传播
                self.error_count += 1
                log.exception("tick 失败，继续下一拍")
            if self._stop.is_set():
                break
            next_at += self.period
            await self._wait_until(next_at)

    def _update_degrade(self) -> None:
        """一拍结束后喂降级控制器一次：occupancy（写入在途占用率）、tick 延迟、内存读数。

        控制器由 Ticker 驱动的理由：单写者、每秒一拍正好"每秒更新一次"，且
        occupancy / tick 延迟都是这一拍刚产出的量，放在这里读到的就是本拍的值，
        不用另起一个每秒任务（那会多一个后台任务、还要处理它和 Ticker 的先后）。
        """
        if self.controller is None:
            return
        occ = self.admission.occupancy if self.admission is not None else 0.0
        if self.memory_reader is not None:
            mem, limit = self.memory_reader()
        else:
            mem, limit = 0, None  # 未注入 reader：不判内存（内存阈值永不触发）
        self.controller.update(occ, self.last_tick_delay_ms, mem, limit)

    def _maybe_trim(self, now: float) -> None:
        """到点（距上次 trim ≥ trim_period）就调一次 malloc_trim(0)。

        放在一拍内同步执行：C2 实测满载下单次 0.4~6.8 ms，对一拍（~85 ms）可
        忽略。非 Linux / 无 libc 时 malloc_trim() 返回 False，这里仍计入
        call_count 并记录耗时（=0），/healthz 上 enabled 为 False 表示平台不支持。
        """
        if not self.trim.enabled:
            return
        if self._trim_last_at is None:
            self._trim_last_at = now   # 第一拍设基准时刻，不立即 trim
            return
        if now - self._trim_last_at < self.trim.period_sec:
            return
        self._trim_last_at = now
        t0 = time.perf_counter()
        before = _rss_anon_bytes()
        ok = malloc_trim()
        dt_ms = (time.perf_counter() - t0) * 1000.0
        after = _rss_anon_bytes()
        self.trim.call_count += 1
        self.trim.total_ms += dt_ms
        self.trim.last_ms = dt_ms
        if ok:
            self.trim.last_freed_bytes = max(0, before - after)
            self.trim.total_freed_bytes += self.trim.last_freed_bytes

    async def _wait_until(self, at: float) -> None:
        """睡到绝对时刻 at（已过期则只让出一次）；期间若 stop() 被调用立即返回。"""
        delay = at - asyncio.get_running_loop().time()
        if self._stop.is_set() or delay <= 0:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
