"""过载降级控制器：按秒判定降级级别与各组件的行为开关。

纯逻辑，不依赖 aiohttp 也不碰事件循环，便于单测。Ticker 每拍结束后调一次
update()；各组件（engine / QueryService / StreamManager / 写入路由）只读
自己的属性来决定行为，级别变化时的副作用（收紧预算、清缓存）由注入的
on_change 回调承担（挂在 app 层的胶水代码里）。

级别阈值（进入 / 退出；退出阈值更低，形成滞回）。每一级有独立的"活跃"状态：
不活跃时满足进入条件才进入，活跃时满足退出条件才退出，当前级别取所有活跃
级别中最高的一级。

| 级别 | 进入条件                              | 退出条件                    |
|---|---------------------------------------|-----------------------------|
| 1 | occupancy > 0.6 或 tick_lag_ms > 1000 | occupancy < 0.4 且 lag < 500 |
| 2 | occupancy > 0.8                       | occupancy < 0.5             |
| 3 | memory > 上限 × 0.78                  | memory < 上限 × 0.70        |
| 4 | memory > 上限 × 0.90                  | memory < 上限 × 0.82        |

内存阈值按上限的比例计算，不写死 200 MB / 230 MB：memory_limit_bytes 为 None
时按 256 MB 计，比例换算到 256 MB 正好是设计文档里的 200 MB / 230 MB。
"""

import logging
from typing import Callable

log = logging.getLogger(__name__)

# 内存上限未知（读不到 cgroup 上限）时按 256 MB 计算（目标容器的 --memory 值）
DEFAULT_MEMORY_LIMIT = 256 * 1024 * 1024

# 进入 / 退出阈值（occupancy 为占用率，memory 为占上限的比例）
L1_OCC_ENTER, L1_OCC_EXIT = 0.6, 0.4
L1_LAG_ENTER_MS, L1_LAG_EXIT_MS = 1000.0, 500.0
L2_OCC_ENTER, L2_OCC_EXIT = 0.8, 0.5
L3_MEM_ENTER, L3_MEM_EXIT = 0.78, 0.70
L4_MEM_ENTER, L4_MEM_EXIT = 0.90, 0.82

# 占用率超过该值时采样权重提到 4（比"第 2 级的 2"更高一档）
OCC_SAMPLE_4 = 0.95


class DegradeController:
    """降级级别状态机。单写者（Ticker 每拍一次 update），无锁。

    属性全部只读；pin() 是测试注入用（把级别固定住，绕过阈值判定）。
    """

    def __init__(self) -> None:
        self._level = 0
        self._active = [False] * 5  # 每级一个滞回状态，下标 1..4
        self._occupancy = 0.0
        self._tick_lag_ms = 0.0
        self._memory_bytes = 0
        self._memory_limit_bytes: int | None = None
        self._pinned: int | None = None
        # 级别变化回调 (旧级别, 新级别) → None，由 app 层注入（收紧/恢复预算、清缓存）。
        # 只在实际变化时调用一次，进入第 3 级的缓存清理因此"只在进入那一刻清一次"。
        self.on_change: Callable[[int, int], None] | None = None

    # ---- 测试注入 ----
    def pin(self, level: int | None) -> None:
        """把级别固定在 level（集成测试用）；传 None 恢复按阈值判定。"""
        self._pinned = level
        if level is not None:
            self._set_level(level)

    # ---- 每秒一次的输入更新 ----
    def update(self, occupancy: float, tick_lag_ms: float,
               memory_bytes: int, memory_limit_bytes: int | None) -> None:
        self._occupancy = float(occupancy)
        self._tick_lag_ms = float(tick_lag_ms)
        self._memory_bytes = int(memory_bytes)
        self._memory_limit_bytes = memory_limit_bytes
        if self._pinned is not None:
            self._set_level(self._pinned)  # 测试固定：保持不动
            return
        act = self._active
        limit = self._memory_limit_bytes or DEFAULT_MEMORY_LIMIT
        mem, occ, lag = self._memory_bytes, self._occupancy, self._tick_lag_ms
        # 从高到低逐级判断进入/退出（滞回：进入与退出阈值不同）
        if act[4]:
            if mem < L4_MEM_EXIT * limit:
                act[4] = False
        elif mem > L4_MEM_ENTER * limit:
            act[4] = True
        if act[3]:
            if mem < L3_MEM_EXIT * limit:
                act[3] = False
        elif mem > L3_MEM_ENTER * limit:
            act[3] = True
        if act[2]:
            if occ < L2_OCC_EXIT:
                act[2] = False
        elif occ > L2_OCC_ENTER:
            act[2] = True
        if act[1]:
            if occ < L1_OCC_EXIT and lag < L1_LAG_EXIT_MS:
                act[1] = False
        elif occ > L1_OCC_ENTER or lag > L1_LAG_ENTER_MS:
            act[1] = True
        target = 0
        for L in (4, 3, 2, 1):  # 取所有活跃级别中最高的一级
            if act[L]:
                target = L
                break
        self._set_level(target)

    def _set_level(self, new: int) -> None:
        old = self._level
        if new == old:
            return
        self._level = new
        # 级别变化记一行：旧级别、新级别、触发的输入值
        log.info("降级级别 %d → %d（occupancy=%.3f, tick_lag_ms=%.0f, "
                 "memory_bytes=%d, memory_limit_bytes=%s）",
                 old, new, self._occupancy, self._tick_lag_ms,
                 self._memory_bytes,
                 "None" if self._memory_limit_bytes is None else self._memory_limit_bytes)
        if self.on_change is not None:
            self.on_change(old, new)

    # ---- 只读属性：行为开关 ----
    @property
    def level(self) -> int:
        return self._level

    @property
    def degraded(self) -> bool:
        return self._level > 0

    @property
    def sample_weight(self) -> int:
        """带权采样权重（必须是整数，DDSketch 的计数是整数）。"""
        if self._occupancy > OCC_SAMPLE_4:
            return 4
        if self._level >= 2:
            return 2
        return 1

    @property
    def replay_paused(self) -> bool:
        """第 1 级：回放查询返回 503（SSE 不在此列，SSE 用降频）。"""
        return self._level >= 1

    @property
    def sse_slowdown(self) -> bool:
        """第 1 级：SSE 组的最小重算间隔从 1 秒变为 5 秒。"""
        return self._level >= 1

    @property
    def tighten_budgets(self) -> bool:
        """第 3 级：收紧在途/RawBudget 上限、拒绝新的 SSE 订阅。"""
        return self._level >= 3

    @property
    def reject_ingest(self) -> bool:
        """第 4 级：写入在读取请求体之前直接 503。"""
        return self._level >= 4

    # ---- 最近一次的输入值（供 /healthz 展示）----
    @property
    def occupancy(self) -> float:
        return self._occupancy

    @property
    def tick_lag_ms(self) -> float:
        return self._tick_lag_ms

    @property
    def memory_bytes(self) -> int:
        return self._memory_bytes

    @property
    def memory_limit_bytes(self) -> int | None:
        return self._memory_limit_bytes

    def status(self) -> dict:
        """/healthz 的 degrade 字段。"""
        return {
            "level": self._level,
            "sample_weight": self.sample_weight,
            "occupancy": self._occupancy,
            "tick_lag_ms": self._tick_lag_ms,
            "memory_bytes": self._memory_bytes,
            "memory_limit_bytes": self._memory_limit_bytes,
        }
