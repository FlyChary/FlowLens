"""回放与统计查询：默认窗口读快照，其他窗口现场合并；最终态结果进 LRU 缓存。

数据只经 engine.collect_parts / engine.collect_stats / engine.snapshot 获取，
本模块不另写时间划分或合并逻辑（CLAUDE.md）。
"""

from collections import OrderedDict
from dataclasses import dataclass

from .bounds import Result, top_k
from .degrade import DegradeController
from .dims import Dim
from .engine import Engine, Snapshot
from .errors import BadRequest, Overloaded
from .timewin import sec, split_range, watermark

# 查询窗口（毫秒）合法范围：1 ~ 600 秒
WINDOW_MIN_MS = 1000
WINDOW_MAX_MS = 600_000
# k 合法范围
K_MIN, K_MAX = 1, 100
# 最终态 Top-K 结果缓存容量
CACHE_CAP = 256
# 默认窗口 = 60 秒，直接读快照
DEFAULT_WINDOW_MS = 60_000


@dataclass(frozen=True)
class TopKAnswer:
    """一次 Top-K 查询的结果。"""

    dim: Dim
    k: int
    window_ms: int
    from_ms: int
    to_ms: int
    final: bool          # True 表示区间已定稿、结果终态（可缓存）
    items: tuple         # Result 元组（≤ k 个）
    unseen_upper: float  # 任何未上榜 key 的次数都不超过它


@dataclass(frozen=True)
class StatsAnswer:
    """一次统计查询的结果：去重人数与 p95 响应字节数。"""

    window_ms: int
    from_ms: int
    to_ms: int
    distinct_users: float
    p95_bytes: float


# ---- 快照 → 答案的纯函数（P2d-2 一）：引擎 QueryService 与前端进程共用，
# ---- 保证两边从同一份快照生成的响应逐字节一致。

def is_snapshot_query(at_ms, window_ms: int, k: int | None, top_n: int) -> bool:
    """默认窗口查询：不带 at、window 为 60 秒；topk 时还要求 k 不超过快照保留的
    条数（k 为 None 表示不查 k，如 stats）。"""
    return at_ms is None and window_ms == DEFAULT_WINDOW_MS and (k is None or k <= top_n)


def topk_from_snapshot(snap: Snapshot, d: Dim, k: int, window_ms: int) -> TopKAnswer:
    """从快照生成默认窗口 Top-K 答案（不做任何合并计算）。"""
    return TopKAnswer(d, k, window_ms, snap.from_ms, snap.to_ms, False,
                      snap.tops[d][:k], snap.unseen_upper[d])


def stats_from_snapshot(snap: Snapshot, window_ms: int) -> StatsAnswer:
    """从快照生成默认窗口统计答案（不做任何合并计算）。"""
    return StatsAnswer(window_ms, snap.from_ms, snap.to_ms,
                       snap.distinct_users, snap.p95_bytes)


class QueryService:
    """回放与统计查询。最终态 Top-K 结果按 (dim, from_ms, to_ms, k) 缓存在 LRU 里。"""

    def __init__(self, engine: Engine,
                 controller: "DegradeController | None" = None):
        self.engine = engine
        # 降级控制器（T8）：第 1 级时回放查询返回 503（读快照的默认查询照常）。
        self.controller = controller if controller is not None else DegradeController()
        self._cache: "OrderedDict[tuple, TopKAnswer]" = OrderedDict()

    def clear_cache(self) -> None:
        """清空结果缓存（降级第 3 级收紧内存时在进入那一刻调用一次）。"""
        self._cache.clear()

    # ---- 缓存（容量 CACHE_CAP 的 LRU，OrderedDict 实现）----
    def _cache_get(self, key: tuple) -> TopKAnswer | None:
        ans = self._cache.get(key)
        if ans is not None:
            self._cache.move_to_end(key)
        return ans

    def _cache_put(self, key: tuple, ans: TopKAnswer) -> None:
        self._cache[key] = ans
        self._cache.move_to_end(key)
        while len(self._cache) > CACHE_CAP:
            self._cache.popitem(last=False)  # 淘汰最久未使用

    @staticmethod
    def _check_window(window_ms: int) -> None:
        if not WINDOW_MIN_MS <= window_ms <= WINDOW_MAX_MS:
            raise BadRequest(f"window 超出范围 {WINDOW_MIN_MS}~{WINDOW_MAX_MS} 毫秒")

    @staticmethod
    def _check_k(k: int) -> None:
        if not K_MIN <= k <= K_MAX:
            raise BadRequest(f"k 超出范围 {K_MIN}~{K_MAX}")

    async def topk(self, d: Dim, window_ms: int, k: int,
                   at_ms: int | None = None, *, from_stream: bool = False) -> TopKAnswer:
        """回放 Top-K。API 层会先校验一遍，这里是第二道防线。

        from_stream=True（SSE 生产者/补帧调用）：不受第 1 级"暂停回放"影响——
        SSE 在降级时只降频、不暂停，帧照常按 interval 推送；暂停只对 HTTP 查询。
        """
        self._check_window(window_ms)
        self._check_k(k)
        now = self.engine.clock.now_ms()

        # 默认窗口（不带 at、60 秒、k 不超过快照名单长度）：直接读快照，不做合并
        if is_snapshot_query(at_ms, window_ms, k, self.engine.cfg.snapshot_top_n):
            return topk_from_snapshot(self.engine.snapshot, d, k, window_ms)

        # 其余都是回放查询（带 at、或 window 不是 60 秒、或 k 超过快照名单长度）。
        # 第 1 级降级时暂停 HTTP 回放查询：返回 503 + Retry-After，读快照的默认查询
        # 照常；SSE 只降频不暂停（from_stream）。
        if self.controller.replay_paused and not from_stream:
            raise Overloaded("回放查询在降级期间暂停", retry_after=1)

        if at_ms is None:
            at_ms = sec(now) * 1000
        if at_ms > now:
            raise BadRequest("at 晚于当前时间")
        from_ms, to_ms = at_ms - window_ms, at_ms
        final = to_ms <= watermark(now)

        if final:
            cached = self._cache_get((d, from_ms, to_ms, k))
            if cached is not None:
                return cached

        # collect_parts 与 top_k 之间不让出事件循环：时间片里的 refine 引用活跃秒桶
        # 的 Count-Min，中途让出会把新写入的数据混进结果
        parts = self.engine.collect_parts(d, split_range(from_ms, to_ms))  # TooOld 原样抛
        ans = top_k(parts, k)
        res = TopKAnswer(d, k, window_ms, from_ms, to_ms, final,
                         tuple(ans.items), ans.unseen_upper)
        if final:
            self._cache_put((d, from_ms, to_ms, k), res)
        return res

    async def stats(self, window_ms: int, at_ms: int | None = None) -> StatsAnswer:
        """统计查询：默认窗口（不带 at、60 秒）读快照，其他走回放合并概要。"""
        self._check_window(window_ms)
        now = self.engine.clock.now_ms()

        if is_snapshot_query(at_ms, window_ms, None, 0):
            return stats_from_snapshot(self.engine.snapshot, window_ms)

        # 带 at 或非 60 秒窗口 = 回放（现场合并概要）。第 1 级降级时暂停，返回 503。
        if self.controller.replay_paused:
            raise Overloaded("统计查询在降级期间暂停", retry_after=1)

        if at_ms is None:
            to_ms = sec(now) * 1000
        else:
            if at_ms > now:
                raise BadRequest("at 晚于当前时间")
            to_ms = at_ms
        from_ms = to_ms - window_ms
        r = split_range(from_ms, to_ms)
        distinct_users = self.engine.collect_stats(Dim.USER, r).hll.estimate()
        p95_bytes = self.engine.collect_stats(Dim.URL, r).dd.quantile(0.95)
        return StatsAnswer(window_ms, from_ms, to_ms, distinct_users, p95_bytes)
