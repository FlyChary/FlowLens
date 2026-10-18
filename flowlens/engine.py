"""统计引擎：拥有活跃环、线段树（按维度）与去重 Bloom。

单一 Engine 对象持有全部统计状态，只在事件循环里读写，因此不需要锁。
collect_parts / collect_stats 是全项目组装"某维度某时间区间"摘要的唯一入口，
快照（T3）、回放（T5）、SSE 补帧（T7）都调用它们，不要另写一套。
"""

import asyncio
from dataclasses import dataclass

import numpy as np

from .bounds import Part, top_k
from .config import Config
from .dedup import GenerationalBloom
from .degrade import DegradeController
from .dims import ALL_DIMS, Dim
from .errors import TooOld
from .hashing import key_hashes
from .model import EventBatch
from .ring import Bucket, Ring
from .segtree import Cover, LeafInput, Tree
from .sketch import Stats
from .timewin import (ACCEPT, LATENESS_MS, LATE, FUTURE, Clock, Range, classify,
                      sec, split_range)


@dataclass(frozen=True)
class IngestResult:
    """一次 ingest 的计数。"""

    accepted: int
    deduped: int
    late: int
    invalid: int


@dataclass
class Counters:
    """累计计数器，供 /healthz（T8）读取。"""

    events_ingested: int = 0
    deduped: int = 0
    late: int = 0
    invalid: int = 0
    late_at_engine: int = 0


@dataclass(frozen=True)
class Snapshot:
    """默认窗口（最近 60 整秒，不含当前秒）的只读快照。

    由 refresh_snapshot 整体重建、Engine.snapshot 整体替换（单写者下赋值即原子），
    读请求（T5 默认窗口）直接读它，不做任何合并计算。
    """

    at_ms: int
    from_ms: int
    to_ms: int
    tops: tuple             # 按维度下标，tops[d] 为该维度的 Result 元组（≤ snapshot_top_n 个）
    unseen_upper: tuple     # 按维度下标，对应 bounds.Answer.unseen_upper
    distinct_users: float   # user 维度的 HLL 去重人数估计
    p95_bytes: float        # url 维度的 p95 响应字节数；区间内没有任何事件时为 math.nan


@dataclass(frozen=True)
class _PlanEdge:
    """一个边缘秒：sec、覆盖比例 frac、是否在环内（决定取环桶还是树叶子）。"""

    sec: int
    frac: float
    in_ring: bool


@dataclass(frozen=True)
class _Plan:
    """_plan 的结果：时间划分后 collect_parts / collect_stats 要消费的东西。"""

    ring_buckets: list       # 环内有数据的整秒桶
    history: tuple | None    # 历史部分 [s0, e0)，没有时为 None
    edges: tuple             # _PlanEdge 序列


class Engine:
    def __init__(self, cfg: Config, clock: Clock,
                 controller: "DegradeController | None" = None):
        self.cfg = cfg
        self.clock = clock
        # 降级控制器（T8）：采样权重在 ingest 时读取；None 时视为不降级（weight 恒 1）
        self.controller = controller if controller is not None else DegradeController()
        self.trees = [Tree(cfg.tree_config(d)) for d in ALL_DIMS]
        self.ring = Ring(cfg.dim_specs(), self._on_finalize)
        self.bloom = GenerationalBloom(cfg.bloom)
        self.counters = Counters()
        self.snapshot: Snapshot | None = None
        now = clock.now_ms()
        self.ring.advance(now)
        self.bloom.advance(now)
        self.refresh_snapshot()  # 启动后立刻就有快照，默认窗口查询不必等第一个 tick

    # ---- 定稿回调：把离开环的秒桶推进对应维度的线段树 ----
    def _on_finalize(self, sec_: int, bucket: Bucket | None) -> None:
        # 按维度幂等：上一次定稿写到一半抛异常时，已写入的维度这一秒不再重写，
        # 否则重试会撞上"秒号必须递增"，之后每拍都失败
        for d in ALL_DIMS:
            tree = self.trees[d]
            if tree.newest is not None and sec_ <= tree.newest:
                continue
            tree.add_leaf(bucket.dims[d].leaf(sec_) if bucket is not None else LeafInput(sec_))

    def advance(self) -> None:
        """推进活跃环与去重 Bloom：把已不可能再收到事件的秒桶定稿进线段树。"""
        now = self.clock.now_ms()
        self.ring.advance(now)
        self.bloom.advance(now)

    # ---- 快照：同步与异步两个入口共用同一份计算，只在让出点不同 ----
    @staticmethod
    def _snapshot_range(now: int) -> Range:
        """默认窗口：最近 60 个整秒 [sec(now)−60, sec(now))，不含当前秒。"""
        n = sec(now)
        return split_range((n - 60) * 1000, n * 1000)

    def _dim_tops(self, d: Dim, r: Range) -> tuple:
        """一个维度的快照 Top：top_k 结果（Result 元组）与其 unseen_upper。"""
        ans = top_k(self.collect_parts(d, r), self.cfg.snapshot_top_n)
        return tuple(ans.items), ans.unseen_upper

    def _snapshot_stats(self, r: Range) -> tuple:
        """快照附带统计：user 维去重人数、url 维 p95 响应字节数。"""
        distinct_users = self.collect_stats(Dim.USER, r).hll.estimate()
        p95_bytes = self.collect_stats(Dim.URL, r).dd.quantile(0.95)
        return distinct_users, p95_bytes

    def _build_snapshot(self, now: int, r: Range, tops: list, unseen: list,
                        distinct_users, p95_bytes) -> Snapshot:
        return Snapshot(now, r.start * 1000, r.stop * 1000, tuple(tops), tuple(unseen),
                        distinct_users, p95_bytes)

    def refresh_snapshot(self) -> Snapshot:
        """重建默认窗口快照并整体替换 self.snapshot。

        区间为最近 60 个整秒 [sec(now)−60, sec(now))，不含当前秒——当前秒还在持续
        写入，纳入进来会让快照在一秒内反复变化。数据只经 collect_parts /
        collect_stats 获取，与回放走完全相同的路径。
        """
        now = self.clock.now_ms()
        r = self._snapshot_range(now)
        tops = []
        unseen = []
        for d in ALL_DIMS:
            t, u = self._dim_tops(d, r)
            tops.append(t)
            unseen.append(u)
        distinct_users, p95_bytes = self._snapshot_stats(r)
        self.snapshot = self._build_snapshot(now, r, tops, unseen, distinct_users, p95_bytes)
        return self.snapshot

    async def refresh_snapshot_async(self) -> Snapshot:
        """refresh_snapshot 的异步版：计算完全相同，只是在两个维度之间、统计概要之前
        各 await asyncio.sleep(0) 让出一次事件循环，避免长时间独占。"""
        now = self.clock.now_ms()
        r = self._snapshot_range(now)
        tops = []
        unseen = []
        for i, d in enumerate(ALL_DIMS):
            t, u = self._dim_tops(d, r)
            tops.append(t)
            unseen.append(u)
            if i < len(ALL_DIMS) - 1:
                await asyncio.sleep(0)  # 两个维度之间让出
        await asyncio.sleep(0)  # 统计概要之前让出
        distinct_users, p95_bytes = self._snapshot_stats(r)
        self.snapshot = self._build_snapshot(now, r, tops, unseen, distinct_users, p95_bytes)
        return self.snapshot

    def tick(self) -> None:
        """后台每秒调用（同步）：先推进环与去重，再刷新快照。"""
        self.advance()
        self.refresh_snapshot()

    # ---- 写入 ----
    def ingest(self, batch: EventBatch, weight: int | None = None) -> IngestResult:
        """写入一批事件。weight 缺省时取控制器当前的 sample_weight（T8 带权采样）。

        采样（weight > 1）：按 event_id 哈希做**确定性**采样，保留比例 1/weight。
        同一事件被重试时取舍一致（取决于 event_id 本身，与时间无关）。采样只作用于
        Top-K 名单（hh）与 url 维度的 p95（DDSketch）；HLL（user 维 stats）与
        event_id 去重始终全量。weight == 1（未降级）时走原来的 DimBucket.observe，
        正常路径结果与 T7b 完全一致。
        """
        now = self.clock.now_ms()
        self.bloom.advance(now)  # 幂等，确保当前范围的代已就绪
        # 采样权重：显式传参优先（测试/基准用），否则取控制器的当前权重
        w = weight if weight is not None else self.controller.sample_weight

        ts = batch.ts
        cls = classify(ts, now)
        invalid = batch.invalid + int((cls == FUTURE).sum())
        late = int((cls == LATE).sum())
        accept = cls == ACCEPT

        deduped = 0
        late_at_engine = 0
        accepted = 0
        n = int(accept.sum())
        if n:
            a_idx = np.flatnonzero(accept)
            a_ts = ts[a_idx]
            # event_id 的哈希在去重时算一次；采样（w>1）还要按它取模判取舍，直接按下标
            # 复用（见下面 h_id = h_id_all[keep]），不再重算——这段只在降级时跑，而
            # 降级本身就因 CPU 紧张，重算浪费。同一批里同一 event_id 两次哈希结果相同
            # （hash 是字符串的纯函数），复用不改变确定性采样语义。
            h_id_all = key_hashes([batch.ids[i] for i in a_idx.tolist()])
            dup, active = self.bloom.check_and_set(h_id_all, a_ts)
            # 干净划分：写进去的 = ~dup & active；没写进去的里，active 的算重复、
            # active 为 False 的（正常不会出现）算迟到
            deduped = int((dup & active).sum())
            late_at_engine += int((~active).sum())
            keep = ~dup & active
            nkeep = int(keep.sum())
            if nkeep:
                k_idx = a_idx[keep]  # 在整批里的下标
                a_bytes = batch.nbytes[k_idx]
                a_ts2 = ts[k_idx]
                # dtype=object：定长字符串数组会把短串补 '\x00'，既改变哈希值，又比 object 慢约 7 倍
                users = np.asarray(batch.users, dtype=object)[k_idx]
                urls = np.asarray(batch.urls, dtype=object)[k_idx]
                # 采样按 event_id 哈希取模判取舍（确定性、与时间无关）：同一事件无论
                # 重试多少次、何时重试，取舍都一致；与"这次是不是重复"解耦。哈希直接
                # 复用去重时算好的 h_id_all（按下标 keep 取子集），不再重算。
                h_id = h_id_all[keep] if w > 1 else None
                # 按秒分组：一次 argsort 排好秒号后按边界切片，而不是对每一秒做整批布尔掩码
                order = np.argsort(a_ts2 // 1000, kind="stable")
                a_ts2 = a_ts2[order]
                if h_id is not None:
                    h_id = h_id[order]
                a_bytes = a_bytes[order]
                users = users[order]
                urls = urls[order]
                s_all = a_ts2 // 1000
                bnd = np.flatnonzero(np.r_[True, s_all[1:] != s_all[:-1]])
                bnd = np.append(bnd, len(s_all))
                # 两维度的哈希始终全量算（HLL 不采样；不采样路径也用到 url 哈希）
                h_user = key_hashes(users)
                h_url = key_hashes(urls)
                weights = np.full(len(s_all), w, dtype=np.uint64)
                if w > 1:
                    # 采样（weight>1）：采样掩码 h_id % w == 0 保留（确定性、与时间无关）
                    sm = (h_id % w) == 0
                else:
                    # 不采样（weight=1，未降级）：与 T7b 完全相同的写入路径
                    payload = {Dim.URL: (h_url, urls), Dim.USER: (h_user, users)}
                for j in range(len(bnd) - 1):
                    s = int(s_all[bnd[j]])
                    sl = slice(int(bnd[j]), int(bnd[j + 1]))
                    b = self.ring.slot(s)
                    if b is None:  # 这一秒已定稿离开环
                        late_at_engine += int(bnd[j + 1] - bnd[j])
                        continue
                    if w > 1:
                        # 这一秒的采样子集（Top-K 名单两维 + url p95 用）
                        lo, hi = int(bnd[j]), int(bnd[j + 1])
                        sel = lo + np.flatnonzero(sm[lo:hi])
                        # Top-K 名单：两维都用采样后事件、权重 w
                        b.dims[Dim.URL].hh.observe(h_url[sel], urls[sel].tolist(), weights[sel])
                        b.dims[Dim.USER].hh.observe(h_user[sel], users[sel].tolist(), weights[sel])
                        # url 维 p95（DDSketch）：采样后事件、权重 w
                        b.dims[Dim.URL].stats.observe(h_url[sel], a_bytes[sel], weights[sel])
                        # user 维 HLL：始终全量、权重 1（不采样）
                        b.dims[Dim.USER].stats.observe(h_user[sl], a_bytes[sl],
                                                       np.ones(hi - lo, dtype=np.uint64))
                    else:
                        for d in ALL_DIMS:
                            h, ks = payload[d]
                            b.dims[d].observe(h[sl], ks[sl].tolist(), a_bytes[sl], weights[sl])
                    accepted += int(bnd[j + 1] - bnd[j])  # 只算真正写进桶的条数

        late += late_at_engine
        c = self.counters
        c.events_ingested += accepted
        c.deduped += deduped
        c.late += late
        c.invalid += invalid
        c.late_at_engine += late_at_engine
        return IngestResult(accepted, deduped, late, invalid)

    # ---- 组装某维度某时间区间（唯一入口）----
    def _plan(self, d: Dim, r: Range) -> _Plan:
        """保留期检查 + 时间划分。collect_parts / collect_stats 共用，不各自判断。

        保留期检查要与实际可用范围一致：线段树保留的是"最新定稿秒往前
        retention_sec 秒"，而最新定稿秒约为 now − 迟到上限（LATENESS_MS // 1000），
        所以区间起点早于 now − retention_sec − 迟到上限（秒）时才真的查不到数据、
        抛 TooOld。若只按 now − retention_sec 判断，window=600s 的查询（起点比 at
        早 600s，而 at 对齐网格后往往比 now 早）会大量误报 TooOld：SSE 长窗口帧
        与普通查询都会受影响。
        """
        now_sec = sec(self.clock.now_ms())
        m = r.start
        if r.edges:
            m = min(m, min(e.sec for e in r.edges))
        if m < now_sec - self.cfg.retention_sec - LATENESS_MS // 1000:
            raise TooOld("区间早于保留期")
        next_ = self.ring.next
        # 环内整秒：[max(start, next), stop) 里有数据的秒桶
        ring_buckets: list[Bucket] = []
        if next_ is not None and r.stop > next_:
            for s in range(max(r.start, next_), r.stop):
                b = self.ring.get(s)
                if b is not None:
                    ring_buckets.append(b)
        # 历史部分：[start, min(stop, next)) 与线段树已冻结区间取交集
        history = None
        he = r.stop if next_ is None else min(r.stop, next_)
        if r.start < he:
            fr = self.trees[d].frozen_range()
            if fr is not None:
                s0, e0 = max(r.start, fr[0]), min(he, fr[1])
                if s0 < e0:
                    history = (s0, e0)
        edges = tuple(_PlanEdge(e.sec, e.frac, next_ is not None and e.sec >= next_)
                      for e in r.edges)
        return _Plan(ring_buckets, history, edges)

    def _cover(self, d: Dim, s0: int, e0: int, stats_only: bool) -> "Cover":
        cov = self.trees[d].cover(s0, e0, stats_only=stats_only)
        if cov is None:
            raise RuntimeError(f"线段树无法覆盖 [{s0},{e0})：内部状态不一致（叶子应当连续）")
        return cov

    def collect_parts(self, d: Dim, r: Range) -> list[Part]:
        plan = self._plan(d, r)
        out: list[Part] = [b.dims[d].part() for b in plan.ring_buckets]
        if plan.history is not None:
            s0, e0 = plan.history
            out.extend(self._cover(d, s0, e0, stats_only=False).parts())
        for e in plan.edges:
            if e.in_ring:
                b = self.ring.get(e.sec)
                if b is not None:
                    out.append(b.dims[d].part(e.frac))
            else:
                leaf = self.trees[d].leaf(e.sec)
                if leaf is not None:
                    out.append(leaf.part(e.frac))
        return out

    def collect_stats(self, d: Dim, r: Range) -> Stats:
        plan = self._plan(d, r)
        st = self.cfg.stats_spec(d).new()
        for b in plan.ring_buckets:
            st.merge(b.dims[d].stats)
        if plan.history is not None:
            s0, e0 = plan.history
            self._cover(d, s0, e0, stats_only=True).merge_stats_into(st)
        for e in plan.edges:
            if e.frac < 0.5:  # 边缘秒覆盖过半才算
                continue
            if e.in_ring:
                b = self.ring.get(e.sec)
                if b is not None:
                    st.merge(b.dims[d].stats)
            else:
                leaf = self.trees[d].leaf(e.sec)
                if leaf is not None and leaf.stats is not None:
                    st.merge(leaf.stats)
        return st
