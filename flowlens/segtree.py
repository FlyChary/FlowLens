"""稀疏线段树：保存离开活跃环的历史数据，支持任意秒级区间查询。

- 第 0 层是冻结叶子，每个对应 1 秒，只留前 leaf_keep 个 key 和缺席上界 u。
- 第 k 层（1..levels）每个节点覆盖 2^k 秒，起点对齐到 2^k 的整数倍。
- 每层同一时刻只有一个"构建中"的节点（_Builder），带累加 Count-Min 和候选堆。
  每个秒桶定稿时，它的 Count-Min 并入所有祖先层的 builder；节点完成时只截断一次，
  误差不会像"子节点合成父节点"那样逐层叠加（设计文档图 3）。

上下界保证（测试中逐 key 验证）：
- 名单内的 key：真实次数 ∈ [lower, upper]；名单外的 key：真实次数 ≤ u。
- 叶子：u = max(活跃秒桶的堆顶, 被截掉条目的最大上界)。
- 节点：u = max(被截掉候选的最大上界, 候选堆最终堆顶 + Σ 各秒的活跃堆顶)。
  后一项的理由：最终不在候选里的 key，最后一次被拒绝或挤出时的累加估计不超过
  当时的候选堆顶（堆顶只增不减）；之后各秒它都不在秒桶名单里，每秒不超过该秒堆顶。

压缩：每个秒桶只留"前 128 个 key + 缺席上界 u"，扔掉 Count-Min

冻结后的 Summary 不再修改。摘除的 Summary 在无人引用后由垃圾回收释放
（设计文档第 9 节的"已废弃"状态在此基础上实现）。
"""

from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from .bounds import Part
from .sketch import CMS, HeavyHitters, IndexedMinHeap, Stats, StatsSpec


@dataclass(frozen=True)
class TreeConfig:
    """由 Config.tree_config 生成，不要手工拼。node_keep、cand_cap 按层号下标（0 号不用）。"""

    levels: int # 最高层数，决定支持多少历史数据的查询
    leaf_keep: int # 叶子结点保留多少个key
    node_keep: tuple[int, ...] # 第k层节点各保留多少个key
    cand_cap: tuple[int, ...] # 第 k 层 builder 的候选堆容量
    cms_width: int  # 必须与活跃秒桶的 Count-Min 一致，要 merge 秒桶的 CMS，CMS._check 要求同宽同深
    cms_depth: int
    retention_sec: int # 保留期（600 秒）
    stats_min_level: int  # 不低于这一层的节点才带 Stats；叶子总是带，默认是3层（8 秒）以上
    stats: StatsSpec # Stats（HLL/DDSketch）


@dataclass
class Summary:
    """冻结后的叶子或节点。"""

    start: int
    level: int
    hashes: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=np.uint64))
    lower: np.ndarray = field(default_factory=lambda: np.empty(0))
    upper: np.ndarray = field(default_factory=lambda: np.empty(0))
    keys: list = field(default_factory=list)
    u: int = 0
    stats: Optional[Stats] = None

    @property
    def span(self) -> int:
        return 1 << self.level

    @property
    def end(self) -> int:
        return self.start + self.span

    def part(self, frac: float = 1.0) -> Part:
        return Part(self.hashes, self.lower, self.upper, self.keys, self.u, frac)

    def _fill(self, rows, u: int) -> None:
        """rows 为按上界降序的 (hash, key, lower, upper)；只保留上界大于 u 的条目。"""
        rows = [r for r in rows if r[3] > u]
        self.u = u
        self.hashes = np.fromiter((r[0] for r in rows), dtype=np.uint64, count=len(rows))
        self.keys = [r[1] for r in rows]
        self.lower = np.fromiter((r[2] for r in rows), dtype=np.float64, count=len(rows))
        self.upper = np.fromiter((r[3] for r in rows), dtype=np.float64, count=len(rows))


class Cover(list):
    """恰好覆盖某个区间的一组 Summary。"""

    def parts(self) -> list[Part]:
        return [s.part() for s in self]

    def merge_stats_into(self, dst: Stats) -> None:
        for s in self:
            dst.merge(s.stats)


@dataclass
class LeafInput:
    """一个刚定稿的活跃秒桶（add_leaf 期间只读）。hh 为 None 表示这一秒没有数据。"""

    sec: int
    hh: Optional[HeavyHitters] = None
    stats: Optional[Stats] = None


class Tree:
    def __init__(self, cfg: TreeConfig):
        self.cfg = cfg
        self.levels: list[dict[int, Summary]] = [{} for _ in range(cfg.levels + 1)]
        self.builders = [None] + [_Builder(k, cfg) for k in range(1, cfg.levels + 1)]
        self.newest: Optional[int] = None

    def frozen_range(self):
        """已冻结、仍在保留期内的秒区间 (lo, hi)；没有数据时返回 None。"""
        if self.newest is None:
            return None
        hi = self.newest + 1
        return max(hi - self.cfg.retention_sec, min(self.levels[0])), hi

    def add_leaf(self, inp: LeafInput) -> None:
        """冻结一个秒桶。秒号必须递增；中间缺的秒按"无数据"补齐。"""
        if self.newest is not None:
            if inp.sec <= self.newest:
                raise ValueError("秒号必须递增")
            for s in range(self.newest + 1, inp.sec):
                self._add_one(LeafInput(s))
        self._add_one(inp)

    def _add_one(self, inp: LeafInput) -> None:
        self.levels[0][inp.sec] = self._freeze_leaf(inp)
        for k in range(1, self.cfg.levels + 1):
            b = self.builders[k]
            b.accept(inp)
            if b.complete(inp.sec):
                start = b.start  # freeze() 会清空 start，必须先取出来
                self.levels[k][start] = b.freeze()
        self.newest = inp.sec
        self._expire()

    def _freeze_leaf(self, inp: LeafInput) -> Summary:
        s = Summary(inp.sec, 0, stats=self.cfg.stats.new())
        if inp.hh is None:
            return s
        s.stats.merge(inp.stats)
        es = inp.hh.top.top(len(inp.hh.top))
        u = inp.hh.u()
        if self.cfg.leaf_keep < len(es):
            u = max(u, es[self.cfg.leaf_keep].count)
            es = es[:self.cfg.leaf_keep]
        s._fill(((e.hash, e.key, e.lower, e.count) for e in es), u)
        return s

    def _expire(self) -> None:
        """摘除整体早于保留期的叶子和节点。"""
        cutoff = self.newest + 1 - self.cfg.retention_sec
        for lv in self.levels:
            for start in [st for st, s in lv.items() if s.end <= cutoff]:
                del lv[start]

    def cover(self, start: int, stop: int, stats_only: bool = False) -> Optional[Cover]:
        """用尽量大的对齐节点恰好覆盖 [start, stop)；有任何一段不可用时返回 None。
        stats_only 为 True 时只用带 Stats 的层。"""
        out, cur = Cover(), start
        while cur < stop:
            pick = self._largest_at(cur, stop, stats_only)
            if pick is None:
                return None
            out.append(pick)
            cur = pick.end
        return out

    def leaf(self, s: int) -> Optional[Summary]:
        return self.levels[0].get(s)

    def _largest_at(self, cur: int, stop: int, stats_only: bool) -> Optional[Summary]:
        for k in range(self.cfg.levels, -1, -1):
            if stats_only and 0 < k < self.cfg.stats_min_level:
                continue
            span = 1 << k
            if cur % span or cur + span > stop:
                continue
            if (s := self.levels[k].get(cur)) is not None:
                return s
        return None


class _Builder:
    """某一层正在构建的节点。候选堆的优先级 = 累加 Count-Min 的估计（只增不减），
    val = (key, 进入候选以来各秒下界之和)；候选被挤出时下界随之丢弃，因此仍是合法下界。"""

    def __init__(self, level: int, cfg: TreeConfig):
        self.level, self.cfg = level, cfg
        self.span = 1 << level
        self.cms = CMS(cfg.cms_width, cfg.cms_depth)
        self.cand = IndexedMinHeap(cfg.cand_cap[level])
        self.start: Optional[int] = None
        self.sum_leaf_u = 0
        self.stats: Optional[Stats] = None

    def _reset(self, start: int) -> None:
        self.start = start
        self.cms.reset()
        self.cand.clear()
        self.sum_leaf_u = 0
        self.stats = self.cfg.stats.new() if self.level >= self.cfg.stats_min_level else None

    def accept(self, inp: LeafInput) -> None:
        if (start := inp.sec // self.span * self.span) != self.start:
            self._reset(start)
        if inp.hh is None:
            return
        self.cms.merge(inp.hh.cms)
        if len(self.cand):
            self.cand.reprioritize(self.cms.estimate(np.array(self.cand.keys, dtype=np.uint64)))
        es = inp.hh.top.entries()
        if es:
            ests = self.cms.estimate(np.fromiter((e.hash for e in es), dtype=np.uint64, count=len(es)))
            for e, est in zip(es, ests.tolist()):
                self._offer(e, est)
        self.sum_leaf_u += inp.hh.u()
        if self.stats is not None:
            self.stats.merge(inp.stats)

    def _offer(self, e, upper: int) -> None:
        got = self.cand.get(e.hash)
        if got is not None:
            _, (key, lower) = got
            self.cand.update(e.hash, upper, (key, lower + e.lower))
        else:
            self.cand.offer(e.hash, upper, (e.key, e.lower))

    def complete(self, s: int) -> bool:
        return s == self.start + self.span - 1

    def freeze(self) -> Summary:
        """截断一次，生成只读节点。"""
        rows = sorted(((h, key, lower, p) for h, p, (key, lower) in self.cand.items()),
                      key=lambda r: (-r[3], r[1]))
        keep = self.cfg.node_keep[self.level]
        max_cut = rows[keep][3] if keep < len(rows) else 0
        cand_min = self.cand.min_prio() if self.cand.full() else 0
        u = max(max_cut, cand_min + self.sum_leaf_u)
        s = Summary(self.start, self.level, stats=self.stats)
        s._fill(((h, key, min(lo, p), p) for h, key, lo, p in rows[:keep]), u)
        self.start, self.stats = None, None  # Stats 的所有权交给 Summary
        return s
