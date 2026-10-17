"""活跃环：最近 66 秒的秒桶。

秒桶整秒早于 watermark 后"定稿"，按秒号递增的顺序交给回调（写入稀疏线段树），
然后槽位清空复用。没有数据的秒也会回调，保证线段树的叶子连续。
"""

from dataclasses import dataclass
from typing import Callable, Optional

import numpy as np

from .bounds import Part
from .dims import ALL_DIMS, Dim
from .segtree import LeafInput
from .sketch import HeavyHitters, Stats, StatsSpec
from .timewin import RING_SLOTS, sec, watermark


@dataclass(frozen=True)
class DimSpec:
    """一个维度秒桶的规格，由 Config.dim_spec 生成。"""

    cms_width: int
    cms_depth: int
    heap_cap: int
    stats: StatsSpec


class DimBucket:
    """某一维度在某一秒的摘要：名单与次数（hh）加附带统计（stats）。
    url 维度的 stats 带 DDSketch，user 维度的带 HLL，调用方不用区分。"""

    def __init__(self, spec: DimSpec):
        self.hh = HeavyHitters(spec.cms_width, spec.cms_depth, spec.heap_cap)
        self.stats: Stats = spec.stats.new()

    def observe(self, hashes: np.ndarray, keys: list, bytes_: np.ndarray, weights: np.ndarray) -> None:
        """记录一批事件在本维度上的出现。weights 为采样权重（未降级时全为 1）。"""
        self.hh.observe(hashes, keys, weights)
        self.stats.observe(hashes, bytes_, weights)

    def part(self, frac: float = 1.0) -> Part:
        return self.hh.part(frac)

    def leaf(self, s: int) -> LeafInput:
        return LeafInput(s, self.hh, self.stats)

    def clear(self) -> None:
        self.hh.clear()
        self.stats.reset()


class Bucket:
    """一秒的完整摘要，按维度下标存放。"""

    def __init__(self, specs: dict[Dim, DimSpec]):
        self.sec: Optional[int] = None
        self.dims = [DimBucket(specs[d]) for d in ALL_DIMS]

    def _reset(self, s: int) -> None:
        for d in self.dims:
            d.clear()
        self.sec = s


FinalizeFn = Callable[[int, Optional[Bucket]], None]


class Ring:
    """ 
    活跃环，最近 66 秒的载体
    - 66 个 Bucket 槽位，每槽存"一秒 × 两个维度"的完整摘要
    Bucket.dims = [DimBucket(url), DimBucket(user)]，DimBucket = HeavyHitters + Stats
     - next 不是指针，是秒号：环当前持有的秒号区间是 [next, next+66)。槽位只是容器，
     s % 66 定位——"秒号 → 槽位"的映射。
    - finalize 回调：环不直接知道线段树，某秒定稿时调 self.finalize(sec, bucket_or_None)，由引擎把数据推进树（T2 里
    trees[d].add_leaf(...)）。这是依赖倒置：环只管"到点了、按顺序交付"。
    """
    def __init__(self, specs: dict[Dim, DimSpec], finalize: FinalizeFn):
         # 第一笔数据：以它为环的起点，66 格
        self.slots = [Bucket(specs) for _ in range(RING_SLOTS)]
        self.next: Optional[int] = None  # 下一个待定稿的秒号
        self.finalize = finalize 

    def _slot(self, s: int) -> Bucket:
        # 通过 %66 确定槽位
        return self.slots[s % len(self.slots)]

    def slot(self, s: int) -> Optional[Bucket]:
        """返回秒 s 的桶以便写入；s 已定稿时返回 None（视为迟到）。"""
        if self.next is None:
            # 这里是环的起点
            self.next = s
        # 这一秒早于环的最旧秒——它已经定稿进了树    
        if s < self.next:
            return None
        # 取出指定槽
        b = self._slot(s)
        if b.sec == s:
            return b
        # （罕见）新秒要用的槽，还装着 66 秒前未定稿的旧秒
        if b.sec is not None and b.sec < s:
            self._finalize_through(b.sec)  # 定稿推进落后：先按顺序定稿到旧秒，再复用槽位
        # 清掉两维度的 HeavyHitters/Stats，槽位从此归第 s 秒
        b._reset(s)
        return b

    def advance(self, now_ms: int) -> None:
        """每秒调用：按秒号顺序定稿所有整秒早于 watermark 的桶。"""
        first = sec(watermark(now_ms))
        if self.next is None:
            self.next = first
        elif first - 1 >= self.next:
            # 交付watermark之前的桶
            self._finalize_through(first - 1)

    def _finalize_through(self, last: int) -> None:
        # 从next一直交付到传入值
        while self.next <= last:
            b = self._slot(self.next)
            if b.sec == self.next:
                self.finalize(self.next, b)
                b.sec = None # 槽标记为空闲
            else:
                # 槽没有使用，没有数据
                self.finalize(self.next, None)
            self.next += 1 # next只在这里增加

    def live(self, start: int, stop: int) -> list[Bucket]:
        """[start, stop) 内仍在环里且有数据的秒桶。"""
        if self.next is None:
            return []
        return [b for s in range(max(start, self.next), stop) if (b := self._slot(s)).sec == s]

    def get(self, s: int) -> Optional[Bucket]:
        b = self._slot(s)
        return b if b.sec == s else None
