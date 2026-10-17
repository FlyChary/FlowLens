from dataclasses import dataclass
from typing import Optional

import numpy as np

from .ddsketch import DDSketch
from .hll import HLL


@dataclass(frozen=True)
class StatsSpec:
    """一个维度附带哪些统计概要：user 维度带 HLL（去重人数），url 维度带 DDSketch（p95）。
    它是创建 Stats 的唯一入口，保证同一维度的所有 Stats 参数一致、可以合并。"""

    hll: bool = False
    hll_p: int = 14
    dd: bool = False
    dd_alpha: float = 0.01
    dd_max_buckets: int = 2048

    def new(self) -> "Stats":
        return Stats(
            hll=HLL(self.hll_p) if self.hll else None,
            dd=DDSketch(self.dd_alpha, self.dd_max_buckets) if self.dd else None,
        )


@dataclass
class Stats:
    """附带在秒桶和线段树节点上的统计概要。字段为 None 表示这个维度不带该项。"""

    hll: Optional[HLL] = None
    dd: Optional[DDSketch] = None

    def observe(self, hashes: np.ndarray, bytes_: np.ndarray, weights: np.ndarray) -> None:
        """记录一批事件：HLL 记 key 的哈希，DDSketch 记响应字节数。"""
        if self.hll is not None:
            self.hll.add(hashes)
        if self.dd is not None:
            self.dd.add(bytes_, weights)

    def merge(self, o: Optional["Stats"]) -> None:
        if o is None:
            return
        if self.hll is not None and o.hll is not None:
            self.hll.merge(o.hll)
        if self.dd is not None and o.dd is not None:
            self.dd.merge(o.dd)

    def clone(self) -> "Stats":
        return Stats(self.hll.clone() if self.hll else None, self.dd.clone() if self.dd else None)

    def reset(self) -> None:
        if self.hll is not None:
            self.hll.reset()
        if self.dd is not None:
            self.dd.reset()
