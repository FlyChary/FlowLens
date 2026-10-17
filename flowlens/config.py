"""集中管理全部参数，并负责生成各组件的规格对象。
其他模块不要自己拼参数，一律从这里取。"""

from dataclasses import dataclass, field

from .dedup import BloomConfig
from .dims import ALL_DIMS, Dim
from .ring import DimSpec
from .segtree import TreeConfig
from .sketch import StatsSpec
from .timewin import RETENTION_SEC


@dataclass(frozen=True)
class Config:
    cms_width: int = 8192  # 秒桶 Count-Min 宽度
    cms_depth: int = 4
    heap_cap: int = 512  # 秒桶名单容量
    leaf_keep: int = 128  # 冻结叶子保留条数
    node_keep_low: int = 256  # 第 1..low_levels_max 层节点保留条数
    node_keep_high: int = 1024
    low_levels_max: int = 3
    levels: int = 9  # 线段树最高层：2^9 = 512 秒
    retention_sec: int = RETENTION_SEC
    stats_min_level: int = 3
    hll_p: int = 14
    dd_alpha: float = 0.01
    dd_max_buckets: int = 2048
    snapshot_top_n: int = 100
    bloom: BloomConfig = field(default_factory=BloomConfig)

    def stats_spec(self, d: Dim) -> StatsSpec:
        """url 带 DDSketch，user 带 HLL。"""
        if d == Dim.USER:
            return StatsSpec(hll=True, hll_p=self.hll_p)
        return StatsSpec(dd=True, dd_alpha=self.dd_alpha, dd_max_buckets=self.dd_max_buckets)

    def dim_spec(self, d: Dim) -> DimSpec:
        return DimSpec(self.cms_width, self.cms_depth, self.heap_cap, self.stats_spec(d))

    def dim_specs(self) -> dict[Dim, DimSpec]:
        return {d: self.dim_spec(d) for d in ALL_DIMS}

    def tree_config(self, d: Dim) -> TreeConfig:
        keep = [0] + [self.node_keep_low if k <= self.low_levels_max else self.node_keep_high
                      for k in range(1, self.levels + 1)]
        return TreeConfig(
            levels=self.levels, leaf_keep=self.leaf_keep, node_keep=tuple(keep),
            cand_cap=tuple(2 * k for k in keep), cms_width=self.cms_width, cms_depth=self.cms_depth,
            retention_sec=self.retention_sec, stats_min_level=self.stats_min_level,
            stats=self.stats_spec(d),
        )
