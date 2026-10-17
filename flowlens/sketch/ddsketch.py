import math

import numpy as np


class DDSketch:
    """对数分桶的分位数概要，相对误差 ≤ α（Masson et al., VLDB 2019）。

    桶 i 覆盖 (γ^(i-1), γ^i]，代表值 2γ^i/(γ+1)。小于 1 的值进零桶（bytes 为整数，θ = 1）。
    计数用连续数组，counts[j] 对应桶编号 offset + j。
    """

    def __init__(self, alpha: float = 0.01, max_buckets: int = 2048):
        if not (0 < alpha < 1) or max_buckets < 2:
            raise ValueError("DDSketch 参数非法")
        self.alpha, self.max_buckets = alpha, max_buckets
        self.gamma = (1 + alpha) / (1 - alpha)
        self._inv_ln_gamma = 1 / math.log(self.gamma)
        self.offset = 0
        self.counts = np.zeros(0, dtype=np.uint64)
        self.zero = 0
        self.n = 0

    @property
    def nbytes(self) -> int:
        return self.counts.nbytes

    def add(self, values: np.ndarray, weights: np.ndarray) -> None:
        """批量记录：values[i] 出现 weights[i] 次。负数被忽略。"""
        values = np.asarray(values, dtype=np.float64)
        weights = np.asarray(weights, dtype=np.uint64)
        ok = values >= 0
        values, weights = values[ok], weights[ok]
        if len(values) == 0:
            return
        self.n += int(weights.sum())
        z = values < 1
        # 零桶需要特殊处理，单独计数
        self.zero += int(weights[z].sum())
        # z取反，取出非零的数
        v, w = values[~z], weights[~z]
        if len(v):
            idx = np.ceil(np.log(v) * self._inv_ln_gamma).astype(np.int64)
            self._add_indices(idx, w)
    
    def _add_indices(self, idx: np.ndarray, w: np.ndarray) -> None:
        """ 稀疏计数数组（带 offset）
        这里有个省内存的设计：桶号可能是很大范围（比如 log 桶号从 0
        到几千），不想要一个从 0 到几千的完整数组。所以用：
        - self.offset：第一个桶的编号。
        - self.counts[j]：对应真实桶号 offset + j 的计数（docstring 第 10
            行明说）。

        即 counts 只覆盖"实际用到的桶号区间 [offset,
        offset+len)"，是个紧凑的滑动窗口。

        逻辑：

        1. lo, hi：这批桶号的最小/最大。
        2. 首次：offset = lo，开一个 hi-lo+1 的零数组。
        3. 已有数组：算新范围 [new_lo, new_hi]，如果比现有 [offset, offset+len)
            大，就扩容——开新数组，把旧的搬进去（grown[...] = self.counts），更新
            offset。这是"数组按需增长"。
        4. 累加：np.bincount(idx - self.offset, weights=w, minlength=...) —— idx
            - offset 把桶号转成本地索引，bincount 按本地索引加权求和，加到
            counts 上。bincount 是 numpy 高效的"按桶累加"，比 np.add.at 快。
        5. _collapse()：桶数超上限就压缩。
        """
        lo, hi = int(idx.min()), int(idx.max())
        if len(self.counts) == 0:
            self.offset = lo
            self.counts = np.zeros(hi - lo + 1, dtype=np.uint64)
        else:
            new_lo = min(lo, self.offset)
            new_hi = max(hi, self.offset + len(self.counts) - 1)
            if new_lo != self.offset or new_hi != self.offset + len(self.counts) - 1:
                grown = np.zeros(new_hi - new_lo + 1, dtype=np.uint64)
                grown[self.offset - new_lo:self.offset - new_lo + len(self.counts)] = self.counts
                self.counts, self.offset = grown, new_lo
        self.counts += np.bincount(idx - self.offset, weights=w, minlength=len(self.counts)).astype(np.uint64)
        self._collapse()

    def _collapse(self) -> None:
        """桶数超过上限时，把最低端的桶并入上方（只损失最低端分位数的精度）。"""
        extra = len(self.counts) - self.max_buckets
        if extra > 0:
            head = self.counts[:extra + 1].sum()
            self.counts = self.counts[extra:].copy()
            self.counts[0] = head
            self.offset += extra

    def merge(self, o: "DDSketch") -> None:
        if o.gamma != self.gamma:
            raise ValueError("α 不同，不能合并")
        self.zero += o.zero
        self.n += o.n
        nz = np.flatnonzero(o.counts)
        if len(nz):
            self._add_indices(nz + o.offset, o.counts[nz])

    def quantile(self, q: float) -> float:
        """q 分位数；空 sketch 返回 nan。目标排名为 ⌊1 + q(n−1)⌋。"""
        if self.n == 0 or not (0 <= q <= 1):
            return math.nan
        r = q * (self.n - 1)
        if self.zero > r:
            return 0.0
        cum = self.zero + np.cumsum(self.counts)
        j = int(np.searchsorted(cum, r, side="right"))
        j = min(j, len(self.counts) - 1)
        return 2 * self.gamma ** (self.offset + j) / (self.gamma + 1)

    def clone(self) -> "DDSketch":
        c = DDSketch(self.alpha, self.max_buckets)
        c.offset, c.counts, c.zero, c.n = self.offset, self.counts.copy(), self.zero, self.n
        return c

    def reset(self) -> None:
        self.offset, self.zero, self.n = 0, 0, 0
        self.counts = np.zeros(0, dtype=np.uint64)
