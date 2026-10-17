import math

import numpy as np

from ..hashing import U64


def register_index(h: np.ndarray, p: int) -> np.ndarray:
    """寄存器编号：哈希的高 p 位。
      HLL 把 64 位哈希切成两半：高 p 位决定"写哪个寄存器"，
      低 64−p 位决定"这个元素贡献多大的 rank"。
      这里就是取高 p 位：右移64−p，剩下的就是 [0, 2^p) 里的下标。
    """
    return (h >> U64(64 - p)).astype(np.int64)


def _clz64(x: np.ndarray) -> np.ndarray:
    """非零 uint64 的前导零个数（向量化二分）。
       数一个 64 位数前面有几个 0
    """
    n = np.zeros(x.shape, dtype=np.uint8)
    for s in (32, 16, 8, 4, 2, 1):
        # 判断出大于s高位全零的
        m = x < (U64(1) << U64(64 - s))
        # 是的话，答案加 s，并且把数左移 s 位，把那一段 0 "吃掉"，继续查
        n[m] += s
        x = np.where(m, x << U64(s), x)
    return n


class HLL:
    """HyperLogLog（64 位哈希，寄存器 1 字节），可只持有一段寄存器 [lo, hi)。

    估计值用 Ertl 的改进估计（"New cardinality estimation algorithms for HyperLogLog
    sketches", 2017），全范围准确，无需经验偏差表，也无需切换线性计数。
    他的"改进估计"是这样计算的。先统计寄存器的直方图：C_k 表示值等于 k 的寄存器有多少个。
    设 q = 64 − p（哈希值去掉桶编号后剩下的位数），那么寄存器的取值范围是 0 到 q + 1。估计值为：
    E = α_∞ × m² / ( m × σ(C_0 / m) + Σ_{k=1}^{q} C_k × 2^(−k) + m × τ(1 − C_(q+1) / m) × 2^(−q) )
    其中 α_∞ = 1 / (2 ln 2) ≈ 0.7213，是一个固定常数，不再随 m 变化
    """

    def __init__(self, p: int = 14, lo: int = 0, hi: int | None = None):
        """ 
        p：用哈希的高 p 位分桶，共 2^p 个桶（p=14 → 16384 个）
        lo/hi：关键设计。这个 HLL 可以只持有全量 2^p 个桶里的一段 [lo, hi)
        reg：uint8 数组，一个桶占 1 字节。rank 最大是 64−p+1 ≤ 61，一字节够
        """
        # 默认区间 [0,2^p)
        hi = (1 << p) if hi is None else hi
        if not (4 <= p <= 18) or not (0 <= lo < hi <= 1 << p):
            raise ValueError("HLL 参数非法")
        self.p, self.lo, self.hi = p, lo, hi
        # 存寄存器的数组
        self.reg = np.zeros(hi - lo, dtype=np.uint8)

    @property
    def nbytes(self) -> int:
        return self.reg.nbytes

    def add(self, h: np.ndarray) -> None:
        """批量插入哈希值。"""
        if len(h) == 0:
            return
        # 确定写到哪个桶里
        idx = register_index(h, self.p) - self.lo
        if idx.min() < 0 or idx.max() >= len(self.reg):
            raise ValueError("哈希不属于本分段")
        # 把高 p 位（索引部分）左移丢掉，数剩余部分的 0，如果全零会给最后一位补1
        w = (h << U64(self.p)) | (U64(1) << U64(self.p - 1))
        # 每个哈希算出 rank，写进对应桶，重复下标取 max
        np.maximum.at(self.reg, idx, _clz64(w) + 1)

    def merge(self, o: "HLL") -> None:
        """逐寄存器取最大值。o 的分段必须落在本分段之内。"""
        if o.p != self.p or o.lo < self.lo or o.hi > self.hi:
            raise ValueError("HLL 精度或分段不兼容")
        seg = self.reg[o.lo - self.lo:o.hi - self.lo]
        # 逐寄存器取最大值
        np.maximum(seg, o.reg, out=seg)

    def clone(self) -> "HLL":
        c = HLL(self.p, self.lo, self.hi)
        c.reg[:] = self.reg
        return c

    def reset(self) -> None:
        self.reg.fill(0)

    def estimate(self) -> float:
        """基数估计。只能在完整的 HLL 上调用。"""
        if self.lo != 0 or self.hi != 1 << self.p:
            raise ValueError("只有完整的 HLL 才能估计")
        m = float(len(self.reg))
        q = 64 - self.p
        # 按桶直方统计
        counts = np.bincount(self.reg, minlength=q + 2).astype(np.float64)
        z = m * _tau(1 - counts[q + 1] / m)
        for k in range(q, 0, -1):
            z = (z + counts[k]) * 0.5
        z += m * _sigma(counts[0] / m)
        # 最终估计值计算
        return 0.0 if math.isinf(z) else 0.5 / math.log(2) * m * m / z


def _sigma(x: float) -> float:
    #  Ertl估算公式中的参数
    if x == 1:
        return math.inf
    y, z = 1.0, x
    while True:
        x *= x
        prev = z
        z += x * y
        y += y
        if z == prev:
            return z


def _tau(x: float) -> float:
    #  Ertl估算公式中的参数
    if x in (0.0, 1.0):
        return 0.0
    y, z = 1.0, 1 - x
    while True:
        x = math.sqrt(x)
        prev = z
        y *= 0.5
        z -= (1 - x) ** 2 * y
        if z == prev:
            return z / 3
