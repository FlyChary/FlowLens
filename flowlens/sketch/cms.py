import numpy as np

from ..hashing import SEED_CMS, U64, double_hash

U32_MAX = np.iinfo(np.uint32).max


class CMS:
    """保守更新的 Count-Min Sketch（numpy 实现，按批更新）。

    宽度必须是 2 的幂。所有需要相加的 CMS 必须宽度、深度相同；哈希种子是全局常量。
    """

    def __init__(self, width: int, depth: int):
        if width <= 0 or width & (width - 1):
            raise ValueError(f"width {width} 不是 2 的幂")
        self.width, self.depth = width, depth
        self.c = np.zeros((depth, width), dtype=np.uint32)
        self._rows = np.arange(depth)[:, None]
        self._r64 = np.arange(depth, dtype=U64)[:, None]

    @property
    def nbytes(self) -> int:
        return self.c.nbytes

    def _cols(self, h: np.ndarray) -> np.ndarray:
        h1, h2 = double_hash(h, SEED_CMS)
        return ((h1[None, :] + self._r64 * h2[None, :]) & U64(self.width - 1)).astype(np.intp)

    def add(self, h: np.ndarray, c: np.ndarray) -> np.ndarray:
        """批量保守更新，返回各 key 更新后的估计值。
        h: 一批 key 的 64 位哈希，一个 key一个元素
        c：每个 key 本次的增量，和 h一一对应
        h 中的哈希必须互不相同（调用方先按 key 聚合）：同一 key 拆成两条会被低估。
        不同 key 共用计数器时，取各自目标值的最大值，每个 key 的估计仍不小于真实值。
        """
        cols = self._cols(h)
        # (depth, n)当前值
        cur = self.c[self._rows, cols]
        target = np.minimum(cur.min(axis=0).astype(np.uint64) + c, U32_MAX).astype(np.uint32)
        rows = np.broadcast_to(self._rows, cols.shape)
        np.maximum.at(self.c, (rows, cols), np.broadcast_to(target, cols.shape))
        return target

    def estimate(self, h: np.ndarray) -> np.ndarray:
        """批量估计（不小于真实值）。"""
        return self.c[self._rows, self._cols(h)].min(axis=0)

    def merge(self, o: "CMS") -> None:
        """逐格相加。对保守更新同样成立：结果仍是合并后数据流的上界。"""
        self._check(o)
        self.c = np.minimum(self.c.astype(np.uint64) + o.c, U32_MAX).astype(np.uint32)

    def copy_from(self, o: "CMS") -> None:
        self._check(o)
        np.copyto(self.c, o.c)

    def reset(self) -> None:
        self.c.fill(0)

    def _check(self, o: "CMS") -> None:
        if self.c.shape != o.c.shape:
            raise ValueError("CMS 尺寸不同")
