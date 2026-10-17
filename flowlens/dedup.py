"""event_id 去重：按 ts 分代的 Bloom filter（numpy 位数组，按批查询与写入）。

每 gen_ms（10 秒）的 ts 一代，同时保留 gens（8）代，覆盖可接收范围
[now − 60 s, now + 5 s]。重试带相同 ts，所以一次只查一代。整代落到 watermark 之前
就清空复用；更早的重试会在迟到检查时被丢掉，不用记。

位数取不小于理论值的 2 的幂，位置直接取哈希高位，不需要 128 位乘法。
全部操作都在单写者的事件循环里执行，不需要锁。
用来拦截重试，供降级控制器（T8）查询：某代插入数超过 per_gen（200万）时误判率会上升，触发降级上报
"""

import math
from dataclasses import dataclass

import numpy as np

from .hashing import SEED_BLOOM, U64, double_hash
from .timewin import FUTURE_SKEW_MS, watermark


@dataclass(frozen=True)
class BloomConfig:
    gen_ms: int = 10_000
    gens: int = 8
    per_gen: int = 2_000_000
    fp_rate: float = 0.001


class _Generation:
    __slots__ = ("id", "words", "n")

    def __init__(self, nwords: int):
        self.id = None
        self.words = np.zeros(nwords, dtype=np.uint64)
        self.n = 0


class GenerationalBloom:
    def __init__(self, cfg: BloomConfig = BloomConfig()):
        self.cfg = cfg
        # 布隆过滤器的理论最优位数
        need = -cfg.per_gen * math.log(cfg.fp_rate) / math.log(2) ** 2
        # 通过位数计算需要长度，最小6个
        self.bits = max(6, math.ceil(math.log2(need)))
        # 每次记录k个点位
        self.k = max(1, round((1 << self.bits) / cfg.per_gen * math.log(2)))
        self.k = min(self.k, 16)
        self.gens = [_Generation((1 << self.bits) // 64) for _ in range(cfg.gens)]

    @property
    def nbytes(self) -> int:
        return sum(g.words.nbytes for g in self.gens)

    def advance(self, now_ms: int) -> None:
        """每秒调用：确保可接收范围内的代都已就绪，回收落到 watermark 之前的代。"""
        lo = watermark(now_ms) // self.cfg.gen_ms
        hi = (now_ms + FUTURE_SKEW_MS) // self.cfg.gen_ms
        if hi - lo + 1 > len(self.gens):
            raise RuntimeError("代数不足以覆盖可接收范围")
        for g in range(lo, hi + 1):
            gen = self.gens[g % len(self.gens)]
            if gen.id != g:
                # 整代清零
                gen.words.fill(0)
                # 计数清零更换代号
                gen.n, gen.id = 0, g

    def saturated(self) -> bool:
        """某一代插入数超过设计容量：误判率会上升，应置 degraded。"""
        return any(g.n > self.cfg.per_gen for g in self.gens)

    def _probe(self, h: np.ndarray):
        """返回 (字下标, 位掩码)，形状均为 (n, k)。"""
        h1, h2 = double_hash(h, SEED_BLOOM)
        i = np.arange(self.k, dtype=U64)[None, :]
        # 双hash生成K个位置
        pos = (h1[:, None] + i * h2[:, None]) >> U64(64 - self.bits)
        # 把每个位置拆成字下标（uint64数组里的第几个）和位掩码（这个字里的哪一位）
        return (pos >> U64(6)).astype(np.intp), U64(1) << (pos & U64(63))

    def check_and_set(self, id_hashes: np.ndarray, ts_ms: np.ndarray):
        """批量判断是否见过；没见过的记录下来。

        返回 (dup, active)：dup[i] 为 True 表示重复（含同一批里后出现的同一 id）；
        active[i] 为 False 表示 ts 所在的代未就绪（调用方应已先做迟到检查）。
        """
        n = len(id_hashes)
        dup = np.zeros(n, dtype=bool)
        gen_ids = ts_ms // self.cfg.gen_ms
        active = np.array([self.gens[g % len(self.gens)].id == g for g in gen_ids.tolist()], dtype=bool)
        for g in np.unique(gen_ids[active]).tolist():
            sel = np.flatnonzero(active & (gen_ids == g))
            gen = self.gens[g % len(self.gens)]
            uniq, first = np.unique(id_hashes[sel], return_index=True)
            dup[sel] = True  # 先全部标为重复，再把每个 id 第一次出现且确实是新的那条改回来
            w, m = self._probe(uniq)
            seen = ((gen.words[w] & m) != 0).all(axis=1)
            new = ~seen
            dup[sel[first[new]]] = False
            if new.any():
                np.bitwise_or.at(gen.words, w[new].ravel(), m[new].ravel())
                gen.n += int(new.sum())
        return dup, active

    def contains(self, id_hashes: np.ndarray, ts_ms: np.ndarray) -> np.ndarray:
        """只查询不写入（测试用）。"""
        out = np.zeros(len(id_hashes), dtype=bool)
        for i, (h, t) in enumerate(zip(id_hashes.tolist(), ts_ms.tolist())):
            g = t // self.cfg.gen_ms
            gen = self.gens[g % len(self.gens)]
            if gen.id == g:
                w, m = self._probe(np.array([h], dtype=np.uint64))
                out[i] = bool(((gen.words[w] & m) != 0).all())
        return out
