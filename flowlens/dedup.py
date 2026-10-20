"""event_id 去重：按 ts 分代的 Bloom filter（numpy 位数组，按批查询与写入）。

每 gen_ms（10 秒）的 ts 一代，同时保留 gens（8）代，覆盖可接收范围
[now − 60 s, now + 5 s]。重试带相同 ts，所以一次只查一代。整代落到 watermark 之前
就清空复用；更早的重试会在迟到检查时被丢掉，不用记。

位数取不小于理论值的 2 的幂，位置直接取哈希高位，不需要 128 位乘法。
全部操作都在单写者的事件循环里执行，不需要锁。
用来拦截重试，供降级控制器（T8）查询：某代插入数超过 per_gen（200万）时误判率会上升，触发降级上报

P2b 一（输出不变的提速）：
- 判"每条事件的代是否就绪"用 numpy 数组运算（各代 id 的数组按 gen_id%代数 取对应代
  的代号与 gen_id 比较），替代逐条的 Python 列表推导（约省 0.1 μs/条）；
- 写位保留 np.bitwise_or.at。曾按指令改为"按字下标排序 + reduceat 合并同一字掩码 +
  对去重字下标一次性 |="，实测（tools/bench_bloom.py 与 tools/exp/p2b_write_diag.py，
  标准镜像 numpy 2.5.4、稳态一代 ~200 万 id、批 1 万）该方案因 argsort 主导反而
  **慢 2.9×~4.8×**（本布隆填充率 ~6%、同一字碰撞很少、没有多少掩码可合并），故未采用。
保留的部分对任意输入产出与改前逐位相同的 (dup, active) 与各代 words/n
（tests/test_dedup.py 有改前参考实现的对照测试守护）。

P2b 二（减少哈希个数，改变误判率）：
- 每个 id 记录 k 个点位。k 的**实测误判率与耗时**（tools/bench_bloom.py，标准镜像
  numpy 2.5.4，per_gen 200 万、写满一代后查 100 万未写入 id）：k=12 误判率 0.030% /
  1.33 μs/条，k=8 0.044% / 0.88，k=6 0.075% / 0.66，k=4 0.197% / 0.45。在"误判率
  ≤0.1%"约束下 k=6 耗时最低，故 BloomConfig 的**默认 k=6**（BloomConfig.k 为 None
  时按现有公式，默认配置下为 12、行为不变；为整数时直接使用该 k）。相比 k=12，
  check_and_set 约省 0.68 μs/条（~50%）。
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
    # P2b 二：默认 k=6（"误判率 ≤0.1% 前提下耗时最低"的实测值，见模块 docstring）。
    # 为 None 时按现有公式算 k（行为不变，默认配置下为 12）；为整数时直接使用该 k。
    k: int | None = 6


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
        # 每次记录k个点位：k 显式指定时直接用，否则按现有公式（行为不变）
        if cfg.k is None:
            self.k = max(1, round((1 << self.bits) / cfg.per_gen * math.log(2)))
            self.k = min(self.k, 16)
        else:
            self.k = cfg.k
        self.gens = [_Generation((1 << self.bits) // 64) for _ in range(cfg.gens)]
        # 各代代号的数组（槽 -> 代号，-1 表示未就绪），判"代是否就绪"用它做数组比较
        self._gen_ids = np.full(cfg.gens, -1, dtype=np.int64)

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
                self._gen_ids[g % len(self.gens)] = g

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

        P2b 一：判"代是否就绪"用 _gen_ids 数组做向量化比较（替代逐条列表推导）；
        写位用 reduceat 合并同一字的掩码后一次性 |=（替代 np.bitwise_or.at）。
        两步对任意输入产出与改前逐位相同的结果。
        """
        n = len(id_hashes)
        dup = np.zeros(n, dtype=bool)
        gen_ids = ts_ms // self.cfg.gen_ms
        # 各代代号数组按 gen_id%代数 取出对应代的代号，与 gen_id 比较：相等的即就绪
        active = self._gen_ids[gen_ids % len(self.gens)] == gen_ids
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
                # P2b 一.2：曾按指令用"按字下标排序 + reduceat 合并同一字掩码 + 对去重
                # 字下标一次性 |=" 替代 np.bitwise_or.at。实测（tools/bench_bloom.py 与
                # tools/exp/p2b_write_diag.py，Docker numpy 2.5.4）在稳态（一代 ~200 万
                # id、批 1 万）下，reduceat 路径因 argsort 主导，比 np.bitwise_or.at
                # 慢 2.9×~4.8×（本布隆填充率 ~6%、同一字碰撞很少，没有多少掩码可合并）。
                # 故保留更快的 np.bitwise_or.at；reduceat 方案在填充率远高于本设计时才
                # 可能反超（见 P2-notes 的说明与用户确认）。
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
