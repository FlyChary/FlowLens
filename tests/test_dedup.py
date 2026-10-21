import math

import numpy as np

from flowlens.dedup import BloomConfig, GenerationalBloom
from flowlens.hashing import SEED_BLOOM, U64, double_hash, key_hashes
from flowlens.timewin import FUTURE_SKEW_MS, watermark

NOW = 1_735_689_600_000


def small():
    b = GenerationalBloom(BloomConfig(per_gen=100_000))
    b.advance(NOW)
    return b


def test_duplicate_detected_across_batches():
    b = small()
    h, ts = key_hashes(["e_9f2"]), np.array([NOW - 1000])
    assert b.check_and_set(h, ts)[0].tolist() == [False]
    assert b.check_and_set(h, ts)[0].tolist() == [True]


def test_duplicate_within_one_batch():
    b = small()
    h = key_hashes(["a", "b", "a", "a"])
    dup, active = b.check_and_set(h, np.full(4, NOW))
    assert dup.tolist() == [False, False, True, True] and active.all()


def test_false_positive_rate():
    b = small()
    b.check_and_set(key_hashes([f"a{i}" for i in range(100_000)]), np.full(100_000, NOW - 3000))
    fp = b.contains(key_hashes([f"b{i}" for i in range(20_000)]), np.full(20_000, NOW - 3000)).mean()
    assert fp < 0.002


def test_generation_recycled():
    b = small()
    h, ts = key_hashes(["old"]), np.array([NOW - 55_000])
    b.check_and_set(h, ts)
    b.advance(NOW + 120_000)
    assert not b.check_and_set(h, ts)[1][0]


# ==== 参考实现：P2b 修改前 flowlens/dedup.py 的 GenerationalBloom 原样复制 ====
# 用作 P2b 一（输出不变提速）的对照基准：改前实现用逐条列表推导判代 +
# np.bitwise_or.at 写位。等价性测试用它与新实现逐步对照（tests/test_dedup.py 本节）。

class _RefGeneration:
    __slots__ = ("id", "words", "n")

    def __init__(self, nwords: int):
        self.id = None
        self.words = np.zeros(nwords, dtype=np.uint64)
        self.n = 0


class RefBloom:
    """改前 GenerationalBloom（per_gen=2M → bits=25）。k 作为参数传入以便与新实现
    用同一 k 对照：本对照测的是"判代优化（+ 写位路径）是否输出不变"，与 k 无关，
    k 只是固定住的一个参数（改前公式给 12，新默认给 6，两者都要逐位一致）。"""

    GEN_MS = 10_000

    def __init__(self, k: int, per_gen: int = 2_000_000, fp_rate: float = 0.001,
                 gens: int = 8):
        self.gen_ms = self.GEN_MS
        self.cfg_gens = gens
        need = -per_gen * math.log(fp_rate) / math.log(2) ** 2
        self.bits = max(6, math.ceil(math.log2(need)))
        self.k = k
        self.gens = [_RefGeneration((1 << self.bits) // 64) for _ in range(gens)]

    def advance(self, now_ms: int) -> None:
        lo = watermark(now_ms) // self.gen_ms
        hi = (now_ms + FUTURE_SKEW_MS) // self.gen_ms
        if hi - lo + 1 > len(self.gens):
            raise RuntimeError("代数不足以覆盖可接收范围")
        for g in range(lo, hi + 1):
            gen = self.gens[g % len(self.gens)]
            if gen.id != g:
                gen.words.fill(0)
                gen.n, gen.id = 0, g

    def _probe(self, h: np.ndarray):
        h1, h2 = double_hash(h, SEED_BLOOM)
        i = np.arange(self.k, dtype=U64)[None, :]
        pos = (h1[:, None] + i * h2[:, None]) >> U64(64 - self.bits)
        return (pos >> U64(6)).astype(np.intp), U64(1) << (pos & U64(63))

    def check_and_set(self, id_hashes: np.ndarray, ts_ms: np.ndarray):
        n = len(id_hashes)
        dup = np.zeros(n, dtype=bool)
        gen_ids = ts_ms // self.gen_ms
        active = np.array([self.gens[g % len(self.gens)].id == g for g in gen_ids.tolist()],
                          dtype=bool)
        for g in np.unique(gen_ids[active]).tolist():
            sel = np.flatnonzero(active & (gen_ids == g))
            gen = self.gens[g % len(self.gens)]
            uniq, first = np.unique(id_hashes[sel], return_index=True)
            dup[sel] = True
            w, m = self._probe(uniq)
            seen = ((gen.words[w] & m) != 0).all(axis=1)
            new = ~seen
            dup[sel[first[new]]] = False
            if new.any():
                np.bitwise_or.at(gen.words, w[new].ravel(), m[new].ravel())
                gen.n += int(new.sum())
        return dup, active


# ==== 等价性：新实现 vs 改前参考实现，逐步对照 ====

def _assert_state(new: GenerationalBloom, ref: RefBloom, tag: str) -> None:
    """每一代的 words 逐位相同、n 相同、代号相同。"""
    assert len(new.gens) == len(ref.gens)
    for g_new, g_ref in zip(new.gens, ref.gens):
        assert (g_new.words == g_ref.words).all(), f"{tag}: 某代 words 不一致"
        assert g_new.n == g_ref.n, f"{tag}: 某代 n 不一致 ({g_new.n} vs {g_ref.n})"
        assert g_new.id == g_ref.id, f"{tag}: 某代代号不一致"


def _run_scenario(script: list, k: int) -> None:
    """按脚本顺序驱动新实现与参考实现（用**同一个 k**），每步之后比较
    (dup, active) 与全部状态。脚本条目：("advance", now_ms) 或 ("check", h, ts)。

    判代优化（+ 写位路径）与 k 无关：新实现与改前参考实现用同一 k 时必须逐位一致。
    """
    new = GenerationalBloom(BloomConfig(k=k))
    ref = RefBloom(k=k)
    for step, op in enumerate(script):
        if op[0] == "advance":
            new.advance(op[1])
            ref.advance(op[1])
            _assert_state(new, ref, f"adv#{step}")
        else:
            _, h, ts = op
            d_new, a_new = new.check_and_set(h, ts)
            d_ref, a_ref = ref.check_and_set(h, ts)
            assert (d_new == d_ref).all(), f"chk#{step}: dup 不一致"
            assert (a_new == a_ref).all(), f"chk#{step}: active 不一致"
            _assert_state(new, ref, f"chk#{step}")


def _scenario_ops():
    """覆盖：批内重复、跨批重复、跨代、未就绪的代、代轮换与清空。"""
    rng = np.random.default_rng(20261020)
    base = NOW
    # 一个 id 池（key_hashes 保证同一 id → 同一 64 位哈希，可制造重复）
    pool = [f"e{i}" for i in range(4096)]
    scripts: list = []
    scripts.append(("advance", base))

    def pick(count, offset_ms):
        idx = rng.integers(0, len(pool), size=count)
        h = key_hashes([pool[i] for i in idx.tolist()])
        ts = (base + offset_ms + rng.integers(0, 9000, size=count)).astype(np.int64)
        return h, ts

    # 当前代（就绪）：批内重复
    h, ts = pick(300, 0)
    scripts.append(("check", h, ts))
    # 跨批重复：重发上一批的一部分 id（同一 ts/代）
    h2, ts2 = h[:150].copy(), ts[:150].copy()
    scripts.append(("check", h2, ts2))
    # 跨代：更早的就绪代（base − 30s，仍在 60s 迟到窗口内）
    h3, ts3 = pick(300, -30_000)
    scripts.append(("check", h3, ts3))
    # 跨批重复 + 跨代：重发 base−30s 代的一部分，再混入当前代
    h4 = np.concatenate([h3[:80], h[:80]])
    ts4 = np.concatenate([ts3[:80], ts[:80]])
    scripts.append(("check", h4, ts4))
    # 未就绪的代：早于 watermark（base−70s，gen < lo）与晚于 now+5s（base+20s，gen > hi）
    h5, ts5 = pick(100, -70_000)     # 太旧 → 未就绪（active=False）
    scripts.append(("check", h5, ts5))
    h6, ts6 = pick(100, 20_000)      # 未来太远 → 未就绪
    scripts.append(("check", h6, ts6))
    # 代轮换与清空：advance 推进 12s（watermark 前移，清掉最早的一代并复用槽）
    scripts.append(("advance", base + 12_000))
    # 清掉的那一代（base 时的最早代）若再发应视为"新"（已被清空）
    h7, ts7 = pick(120, -60_000)     # base+12s 的水mark 前约 base−48s，base−60s 仍未就绪
    scripts.append(("check", h7, ts7))
    # 推进 60s，触发更多轮换
    scripts.append(("advance", base + 60_000))
    # 轮换后的当前代 + 与更早未清空代的跨批重复
    h8, ts8 = pick(300, 30_000)      # base+60s 的当前代
    scripts.append(("check", h8, ts8))
    h9 = np.concatenate([h8[:60], h[:60]])
    ts9 = np.concatenate([ts8[:60], ts[:60]])   # 混入 base 时当前代的旧 id（可能已清空）
    scripts.append(("check", h9, ts9))
    # 最后一轮换：再推进 100s，覆盖整圈
    scripts.append(("advance", base + 160_000))
    h10, ts10 = pick(400, 80_000)
    scripts.append(("check", h10, ts10))
    # 全批同一 id（批内全重复）
    one = key_hashes(["same"] * 200)
    scripts.append(("check", one, np.full(200, base + 80_000, dtype=np.int64)))
    return scripts


def test_equivalence_new_vs_reference():
    """新实现（数组判代 + 保留的写位路径）与改前参考实现，在含批内/跨批/跨代/
    未就绪代/轮换清空的随机序列下，每步的 (dup, active) 与每一代 words/n/代号 全同。

    对 k=6（新默认）与 k=12（改前公式）两个取值都跑，证明判代优化对任意固定 k
    都输出不变。"""
    script = _scenario_ops()
    _run_scenario(script, k=6)
    _run_scenario(script, k=12)
