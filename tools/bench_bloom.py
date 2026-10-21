"""P2b 一/二：Bloom 去重的子步骤基准与误判率实测（不修改服务，只测量）。

两部分：

一、子步骤耗时（P2b 一.1）：在一代已写入约 200 万 id（接近设计容量）的稳态下，
   每批 1 万条，分别计时 check_and_set 的各子步骤（判断代是否就绪、批内去重
   np.unique、计算探测位置 _probe、读取并判断、写入位），报告每条 μs。
   为了"优化前后"同框对照，这里把**旧实现**（逐条列表推导判代 + np.bitwise_or.at
   写位）与**新实现**（numpy 数组判代 + reduceat 合并掩码后写位）各写一份镜像，
   两者对同一批输入产出**逐位相同**的 (dup, active)（每批断言），计时各自独立。
   镜像与 flowlens/dedup.py 的 check_and_set 逐步对应（工具只测量，逻辑真相在
   tests/test_dedup.py 的参考实现对照测试里守护）。

二、误判率与 k（P2b 二.2）：在一代写入 per_gen（默认 200 万）个不同 id 后，再
   查询 100 万个从未写入的 id，统计被误判为"见过"的比例；对 k = 12/8/6/4 分别
   实测误判率与 check_and_set 每条的耗时（用真实的 GenerationalBloom）。

计时口径：每项若干轮（总事件数约 300 万），分 5 组取中位数。
用法：python tools/bench_bloom.py [--batch 10000] [--rounds 30] [--fill 2000000]
"""
import argparse
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np

from flowlens.dedup import BloomConfig, GenerationalBloom
from flowlens.hashing import U64, SEED_BLOOM, double_hash
from flowlens.timewin import FUTURE_SKEW_MS, watermark

BASE_GEN = 173_568_960  # 固定的代号（任意，保证 ts 落在就绪代内）


# ---- 稳态 Bloom 状态（镜像 flowlens/dedup.py 的字段，供镜像子步骤运算）----

class _GenState:
    __slots__ = ("words", "n", "id")

    def __init__(self, nwords: int):
        self.words = np.zeros(nwords, dtype=np.uint64)
        self.n = 0
        self.id = None


class _BloomState:
    """与 GenerationalBloom 同形的最小状态：k、bits、代数、各代 words/n/id，
    以及各代 id 的数组（新实现判代用）。gen_ms 固定 10 秒（与真实配置一致）。"""

    GEN_MS = 10_000

    def __init__(self, nwords: int, k: int, ngens: int, bits: int = 25):
        self.k = k
        self.bits = bits
        self.ngens = ngens
        self.gens = [_GenState(nwords) for _ in range(ngens)]
        self._gen_ids = np.full(ngens, -1, dtype=np.int64)  # 槽 -> 代号，-1 未就绪

    def advance(self, now_ms: int) -> None:
        """与真实 GenerationalBloom.advance 相同语义（供预热把某代设为就绪）。"""
        lo = watermark(now_ms) // self.GEN_MS
        hi = (now_ms + FUTURE_SKEW_MS) // self.GEN_MS
        for g in range(lo, hi + 1):
            gen = self.gens[g % self.ngens]
            if gen.id != g:
                gen.words.fill(0)
                gen.n, gen.id = 0, g
                self._gen_ids[g % self.ngens] = g


def _probe(h: np.ndarray, k: int, bits: int):
    """与 GenerationalBloom._probe 相同：返回 (字下标, 位掩码)，形状 (n, k)。"""
    h1, h2 = double_hash(h, SEED_BLOOM)
    i = np.arange(k, dtype=U64)[None, :]
    pos = (h1[:, None] + i * h2[:, None]) >> U64(64 - bits)
    return (pos >> U64(6)).astype(np.intp), U64(1) << (pos & U64(63))


def _check_old(st: _BloomState, id_hashes: np.ndarray, ts_ms: np.ndarray):
    """旧实现镜像（改前 check_and_set 的逐步对应）：逐条列表推导判代 +
    np.bitwise_or.at 写位。返回 (dup, active)。"""
    n = len(id_hashes)
    dup = np.zeros(n, dtype=bool)
    gen_ids = ts_ms // st.GEN_MS
    active = np.array([st.gens[g % st.ngens].id == g for g in gen_ids.tolist()],
                      dtype=bool)
    for g in np.unique(gen_ids[active]).tolist():
        sel = np.flatnonzero(active & (gen_ids == g))
        gen = st.gens[g % st.ngens]
        uniq, first = np.unique(id_hashes[sel], return_index=True)
        dup[sel] = True
        w, m = _probe(uniq, st.k, st.bits)
        seen = ((gen.words[w] & m) != 0).all(axis=1)
        new = ~seen
        dup[sel[first[new]]] = False
        if new.any():
            np.bitwise_or.at(gen.words, w[new].ravel(), m[new].ravel())
            gen.n += int(new.sum())
    return dup, active


def _write_reduceat(st_words: np.ndarray, w: np.ndarray, m: np.ndarray):
    """P2b 一.2 提议的写位：按字下标排序，reduceat 合并同一字的掩码，对去重字下标 |=。
    实测（p2b_write_diag.py / 本节）在本布隆填充率下比 np.bitwise_or.at 慢 2.9×~4.8×，
    故 dedup.py 保留 at；此处保留该实现用于对照计时。"""
    order = np.argsort(w, kind="stable")
    sw, sm = w[order], m[order]
    bnd = np.flatnonzero(np.r_[True, sw[1:] != sw[:-1]])
    st_words[sw[bnd]] |= np.bitwise_or.reduceat(sm, bnd)


def _check_new(st: _BloomState, id_hashes: np.ndarray, ts_ms: np.ndarray):
    """新实现镜像（与改后 flowlens/dedup.py 的 check_and_set 逐步对应）：numpy 数组判代 +
    保留的 np.bitwise_or.at 写位。返回 (dup, active)，与旧实现逐位相同。"""
    n = len(id_hashes)
    dup = np.zeros(n, dtype=bool)
    gen_ids = ts_ms // st.GEN_MS
    active = st._gen_ids[gen_ids % st.ngens] == gen_ids
    for g in np.unique(gen_ids[active]).tolist():
        sel = np.flatnonzero(active & (gen_ids == g))
        gen = st.gens[g % st.ngens]
        uniq, first = np.unique(id_hashes[sel], return_index=True)
        dup[sel] = True
        w, m = _probe(uniq, st.k, st.bits)
        seen = ((gen.words[w] & m) != 0).all(axis=1)
        new = ~seen
        dup[sel[first[new]]] = False
        if new.any():
            np.bitwise_or.at(gen.words, w[new].ravel(), m[new].ravel())
            gen.n += int(new.sum())
    return dup, active


def time_check(fn, st: _BloomState, pool: np.ndarray, ts_pool: np.ndarray,
               batch: int, rounds: int) -> float:
    """fn 跑 rounds 轮（每轮一个**全新** batch，都会走写路径）的总秒数，分 5 组取
    组耗时中位数×轮数。pool/ts_pool 需含 rounds×batch 条互不相同的 id。"""
    g = max(1, rounds // 5)
    per = []
    for r0 in range(0, rounds, g):
        r1 = min(rounds, r0 + g)
        t0 = time.perf_counter()
        for r in range(r0, r1):
            i = r * batch
            fn(st, pool[i:i + batch], ts_pool[i:i + batch])
        per.append((time.perf_counter() - t0) / (r1 - r0))
    return statistics.median(per) * rounds


def timed_substeps(which: str, st: _BloomState, pool: np.ndarray, ts_pool: np.ndarray,
                   batch: int, rounds: int) -> dict:
    """跑 rounds 个全新 batch，累加各子步骤的墙钟秒数，返回 {子步骤: 每条μs}。
    与 check_and_set 的逐步顺序对应；稳态下每批都在同一代、每批 id 全新（都写入）。"""
    acc = {"判代": 0.0, "批内去重": 0.0, "_probe": 0.0, "读取判断": 0.0, "写入位": 0.0}
    ng, bits = st.ngens, st.bits
    for r in range(rounds):
        i = r * batch
        ids = pool[i:i + batch]
        ts = ts_pool[i:i + batch]
        dup = np.zeros(len(ids), dtype=bool)
        gen_ids = ts // st.GEN_MS
        t0 = time.perf_counter()
        if which == "old":
            active = np.array([st.gens[g % ng].id == g for g in gen_ids.tolist()],
                              dtype=bool)
        else:
            active = st._gen_ids[gen_ids % ng] == gen_ids
        acc["判代"] += time.perf_counter() - t0
        for g in np.unique(gen_ids[active]).tolist():
            sel = np.flatnonzero(active & (gen_ids == g))
            gen = st.gens[g % ng]
            t0 = time.perf_counter()
            uniq, first = np.unique(ids[sel], return_index=True)
            acc["批内去重"] += time.perf_counter() - t0
            dup[sel] = True
            t0 = time.perf_counter()
            w, m = _probe(uniq, st.k, bits)
            acc["_probe"] += time.perf_counter() - t0
            t0 = time.perf_counter()
            seen = ((gen.words[w] & m) != 0).all(axis=1)
            acc["读取判断"] += time.perf_counter() - t0
            new = ~seen
            dup[sel[first[new]]] = False
            t0 = time.perf_counter()
            if new.any():
                # 新旧写位都用 np.bitwise_or.at（dedup.py 保留了 at；reduceat 实测更慢，
                # 见 _write_reduceat 与 p2b_write_diag.py）
                np.bitwise_or.at(gen.words, w[new].ravel(), m[new].ravel())
                gen.n += int(new.sum())
            acc["写入位"] += time.perf_counter() - t0
    ntot = len(pool)
    return {k: v / ntot * 1e6 for k, v in acc.items()}


def main() -> None:
    ap = argparse.ArgumentParser(description="P2b Bloom 子步骤/误判率基准")
    ap.add_argument("--batch", type=int, default=10_000)
    ap.add_argument("--rounds", type=int, default=30, help="每方案轮数（每轮一个 batch）")
    ap.add_argument("--fill", type=int, default=2_000_000, help="预热一代写入的不同 id 数")
    ap.add_argument("--fp-probe", type=int, default=1_000_000, help="误判率查询的 id 数")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    rng = np.random.default_rng(20261020)
    nwords = (1 << 25) // 64  # 与默认 per_gen=2M 相同（bits=25）

    # ==== 一、子步骤耗时（旧 vs 新，稳态同一代）====
    print(f"== 一、子步骤耗时（预热 {args.fill} id/代，每批 {args.batch}，"
          f"每方案 {args.rounds} 轮，bits=25）==")
    for which in ("old", "new"):
        label = "旧(逐条判代+bitwise_or.at)" if which == "old" else "新(数组判代+bitwise_or.at)"
        fn = _check_old if which == "old" else _check_new
        st = _BloomState(nwords, 12, 8)
        st.advance(BASE_GEN * st.GEN_MS)
        fill_ids = rng.integers(0, 1 << 64, size=args.fill, dtype=np.uint64)
        fts = (BASE_GEN * st.GEN_MS +
               rng.integers(0, st.GEN_MS, size=args.fill)).astype(np.int64)
        fn(st, fill_ids, fts)  # 预热（旧新填出来的 words 相同）
        # 计时用 rounds×batch 条互不相同的全新 id（每批都走写路径）。
        # time_check 与 timed_substeps 各用一份独立的新池：前者写完，后者若复用同一池
        # 就全是重复（写路径被跳过、写入位归零），故用 pool2 让子步骤也测到写路径。
        pool = rng.integers(0, 1 << 64, size=args.rounds * args.batch, dtype=np.uint64)
        tsp = (BASE_GEN * st.GEN_MS +
               rng.integers(0, st.GEN_MS, size=args.rounds * args.batch)).astype(np.int64)
        pool2 = rng.integers(0, 1 << 64, size=args.rounds * args.batch, dtype=np.uint64)
        tsp2 = (BASE_GEN * st.GEN_MS +
                rng.integers(0, st.GEN_MS, size=args.rounds * args.batch)).astype(np.int64)
        total = time_check(fn, st, pool, tsp, args.batch, args.rounds)
        sub = timed_substeps(which, st, pool2, tsp2, args.batch, args.rounds)
        per_ev = total / (args.batch * args.rounds) * 1e6
        sub_sum = sum(sub.values())
        parts = "  ".join(f"{k} {v:.3f}" for k, v in sub.items())
        print(f"[{label}] check_and_set 合计 {per_ev:6.3f} μs/条 (子步之和 {sub_sum:.3f}) | "
              f"{parts} (μs/条)")
        # 交叉核对：同一预热状态下旧新对单批输出逐位一致
        tids = rng.integers(0, 1 << 64, size=args.batch, dtype=np.uint64)
        tts = (BASE_GEN * st.GEN_MS +
               rng.integers(0, st.GEN_MS, size=args.batch)).astype(np.int64)
        st2 = _BloomState(nwords, 12, 8)
        st2.advance(BASE_GEN * st2.GEN_MS)
        _check_new(st2, fill_ids, fts)
        d1, a1 = _check_old(st2, tids, tts)
        st3 = _BloomState(nwords, 12, 8)
        st3.advance(BASE_GEN * st3.GEN_MS)
        _check_new(st3, fill_ids, fts)
        d2, a2 = _check_new(st3, tids, tts)
        assert (d1 == d2).all() and (a1 == a2).all(), "旧新输出不一致"
    print("  （旧新对同一输入 dup/active 逐位一致：断言通过）")

    # 一.2 写位方法对照：np.bitwise_or.at vs 排序+reduceat（本布隆填充率下 at 更快）
    print("\n  一.2 写位方法对照（同一稳态代，一批全新 id 的写位耗时）：")
    stw = _BloomState(nwords, 12, 8)
    stw.advance(BASE_GEN * stw.GEN_MS)
    fw = rng.integers(0, 1 << 64, size=args.fill, dtype=np.uint64)
    ftw = (BASE_GEN * stw.GEN_MS +
           rng.integers(0, stw.GEN_MS, size=args.fill)).astype(np.int64)
    _check_new(stw, fw, ftw)
    hw = rng.integers(0, 1 << 64, size=args.batch, dtype=np.uint64)
    tw = (BASE_GEN * stw.GEN_MS +
          rng.integers(0, stw.GEN_MS, size=args.batch)).astype(np.int64)
    genw = stw.gens[BASE_GEN % 8]
    gw, gm = _probe(hw, 12, 25)
    gseen = ((genw.words[gw] & gm) != 0).all(axis=1)
    gnew = ~gseen
    gwn = gw[gnew].ravel()
    gmn = gm[gnew].ravel()
    ta, tr = [], []
    for _ in range(20):
        wa = genw.words.copy()
        t0 = time.perf_counter(); np.bitwise_or.at(wa, gwn, gmn); ta.append(time.perf_counter() - t0)
        wr = genw.words.copy()
        t0 = time.perf_counter(); _write_reduceat(wr, gwn, gmn); tr.append(time.perf_counter() - t0)
        assert (wa == wr).all(), "两种写位结果不一致"
    print(f"    bitwise_or.at   {statistics.median(ta) * 1e3:8.2f} ms/批（{len(gwn)} 个位）")
    print(f"    排序+reduceat   {statistics.median(tr) * 1e3:8.2f} ms/批")
    print(f"    → at 快 {statistics.median(tr) / statistics.median(ta):.2f}×，"
          f"dedup.py 保留 at（见 p2b_write_diag.py）")

    # ==== 二、误判率与 k（真实 GenerationalBloom）====
    print(f"\n== 二、误判率与 k（一代写入 {args.fill} 不同 id，"
          f"再查 {args.fp_probe} 个未写入 id）==")
    ntimed = args.batch * args.rounds
    for k in (12, 8, 6, 4):
        base_gen = BASE_GEN
        # 计时：稳态（预填 fill 个不同 id 接近容量）下，对**每批全新 id**（都会写入、
        # 走写路径）的 check_and_set；5 个独立 pass（各自用不相交的全新 id）取中位
        bt = GenerationalBloom(BloomConfig(k=k))
        bt.advance(base_gen * 10_000)
        prefill = rng.integers(0, 1 << 64, size=args.fill, dtype=np.uint64)
        pts = (base_gen * 10_000 +
               rng.integers(0, 10_000, size=args.fill)).astype(np.int64)
        for i in range(0, args.fill, args.batch):
            bt.check_and_set(prefill[i:i + args.batch], pts[i:i + args.batch])
        all_tids = rng.integers(0, 1 << 64, size=5 * ntimed, dtype=np.uint64)
        all_tts = (base_gen * 10_000 +
                   rng.integers(0, 10_000, size=5 * ntimed)).astype(np.int64)
        per_pass = []
        for p in range(5):
            tids = all_tids[p * ntimed:(p + 1) * ntimed]
            tts = all_tts[p * ntimed:(p + 1) * ntimed]
            t0 = time.perf_counter()
            for i in range(0, ntimed, args.batch):
                bt.check_and_set(tids[i:i + args.batch], tts[i:i + args.batch])
            per_pass.append(time.perf_counter() - t0)
        t = statistics.median(per_pass) / ntimed
        # 单独测误判率：干净一代写满后，查一批未写入 id 的 dup 率
        b = GenerationalBloom(BloomConfig(k=k))
        b.advance(base_gen * 10_000)
        fill = rng.integers(0, 1 << 64, size=args.fill, dtype=np.uint64)
        fts2 = (base_gen * 10_000 +
                rng.integers(0, 10_000, size=args.fill)).astype(np.int64)
        for i in range(0, args.fill, args.batch):
            b.check_and_set(fill[i:i + args.batch], fts2[i:i + args.batch])
        q = rng.integers(0, 1 << 64, size=args.fp_probe, dtype=np.uint64)
        qts = (base_gen * 10_000 +
               rng.integers(0, 10_000, size=args.fp_probe)).astype(np.int64)
        dup, _ = b.check_and_set(q, qts)
        fp = float(dup.mean())
        print(f"k={k:2d}  误判率 {fp * 100:.4f}%   check_and_set {t * 1e6:6.3f} μs/条")
    print("\nSUMMARY  见上：一(子步骤 μs/条)、二(误判率 % / k 耗时)")


if __name__ == "__main__":
    main()
