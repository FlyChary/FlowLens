"""临时诊断（P2b，不入库）：Bloom 写位 np.bitwise_or.at vs 排序+reduceat，
交替计时（抵消 CPU 漂移），在稳态（一代已填 2M id）下对一批全新 id 的写位计时。"""
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import numpy as np

from flowlens.hashing import SEED_BLOOM, U64, double_hash

bits, k, nwords = 25, 12, (1 << 25) // 64


def probe(h):
    h1, h2 = double_hash(h, SEED_BLOOM)
    i = np.arange(k, dtype=U64)[None, :]
    pos = (h1[:, None] + i * h2[:, None]) >> U64(64 - bits)
    return (pos >> U64(6)).astype(np.intp), U64(1) << (pos & U64(63))


def write_at(words, wn, mn):
    np.bitwise_or.at(words, wn, mn)


def write_reduceat(words, wn, mn):
    order = np.argsort(wn, kind="stable")
    sw, sm = wn[order], mn[order]
    bnd = np.flatnonzero(np.r_[True, sw[1:] != sw[:-1]])
    words[sw[bnd]] |= np.bitwise_or.reduceat(sm, bnd)


rng = np.random.default_rng(0)
base = np.zeros(nwords, dtype=np.uint64)
fill = rng.integers(0, 1 << 64, size=2_000_000, dtype=np.uint64)
w, m = probe(fill)
np.bitwise_or.at(base, w.ravel(), m.ravel())

BATCH = 10_000
fresh = rng.integers(0, 1 << 64, size=BATCH, dtype=np.uint64)
w, m = probe(fresh)
seen = ((base[w] & m) != 0).all(axis=1)
new = ~seen
wn = w[new].ravel()
mn = m[new].ravel()
print(f"batch={BATCH}  new_bits_to_set~{len(wn)}  words={nwords}  (探测/word≈{len(wn)/nwords:.2f})")

reps = 30
ta = []
tr = []
for _ in range(reps):
    wa = base.copy()
    t0 = time.perf_counter(); write_at(wa, wn, mn); ta.append(time.perf_counter() - t0)
    wr = base.copy()
    t0 = time.perf_counter(); write_reduceat(wr, wn, mn); tr.append(time.perf_counter() - t0)
    assert (wa == wr).all(), "两写法结果不一致"
print(f"at        : med {statistics.median(ta)*1e6:8.1f} us   min {min(ta)*1e6:8.1f}")
print(f"reduceat  : med {statistics.median(tr)*1e6:8.1f} us   min {min(tr)*1e6:8.1f}")
print(f"at 相对 reduceat 快 {statistics.median(tr)/statistics.median(ta):.2f}x")
