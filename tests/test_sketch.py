import math
from collections import Counter

import numpy as np
import pytest

from flowlens.hashing import key_hash, key_hashes
from flowlens.sketch import CMS, DDSketch, HLL, HeavyHitters, IndexedMinHeap

from conftest import batches, zipf_keys


def test_cms_never_underestimates():
    s = CMS(1024, 4)
    keys = zipf_keys(50_000, 5000, 1)
    truth = Counter(keys)
    hs = key_hashes(list(truth))
    s.add(hs, np.array(list(truth.values()), dtype=np.uint64))
    assert (s.estimate(hs) >= np.array(list(truth.values()))).all()


def test_cms_merge_is_upper_bound():
    a, b = CMS(512, 4), CMS(512, 4)
    keys = zipf_keys(20_000, 3000, 2)
    for i, part in enumerate((keys[::2], keys[1::2])):
        c = Counter(part)
        (a if i == 0 else b).add(key_hashes(list(c)), np.array(list(c.values()), dtype=np.uint64))
    a.merge(b)
    truth = Counter(keys)
    assert (a.estimate(key_hashes(list(truth))) >= np.array(list(truth.values()))).all()


@pytest.mark.parametrize("nkeys", [50, 5000, 50_000])  # 堆不满、长尾、几乎全唯一
def test_heavy_hitters_bounds_hold(nkeys):
    hh = HeavyHitters(1024, 4, 128)
    keys = zipf_keys(40_000, nkeys, nkeys)
    for b in batches(keys, 700):
        hh.observe(*b)
    for k, v in Counter(keys).items():
        lo, hi = hh.bounds(key_hash(k))
        assert lo <= v <= hi, (k, v, lo, hi)


def test_topk_recall_on_long_tail():
    hh = HeavyHitters(2048, 4, 256)
    keys = zipf_keys(200_000, 20_000, 7)
    for b in batches(keys):
        hh.observe(*b)
    best = {k for k, _ in Counter(keys).most_common(10)}
    got = {e.key for e in hh.top.top(10)}
    assert len(best & got) >= 9


def test_topk_min_non_decreasing():
    hh = HeavyHitters(256, 4, 32)
    prev = 0
    for b in batches(zipf_keys(30_000, 10_000, 3), 50):
        hh.observe(*b)
        assert hh.u() >= prev
        prev = hh.u()


def test_heap_invariants_under_random_ops():
    rng = np.random.default_rng(1)
    q = IndexedMinHeap(64)
    for step in range(20_000):
        k, p = int(rng.integers(200)), int(rng.integers(1000))
        if k in q:
            q.update(k, p, None)
        else:
            q.offer(k, p, None)
        if step % 997 == 0:
            q.reprioritize([x // 2 for x in q.prios])
        for i in range(len(q)):
            left = 2 * i + 1
            assert left >= len(q) or q.prios[left] >= q.prios[i]
            assert q.idx[q.keys[i]] == i


@pytest.mark.parametrize("n", [0, 1, 10, 100, 1000, 10_000, 40_000, 100_000, 1_000_000])
def test_hll_accuracy(n):
    s = HLL(14)
    for i in range(0, n, 100_000):
        s.add(key_hashes([f"u_{n}_{j}" for j in range(i, min(n, i + 100_000))]))
    est = s.estimate()
    if n == 0:
        assert est == 0
    else:
        assert abs(est - n) <= max(1, 4 * 1.04 / math.sqrt(1 << 14) * n)


def test_hll_segments_merge_to_whole():
    hs = key_hashes([f"user-{i}" for i in range(50_000)])
    whole = HLL(14)
    whole.add(hs)
    shard = (hs >> np.uint64(61)).astype(int)  # 高 3 位分成 8 段
    merged = HLL(14)
    for i in range(8):
        seg = HLL(14, i * 2048, (i + 1) * 2048)
        seg.add(hs[shard == i])
        merged.merge(seg)
    assert merged.estimate() == whole.estimate()


def test_ddsketch_relative_error():
    rng = np.random.default_rng(42)
    data = np.floor(np.exp(np.log(800) + 1.6 * rng.standard_normal(100_000)))
    data[rng.random(100_000) < 0.02] = 0
    s = DDSketch(0.01)
    for chunk in np.array_split(data, 37):
        s.add(chunk, np.ones(len(chunk), dtype=np.uint64))
    srt = np.sort(data)
    for q in (0, 0.01, 0.5, 0.9, 0.95, 0.99, 0.999, 1):
        want = srt[int(math.floor(q * (len(srt) - 1)))]
        got = s.quantile(q)
        assert got == 0 if want == 0 else abs(got - want) / want <= 0.01 + 1e-9


def test_ddsketch_merge_equals_direct():
    v = np.arange(1, 5001, dtype=float)
    a, b, c = DDSketch(), DDSketch(), DDSketch()
    a.add(v, np.ones(5000))
    b.add(v * 37, np.full(5000, 2))
    c.add(v, np.ones(5000))
    c.add(v * 37, np.full(5000, 2))
    a.merge(b)
    assert all(a.quantile(q) == c.quantile(q) for q in (0.1, 0.5, 0.95))
