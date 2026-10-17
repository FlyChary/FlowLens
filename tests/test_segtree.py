from collections import Counter

import numpy as np
import pytest

from flowlens.bounds import for_key, for_keys, top_k
from flowlens.hashing import key_hash, key_hashes
from flowlens.segtree import LeafInput, Tree, TreeConfig
from flowlens.sketch import HeavyHitters, StatsSpec

CMS_W, CMS_D, HEAP = 256, 4, 48  # 故意很窄，让截断和碰撞都频繁发生
STATS = StatsSpec(hll=True, hll_p=12, dd=True)


def cfg(levels=5, leaf_keep=16, keep=32, cand=64, retention=100, width=CMS_W):
    return TreeConfig(levels, leaf_keep, (0,) + (keep,) * levels, (0,) + (cand,) * levels,
                      width, CMS_D, retention, 3, STATS)


def hh_of(counts: dict, heap=HEAP, width=CMS_W) -> HeavyHitters:
    hh = HeavyHitters(width, CMS_D, heap)
    if counts:
        ks = list(counts)
        hh.observe(key_hashes(ks), ks, np.array([counts[k] for k in ks], dtype=np.uint64))
    return hh


class World:
    """逐秒生成流量：热点集合随时间漂移，长尾里有大量一次性 key。"""

    def __init__(self, seed=1):
        self.rng = np.random.default_rng(seed)
        self.truth: dict[int, Counter] = {}
        w = 1.0 / np.arange(1, 2001) ** 1.2
        self.p = w / w.sum()

    def second(self, s: int) -> HeavyHitters:
        n = int(self.rng.integers(300, 600))
        ranks = self.rng.choice(2000, size=n, p=self.p)
        once = self.rng.random(n) < 0.2
        keys = [f"once-{s}-{i}" if o else f"/k/{(r + s // 20) % 2000}" for i, (r, o) in enumerate(zip(ranks, once))]
        c = Counter(keys)
        self.truth[s] = c
        hh = HeavyHitters(CMS_W, CMS_D, HEAP)
        # 分成几小批写入，模拟真实的批处理
        for i in range(0, len(keys), 97):
            ks = keys[i:i + 97]
            hh.observe(key_hashes(ks), ks, np.ones(len(ks), dtype=np.uint64))
        return hh

    def exact(self, a: int, b: int) -> Counter:
        out = Counter()
        for s in range(a, b):
            out.update(self.truth.get(s, {}))
        return out


@pytest.fixture(scope="module")
def built():
    tr, w = Tree(cfg()), World()
    for s in range(1000, 1160):
        tr.add_leaf(LeafInput(s, w.second(s)))
    return tr, w


def test_cover_exact_and_minimal(built):
    tr, _ = built
    lo, hi = tr.frozen_range()
    assert hi - lo == 100
    rng = np.random.default_rng(2)
    for _ in range(2000):
        a = int(rng.integers(lo, hi))
        b = int(rng.integers(a + 1, hi + 1))
        cov = tr.cover(a, b)
        cur = a
        for s in cov:
            assert s.start == cur and s.start % s.span == 0
            cur = s.end
        assert cur == b and len(cov) <= 2 * (tr.cfg.levels + 1)
    assert tr.cover(lo - 1, lo + 5) is None


def test_bounds_always_hold(built):
    """最关键的测试：任意区间、任意 key，真实次数都必须落在返回的上下界内。"""
    tr, w = built
    lo, hi = tr.frozen_range()
    rng = np.random.default_rng(3)
    for _ in range(300):
        a = int(rng.integers(lo, hi))
        b = int(rng.integers(a + 1, hi + 1))
        truth = w.exact(a, b)
        keys = list(truth)
        v = np.array([truth[k] for k in keys], dtype=float)
        l, u = for_keys(tr.cover(a, b).parts(), key_hashes(keys))
        bad = np.flatnonzero((v < l) | (v > u))
        assert len(bad) == 0, (a, b, keys[bad[0]], v[bad[0]], l[bad[0]], u[bad[0]]) if len(bad) else None


def test_topk_recall_over_long_range(built):
    tr, w = built
    lo, hi = tr.frozen_range()
    ans = top_k(tr.cover(lo, hi).parts(), 10)
    best = {k for k, _ in w.exact(lo, hi).most_common(10)}
    assert len(best & {r.key for r in ans.items}) >= 8


def test_gap_filled_with_empty_leaves():
    tr = Tree(cfg())
    tr.add_leaf(LeafInput(10, hh_of({"a": 3})))
    tr.add_leaf(LeafInput(15, hh_of({"a": 3})))
    cov = tr.cover(10, 16)
    assert cov is not None and for_key(cov.parts(), key_hash("a")) == (6, 6)


def test_stats_cover_has_sketches(built):
    tr, _ = built
    lo, hi = tr.frozen_range()
    for s in tr.cover(lo, hi, stats_only=True):
        assert s.stats is not None and s.stats.hll is not None and s.stats.dd is not None
        assert s.level == 0 or s.level >= tr.cfg.stats_min_level


def node_bounds(tr, a, b, key):
    cov = tr.cover(a, b)
    assert len(cov) == 1
    return for_key(cov.parts(), key_hash(key))


def test_node_u_includes_leaf_heap_mins():
    """每秒都挤不进秒桶名单的 key：节点的缺席上界必须包含 Σ 各秒堆顶。"""
    tr = Tree(cfg(levels=3, keep=1000, cand=1000, retention=64, width=1024))
    for s in range(8):
        tr.add_leaf(LeafInput(s, hh_of({"hot1": 50, "hot2": 50, "mid": 5}, heap=2, width=1024)))
    assert node_bounds(tr, 0, 8, "mid")[1] >= 40


def test_node_u_includes_candidate_min():
    """曾经进过候选、后来被挤出的 key：节点的缺席上界必须包含候选堆的最终堆顶。"""
    tr = Tree(cfg(levels=3, keep=2, cand=2, retention=64, width=1024))
    tr.add_leaf(LeafInput(0, hh_of({"X": 10}, heap=100, width=1024)))
    tr.add_leaf(LeafInput(1, hh_of({"Y": 20, "Z": 20}, heap=100, width=1024)))
    for s in range(2, 8):
        tr.add_leaf(LeafInput(s))
    assert node_bounds(tr, 0, 8, "X")[1] >= 10
