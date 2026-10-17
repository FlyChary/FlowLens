import numpy as np

from flowlens.config import Config
from flowlens.dims import Dim
from flowlens.hashing import key_hashes
from flowlens.ring import Ring
from flowlens.timewin import RING_SLOTS, sec

SPECS = Config(cms_width=256, heap_cap=16, hll_p=12).dim_specs()


def obs(bucket, d, key, nbytes=842):
    bucket.dims[d].observe(key_hashes([key]), [key], np.array([nbytes]), np.ones(1, dtype=np.uint64))


def test_finalize_in_order_including_empty_seconds():
    got, with_data = [], []
    r = Ring(SPECS, lambda s, b: (got.append(s), b is not None and with_data.append(s)))
    now = 1_000_000_000
    r.advance(now)
    obs(r.slot(sec(now) - 30), Dim.URL, "/a")
    obs(r.slot(sec(now) - 10), Dim.USER, "u_1")
    r.advance(now + 61_000)
    assert got == list(range(got[0], got[0] + len(got)))
    assert len(with_data) == 2


def test_dim_bucket_feeds_its_own_stats():
    r = Ring(SPECS, lambda s, b: None)
    b = r.slot(100)
    obs(b, Dim.URL, "/a")
    obs(b, Dim.USER, "u_1")
    assert b.dims[Dim.URL].stats.dd.n == 1 and b.dims[Dim.URL].stats.hll is None
    assert b.dims[Dim.USER].stats.hll is not None and b.dims[Dim.USER].stats.dd is None


def test_finalized_second_rejected():
    r = Ring(SPECS, lambda s, b: None)
    now = 1_000_000_000
    r.advance(now)
    r.advance(now + 5000)
    assert r.slot(sec(now) - 60) is None


def test_slot_reuse_finalizes_old_first():
    got = []
    r = Ring(SPECS, lambda s, b: b is not None and got.append(s))
    obs(r.slot(5000), Dim.URL, "x")
    r.slot(5000 + RING_SLOTS)
    assert got == [5000]
