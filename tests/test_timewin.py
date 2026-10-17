import numpy as np
import pytest

from flowlens.timewin import (ACCEPT, FUTURE, FUTURE_SKEW_MS, LATE, LATENESS_MS, RING_SLOTS, Edge, classify,
                              sec, split_range, watermark)


def test_classify_boundaries():
    now = 100_500
    ts = np.array([now - LATENESS_MS, now - LATENESS_MS - 1, now + FUTURE_SKEW_MS, now + FUTURE_SKEW_MS + 1])
    assert classify(ts, now).tolist() == [ACCEPT, LATE, ACCEPT, FUTURE]


def test_accept_range_fits_ring():
    for now in range(1_000_000, 1_003_000, 7):
        assert sec(now + FUTURE_SKEW_MS) - sec(watermark(now)) + 1 <= RING_SLOTS


@pytest.mark.parametrize("f,t,start,stop,edges", [
    (1000, 5000, 1, 5, ()),
    (1400, 5600, 2, 5, (Edge(1, 0.6), Edge(5, 0.6))),
    (1200, 1700, 1, 1, (Edge(1, 0.5),)),
    (1000, 2000, 1, 2, ()),
    (3000, 3000, 0, 0, ()),
])
def test_split_range(f, t, start, stop, edges):
    r = split_range(f, t)
    assert (r.start, r.stop) == (start, stop)
    assert [(e.sec, round(e.frac, 9)) for e in r.edges] == [(e.sec, e.frac) for e in edges]
