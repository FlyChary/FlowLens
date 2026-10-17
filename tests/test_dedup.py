import numpy as np

from flowlens.dedup import BloomConfig, GenerationalBloom
from flowlens.hashing import key_hashes

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
