import numpy as np
import pytest

from flowlens.hashing import key_hashes


def zipf_keys(n: int, nkeys: int, seed: int, s: float = 1.1, prefix: str = "/api/item/") -> list[str]:
    rng = np.random.default_rng(seed)
    w = 1.0 / np.arange(1, nkeys + 1) ** s
    idx = rng.choice(nkeys, size=n, p=w / w.sum())
    return [f"{prefix}{i}" for i in idx.tolist()]


def batches(keys: list[str], size: int = 1000):
    """把 key 序列切成批，返回 (hashes, keys, weights)。"""
    for i in range(0, len(keys), size):
        ks = keys[i:i + size]
        yield key_hashes(ks), ks, np.ones(len(ks), dtype=np.uint64)


@pytest.fixture
def zipf():
    return zipf_keys
