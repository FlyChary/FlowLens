"""统计核心吞吐基准（不含 HTTP 与 JSON 解析）：一批事件从算哈希到写入活跃环的完整开销。

用法：python tools/bench_core.py [长尾|均匀|随机]
"""

import sys
import time

import numpy as np

from flowlens.config import Config
from flowlens.dedup import GenerationalBloom
from flowlens.dims import Dim
from flowlens.hashing import key_hashes
from flowlens.ring import Ring
from flowlens.timewin import ACCEPT, classify, sec


def make_events(n, mode, rng):
    if mode == "随机":
        urls = [f"/r/{i}" for i in range(n)]
        users = [f"u{i}" for i in range(n)]
    else:
        nk = 20_000
        w = 1.0 / np.arange(1, nk + 1) ** (1.1 if mode == "长尾" else 0.2)
        idx = rng.choice(nk, size=n, p=w / w.sum())
        urls = [f"/api/item/{i}" for i in idx.tolist()]
        users = [f"u{i}" for i in rng.integers(0, 200_000, n).tolist()]
    return urls, users, rng.integers(100, 50_000, n), [f"e{i}" for i in range(n)]


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "长尾"
    rng = np.random.default_rng(0)
    n, batch = 400_000, 1000
    urls, users, nbytes, ids = make_events(n, mode, rng)
    now = 1_735_689_600_000
    cfg = Config()
    ring = Ring(cfg.dim_specs(), lambda s, b: None)
    bloom = GenerationalBloom(cfg.bloom)
    ring.advance(now)
    bloom.advance(now)
    ts_all = now - rng.integers(0, 2000, n)
    ones = np.ones(batch, dtype=np.uint64)

    t0 = time.perf_counter()
    for i in range(0, n, batch):
        sl = slice(i, i + batch)
        u_sl, s_sl, b_sl = urls[sl], users[sl], nbytes[sl]
        ts = ts_all[sl]
        ok = classify(ts, now) == ACCEPT
        dup, _ = bloom.check_and_set(key_hashes(ids[sl]), ts)
        keep = ok & ~dup
        hu, hs = key_hashes(u_sl), key_hashes(s_sl)
        secs = ts // 1000
        for s in np.unique(secs[keep]).tolist():
            m = keep & (secs == s)
            b = ring.slot(s)
            sel = np.flatnonzero(m)
            idx = sel.tolist()
            b.dims[Dim.URL].observe(hu[sel], [u_sl[j] for j in idx], b_sl[sel], ones[:len(sel)])
            b.dims[Dim.USER].observe(hs[sel], [s_sl[j] for j in idx], b_sl[sel], ones[:len(sel)])
    dt = time.perf_counter() - t0
    print(f"{mode}：{n} 条用时 {dt:.2f} s，约 {n / dt / 1000:.0f} k 事件/秒（单核，不含 HTTP 与 JSON）")


if __name__ == "__main__":
    main()
