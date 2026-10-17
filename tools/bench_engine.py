"""parse_events + engine.ingest 吞吐基准 + 单次定稿耗时：长尾 URL、大量不同用户。

用法：python tools/bench_engine.py

分三部分测量，互不干扰：

一、吞吐（口径与 tools/bench_core.py 一致）：now 固定、事件时间戳在 now 前 ~2 秒内
聚簇（对应高到达率下批内 ts 的形态）。分项测 parse_events 与 engine.ingest，
对比 1000 条 / 5000 条两种批大小。

二、单次定稿进树耗时：模拟后台每秒 tick，让秒桶逐个定稿推进线段树，记录每次
finalize（一个秒桶进树）的耗时中位数与最大值——这是 tick 的单次开销，不能阻塞
事件循环、拖过查询的 50 ms 延迟预算。

三、快照耗时（T3）：准备"满载"的 60 秒窗口——60 秒里每秒写满（两个维度的秒桶
名单都满），分别测 refresh_snapshot 与完整 tick（含一次定稿）各 20 次的中位数
与最大值。这是后台每秒的固定开销，同样受 50 ms 预算约束。
"""

import statistics
import time

import numpy as np
import orjson

from flowlens.config import Config
from flowlens.dims import ALL_DIMS
from flowlens.engine import Engine
from flowlens.model import parse_events
from flowlens.timewin import FakeClock, split_range

BASE = 1_735_689_600_000
N_TOTAL = 100_000   # 每种批大小下处理的总事件数
N_URL = 20_000      # 长尾 URL 池
N_USER = 200_000    # 大量不同用户（与 bench_core 口径一致）
SPREAD_MS = 2000    # ts 在 now 前 2 秒内聚簇
BATCHES_TO_TEST = (1000, 5000)
FINALIZE_SECONDS = 200  # 定稿测量覆盖的秒数


def make_body(i: int, rng, batch: int, now_ms: int) -> bytes:
    """一批事件，ts 聚簇在 now_ms 前 2 秒内。"""
    w = 1.0 / np.arange(1, N_URL + 1) ** 1.1
    idx = rng.choice(N_URL, size=batch, p=w / w.sum())
    urls = [f"/api/item/{u}" for u in idx.tolist()]
    users = [f"u{u}" for u in rng.integers(0, N_USER, batch).tolist()]
    ts = [now_ms - int(x) for x in rng.integers(1, SPREAD_MS, batch)]
    evs = [{
        "event_id": f"e{i * batch + j}",
        "user_id": users[j],
        "url": urls[j],
        "ts": ts[j],
        "bytes": 1000 + (j % 100),
    } for j in range(batch)]
    return orjson.dumps(evs)


def measure_throughput(batch: int) -> dict:
    """now 固定，分项测 parse_events 与 engine.ingest。"""
    rng = np.random.default_rng(0)
    clock = FakeClock(BASE)
    eng = Engine(Config(), clock)
    bodies = [make_body(i, rng, batch, BASE) for i in range(N_TOTAL // batch)]
    eng.ingest(parse_events(bodies[0]))  # 预热
    t_parse = t_ing = 0.0
    for body in bodies[1:]:
        t0 = time.perf_counter(); b = parse_events(body); t_parse += time.perf_counter() - t0
        t0 = time.perf_counter(); eng.ingest(b); t_ing += time.perf_counter() - t0
    total = (len(bodies) - 1) * batch
    return {"batch": batch, "total": total,
            "parse_us": t_parse / total * 1e6, "ingest_us": t_ing / total * 1e6,
            "total_rate": total / (t_parse + t_ing) / 1000,
            "parse_rate": total / t_parse / 1000, "ingest_rate": total / t_ing / 1000}


def measure_finalize() -> dict:
    """逐秒推进时钟 + tick，记录每次 finalize（秒桶进树）的耗时。"""
    rng = np.random.default_rng(1)
    clock = FakeClock(BASE)
    eng = Engine(Config(), clock)
    finalize_us: list[float] = []
    orig = eng._on_finalize

    def timed(s, b):
        t0 = time.perf_counter(); orig(s, b); finalize_us.append((time.perf_counter() - t0) * 1e6)
    eng.ring.finalize = timed

    for t in range(1, FINALIZE_SECONDS + 1):
        now = BASE + t * 1000
        # 往当前秒写入一批数据（下一轮 60 秒后会定稿）
        eng.ingest(parse_events(make_body(t, rng, 1000, now)))
        clock.advance(1000)
        eng.tick()  # 把刚过 watermark 的秒定稿进树
    return {"median": statistics.median(finalize_us), "max": max(finalize_us),
            "count": len(finalize_us)}


def _make_full_body(seed: int, batch: int, s: int) -> bytes:
    """第 s 秒内的一批事件，URL 用非数字段（/api/item/xN），归一化后仍是多个不同 URL。
    make_body 的 /api/item/{u} 会被 normalize_url 归一成单个 /api/item/{id}，
    那样 url 维度的名单永远只有 1 个 key，测不出满载。"""
    rng = np.random.default_rng(seed)
    w = 1.0 / np.arange(1, N_URL + 1) ** 1.1
    idx = rng.choice(N_URL, size=batch, p=w / w.sum())
    urls = [f"/api/item/x{u}" for u in idx.tolist()]
    users = [f"u{u}" for u in rng.integers(0, N_USER, batch).tolist()]
    ts = [s * 1000 + int(x) for x in rng.integers(0, 1000, batch)]
    evs = [{
        "event_id": f"e{seed * 10000 + j}",
        "user_id": users[j],
        "url": urls[j],
        "ts": ts[j],
        "bytes": 1000 + (j % 100),
    } for j in range(batch)]
    return orjson.dumps(evs)


def _fill_second(eng, s: int, seed: int) -> None:
    """往第 s 秒写入 1500 条事件（长尾分布下足以让该秒两个维度的名单都写满 512/512）。"""
    eng.ingest(parse_events(_make_full_body(seed, 1500, s)))


def _full_window_engine() -> tuple[Engine, FakeClock, int]:
    """新引擎，窗口内每一秒（含当前秒）都已写满。返回 (eng, clock, 起始秒 n)。"""
    clock = FakeClock(BASE)
    eng = Engine(Config(), clock)
    n = BASE // 1000
    for off in range(61):  # 秒 n-60 .. n
        _fill_second(eng, n - 60 + off, 1000 + off)
    # 校验满载：窗口中间某一秒的两个维度名单都应达到容量上限
    mid = eng.ring.get(n - 30)
    assert mid is not None
    assert all(len(b.hh.top) == 512 for b in mid.dims), "窗口未写满，快照基准无意义"
    return eng, clock, n


def _timed(fn) -> float:
    t0 = time.perf_counter()
    fn()
    return (time.perf_counter() - t0) * 1e6


def _kth_ties(eng) -> list[int]:
    """每个维度"第 k 名点估计"的并列数量（k = snapshot_top_n）。

    把默认窗口的各时间片合并出每个 key 的点估计，取第 k 大的值，数出点估计等于
    它的 key 有多少个。这一数越大，"前 k"里要按 key 排序平局的候选越多。
    """
    now = eng.clock.now_ms()
    n = now // 1000
    r = split_range((n - 60) * 1000, n * 1000)
    k = eng.cfg.snapshot_top_n
    out = []
    for d in ALL_DIMS:
        parts = eng.collect_parts(d, r)
        H = np.concatenate([p.hashes for p in parts])
        full = [p.frac >= 1 for p in parts]
        P = np.concatenate([p.upper if f else p.frac * p.upper for p, f in zip(parts, full)])
        if not len(H):
            out.append(0)
            continue
        _, inv = np.unique(H, return_inverse=True)
        point = np.bincount(inv, weights=P)
        if len(point) < k:  # 唯一 key 不足 k 个，没有"第 k 名"
            out.append(int(len(point)))
            continue
        cut = float(np.partition(point, len(point) - k)[len(point) - k])
        out.append(int((point == cut).sum()))
    return out


def measure_snapshot(times: int = 20) -> dict:
    """满载窗口下测 refresh_snapshot 与完整 tick（含一次定稿）的耗时。

    两组测量各自用新引擎：先把 60 秒窗口写满，之后每轮推进时钟 1 秒、补满新的
    当前秒、再测一次，窗口保持满载。tick 组每轮时钟恰好推进 1 秒，advance 里
    正好定稿一秒，对应"完整 tick（含一次定稿）"。
    """
    eng, clock, n = _full_window_engine()
    ties = _kth_ties(eng)  # 初始满载窗口下，每维"第 k 名点估计"的并列数量
    eng.refresh_snapshot()  # 预热
    snap_us = []
    for t in range(times):
        clock.advance(1000)
        _fill_second(eng, n + 1 + t, 2000 + t)
        snap_us.append(_timed(eng.refresh_snapshot))

    eng, clock, n = _full_window_engine()
    eng.tick()  # 预热
    tick_us = []
    for t in range(times):
        clock.advance(1000)
        _fill_second(eng, n + 1 + t, 3000 + t)
        tick_us.append(_timed(eng.tick))  # advance（定稿 1 秒）+ refresh_snapshot
    return {"snap_median": statistics.median(snap_us), "snap_max": max(snap_us),
            "tick_median": statistics.median(tick_us), "tick_max": max(tick_us),
            "count": times, "ties": ties}


def main():
    for batch in BATCHES_TO_TEST:
        r = measure_throughput(batch)
        print(f"批大小 {r['batch']}：{r['total']} 条事件（长尾 URL / 大量不同用户）")
        print(f"  parse_events : {r['parse_us']:.2f} μs/条  ({r['parse_rate']:.0f} k 条/秒)")
        print(f"  engine.ingest: {r['ingest_us']:.2f} μs/条  ({r['ingest_rate']:.0f} k 条/秒)")
        print(f"  合计         : {(r['parse_us'] + r['ingest_us']):.2f} μs/条  ({r['total_rate']:.0f} k 条/秒)")
    f = measure_finalize()
    print(f"单次定稿进树（{f['count']} 次）：中位数 {f['median']:.0f} μs，最大 {f['max']:.0f} μs")
    s = measure_snapshot()
    print(f"满载窗口快照（{s['count']} 次）：")
    print(f"  refresh_snapshot: 中位数 {s['snap_median']:.0f} μs，最大 {s['snap_max']:.0f} μs")
    print(f"  完整 tick(含一次定稿): 中位数 {s['tick_median']:.0f} μs，最大 {s['tick_max']:.0f} μs")
    print(f"  第 k 名点估计的并列数量（url, user）: {s['ties'][0]}, {s['ties'][1]}")


if __name__ == "__main__":
    main()
