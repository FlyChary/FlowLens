"""parse_events 吞吐基准：1000 条事件（URL 带 query、大量重复）的解析耗时。

用法：python tools/bench_model.py

重复的 URL 用完全相同的原始串（含 query）出现多次，这样归一化缓存命中，
测的是"缓存命中"场景的每条事件开销（见 TASKS.md T1 的性能要求）。
"""

import time

import orjson

from flowlens.model import parse_events

N = 1000       # 事件数
POOL = 200     # 不同原始 URL 数：1000 条从 200 个里取，多数命中缓存
RUNS = 20      # 取多次的最优/中位
NOW = 1_735_689_600_000


def make_body(n: int, pool: int) -> bytes:
    raw_urls = [f"/api/item/{u}/detail?ref=3&page=1" for u in range(pool)]
    events = [{
        "event_id": f"e{i}",
        "user_id": f"u{i % 300}",
        "url": raw_urls[i % pool],  # 重复出现 → 归一化缓存命中
        "ts": NOW + i,
        "bytes": 1000 + i,
    } for i in range(n)]
    return orjson.dumps(events)


def main():
    body = make_body(N, POOL)
    # 预热，填满归一化缓存
    for _ in range(3):
        b = parse_events(body)
    assert len(b) == N and b.invalid == 0, "基准数据应全部合法"

    times_us = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        parse_events(body)
        times_us.append((time.perf_counter() - t0) * 1e6)  # 微秒

    best = min(times_us)
    med = sorted(times_us)[RUNS // 2]
    # 每条耗时（μs）= 总耗时 / 条数；每秒条数 = 条数 / (总耗时秒) = N * 1e6 / 总耗时μs
    per_event = best / N
    per_sec = N * 1e6 / best
    print(f"{N} 条事件（{POOL} 种 URL，缓存命中）：每条约 {per_event:.2f} μs，"
          f"约 {per_sec:,.0f} 条/秒（中位总耗时 {med:.0f} μs，取 {RUNS} 次最优）")


if __name__ == "__main__":
    main()
