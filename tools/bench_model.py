"""parse_events 吞吐基准：1000 条事件的解析耗时。

用法：python tools/bench_model.py

两组：
1. "缓存命中"：重复 URL 用完全相同的原始串（含 query）出现多次，归一化缓存命中，
   测的是最理想场景的每条事件开销。
2. "真实流量"（T10b）：原始 URL 由 flowgen 生成——带随机 query、路径 {id} 段、
   按比例带 scheme://host。真实流量里 query 几乎每次都不同，若归一化缓存以整条
   原始 URL 为键则几乎命中不了、每条都要走正则。归一化缓存改为"先剥 query/fragment
   再查"后，同一 key 的不同 query 变体命中同一缓存项。分别报告：
   - "修改前（整条原始串为键，真实流量下几乎不命中）"：用 _normalize_url（缓存未
     命中的正则路径，等价于旧缓存对随机 query 的每条耗时）；
   - "修改后（剥 query/fragment 后为键，命中）"：用 normalize_url（走缓存）。
   两者逐条归一化 N 条真实流量 URL 的耗时对比。
"""

import time

import numpy as np
import orjson

from flowlens.model import _normalize_url, normalize_url, parse_events

N = 1000       # 事件数
POOL = 200     # 缓存命中组：不同原始 URL 数（1000 条从 200 个里取，多数命中）
RUNS = 20      # 取多次的中位
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


def make_real_urls(n: int) -> list[str]:
    """flowgen 生成的真实流量原始 URL：带 {id} 路径段、随机 query、按比例 scheme://host。

    每条都不同（query 随机、{id} 替换随机），模拟真实流量里"同一路径、不同 query"
    的情形——归一化后落到相同的 key。
    """
    from flowgen import key_of, raw_url
    rng = np.random.default_rng(7)
    return [raw_url(key_of(i % 500), rng) for i in range(n)]


def _median_us(fn, runs: int = RUNS) -> float:
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1e6)
    return sorted(times)[runs // 2]


def main():
    # ---- 1. 缓存命中：1000 条、200 种重复 URL ----
    body = make_body(N, POOL)
    for _ in range(3):  # 预热，填满归一化缓存
        parse_events(body)
    b = parse_events(body)
    assert len(b) == N and b.invalid == 0, "基准数据应全部合法"
    med_hit = _median_us(lambda: parse_events(body))
    print(f"[缓存命中] {N} 条事件（{POOL} 种 URL）：每条约 {med_hit / N:.2f} μs，"
          f"约 {N * 1e6 / med_hit:,.0f} 条/秒")

    # ---- 2. 真实流量：原始 URL 由 flowgen 生成，带随机 query 与路径 {id} ----
    real = make_real_urls(N)
    # 归一化结果应全部等于其 key（flowgen 保证），且缓存能命中（同 key 共享）
    for raw in real:
        assert normalize_url(raw) == _normalize_url(raw)
    # 预热：把 500 个 key 都填进缓存
    for raw in real[:500]:
        normalize_url(raw)

    # "修改前"：整条原始串为键、真实流量下几乎不命中 ≈ 每条走 _normalize_url（正则路径）
    med_before = _median_us(lambda: [_normalize_url(r) for r in real])
    # "修改后"：剥 query/fragment 后为键、命中缓存 ≈ 每条走 normalize_url
    med_after = _median_us(lambda: [normalize_url(r) for r in real])
    print(f"[真实流量] {N} 条（flowgen，带随机 query 与路径 {{id}}）：")
    print(f"    修改前（整条原始串为键，几乎不命中）：每条约 {med_before / N:.2f} μs")
    print(f"    修改后（剥 query/fragment 后为键，命中）：每条约 {med_after / N:.2f} μs")


if __name__ == "__main__":
    main()
