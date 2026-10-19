"""parse_events / normalize_url 吞吐基准。

用法：python tools/bench_model.py

T11b 关注三件事：
1. "缓存命中"：重复 URL 命中归一化缓存，最理想场景每条开销。
2. "未命中路径"（T11b 首要优化点）：归一化缓存未命中时走的正则/替换路径。
   对比改前（旧 _normalize_url：每次都跑 scheme/多斜杠正则 + 逐字符判 ID）
   与改后（新 _normalize_url_impl：无 "//" 跳正则 + 预编译正则 fullmatch 判 ID）
   的每条耗时——这是"未命中 ~4.7μs → <1.5μs"目标的直接测量。
3. "命中率"（T11b 第 1 节第 3 项）：真实流量下归一化缓存的命中率，对比
   改前策略（一切路径都写缓存）与改后策略（发生 ID 替换的路径不写缓存）。

   改后策略的好处不在"提升命中率"（带 ID 的路径无论缓存与否都命中不了），
   而在**不污染缓存**：带 ID 的路径每条都不同、几乎不会再次出现，写进缓存只会
   占用槽位、在缓存满时把能命中的稳定路径挤出去。本基准用"清缓存→跑一遍→数命中"
   的口径报告两组命中率。
"""

import time

import numpy as np
import orjson

from flowlens.model import (_normalize_url_impl, normalize_url, parse_events,
                            _strip_query_fragment)

N = 1000       # 事件数
POOL = 200     # 缓存命中组：不同原始 URL 数（1000 条从 200 个里取，多数命中）
RUNS = 20      # 取多次的中位
NOW = 1_735_689_600_000

# ---- 改前的 _normalize_url（T11b 优化前的原始实现，作为"修改前"基准） ----
# 每次都跑 scheme/多斜杠两个正则，ID 段判断用逐字符 Python 循环（_is_hex）。
# 这里原样复刻（含缓存/驻留无关的归一化本体），只用于对比耗时。

import re as _re

_BASE_SCHEME_HOST = _re.compile(r"^(?:[A-Za-z][A-Za-z0-9+.\-]*:)?//[^/?#]*")
_BASE_MULTI_SLASH = _re.compile(r"/{2,}")
_BASE_HEX = frozenset("0123456789abcdefABCDEF")
_BASE_KEY_MAX = 128


def _base_is_hex(s: str) -> bool:
    return all(c in _BASE_HEX for c in s)


def _base_is_id_segment(seg: str) -> bool:
    if not seg.isascii():
        return False
    if seg.isdigit():
        return True
    if len(seg) == 36 and seg[8] == seg[13] == seg[18] == seg[23] == "-":
        return (_base_is_hex(seg[:8]) and _base_is_hex(seg[9:13])
                and _base_is_hex(seg[14:18]) and _base_is_hex(seg[19:23])
                and _base_is_hex(seg[24:]))
    return len(seg) >= 16 and _base_is_hex(seg)


def _base_normalize(raw: str) -> str:
    """改前 _normalize_url（输入是剥掉 query/fragment 后的 head，与改后口径一致）。"""
    s = _BASE_SCHEME_HOST.sub("", raw, count=1)
    s = _BASE_MULTI_SLASH.sub("/", s)
    if not s:
        s = "/"
    elif s[0] != "/":
        s = "/" + s
    if len(s) > 1:
        s = s.rstrip("/")
    parts = s.split("/")
    for i in range(1, len(parts)):
        if _base_is_id_segment(parts[i]):
            parts[i] = "{id}"
    s = "/".join(parts)
    b = s.encode("utf-8")
    if len(b) > _BASE_KEY_MAX:
        s = b[:_BASE_KEY_MAX].decode("utf-8", "ignore")
        if len(s) > 1 and s.endswith("/"):
            s = s.rstrip("/")
    return s


def make_body(n: int, pool: int) -> bytes:
    raw_urls = [f"/api/item/{u}/detail?ref=3&page=1" for u in range(pool)]
    events = [{
        "event_id": f"e{i}",
        "user_id": f"u{i % 300}",
        "url": raw_urls[i % pool],
        "ts": NOW + i,
        "bytes": 1000 + i,
    } for i in range(n)]
    return orjson.dumps(events)


def make_real_urls(n: int) -> list[str]:
    """flowgen 生成的真实流量原始 URL：带 {id} 路径段、随机 query、按比例 scheme://host。"""
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


def _hit_rate(urls: list[str], *, cache_id_paths: bool) -> tuple[float, int]:
    """清缓存后跑一遍 urls 的 normalize_url，按"调用前 head 已在缓存"数命中。

    cache_id_paths=False 模拟 T11b 改后策略（带 ID 不写缓存），True 模拟改前策略
    （一切路径都写缓存，monkeypatch 让 _normalize_url_impl 恒返回 replaced=False）。
    """
    import flowlens.model as m
    m.clear_norm_cache()
    orig_impl = m._normalize_url_impl

    def legacy_impl(head: str) -> tuple[str, bool]:
        out, _ = orig_impl(head)
        return out, False  # 恒"未替换" → 一切路径都写缓存（改前策略）

    if cache_id_paths:
        m._normalize_url_impl = legacy_impl
    try:
        hit = 0
        for raw in urls:
            head = _strip_query_fragment(raw)
            if head in m._norm_cache:
                hit += 1
            m.normalize_url(raw)
    finally:
        m._normalize_url_impl = orig_impl
        m.clear_norm_cache()
    return hit / len(urls), hit


def main():
    # ---- 1. 缓存命中：1000 条、200 种重复 URL ----
    body = make_body(N, POOL)
    for _ in range(3):  # 预热，填满归一化缓存
        parse_events(body)
    b = parse_events(body)
    assert len(b) == N and b.invalid == 0, "基准数据应全部合法"
    med_hit = _median_us(lambda: parse_events(body))
    print(f"[1 缓存命中] {N} 条（{POOL} 种 URL）：每条约 {med_hit / N:.2f} μs，"
          f"约 {N * 1e6 / med_hit:,.0f} 条/秒")

    # ---- 2. 未命中路径：改前 vs 改后（同一批 head，归一化缓存未命中时才走的路径） ----
    real = make_real_urls(N)
    heads = [_strip_query_fragment(r) for r in real]
    # 正确性自检：改前/改后归一化结果逐字节一致
    for h in heads[:100]:
        assert _base_normalize(h) == _normalize_url_impl(h)[0], h
    med_before = _median_us(lambda: [_base_normalize(h) for h in heads])
    med_after = _median_us(lambda: [_normalize_url_impl(h)[0] for h in heads])
    print(f"[2 未命中路径] {N} 条真实流量 URL（flowgen）：")
    print(f"    改前（旧 _normalize_url，逐字符判 ID）：每条约 {med_before / N:.2f} μs")
    print(f"    改后（新 _normalize_url_impl，正则 fullmatch）：每条约 {med_after / N:.2f} μs"
          f"（{med_before / max(med_after, 1e-9):.2f}×）")

    # ---- 3. 命中率：改前 vs 改后策略 ----
    rng = np.random.default_rng(11)
    from flowgen import key_of, raw_url
    long_urls = [raw_url(key_of(i % 500), rng) for i in range(4000)]
    rate_after, hit_after = _hit_rate(long_urls, cache_id_paths=False)
    rate_before, hit_before = _hit_rate(long_urls, cache_id_paths=True)
    print(f"[3 命中率] {len(long_urls)} 条 flowgen URL：")
    print(f"    改前策略（一切路径都写缓存）：{hit_before}/{len(long_urls)} = {rate_before:.1%}")
    print(f"    改后策略（带 ID 不写缓存）：{hit_after}/{len(long_urls)} = {rate_after:.1%}")
    print(f"    （带 ID 路径无论缓存与否都命中不了；改后策略的意义是不占缓存槽位、"
          f"避免在缓存满时挤出稳定路径）")


if __name__ == "__main__":
    main()
