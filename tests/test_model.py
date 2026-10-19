import orjson
import pytest

from flowlens.errors import BadRequest
from flowlens.model import (EventBatch, normalize_url, parse_events, validate,
                            _normalize_url_impl, _strip_query_fragment)

NOW = 1_735_689_600_000


# ---- T11b 参考实现：修改前的 normalize_url / _normalize_url，原样复制 ----
#
# 用"改动前的实现作为参考答案"证明 T11b 的快路径（跳过正则 + 正则判 ID + 带 ID
# 不写缓存）对任何输入产出的**归一化字符串**与改前逐字节一致。下面几个函数是
# T11b 修改前 model.py 里 normalize_url / _normalize_url / _strip_query_fragment /
# _is_id_segment / _is_hex 的逐字拷贝（含 _HEX_DIGITS 常量与逐字符判断），独立于
# 被测代码，只依赖标准库。

_REF_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


def ref__is_hex(s: str) -> bool:
    return all(c in _REF_HEX_DIGITS for c in s)


def ref__is_id_segment(seg: str) -> bool:
    """纯数字、UUID（8-4-4-4-12 十六进制，不区分大小写）、长度不少于 16 的十六进制。"""
    if not seg.isascii():
        return False
    if seg.isdigit():
        return True
    if len(seg) == 36 and seg[8] == seg[13] == seg[18] == seg[23] == "-":
        return (ref__is_hex(seg[:8]) and ref__is_hex(seg[9:13]) and ref__is_hex(seg[14:18])
                and ref__is_hex(seg[19:23]) and ref__is_hex(seg[24:]))
    return len(seg) >= 16 and ref__is_hex(seg)


def ref__normalize_url(raw: str) -> str:
    import re as _re
    _SCHEME_HOST = _re.compile(r"^(?:[A-Za-z][A-Za-z0-9+.\-]*:)?//[^/?#]*")
    _MULTI_SLASH = _re.compile(r"/{2,}")
    KEY_MAX_BYTES = 128
    s = _SCHEME_HOST.sub("", raw, count=1)
    q, h = s.find("?"), s.find("#")
    if q == -1 or (h != -1 and h < q):
        q = h
    if q != -1:
        s = s[:q]
    s = _MULTI_SLASH.sub("/", s)
    if not s:
        s = "/"
    elif s[0] != "/":
        s = "/" + s
    if len(s) > 1:
        s = s.rstrip("/")
    parts = s.split("/")
    for i in range(1, len(parts)):
        if ref__is_id_segment(parts[i]):
            parts[i] = "{id}"
    s = "/".join(parts)
    b = s.encode("utf-8")
    if len(b) > KEY_MAX_BYTES:
        s = b[:KEY_MAX_BYTES].decode("utf-8", "ignore")
        if len(s) > 1 and s.endswith("/"):
            s = s.rstrip("/")
    return s


def ref_normalize_url(raw: str) -> str:
    """改前 normalize_url 的返回值口径（缓存不影响返回值内容，这里不做缓存/驻留）。"""
    return ref__normalize_url(_strip_query_fragment(raw))


# T11b 专门构造的边界：Unicode 数字、全角字符、恰好 15/16 位十六进制、大小写混合
# UUID、截断边界。这些是"新正则 fullmatch 判 ID"最容易与旧逐字符实现产生分歧的地方。
_T11B_EDGE = [
    # 恰好 16 位十六进制（命中）与恰好 15 位（不命中）
    "/a/0123456789abcdef",   # 16 位 → {id}
    "/a/0123456789abcde",    # 15 位 → 原样
    "/a/ffffffffffffffff",   # 16 位全 f → {id}
    "/a/ffffffffff",         # 10 位 → 原样
    # 大小写混合 UUID
    "/o/aB12cD34-eF56-7890-abcd-EF1234567890",
    # UUID 但末位是 g（不是合法 UUID，退到 37 位十六进制判断——含 '-' 与 g → 否）
    "/o/550e8400-e29b-41d4-a716-44665544000g",
    # 全角数字/字符（isascii 为假 → 不判 ID）
    "/a/０１２３４５６７８９０１２３",        # 全角数字 12 个
    "/a/０１２３４５６７８９０１２３４５６７",  # 全角 16 个
    "/a/中",
    # 其他语言数字字符（\d 会匹配、[0-9] 不会；须与旧 isdigit+isascii 一致 → 不判 ID）
    "/a/٠١٢٣٤٥٦٧٨٩٠١٢",   # 阿拉伯-印度数字 11 个
    "/a/۰۱۲۳۴۵۶۷۸۹۰۱۲۳۴۵",  # 扩展阿拉伯-印度数字 16 个
    "/a/०१२३४५६७८९०१२",    # 天城文数字 11 个
    # 数字段（纯 ASCII 数字，命中）
    "/order/123", "/order/0", "/order/000",
    # 15 位/16 位边界 + 前导 0
    "/a/000000000000000",    # 15 位
    "/a/0000000000000000",   # 16 位
    # 截断边界：结果恰好在 128 字节
    "/" + "m" * 126 + "/b",
    "/" + "m" * 127,
    "/" + "m" * 128,
    "/" + "m" * 128 + "/x",
    # 中文长路径截断（多字节字符中间）
    "/" + "中" * 100,
    "/" + "a" * 63 + "/" + "中" * 20,
    # 空 / 单字符 / 边界斜杠
    "", "/", "//", "///", "///api//users", "/a///b", "api/users",
    "/a/", "/a/b/", "//cdn.example.com/a.js",
    # 带 scheme/host
    "https://example.com/api/v1/users", "http://host:8080/x",
    "HTTPS://EXAMPLE.com/Path", "https://h/p?q=1#f?2", "https://host?x=1",
    # query/fragment 各种位置
    "?x=1", "#f", "/a#f?q=1", "/a/b?x=1&y=2", "/a/b#f",
    # 单段（无斜杠，不判 ID——第 0 段是根）
    "123", "0123456789abcdef", "550e8400-e29b-41d4-a716-446655440000",
    # 特殊字符
    "/a/{id}", "/a/{}", "/a/{}", "/a/-", "/a/--", "/a/...",
    "/a/x-y", "/a/a-b-c-d-e-f-g-h-i-j-k-l",
]


def _ev(eid="e1", uid="u1", url="/api/a", ts=NOW, nbytes=100):
    return {"event_id": eid, "user_id": uid, "url": url, "ts": ts, "bytes": nbytes}


# ---- normalize_url：表驱动 ----

@pytest.mark.parametrize("raw, want", [
    # query
    ("/api/users?x=1", "/api/users"),
    ("/a/b?x=1&y=2", "/a/b"),
    # fragment
    ("/api/users#frag", "/api/users"),
    # 两者同时存在
    ("/api/users?x=1#frag", "/api/users"),
    ("/a#f?q=1", "/a"),  # # 在 ? 前面，取更早的
    # 连续斜杠
    ("///api//users", "/api/users"),
    ("/a///b", "/a/b"),
    # 末尾斜杠
    ("/api/users/", "/api/users"),
    ("/a/b/c/", "/a/b/c"),
    # 根路径
    ("/", "/"),
    # 带 scheme 和主机
    ("https://example.com/api/v1/users", "/api/v1/users"),
    ("http://host:8080/x", "/x"),
    ("HTTPS://EXAMPLE.com/Path", "/Path"),  # 不改大小写
    # 协议相对（// 开头）视为带主机名
    ("//cdn.example.com/a.js", "/a.js"),
    ("//host:8080/x", "/x"),
    # 截断到 128 字节后末尾是 /（第 128 字节正好是 /），再去掉这个 /
    ("/" + "m" * 126 + "/b", "/" + "m" * 126),
    # 纯数字段
    ("/order/123", "/order/{id}"),
    ("/a/0/b/007", "/a/{id}/b/{id}"),
    # UUID（不区分大小写）
    ("/o/550e8400-e29b-41d4-a716-446655440000", "/o/{id}"),
    ("/o/550E8400-E29B-41D4-A716-446655440000", "/o/{id}"),
    # 长十六进制段（>=16）替换
    ("/a/ab12cd34ef56ab78", "/a/{id}"),
    ("/a/0000000000000000", "/a/{id}"),
    # 短十六进制段（<16）不替换
    ("/a/ab12cd34ef56ab7", "/a/ab12cd34ef56ab7"),
    ("/a/fffffffffffff", "/a/fffffffffffff"),
    # 含非数字字母的段不替换
    ("/a/ab12cd34ef56abz8", "/a/ab12cd34ef56abz8"),
    # 中文路径（不截断、不动）
    ("/搜索/用户", "/搜索/用户"),
    # 截断落在多字节字符中间：1 + 100×3 = 301 字节 → 截到 127 字节（/ + 42 个"中"）
    ("/" + "中" * 100, "/" + "中" * 42),
    # 空字符串 → /
    ("", "/"),
    # 不带斜杠开头时补一个
    ("api/users", "/api/users"),
])
def test_normalize_url(raw, want):
    assert normalize_url(raw) == want


def test_truncation_kept_under_limit_and_not_split():
    raw = "/" + "中" * 100  # 301 字节，128 字节边界正好切在第 43 个"中"（3 字节）的中间
    out = normalize_url(raw)
    # 长度仍在 128 字节内
    assert len(out.encode("utf-8")) <= 128
    # 截断没有把半截字节解码成 U+FFFD 替换字符，即没有截在多字节字符中间
    assert "�" not in out
    # 结果必须是原串的合法前缀：若把半截字节换成替换字符就不再是前缀
    assert raw.startswith(out)
    # 边界正好切到第 43 个字中间，最多保留到第 42 个完整"中"
    assert out == "/" + "中" * 42


def test_normalize_url_cache_is_shared():
    # 同一个 raw 反复归一化，结果稳定且命中缓存
    a = normalize_url("/a/b?x=1")
    b = normalize_url("/a/b?x=1")
    assert a == b == "/a/b"


def test_normalize_url_interns_result():
    """C3：归一化结果做驻留——路径带不同 ID 的原始 URL 归一化后内容相同，
    应返回同一个字符串对象（不走归一化缓存：两个 raw 的缓存键不同）。"""
    import flowlens.model as m
    # 清掉 norm 缓存与 url 驻留表，确保这条断言靠"驻留"而不是"norm 缓存命中"
    m.clear_norm_cache()
    if m.url_intern is not None:
        m.url_intern.clear()
    a = normalize_url("/order/1")   # → /order/{id}
    b = normalize_url("/order/2")   # → /order/{id}（不同的缓存键 /order/2）
    assert a == b == "/order/{id}"
    assert a is b, "归一化结果应被驻留成同一对象"


def test_normalize_url_same_result_before_and_after_cache_change():
    """T10b：归一化缓存改"先剥 query/fragment 再查"后，结果必须与修改前逐字节一致。

    修改前的 normalize_url 在缓存未命中时返回 _normalize_url(raw)；_normalize_url
    本身没有被改（这次只改了 normalize_url 用什么当缓存键），所以 _normalize_url(raw)
    就是修改前的归一化结果。对大量 raw（含下表全部既有用例 + flowgen 生成的带
    scheme/host/query/fragment 与 {id} 段的变化）逐一断言两者相等。
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
    import flowgen

    # 既有的全部表驱动用例（左值）——确保这些边界形式结果不变
    existing = [
        "/api/users?x=1", "/a/b?x=1&y=2", "/api/users#frag", "/api/users?x=1#frag",
        "/a#f?q=1", "///api//users", "/a///b", "/api/users/", "/a/b/c/", "/",
        "https://example.com/api/v1/users", "http://host:8080/x",
        "HTTPS://EXAMPLE.com/Path", "//cdn.example.com/a.js", "//host:8080/x",
        "/" + "m" * 126 + "/b", "/order/123", "/a/0/b/007",
        "/o/550e8400-e29b-41d4-a716-446655440000",
        "/o/550E8400-E29B-41D4-A716-446655440000", "/a/ab12cd34ef56ab78",
        "/a/0000000000000000", "/a/ab12cd34ef56ab7", "/a/fffffffffffff",
        "/a/ab12cd34ef56abz8", "/搜索/用户", "/" + "中" * 100, "", "api/users",
        # 额外边界：query 紧跟 host（无路径）、只有 query、只有 fragment
        "https://host?x=1", "?x=1", "#f", "//host#f?q=2", "https://h/p?q=1#f?2",
    ]
    # T11b：对照参考用改前的 ref__normalize_url（_normalize_url 已改名 _normalize_url_impl）
    assert all(normalize_url(r) == ref__normalize_url(_strip_query_fragment(r))
               for r in existing)

    # flowgen 生成的真实流量：带 {id} 模板、scheme://host、query、fragment 的各种组合
    rng = __import__("numpy").random.default_rng(0)
    for i in range(2000):
        key = flowgen.key_of(i)
        raw = flowgen.raw_url(key, rng)
        assert normalize_url(raw) == ref__normalize_url(_strip_query_fragment(raw)) == key, raw


def test_normalize_url_oversized_not_cached():
    # 超过 1024 字符的原始 URL：照常归一化，但不写入缓存
    import flowlens.model as m
    long_raw = "/seg/" + "m" * 1100  # 远超 1024
    assert normalize_url(long_raw) == normalize_url("/seg/" + "m" * 1100)  # 结果稳定
    assert long_raw not in m._norm_cache
    # 短 URL 仍会写缓存
    normalize_url("/short/x")
    assert "/short/x" in m._norm_cache


# ---- T11b：归一化快路径的等价性（改前实现作为参考答案） ----

def test_normalize_equivalence_edge_cases():
    """T11b：新 _normalize_url_impl 对全部表驱动用例 + 专门边界，与改前参考实现逐字节一致。"""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
    import flowgen

    # 既有的全部表驱动用例的左值（raw）
    table_raws = [
        "/api/users?x=1", "/a/b?x=1&y=2", "/api/users#frag", "/api/users?x=1#frag",
        "/a#f?q=1", "///api//users", "/a///b", "/api/users/", "/a/b/c/", "/",
        "https://example.com/api/v1/users", "http://host:8080/x",
        "HTTPS://EXAMPLE.com/Path", "//cdn.example.com/a.js", "//host:8080/x",
        "/" + "m" * 126 + "/b", "/order/123", "/a/0/b/007",
        "/o/550e8400-e29b-41d4-a716-446655440000",
        "/o/550E8400-E29B-41D4-A716-446655440000", "/a/ab12cd34ef56ab78",
        "/a/0000000000000000", "/a/ab12cd34ef56ab7", "/a/fffffffffffff",
        "/a/ab12cd34ef56abz8", "/搜索/用户", "/" + "中" * 100, "", "api/users",
    ]
    cases = table_raws + _T11B_EDGE
    assert cases  # 防退化
    for raw in cases:
        new, _ = _normalize_url_impl(_strip_query_fragment(raw))
        assert new == ref__normalize_url(_strip_query_fragment(raw)), raw
        # 经公开入口（含缓存/驻留）也必须与参考一致（缓存不改变返回值内容）
        assert normalize_url(raw) == ref_normalize_url(raw), raw


def test_normalize_equivalence_flowgen_random_urls():
    """T11b：flowgen 生成的大量随机 URL（带 scheme/host/query/fragment 与 {id} 段），
    新实现与改前参考实现逐一比较，结果必须完全相同。"""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
    import flowgen
    rng = __import__("numpy").random.default_rng(1)
    for i in range(5000):
        key = flowgen.key_of(i)
        raw = flowgen.raw_url(key, rng)
        head = _strip_query_fragment(raw)
        new, _ = _normalize_url_impl(head)
        want = ref__normalize_url(head)
        assert new == want == key, (raw, new, want)


def test_normalize_fastpath_skips_regex_when_no_slashes():
    """T11b 第 1 项的行为证据：无 "//" 的路径走快路径（不调 scheme/多斜杠正则）。
    用 monkeypatch 让两个正则抛错，快路径不该碰它们。"""
    import flowlens.model as m
    import pytest as _pytest

    class _Boom:
        def sub(self, *a, **k):
            raise AssertionError("无 // 时不应调用该正则")

    saved = (m._SCHEME_HOST, m._MULTI_SLASH)
    m._SCHEME_HOST, m._MULTI_SLASH = _Boom(), _Boom()
    try:
        # 无 "//"：快路径，不碰正则
        out, replaced = m._normalize_url_impl("/api/item/x0")
        assert out == "/api/item/x0" and replaced is False
        out, replaced = m._normalize_url_impl("/api/order/123")
        assert out == "/api/order/{id}" and replaced is True
    finally:
        m._SCHEME_HOST, m._MULTI_SLASH = saved
    # 有 "//"：仍走正则（结果正确）
    out, _ = m._normalize_url_impl("https://h//x")
    assert out == "/x"


def test_normalize_id_paths_not_cached():
    """T11b 第 3 项：发生 ID 段替换的路径不写缓存；稳定路径照常写。"""
    import flowlens.model as m
    m.clear_norm_cache()
    try:
        assert m.normalize_url("/order/1") == "/order/{id}"
        assert m.normalize_url("/order/2") == "/order/{id}"
        # 带 ID 的路径没有进入缓存（/order/1、/order/2 都不是缓存键）
        assert "/order/1" not in m._norm_cache
        assert "/order/2" not in m._norm_cache
        # 稳定路径进缓存
        assert m.normalize_url("/stable/path") == "/stable/path"
        assert "/stable/path" in m._norm_cache
    finally:
        m.clear_norm_cache()


# ---- parse_events：正常批 ----

def test_parse_normal_batch():
    body = orjson.dumps([
        _ev(eid="e1", uid="u1", url="/a/x?y=1", ts=NOW, nbytes=100),
        _ev(eid="e2", uid="u2", url="/b", ts=NOW + 1, nbytes=0),
    ])
    b = parse_events(body)
    assert isinstance(b, EventBatch)
    assert len(b) == 2
    assert b.invalid == 0
    assert b.ids == ["e1", "e2"]
    assert b.users == ["u1", "u2"]
    assert b.urls == ["/a/x", "/b"]  # url 已归一化（query 去掉）
    assert b.ts.tolist() == [NOW, NOW + 1]
    assert b.nbytes.tolist() == [100, 0]
    assert b.ts.dtype == "int64" and b.nbytes.dtype == "int64"


def test_parse_empty_array():
    b = parse_events(b"[]")
    assert len(b) == 0 and b.invalid == 0
    assert b.ids == [] and b.ts.size == 0


# ---- parse_events：整体不合法 → BadRequest ----

@pytest.mark.parametrize("body", [
    b'{"a": 1}',       # 顶层是对象
    b'"str"',          # 顶层是字符串
    b"123",            # 顶层是数字
    b"null",           # 顶层是 null
    b"{not json",      # 非法 JSON
    b"",               # 空输入
])
def test_parse_rejects_non_array(body):
    with pytest.raises(BadRequest):
        parse_events(body)


# ---- parse_events：单条不合法只计数，不影响其他 ----

def test_element_not_object():
    body = orjson.dumps([_ev(), "not-obj", 5, None, [_ev()]])
    b = parse_events(body)
    assert len(b) == 1 and b.invalid == 4


@pytest.mark.parametrize("field, bad", [
    ("event_id", 123),        # 类型错误
    ("user_id", None),
    ("url", 42),
    ("ts", "123"),            # 类型错误
    ("bytes", 1.5),           # float 不算整数
])
def test_wrong_field_type_rejected(field, bad):
    e = _ev()
    e[field] = bad
    b = parse_events(orjson.dumps([e]))
    assert len(b) == 0 and b.invalid == 1


@pytest.mark.parametrize("field", ["event_id", "user_id", "url", "ts", "bytes"])
def test_missing_field_rejected(field):
    e = _ev()
    del e[field]
    b = parse_events(orjson.dumps([e]))
    assert len(b) == 0 and b.invalid == 1


def test_empty_string_field_rejected():
    for f in ("event_id", "user_id", "url"):
        e = _ev()
        e[f] = ""
        assert parse_events(orjson.dumps([e])).invalid == 1


@pytest.mark.parametrize("field, val", [
    ("ts", True),    # bool 冒充整数
    ("ts", False),
    ("bytes", True),
    ("bytes", False),
])
def test_bool_not_integer(field, val):
    e = _ev()
    e[field] = val
    b = parse_events(orjson.dumps([e]))
    assert len(b) == 0 and b.invalid == 1


def test_ts_zero_rejected():
    b = parse_events(orjson.dumps([_ev(ts=0)]))
    assert len(b) == 0 and b.invalid == 1


def test_negative_ts_rejected():
    b = parse_events(orjson.dumps([_ev(ts=-1)]))
    assert len(b) == 0 and b.invalid == 1


def test_ts_beyond_int64_single_invalid():
    # ts = 2^63：能解析，但超出 int64 上限 2^63−1 → 这一条 invalid，同批其他保留
    too_big = 1 << 63
    body = orjson.dumps([_ev(eid="ok", ts=NOW), _ev(eid="big", ts=too_big)])
    b = parse_events(body)
    assert len(b) == 1 and b.invalid == 1
    assert b.ids == ["ok"]
    assert b.ts.tolist() == [NOW]  # 没有抛 OverflowError


def test_int_beyond_uint64_single_invalid():
    # 某整数超过 2^64−1：orjson 把它转成 float，与普通小数无法区分，
    # 按"非整数 = 单条非法"处理，不影响同批其他事件（不抛异常）。
    # orjson.dumps 造不出这种输入（序列化时就 TypeError），所以手工拼原始字节。
    big = str(1 << 64)  # "18446744073709551616"

    def raw(ts: str, nbytes: str) -> str:
        return '{"event_id":"e1","user_id":"u1","url":"/api/a","ts":%s,"bytes":%s}' % (ts, nbytes)

    # ts 超过 2^64−1：单条非法，同批另一条保留
    body_ts = ("[" + raw(big, "100") + "," + raw("1735689600000", "200") + "]").encode()
    b = parse_events(body_ts)  # 不抛
    assert len(b) == 1 and b.invalid == 1
    assert b.ids == ["e1"]
    assert b.nbytes.tolist() == [200]
    # bytes 字段同样：单条非法
    body_b = ("[" + raw("1735689600000", big) + "," + raw("1735689600001", "300") + "]").encode()
    b = parse_events(body_b)
    assert len(b) == 1 and b.invalid == 1
    assert b.nbytes.tolist() == [300]


def test_negative_bytes_rejected():
    b = parse_events(orjson.dumps([_ev(nbytes=-1)]))
    assert len(b) == 0 and b.invalid == 1


def test_overlong_event_id_rejected():
    b = parse_events(orjson.dumps([_ev(eid="x" * 257)]))
    assert len(b) == 0 and b.invalid == 1


def test_event_id_at_limit_ok():
    b = parse_events(orjson.dumps([_ev(eid="x" * 256)]))
    assert len(b) == 1 and b.invalid == 0


def test_extra_fields_ignored():
    e = _ev()
    e["extra"] = "keep"
    e["foo"] = 123
    b = parse_events(orjson.dumps([e]))
    assert len(b) == 1 and b.invalid == 0


def test_invalid_count_and_valid_kept():
    # 混合批：2 合法 + 3 非法
    body = orjson.dumps([
        _ev(eid="ok1"),
        _ev(eid="ok2", ts=NOW + 1),
        _ev(ts=0),                       # ts=0
        {"event_id": "x", "ts": NOW},    # 缺 user_id/url/bytes
        "not-obj",
    ])
    b = parse_events(body)
    assert len(b) == 2
    assert b.invalid == 3
    assert b.ids == ["ok1", "ok2"]


def test_user_truncated_to_128_bytes():
    b = parse_events(orjson.dumps([_ev(uid="a" * 200)]))
    assert len(b) == 1
    assert b.users[0] == "a" * 128


def test_user_multibyte_not_split():
    b = parse_events(orjson.dumps([_ev(uid="中" * 100)]))  # 300 字节 → 42 个
    assert b.users[0] == "中" * 42


# ---- T11b：validate 列式快路径的等价性（改前实现作为参考答案） ----

import numpy as _np

_REF_EVENT_ID_MAX_BYTES = 256
_REF_KEY_MAX_BYTES = 128
_REF_INT64_MAX = (1 << 63) - 1


def ref__is_int(x) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def ref__truncate_utf8(s: str) -> str:
    if s.isascii():
        return s[:_REF_KEY_MAX_BYTES]
    b = s.encode("utf-8")
    if len(b) > _REF_KEY_MAX_BYTES:
        s = b[:_REF_KEY_MAX_BYTES].decode("utf-8", "ignore")
    return s


def ref_validate(objs: list) -> EventBatch:
    """T11b 修改前 validate 的逐字拷贝：逐条校验与归一化，单条不合法计入 invalid 剔除。

    归一化用 ref_normalize_url（改前口径），与 model.validate 的 normalize_url 在
    返回值内容上一致（缓存/驻留不改变字符串内容），故可逐列对比。
    """
    ids: list[str] = []
    users: list[str] = []
    urls: list[str] = []
    ts: list[int] = []
    nbytes: list[int] = []
    invalid = 0
    for obj in objs:
        if not isinstance(obj, dict):
            invalid += 1
            continue
        eid = obj.get("event_id")
        uid = obj.get("user_id")
        url = obj.get("url")
        t = obj.get("ts")
        b = obj.get("bytes")
        if not isinstance(eid, str) or not eid:
            invalid += 1
            continue
        if len(eid) > _REF_EVENT_ID_MAX_BYTES or (
                not eid.isascii() and len(eid.encode("utf-8")) > _REF_EVENT_ID_MAX_BYTES):
            invalid += 1
            continue
        if not isinstance(uid, str) or not uid:
            invalid += 1
            continue
        if not isinstance(url, str) or not url:
            invalid += 1
            continue
        if not ref__is_int(t) or not ref__is_int(b):
            invalid += 1
            continue
        if t <= 0 or t > _REF_INT64_MAX or b < 0 or b > _REF_INT64_MAX:
            invalid += 1
            continue
        ids.append(eid)
        users.append(ref__truncate_utf8(uid))
        urls.append(ref_normalize_url(url))
        ts.append(t)
        nbytes.append(b)
    return EventBatch(
        ids, users, urls,
        _np.array(ts, dtype=_np.int64), _np.array(nbytes, dtype=_np.int64), invalid,
    )


def _assert_same_batch(a: EventBatch, b: EventBatch) -> None:
    """逐列比较两个 EventBatch 完全一致（ids/users/urls/ts/nbytes/invalid）。"""
    assert a.invalid == b.invalid, (a.invalid, b.invalid)
    assert len(a) == len(b)
    assert a.ids == b.ids
    assert a.users == b.users
    assert a.urls == b.urls
    assert a.ts.tolist() == b.ts.tolist()
    assert a.nbytes.tolist() == b.nbytes.tolist()


def test_validate_equivalence_all_valid():
    """T11b：全部合法（走列式快路径）时与改前参考实现逐列一致。"""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
    import flowgen
    rng = __import__("numpy").random.default_rng(3)
    objs = []
    for i in range(3000):
        key = flowgen.key_of(i)
        objs.append({
            "event_id": f"e{i}",
            "user_id": f"u{i % 500}",
            "url": flowgen.raw_url(key, rng),
            "ts": NOW + i,
            "bytes": (i * 13) % 200000,
        })
    _assert_same_batch(validate(objs), ref_validate(objs))
    # 含超长 ASCII user_id（快路径里 len<=128 跳过截断的分支）
    objs2 = [objs[0]] + [{
        "event_id": "e_long", "user_id": "a" * 200, "url": "/x",
        "ts": NOW, "bytes": 1}]
    _assert_same_batch(validate(objs2), ref_validate(objs2))


def test_validate_equivalence_mixed_invalid():
    """T11b：各种非法类型混入（走逐条慢路径）时与改前参考实现逐列一致。"""
    base = _ev()
    cases = [
        list(_ev() for _ in range(3)),                 # 全合法
        [None, 5, "s", 1.5, [1], "x"],                 # 非 dict
        [_ev(eid=123), _ev(eid=None), _ev(eid="")],    # event_id 类型/空
        [_ev(uid=1), _ev(uid=None), _ev(uid="")],      # user_id
        [_ev(url=1), _ev(url=None), _ev(url="")],      # url
        [_ev(ts="123"), _ev(ts=1.5), _ev(ts=None)],    # ts 类型
        [_ev(nbytes=1.5), _ev(nbytes=None)],           # bytes 类型
        [_ev(ts=True), _ev(ts=False), _ev(nbytes=True)],  # bool 冒充整数
        [_ev(ts=0), _ev(ts=-1), _ev(nbytes=-1)],       # 越界
        [_ev(ts=1 << 63), _ev(ts=(1 << 63) - 1)],     # int64 上界
        [_ev(eid="x" * 257), _ev(eid="x" * 256)],     # event_id 长度
        [_ev(uid="a" * 200), _ev(uid="中" * 100)],    # user_id 截断（ASCII/多字节）
        [_ev(eid="中" * 257), _ev(eid="中" * 86)],    # 非 ASCII event_id（字节长度）
        [],                                            # 空列表
    ]
    for objs in cases:
        _assert_same_batch(validate(objs), ref_validate(objs))


def test_validate_equivalence_float_from_oversized_int():
    """T11b：orjson 把 >=2^64 的整数转成 float，快/慢路径都按"非整数"单条非法处理。"""
    from flowlens.model import load_array
    big = str(1 << 64)

    def raw(ts, nb):
        return '{"event_id":"e1","user_id":"u1","url":"/api/a","ts":%s,"bytes":%s}' % (ts, nb)
    body = ("[" + raw(big, "100") + "," + raw(str(NOW), "200") + "]").encode()
    objs = load_array(body)  # ts 已被 orjson 转成 float
    _assert_same_batch(validate(objs), ref_validate(objs))


def test_validate_fast_vs_slow_path_agree_on_mixed():
    """T11b：同一批里混入一条非法 → 走慢路径；只留合法的 → 走快路径；两者一致。
    证明"整块合法走快路径、否则走慢路径"的切换不改变结果。"""
    objs = [_ev(eid=f"e{i}", ts=NOW + i, url="/a/x?q=%d" % i) for i in range(50)]
    _assert_same_batch(validate(objs), ref_validate(objs))
    # 注入一条非法（慢路径），剔除它后剩下的与快路径逐列一致
    objs_bad = objs[:25] + [_ev(ts=0)] + objs[25:]
    slow = validate(objs_bad)
    assert slow.invalid == 1
    _assert_same_batch(slow, ref_validate(objs_bad))
    # 慢路径剔除非法后的有效列 == 快路径（全合法批）的有效列
    fast = validate(objs)
    assert fast.invalid == 0
    assert slow.ids == fast.ids and slow.urls == fast.urls
    assert slow.ts.tolist() == fast.ts.tolist()
