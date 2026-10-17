import orjson
import pytest

from flowlens.errors import BadRequest
from flowlens.model import EventBatch, normalize_url, parse_events

NOW = 1_735_689_600_000


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


def test_normalize_url_oversized_not_cached():
    # 超过 1024 字符的原始 URL：照常归一化，但不写入缓存
    import flowlens.model as m
    long_raw = "/seg/" + "m" * 1100  # 远超 1024
    assert normalize_url(long_raw) == normalize_url("/seg/" + "m" * 1100)  # 结果稳定
    assert long_raw not in m._norm_cache
    # 短 URL 仍会写缓存
    normalize_url("/short/x")
    assert "/short/x" in m._norm_cache


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
