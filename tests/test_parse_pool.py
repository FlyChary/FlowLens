"""P1 起保留的纯计算/纯数据测试：parse_body、parse_chunks、ParsedBatch。

P2d-1 起进程池（ParsePool / FLOWLENS_PARSE_WORKERS / resolve_worker_count / 哈希自检 /
真实 spawn 进程池用例）已删除——解析被前端进程取代（见 docs/lanes/P2-notes.md P2d-1）。
本文件保留 parse_body（前端进程的执行体）、parse_chunks/concat_parsed（P2d-2 分块
解析，等价性对照 parse_body）与 ParsedBatch 的纯函数测试。
"""

import pickle
import random

import orjson
import pytest

from flowlens.errors import BadRequest
from flowlens.hashing import key_hashes
from flowlens.model import load_array, validate
from flowlens.parse_pool import (FRONT_PARSE_CHUNK, ParsedBatch, concat_parsed,
                                 parse_body, parse_chunks)

NOW = 1_735_689_600_000


def _evs(n: int, seed: int = 0) -> list:
    r = random.Random(seed)
    return [{"event_id": f"e{seed * 100000 + i}", "user_id": f"u{r.randrange(50)}",
             "url": f"/api/item/x{r.randrange(30)}?q={i}",
             "ts": NOW - 1000 - (i % 900), "bytes": 10 + (i % 40)} for i in range(n)]


# ---- parse_body：纯计算部分（前端进程的执行体，本进程可直接调，不碰统计状态）----

def test_parse_body_matches_validate():
    body = orjson.dumps(_evs(1000))
    b = validate(load_array(body))
    pb = parse_body(body)
    assert pb.invalid == b.invalid
    assert (pb.ts == b.ts).all() and (pb.nbytes == b.nbytes).all()
    assert (pb.h_id == key_hashes(b.ids)).all()
    assert (pb.h_url == key_hashes(b.urls)).all()
    assert (pb.h_user == key_hashes(b.users)).all()
    assert pb.urls == b.urls and pb.users == b.users


def test_parse_body_empty_and_all_invalid():
    assert len(parse_body(b"[]")) == 0 and parse_body(b"[]").invalid == 0
    # 全非法：0 条有效、invalid 计数正确，不抛
    pb = parse_body(orjson.dumps([5, "str", None]))
    assert len(pb) == 0 and pb.invalid == 3


def test_parse_body_bad_request_raised():
    # 整个请求体没有任何合法 JSON → BadRequest 原样抛出（前端错误中间件转成 400）
    with pytest.raises(BadRequest):
        parse_body(b"not-json-at-all")


def test_parsedbatch_pickle_roundtrip():
    b = validate(load_array(orjson.dumps(_evs(500))))
    pb = ParsedBatch(b.ts, b.nbytes, key_hashes(b.ids), key_hashes(b.urls),
                     key_hashes(b.users), b.urls, b.users, b.invalid)
    pb2 = pickle.loads(pickle.dumps(pb))
    assert (pb2.ts == pb.ts).all() and (pb2.nbytes == pb.nbytes).all()
    assert (pb2.h_id == pb.h_id).all() and (pb2.h_url == pb.h_url).all()
    assert (pb2.h_user == pb.h_user).all() and pb2.urls == pb.urls and pb2.users == pb.users
    assert pb2.invalid == pb.invalid


# ---- parse_chunks / concat_parsed（P2d-2 四）：分块结果与 parse_body 逐项相同 ----

def _ndjson_body(n: int, seed: int) -> bytes:
    """NDJSON：每行一条（含空行、以及掺入的非法行——占位 None 由 validate 计 invalid）。"""
    r = random.Random(seed)
    lines = []
    for i in range(n):
        if i % 17 == 5:
            lines.append("this is not json")   # 非法行 → invalid
        elif i % 23 == 11:
            lines.append("")                   # 空行 → 跳过
        else:
            lines.append(orjson.dumps({"event_id": f"n{seed}-{i}",
                                       "user_id": f"u{r.randrange(30)}",
                                       "url": f"/n/{r.randrange(20)}?q={i}",
                                       "ts": NOW - 1000 - (i % 900),
                                       "bytes": 5 + (i % 40)}).decode())
    return ("\n".join(lines)).encode("utf-8")


def _all_invalid_body() -> bytes:
    """整批元素全部非法（非 dict / 缺字段 / 超范围）：valid 0 条、invalid = 条数。"""
    evs = [5, "str", None,
           {"user_id": "u"}, {"event_id": "e", "url": "/a"},   # 各缺字段
           {"event_id": "e", "user_id": "u", "url": "/a", "ts": -1, "bytes": 10},
           {"event_id": "e", "user_id": "u", "url": "/a", "ts": 10, "bytes": -5}]
    return orjson.dumps(evs)


def _single_object_body() -> bytes:
    """单个 JSON 对象（既不是数组、也不是多行）：单行 NDJSON 文档。"""
    return orjson.dumps({"event_id": "one", "user_id": "u1", "url": "/one",
                         "ts": NOW - 100, "bytes": 42})


def _bodies():
    """长尾、全随机、含各种非法事件、空数组、NDJSON、单个对象。"""
    return {
        "longtail": orjson.dumps(_evs(4500, seed=3)),
        "random": _random_body(3700, seed=4),
        "with_invalid": _all_invalid_body(),
        "empty": b"[]",
        "ndjson": _ndjson_body(2100, seed=5),
        "single_object": _single_object_body(),
    }


def _random_body(n: int, seed: int) -> bytes:
    r = random.Random(seed)
    evs = [{"event_id": f"r{seed}-{r.getrandbits(40)}",
            "user_id": f"ru{r.getrandbits(20)}",
            "url": f"/r/x{r.randrange(100000)}",
            "ts": NOW - 1000 - (i % 900), "bytes": 3 + (i % 60)} for i in range(n)]
    return orjson.dumps(evs)


def _pb_equal(a: ParsedBatch, b: ParsedBatch) -> None:
    assert (a.ts == b.ts).all() and (a.nbytes == b.nbytes).all()
    assert (a.h_id == b.h_id).all() and (a.h_url == b.h_url).all()
    assert (a.h_user == b.h_user).all()
    assert a.urls == b.urls and a.users == b.users and a.invalid == b.invalid


@pytest.mark.parametrize("name", ["longtail", "random", "with_invalid", "empty",
                                  "ndjson", "single_object"])
@pytest.mark.parametrize("chunk", [1, 7, 1000, 10000])
def test_parse_chunks_equals_parse_body(name, chunk):
    """concat_parsed(list(parse_chunks(raw, c))) 与 parse_body(raw) 逐字段相同，
    c 取 1 / 7 / 1000 / 10000（覆盖比批次大、比批次小的各种切分）。"""
    raw = _bodies()[name]
    expected = parse_body(raw)
    parts = list(parse_chunks(raw, chunk))
    assert all(len(p) == 0 or (len(p) <= chunk) for p in parts)
    got = concat_parsed(parts)
    assert len(got) == len(expected)
    _pb_equal(got, expected)


def test_parse_chunks_default_chunk_is_1000():
    raw = orjson.dumps(_evs(2500, seed=9))
    parts = list(parse_chunks(raw))   # 默认 FRONT_PARSE_CHUNK
    assert len(parts) == 3 and [len(p) for p in parts] == [1000, 1000, 500]
    assert FRONT_PARSE_CHUNK == 1000
    _pb_equal(concat_parsed(parts), parse_body(raw))


def test_parse_chunks_empty_yields_one_empty_part():
    """空数组仍产出一个空块（range 的 max(len, 1)），拼接结果与 parse_body 相同。"""
    parts = list(parse_chunks(b"[]", 7))
    assert len(parts) == 1 and len(parts[0]) == 0 and parts[0].invalid == 0
    _pb_equal(concat_parsed(parts), parse_body(b"[]"))


def test_parse_chunks_bad_request_raised():
    """整个请求体没有任何合法 JSON：parse_chunks 的第一个 next 就抛 BadRequest
    （前端错误中间件转 400），不会先产出空块。"""
    with pytest.raises(BadRequest):
        next(parse_chunks(b"not-json-at-all"))


def test_parsedbatch_chunk():
    b = validate(load_array(orjson.dumps(_evs(4500))))
    pb = ParsedBatch(b.ts, b.nbytes, key_hashes(b.ids), key_hashes(b.urls),
                     key_hashes(b.users), b.urls, b.users, b.invalid)
    # 切成 2000/2000/500
    c0, c1, c2 = pb.chunk(0, 2000), pb.chunk(2000, 4000), pb.chunk(4000, 4500)
    assert len(c0) == 2000 and len(c1) == 2000 and len(c2) == 500
    # 子块各字段对齐原批对应切片
    assert (c2.ts == pb.ts[4000:4500]).all() and c2.urls == pb.urls[4000:4500]
    assert c0.invalid == c1.invalid == c2.invalid == 0  # 子块 invalid 一律 0（见 chunk docstring）
    assert pb.invalid == 0
