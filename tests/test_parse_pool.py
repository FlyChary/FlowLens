"""P1 起保留的纯计算/纯数据测试：parse_body、ParsedBatch。

P2d-1 起进程池（ParsePool / FLOWLENS_PARSE_WORKERS / resolve_worker_count / 哈希自检 /
真实 spawn 进程池用例）已删除——解析被前端进程取代（见 docs/lanes/P2-notes.md P2d-1）。
本文件只保留 parse_body（前端进程的执行体）与 ParsedBatch 的纯函数测试。
"""

import pickle
import random

import orjson
import pytest

from flowlens.errors import BadRequest
from flowlens.hashing import key_hashes
from flowlens.model import load_array, validate
from flowlens.parse_pool import ParsedBatch, parse_body

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
