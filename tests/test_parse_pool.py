"""P1 解析进程池：parse_body（纯计算）、ParsedBatch、worker 数计算、真实 spawn 进程池。

真子进程用例依赖"启动 pytest 时就固定了 PYTHONHASHSEED"——spawn 子进程用固定种子重算
key_hash 与主进程比对（parse_pool 自检），主进程解释器种子在启动时定、中途设 env 改不了。
Windows 本地：$env:PYTHONHASHSEED="0"; pytest … 生效。Docker：test 阶段继承 ENV PYTHONHASHSEED=0，
生效。无固定种子时真池用例跳过（核心等价性仍由进程内用例覆盖，见 test_ingest）。
"""

import os
import pickle
import random

import numpy as np
import orjson
import pytest

from flowlens.config import Config
from flowlens.engine import Engine
from flowlens.errors import BadRequest
from flowlens.hashing import key_hash, key_hashes
from flowlens.model import load_array, validate
from flowlens.parse_pool import (ParsedBatch, ParsePool, _SELFTEST_STR, parse_body,
                                 resolve_worker_count)
from flowlens.timewin import FakeClock

NOW = 1_735_689_600_000


def _hash_seed_fixed() -> bool:
    s = os.environ.get("PYTHONHASHSEED", "")
    return bool(s) and s != "random"


requires_seed = pytest.mark.skipif(
    not _hash_seed_fixed(), reason="真子进程用例需要固定 PYTHONHASHSEED")


def _evs(n: int, seed: int = 0) -> list:
    r = random.Random(seed)
    return [{"event_id": f"e{seed * 100000 + i}", "user_id": f"u{r.randrange(50)}",
             "url": f"/api/item/x{r.randrange(30)}?q={i}",
             "ts": NOW - 1000 - (i % 900), "bytes": 10 + (i % 40)} for i in range(n)]


# ---- parse_body：纯计算部分（子进程任务函数，主进程可直接调，不碰统计状态）----

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
    # 整个请求体没有任何合法 JSON → BadRequest 原样抛出（决定一：异常回传主进程）
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


# ---- worker 数计算（纯函数，不依赖核数/进程）----

def test_resolve_workers_from_cpus(monkeypatch):
    """默认上限 2（P1c 一.2）：min(2, 核数−1)，核数 1 不启用。"""
    import flowlens.parse_pool as pp
    for n_cpus, expected in [(8, 2), (4, 2), (3, 2), (2, 1), (1, 0), (0, 0)]:
        monkeypatch.setattr(pp, "available_cpus", lambda n=n_cpus: n_cpus)
        monkeypatch.delenv("FLOWLENS_PARSE_WORKERS", raising=False)
        assert resolve_worker_count() == expected


def test_workers_env_override(monkeypatch):
    import flowlens.parse_pool as pp
    monkeypatch.setattr(pp, "available_cpus", lambda: 8)
    monkeypatch.setenv("FLOWLENS_PARSE_WORKERS", "2")
    assert resolve_worker_count() == 2
    monkeypatch.setenv("FLOWLENS_PARSE_WORKERS", "3")   # 实验可显式开 3（超过默认上限）
    assert resolve_worker_count() == 3
    monkeypatch.setenv("FLOWLENS_PARSE_WORKERS", "0")   # 显式禁用
    assert resolve_worker_count() == 0
    monkeypatch.setenv("FLOWLENS_PARSE_WORKERS", "9")   # 夹到 [0,3]
    assert resolve_worker_count() == 3
    monkeypatch.setenv("FLOWLENS_PARSE_WORKERS", "not-a-number")
    assert resolve_worker_count() == 0
    monkeypatch.delenv("FLOWLENS_PARSE_WORKERS")
    assert resolve_worker_count() == 2  # 覆盖清除后回默认 min(2, 8−1)


def test_one_core_disabled(monkeypatch):
    # P1 §3.4：核数为 1 时不启用进程池
    import flowlens.parse_pool as pp
    monkeypatch.setattr(pp, "available_cpus", lambda: 1)
    monkeypatch.delenv("FLOWLENS_PARSE_WORKERS", raising=False)
    assert resolve_worker_count() == 0


# ---- 真实 spawn 进程池 ----

@requires_seed
def test_pool_spawn_end_to_end_equivalence():
    """P1 §3.1：同一 body 经真子进程 parse → engine.ingest_parsed，与
    engine.ingest(parse_events) 写两个全新引擎，同区间 topk(url/user)/stats/计数器逐项相等。"""
    from flowlens.ingest import Admission, IngestService
    body = orjson.dumps(_evs(6000))
    e1 = Engine(Config(), FakeClock(NOW))
    s1 = IngestService(e1, Admission())
    e2 = Engine(Config(), FakeClock(NOW))
    pool = ParsePool(2)
    s2 = IngestService(e2, Admission(), parse_pool=pool)
    try:
        r1 = _async(s1.ingest(body, None))
        r2 = _async(s2.ingest(body, None))
        assert r1 == r2
        snap1, snap2 = e1.refresh_snapshot(), e2.refresh_snapshot()
        assert (snap1.tops, snap1.distinct_users, snap1.p95_bytes) == \
               (snap2.tops, snap2.distinct_users, snap2.p95_bytes)
        c1, c2 = e1.counters, e2.counters
        assert (c1.events_ingested, c1.deduped, c1.late, c1.invalid) == \
               (c2.events_ingested, c2.deduped, c2.late, c2.invalid)
    finally:
        pool.close()


def _async(coro):
    import asyncio
    return asyncio.run(coro)


@requires_seed
def test_pool_selfcheck_mismatch_raises():
    """决定三：种子不一致时子进程自检失败，池**构造**（预热）阶段即抛 BrokenProcessPool，
    且半成品池被清理（executor 置 None、无残留子进程）。"""
    from concurrent.futures.process import BrokenProcessPool

    class _BadSeedPool(ParsePool):
        def _build(self):
            self._expected = key_hash(_SELFTEST_STR) ^ 1  # 故意错一个 bit
            super()._build()

    with pytest.raises(BrokenProcessPool):
        _BadSeedPool(2)


@requires_seed
def test_pool_rebuild_after_crash():
    """P1 §3.3：子进程崩溃后重建，后续请求正常。"""
    pool = ParsePool(2)
    body = orjson.dumps(_evs(50))
    try:
        assert pool.submit(body).result().invalid == 0
        for p in pool._executor._processes.values():
            p.terminate()
        import time
        time.sleep(0.2)
        pool.rebuild()
        # 重建后能正常解析
        assert pool.submit(body).result().invalid == 0
    finally:
        pool.close()


@requires_seed
def test_pool_close_no_residual_workers():
    """P1 §3.5：close 后无残留子进程。"""
    import time
    pool = ParsePool(2)
    try:
        assert pool.submit(b"[]").result().invalid == 0
        # 记下 worker 进程对象，close 后逐个确认已退出
        procs = list(pool._executor._processes.values())
        assert len(procs) == 2
    finally:
        pool.close()
    assert pool._executor is None
    for p in procs:
        p.join(timeout=5)
        assert not p.is_alive(), "close 后子进程应已退出、无残留"
