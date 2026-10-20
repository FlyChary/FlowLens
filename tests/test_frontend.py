"""P2d-1 四：前端进程内部逻辑（不真起进程、不依赖 Unix 套接字，跨平台可跑）。

- per_front_limit 的下限规则
- _error_from_ack 按 status 还原异常
- FrontIngestService.ingest 的顺序（预留→解析→提交→finally 释放、503 时不解析）
- EngineConnection 的 req_id 关联（ACK 回来唤醒对应 future、断开时在途请求失败）
- create_front_app 的组装（SlotAdmission / DegradeView / 路由）
"""

import asyncio
import multiprocessing as mp
import pickle

import numpy as np
import orjson
import pytest

from flowlens.engine import IngestResult
from flowlens.errors import (BadRequest, Overloaded, PayloadTooLarge, TooOld,
                             UnsupportedEncoding)
from flowlens.frontend import (EngineConnection, FrontIngestService,
                               _error_from_ack, create_front_app)
from flowlens.ipc import _HDR, MSG_ACK
from flowlens.parse_pool import ParsedBatch
from flowlens.supervisor import (DegradeView, SharedState, SlotAdmission,
                                 per_front_limit)

NOW = 1_735_689_600_000


# ---- per_front_limit ----

def test_per_front_limit_split():
    assert per_front_limit(16 * 1024 * 1024, 4, 4 * 1024 * 1024) == 4 * 1024 * 1024


def test_per_front_limit_floor_applies():
    """平均后低于下限 floor 时抬到 floor（单个合法大请求要预留得到）。"""
    # 10 MB / 4 = 2.5 MB < 4 MB → 抬到 4 MB（合计实际 16 MB，P2-notes 要记录）
    assert per_front_limit(10 * 1024 * 1024, 4, 4 * 1024 * 1024) == 4 * 1024 * 1024


def test_per_front_limit_above_floor():
    assert per_front_limit(64 * 1024 * 1024, 4, 4 * 1024 * 1024) == 16 * 1024 * 1024


# ---- _error_from_ack ----

def test_error_from_ack_by_status():
    assert isinstance(_error_from_ack((1, "error", 400, "bad", None)), BadRequest)
    assert isinstance(_error_from_ack((1, "error", 413, "big", None)), PayloadTooLarge)
    assert isinstance(_error_from_ack((1, "error", 415, "enc", None)), UnsupportedEncoding)
    assert isinstance(_error_from_ack((1, "error", 422, "old", None)), TooOld)
    e = _error_from_ack((1, "error", 503, "满", 2))
    assert isinstance(e, Overloaded) and e.retry_after == 2 and e.message == "满"
    assert isinstance(_error_from_ack((1, "error", 500, "?", None)), Overloaded)


# ---- 构造辅助 ----

def _parsed(n: int = 3, seed: int = 0) -> ParsedBatch:
    return ParsedBatch(np.full(n, NOW, dtype=np.int64), np.full(n, 10, dtype=np.int64),
                       np.arange(n, dtype=np.uint64), np.arange(n, dtype=np.uint64),
                       np.arange(n, dtype=np.uint64),
                       [f"/u{i % 2}" for i in range(n)], [f"u{i % 2}" for i in range(n)], 0)


def _body3() -> bytes:
    return orjson.dumps([{"event_id": f"e{i}", "user_id": "u", "url": "/a",
                          "ts": NOW, "bytes": 1} for i in range(3)])


def _make_adm(limit: int) -> SlotAdmission:
    ctx = mp.get_context("spawn")
    return SlotAdmission(limit, ctx.Array("q", 1), ctx.Array("q", 1), 0)


class _Tight:
    """shared 替身：FrontIngestService 只读 shared.tight.value。"""
    def __init__(self, value: int = 0):
        self._v = value

    @property
    def tight(self):
        class V:
            pass
        v = V()
        v.value = self._v
        return v


class _FakeConn:
    """EngineConnection 替身：记录提交的 parsed，返回固定结果或抛异常。"""
    def __init__(self, result=None, exc=None):
        self.submitted = []
        self._result, self._exc = result, exc

    async def submit(self, parsed):
        self.submitted.append(parsed)
        if self._exc is not None:
            raise self._exc
        return self._result


# ---- FrontIngestService.ingest 的顺序 ----

def test_front_ingest_order_reserve_submit_release():
    """预留在提交之前，释放在 finally（成功路径）。"""
    body = _body3()
    conn = _FakeConn(result=IngestResult(3, 0, 0, 0))
    adm = _make_adm(1024 * 1024)
    svc = FrontIngestService(conn, adm, _Tight(0), 1024 * 1024, 512 * 1024)
    r = asyncio.run(svc.ingest(body, None))
    assert r.accepted == 3
    assert len(conn.submitted) == 1
    assert adm.pending == 0   # finally 已释放


def test_front_ingest_reject_does_not_submit():
    """在途额度满：503（Overloaded），不解析、不提交（没写去重）。"""
    conn = _FakeConn(result=IngestResult(0, 0, 0, 0))
    adm = _make_adm(10)
    adm.try_reserve(10)   # 占满
    svc = FrontIngestService(conn, adm, _Tight(0), 10, 5)
    with pytest.raises(Overloaded):
        asyncio.run(svc.ingest(b"[]", None))
    assert conn.submitted == []
    assert adm.pending == 10   # 拒绝后占用不变


def test_front_ingest_tight_limit_from_shared():
    """shared.tight=1 时用收紧上限；超限请求 503，错误信息带 MB（< 正常上限）。"""
    from flowlens.ingest import MAX_PENDING_BYTES
    body = _body3()
    conn = _FakeConn(result=IngestResult(3, 0, 0, 0))
    adm = _make_adm(MAX_PENDING_BYTES)
    svc = FrontIngestService(conn, adm, _Tight(1), MAX_PENDING_BYTES, 512 * 1024)
    n = len(body)
    adm.try_reserve(max(0, 512 * 1024 - n // 2))   # 抬高到超过收紧线
    with pytest.raises(Overloaded) as ei:
        asyncio.run(svc.ingest(body, None))
    assert "MB" in ei.value.message
    assert conn.submitted == []


def test_front_ingest_engine_error_releases():
    """引擎回 error：submit 抛对应异常，finally 仍释放预留。"""
    conn = _FakeConn(exc=BadRequest("坏体"))
    adm = _make_adm(1024 * 1024)
    svc = FrontIngestService(conn, adm, _Tight(0), 1024 * 1024, 512 * 1024)
    with pytest.raises(BadRequest):
        asyncio.run(svc.ingest(_body3(), None))
    assert adm.pending == 0


# ---- EngineConnection：req_id 关联与断开 ----

class _PipeReader:
    """readexactly 替身：从 asyncio.Queue 取字节段（空时阻塞，不提前 EOF）；
    推入 _EOF 标记后才抛 IncompleteReadError（模拟连接断开）。"""
    _EOF = object()

    def __init__(self):
        self.q: asyncio.Queue = asyncio.Queue()
        self._buf = bytearray()

    async def feed(self, b: bytes):
        await self.q.put(b)

    async def feed_eof(self):
        await self.q.put(self._EOF)

    async def readexactly(self, n: int) -> bytes:
        while len(self._buf) < n:
            item = await self.q.get()
            if item is self._EOF:
                raise asyncio.IncompleteReadError(bytes(self._buf), n)
            self._buf.extend(item)
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out


class _NoopWriter:
    def __init__(self):
        self.data = bytearray()
    def write(self, b):
        self.data.extend(b)
    async def drain(self):
        pass


def _ack_bytes(req_id: int, ok: bool = True) -> bytes:
    payload = pickle.dumps((req_id, "ok", 5, 1, 2, 3) if ok
                           else (req_id, "error", 503, "满", 1), protocol=5)
    return _HDR.pack(len(payload), MSG_ACK) + payload


def test_engine_connection_correlates_ack():
    """两个并发提交，ACK 按 req_id 各自唤醒；字段顺序 (accepted,deduped,late,invalid)。"""
    lost = []

    async def go():
        rd, wr = _PipeReader(), _NoopWriter()
        conn = EngineConnection(rd, wr, on_lost=lambda: lost.append(1))
        f1 = asyncio.ensure_future(conn.submit(_parsed(1)))
        f2 = asyncio.ensure_future(conn.submit(_parsed(1)))
        await asyncio.sleep(0.01)   # 让两个 submit 都挂上
        # 两条 WireBatch 已写出（req_id 1、2）；回对应 ok ACK
        await rd.feed(_ack_bytes(1))
        await rd.feed(_ack_bytes(2))
        r1, r2 = await asyncio.wait_for(asyncio.gather(f1, f2), 2.0)
        assert (r1.accepted, r1.deduped, r1.late, r1.invalid) == (5, 1, 2, 3)
        assert (r2.accepted, r2.deduped, r2.late, r2.invalid) == (5, 1, 2, 3)
        assert lost == []

    asyncio.run(go())


def test_engine_connection_error_ack():
    """error ACK 唤醒对应 future 并抛还原后的异常。"""

    async def go():
        rd, wr = _PipeReader(), _NoopWriter()
        conn = EngineConnection(rd, wr, on_lost=lambda: None)
        f = asyncio.ensure_future(conn.submit(_parsed(1)))
        await asyncio.sleep(0.01)
        await rd.feed(_ack_bytes(1, ok=False))
        with pytest.raises(Overloaded) as ei:
            await asyncio.wait_for(f, 2.0)
        assert ei.value.message == "满"

    asyncio.run(go())


def test_engine_connection_lost_fails_waiting():
    """连接断开：在途请求以 Overloaded 失败，on_lost 被回调。"""
    lost = []

    async def go():
        rd, wr = _PipeReader(), _NoopWriter()
        conn = EngineConnection(rd, wr, on_lost=lambda: lost.append(1))
        f = asyncio.ensure_future(conn.submit(_parsed(1)))
        await asyncio.sleep(0.01)
        await rd.feed_eof()                       # 模拟引擎断开
        with pytest.raises(Overloaded):
            await asyncio.wait_for(f, 2.0)
        assert lost == [1]

    asyncio.run(go())


# ---- create_front_app：组装正确（不真起服务器）----

def test_create_front_app_state():
    shared = SharedState(mp.get_context("spawn"), 2)
    from flowlens.api.app import degrade_controller_key, raw_budget_key
    app = create_front_app(_FakeConn(), shared, 0, 2, "/tmp/unused.sock",
                           4 * 1024 * 1024, 2 * 1024 * 1024,
                           8 * 1024 * 1024, 4 * 1024 * 1024)
    assert isinstance(app[raw_budget_key], SlotAdmission)
    assert isinstance(app[degrade_controller_key], DegradeView)
    # 在途额度镜像到第 slot=0 槽、原始体镜像到第 0 槽
    assert app[raw_budget_key]._i == 0


def test_create_front_app_routes_ingest_and_catchall():
    shared = SharedState(mp.get_context("spawn"), 1)
    app = create_front_app(_FakeConn(), shared, 0, 1, "/tmp/unused.sock",
                           4 * 1024 * 1024, 2 * 1024 * 1024,
                           8 * 1024 * 1024, 4 * 1024 * 1024)
    canonicals = {r.resource.canonical for r in app.router.routes()}
    assert "/v1/ingest" in canonicals
    assert "/{tail}" in canonicals   # catch-all 转发
