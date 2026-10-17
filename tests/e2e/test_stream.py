"""T7 e2e：SSE 实时推送（GET /v1/topk/stream）。

用真实事件循环时间与真实 TCP 服务器（aiohttp TestServer），数据用真实时钟驱动引擎
准备（ts 取当前时刻附近，落在活跃环内，60 秒窗口查询无需等定稿）。SSE 的帧时刻按
interval 对齐网格、由真实时间推进，所以引擎必须用真实时钟（FakeClock 是静态的，
会让生产者"睡到的网格点"与实际流逝时间脱节）。

不引入新依赖：用 aiohttp 测试客户端读原始 SSE 流（readuntil 按空行切帧）、自己解析。
慢客户端 = 建连后不读 resp.content；带 Last-Event-ID 重连用 headers 传。
"""

import asyncio
import json
import time

from aiohttp.test_utils import TestClient, TestServer

from flowlens.api.app import create_app, query_service_key, stream_manager_key
from flowlens.api.stream import Subscriber
from flowlens.config import Config
from flowlens.engine import Engine
from flowlens.ingest import Admission, IngestService
from flowlens.query import QueryService
from flowlens.timewin import Clock


def _strict_json(text: str):
    """严格 JSON：遇到 NaN/Infinity 等非标准常量直接报错。"""
    def _reject(const):
        raise ValueError(f"响应里出现非标准 JSON 常量: {const}")
    return json.loads(text, parse_constant=_reject)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _evs(n, ts_base, url_prefix="/x"):
    return [{"event_id": f"e{i}", "user_id": f"u{i % 20}",
             "url": f"{url_prefix}{i % 10}",
             "ts": ts_base - (i % 500), "bytes": 100} for i in range(n)]


def parse_block(text: str) -> dict:
    """解析一块 SSE 报文（一帧或 retry 或 ping）：{kind, id, event, data, comment}。

    kind：'retry' / 'ping' / 'event'。data 是 data 行内容（事件帧）。
    """
    out = {"kind": None, "id": None, "event": None, "data": "", "comment": None}
    lines = [ln for ln in text.split("\n") if ln != ""]
    if not lines:
        return out
    if lines[0].startswith("retry:"):
        out["kind"] = "retry"
        out["data"] = lines[0][6:].strip()
        return out
    if lines[0].startswith(":"):
        out["kind"] = "ping"
        out["comment"] = lines[0]
        return out
    out["kind"] = "event"
    for line in lines:
        if line.startswith("id:"):
            out["id"] = line[3:].strip()
        elif line.startswith("event:"):
            out["event"] = line[6:].strip()
        elif line.startswith("data:"):
            out["data"] = line[5:].lstrip()
    return out


def _serve(run, *, tick_period: float = 1000.0):
    """起真实服务器（真实时钟），await run(client, engine, app) 后干净关闭。"""
    async def main():
        engine = Engine(Config(), Clock())
        qs = QueryService(engine)
        app = create_app(engine, IngestService(engine, Admission()), qs,
                         tick_period=tick_period)
        server = TestServer(app)
        client = TestClient(server)
        await server.start_server()
        try:
            await run(client, engine, app)
        finally:
            try:
                await client.close()
            except Exception:
                pass
    asyncio.run(main())


async def _ingest(client, evs) -> None:
    async with client.post("/v1/ingest", data=json.dumps(evs).encode()) as r:
        assert r.status == 202, (r.status, await r.read())


async def _read_block(resp) -> str:
    """读一块 SSE（到空行 \\n\\n 为止）；流结束（EOF）返回 None。"""
    try:
        raw = await asyncio.wait_for(resp.content.readuntil(b"\n\n"), timeout=20)
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, EOFError):
        return None
    except asyncio.TimeoutError:
        raise AssertionError("读 SSE 超时")
    return raw.decode()


async def _open_stream(client, url, headers=None):
    """开一个 SSE 连接，返回已 prepare 的 resp（调用方负责关闭）。"""
    resp = await client.get(url, headers=headers or {})
    assert resp.status == 200, f"SSE 连接应 200，实际 {resp.status}: {await resp.read()}"
    assert resp.headers.get("Content-Type") == "text/event-stream"
    return resp


# ---- 帧格式 ----

def test_frame_format_and_retry():
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        resp = await _open_stream(
            client, "/v1/topk/stream?window=60s&k=10&by=url&interval=200ms")
        try:
            # 连接开头先发 retry: 3000
            b = parse_block(await _read_block(resp))
            assert b["kind"] == "retry" and b["data"] == "3000", b
            # 第一个数据帧：id / event / data 三行
            f = parse_block(await _read_block(resp))
            assert f["kind"] == "event" and f["event"] == "topk"
            assert f["id"] is not None and int(f["id"]) > 0
            body = _strict_json(f["data"])
            assert set(body) == {"window_ms", "from", "to", "by", "k", "final",
                                 "degraded", "unseen_upper", "items"}
            assert body["by"] == "url" and body["k"] == 10 and body["window_ms"] == 60_000
            assert isinstance(body["items"], list)
            for it in body["items"]:  # data 与 /v1/topk 同构
                assert set(it) == {"key", "count", "err"}
        finally:
            resp.close()
    _serve(run)


# ---- id 严格递增、相邻差等于 interval ----

def test_ids_strictly_increasing_by_interval():
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        resp = await _open_stream(
            client, "/v1/topk/stream?window=60s&k=10&by=url&interval=500ms")
        try:
            await _read_block(resp)  # retry
            ids = []
            for _ in range(4):
                f = parse_block(await _read_block(resp))
                assert f["kind"] == "event"
                ids.append(int(f["id"]))
            for a, b in zip(ids, ids[1:]):  # 严格递增、无重复，相邻差 500ms
                assert b > a, ids
                assert b - a == 500, ids
        finally:
            resp.close()
    _serve(run)


# ---- 共享计算：同参数订阅者共用一份计算 ----

def test_shared_computation_same_params():
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        qs = app[query_service_key]
        manager = app[stream_manager_key]
        calls = []
        real = qs.topk

        async def spy(d, window_ms, k, at_ms=None):
            calls.append(time.monotonic())
            return await real(d, window_ms, k, at_ms)
        qs.topk = spy
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=200ms"
        try:
            # 结构性证明：两个同参数订阅者落在同一个组、共用同一个后台生产任务
            # （计算只由这一个任务做，不是每个订阅者各算一份）
            r1 = await _open_stream(client, url)
            await _read_block(r1)  # retry
            r2 = await _open_stream(client, url)
            await _read_block(r2)
            assert len(manager.groups) == 1  # 一个组
            group = next(iter(manager.groups.values()))
            assert len(group.subscribers) == 2  # 两个订阅者都在这个组里
            assert group.task is not None and not group.task.done()  # 只有一个后台任务
            task_id = group.task
            await asyncio.sleep(0.5)  # 等两个订阅者的首次补发（也会调 topk）跑完
            assert group.task is task_id  # 仍是同一个任务，没有多开
            # 稳态：两个订阅者共用一个生产任务时，后台计算仍是"每秒最多一次"的单
            # 生产者频率（~1/s，3s 约 2~3 次）。若每个订阅者各算一份，3s 会 ~6 次
            # （翻倍）。断言落在单生产者的区间，即"与只有一个订阅者时相同"。
            calls.clear()
            t0 = time.monotonic()
            while time.monotonic() - t0 < 3.0:
                await asyncio.sleep(0.2)
            c_both = len(calls)
            assert 2 <= c_both <= 4, c_both
        finally:
            r1.close()
            qs.topk = real
    _serve(run)


# ---- 计算频率：interval=200ms 时，每秒最多重新计算一次 ----

def test_recompute_at_most_once_per_second():
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        qs = app[query_service_key]
        calls = []
        real = qs.topk

        async def spy(d, window_ms, k, at_ms=None):
            calls.append(time.monotonic())
            return await real(d, window_ms, k, at_ms)
        qs.topk = spy
        try:
            resp = await _open_stream(
                client, "/v1/topk/stream?window=60s&k=10&by=url&interval=200ms")
            await _read_block(resp)  # retry
            # 跑 2.5 秒：不设限每 200ms 算一次 ≈ 12 次；限流后约 1 次/秒 → 2~4 次
            calls.clear()
            t0 = time.monotonic()
            while time.monotonic() - t0 < 2.5:
                await asyncio.sleep(0.1)
            c = len(calls)
            assert 2 <= c <= 5, c
            resp.close()
        finally:
            qs.topk = real
    _serve(run)


# ---- 上限：订阅组数与连接数超过时返回 503 + Retry-After（JSON）----

def test_group_and_connection_limits_503():
    async def run(client, engine, app):
        await _ingest(client, _evs(20, _now_ms()))
        manager = app[stream_manager_key]

        # 组数上限：建满 max_groups 个组后，再来一个不同参数 → 503 + Retry-After（JSON）
        manager.max_groups = 3
        opens = []
        for i in range(3):  # k=1/2/3 三个不同参数 → 三个不同组
            opens.append(await _open_stream(
                client, f"/v1/topk/stream?window=60s&k={i + 1}&by=url&interval=1s"))
        assert len(manager.groups) == manager.max_groups
        r4 = await client.get("/v1/topk/stream?window=60s&k=4&by=user&interval=1s")
        assert r4.status == 503, r4.status
        assert r4.headers.get("Retry-After") == "1"
        err = _strict_json(await r4.text())
        assert "error" in err
        r4.close()
        for r in opens:
            r.close()
        await asyncio.sleep(0.3)  # 等 leave 清理

        # 连接数上限：max_subscribers=1，第二个连接 → 503
        manager.max_subscribers = 1
        a = await _open_stream(client, "/v1/topk/stream?window=60s&k=5&by=url&interval=1s")
        b = await client.get("/v1/topk/stream?window=60s&k=5&by=url&interval=1s")
        assert b.status == 503, b.status
        assert b.headers.get("Retry-After") == "1"
        assert "error" in _strict_json(await b.text())
        b.close()
        a.close()
    _serve(run)


# ---- 慢客户端：只保留最新一帧（队列容量 1），其他订阅者正常 ----

def test_slow_client_queue_bounded_and_others_ok():
    async def run(client, engine, app):
        await _ingest(client, _evs(80, _now_ms()))
        manager = app[stream_manager_key]
        url = "/v1/topk/stream?window=60s&k=20&by=url&interval=200ms"
        # 慢客户端：建连后**从不读取**（服务端 write 进内核缓冲，队列最多 1）
        slow = await _open_stream(client, url)
        # 正常客户端：同组、持续读取
        norm = await _open_stream(client, url)
        await _read_block(norm)  # retry
        assert len(manager.groups) == 1
        got = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < 2.0 and got < 6:
            f = parse_block(await _read_block(norm))
            if f["kind"] == "event":
                got += 1
        # 慢客户端订阅者的队列始终不超过 1 帧（容量 1，满则丢旧帧）
        grp = next(iter(manager.groups.values()))
        for sub in grp.subscribers.values():
            assert sub.queue.qsize() <= 1, sub.queue.qsize()
        # 正常客户端持续收到帧（同组不受慢客户端拖累）
        assert got >= 3, got
        slow.close()
        norm.close()
    _serve(run)


# ---- 慢客户端丢弃：队列满了先丢旧帧、丢弃计数增加（确定性单测）----

def test_subscriber_queue_drops_old_frames():
    async def run(client, engine, app):
        sub = Subscriber()
        sub.offer(100, {"i": 1})
        sub.offer(200, {"i": 2})
        sub.offer(300, {"i": 3})  # 队列容量 1：前两帧被丢，只剩最新
        assert sub.queue.qsize() == 1
        assert sub.dropped == 2  # 丢弃计数增加
        tid, payload = sub.queue.get_nowait()
        assert tid == 300 and payload == {"i": 3}
    _serve(run)


# ---- 断线重连：Last-Event-ID 补发，id 连续 ----

def test_last_event_id_catchup_contiguous():
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=200ms"
        # 第一次连接，读 3 个数据帧，记下最后一个 id
        raw = await _open_stream(client, url)
        ids = []
        for _ in range(4):  # 第一块是 retry，后三块是数据帧
            f = parse_block(await _read_block(raw))
            if f["kind"] == "event":
                ids.append(int(f["id"]))
        last_id = ids[-1]
        raw.close()
        # 带 Last-Event-ID 重连：补发帧 id 从 last_id+200 开始、连续、无缝接实时帧
        raw2 = await _open_stream(client, url, headers={"Last-Event-ID": str(last_id)})
        await _read_block(raw2)  # retry
        got = []
        t0 = time.monotonic()
        while len(got) < 4 and time.monotonic() - t0 < 8:
            f = parse_block(await _read_block(raw2))
            if f["kind"] == "event":
                got.append(int(f["id"]))
        assert got[0] == last_id + 200, (last_id, got)  # 补发从断点下一格开始
        for a, b in zip(got, got[1:]):  # 严格连续、不重不漏（含补发→实时接缝）
            assert b - a == 200, got
        raw2.close()
    _serve(run)


def test_last_event_id_bad_or_future_ignored():
    async def run(client, engine, app):
        await _ingest(client, _evs(20, _now_ms()))
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=500ms"

        # 格式不对（非数字）→ 忽略，直接从实时帧开始（id 对齐 500ms 网格、<= now）
        raw = await _open_stream(client, url, headers={"Last-Event-ID": "not-a-number"})
        await _read_block(raw)  # retry
        f = parse_block(await _read_block(raw))
        assert f["kind"] == "event"
        assert int(f["id"]) % 500 == 0
        assert int(f["id"]) <= _now_ms()
        raw.close()

        # 未来时间 → 忽略，从实时帧开始（没被未来的 id 带偏）
        future = _now_ms() + 60_000
        raw2 = await _open_stream(client, url, headers={"Last-Event-ID": str(future)})
        await _read_block(raw2)
        f2 = parse_block(await _read_block(raw2))
        assert int(f2["id"]) <= _now_ms()
        raw2.close()
    _serve(run)


# ---- 心跳：调小心跳周期后能收到 ": ping" ----

def test_heartbeat_ping():
    async def run(client, engine, app):
        await _ingest(client, _evs(20, _now_ms()))
        manager = app[stream_manager_key]
        manager.heartbeat_period = 0.3  # 调小心跳周期
        resp = await _open_stream(
            client, "/v1/topk/stream?window=60s&k=10&by=url&interval=2s")
        try:
            await _read_block(resp)  # retry
            # interval=2s（帧稀疏）但心跳 0.3s：2 秒内应收到一次 ": ping"
            saw_ping = False
            t0 = time.monotonic()
            while time.monotonic() - t0 < 2.0 and not saw_ping:
                b = parse_block(await _read_block(resp))
                if b["kind"] == "ping":
                    saw_ping = True
        finally:
            resp.close()
        assert saw_ping
    _serve(run)


# ---- 客户端断开后订阅者被移除；最后一个离开后订阅组被释放 ----

def test_disconnection_removes_subscriber_and_group():
    async def run(client, engine, app):
        await _ingest(client, _evs(20, _now_ms()))
        manager = app[stream_manager_key]
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=200ms"
        a = await _open_stream(client, url)
        b = await _open_stream(client, url)
        await _read_block(a)  # 触发服务端 handler 开始
        await _read_block(b)
        assert len(manager.groups) == 1
        assert manager.subscriber_count() == 2
        # a 断开：服务端检测到后移除订阅者
        a.close()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and manager.subscriber_count() > 1:
            await asyncio.sleep(0.1)
        assert manager.subscriber_count() == 1, "断开后订阅者没被移除"
        # b 断开：最后一个离开 → 组被释放
        b.close()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and len(manager.groups) > 0:
            await asyncio.sleep(0.1)
        assert len(manager.groups) == 0, "最后一个离开后组没被释放"
    _serve(run)


# ---- T7b：新订阅者立即收到最近一帧 ----

def test_new_subscriber_immediate_frame():
    """interval=60s：第二个订阅者加入已有组后 1 秒内收到第一帧（不用等下个网格点）。

    第一帧是组里已有的最近一帧（立即补发），id 仍在 60s 网格上；之后照常跟随实时帧。
    """
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=60s"
        first = await _open_stream(client, url)
        await _read_block(first)  # retry
        await _read_block(first)  # 首个数据帧（组里第一帧）
        # 等组里已有"最近一帧"
        r2 = await _open_stream(client, url)
        t0 = time.monotonic()
        b = parse_block(await _read_block(r2))  # retry
        assert b["kind"] == "retry"
        f = parse_block(await _read_block(r2))
        wait = time.monotonic() - t0
        assert f["kind"] == "event", f
        assert wait < 1.0, f"第二订阅者 {wait:.2f}s 才收到第一帧"
        assert int(f["id"]) % 60_000 == 0  # id 在 60s 网格上
        first.close()
        r2.close()
    _serve(run)


# ---- T7b：补帧不重算 —— 同组大量订阅者同时带 Last-Event-ID 重连，后台 topk 调用
#      次数不随订阅者数增加（优先用组里保留的最近 30 帧）----

def test_catchup_no_recompute_across_subscribers():
    """同组 10 个订阅者同时带 Last-Event-ID 重连，后台 topk 次数不随订阅者数增长。

    常驻读者保持组存活并让生产者把帧填进组缓存（最近 30 帧）。用较早一帧的 id 作
    Last-Event-ID 让 10 个订阅者重连，缺口里的帧都已在缓存里 → 补发走缓存、不重算；
    若无缓存，10 个订阅者各自重算缺口（≥30 帧/人）→ 次数会随订阅者数线性放大。
    """
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        qs = app[query_service_key]
        calls = 0
        real = qs.topk

        async def spy(d, window_ms, k, at_ms=None):
            nonlocal calls
            calls += 1
            return await real(d, window_ms, k, at_ms)
        qs.topk = spy
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=2s"
        # 常驻读者：保持组存活，让生产者产帧并填进组缓存；记下较早一帧的 id 当重连起点
        reader = await _open_stream(client, url)
        await _read_block(reader)  # retry
        ids = []
        t0 = time.monotonic()
        while len(ids) < 4 and time.monotonic() - t0 < 12:
            f = parse_block(await _read_block(reader))
            if f["kind"] == "event":
                ids.append(int(f["id"]))
        assert len(ids) == 4, f"常驻读者没读到 4 帧: {ids}"
        old_id = ids[0]  # 用最早一帧的 id：重连缺口 = [old_id+2s, 当前)，都在缓存里
        calls = 0
        reconnects = [await _open_stream(
            client, url, headers={"Last-Event-ID": str(old_id)})
            for _ in range(10)]
        # 等每个重连者都补完缺口并收到一帧实时帧
        got = [0] * 10
        t0 = time.monotonic()
        while min(got) < 1 and time.monotonic() - t0 < 10:
            for i, r in enumerate(reconnects):
                try:
                    f = parse_block(await asyncio.wait_for(
                        _read_block(r), timeout=0.3))
                    if f["kind"] == "event":
                        got[i] = 1
                except asyncio.TimeoutError:
                    pass
        assert min(got) == 1, f"有重连者没收到帧: {got}"
        for r in reconnects:
            r.close()
        # 补发走缓存：后台 topk 只有生产者自己的"每组每秒至多一次"，不随订阅者数
        # 线性放大（无缓存时 10 人各补 ~3 帧 → ≥30 次；有缓存 → 仅生产者的数次）。
        assert calls < 16, f"补帧重算次数 {calls} 随订阅者数增长"
        reader.close()
    _serve(run)


# ---- T7b：Last-Event-ID 不在当前 interval 网格上时，起始游标向上对齐网格 ----

def test_last_event_id_off_grid_aligned():
    """Last-Event-ID 与当前 interval 网格不对齐（如客户端换了 interval）时，
    起始游标向上对齐到网格，补帧与实时帧的 id 都落在网格上。"""
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=1s"
        # 用一个不在 1s 网格上的 Last-Event-ID（模拟换了 interval：上次 500ms 的 id）
        raw = await _open_stream(client, url, headers={"Last-Event-ID": "1234567890123"})
        await _read_block(raw)  # retry
        f = parse_block(await _read_block(raw))
        assert f["kind"] == "event"
        # 起始 id 向上对齐到 1s 网格：id 是 1000 的整数倍
        assert int(f["id"]) % 1000 == 0, f["id"]
        # 且晚于 Last-Event-ID（不重发已见的帧）
        assert int(f["id"]) > 1234567890123, f["id"]
        raw.close()
    _serve(run)


# ---- T7b：interval>1s 时每一帧都重新计算，帧的 data.to 应等于 id ----

def test_recompute_when_interval_gt_1s():
    async def run(client, engine, app):
        await _ingest(client, _evs(50, _now_ms()))
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=1500ms"
        resp = await _open_stream(client, url)
        try:
            await _read_block(resp)  # retry
            for _ in range(3):
                f = parse_block(await _read_block(resp))
                assert f["kind"] == "event"
                body = _strict_json(f["data"])
                # interval=1500ms > 1s：每一帧都应重新计算，data.to 等于 id
                assert body["to"] == int(f["id"]), (body["to"], f["id"])
                assert body["to"] - body["from"] == 60_000
        finally:
            resp.close()
    _serve(run)


# ---- T7b：应用关闭时所有流立即正常结束（多个活跃订阅者，2 秒内完成）----

def test_shutdown_ends_all_streams_fast():
    async def main():
        engine = Engine(Config(), Clock())
        qs = QueryService(engine)
        app = create_app(engine, IngestService(engine, Admission()), qs,
                         tick_period=1000.0)
        server = TestServer(app)
        client = TestClient(server)
        await server.start_server()
        url = "/v1/topk/stream?window=60s&k=10&by=url&interval=2s"
        # 3 个活跃订阅者
        conns = [await client.get(url) for _ in range(3)]
        for c in conns:
            assert c.status == 200
        assert app[stream_manager_key].subscriber_count() == 3
        # 关闭应用：清理应在 2 秒内完成（不再靠 aiohttp 的 60s 强制取消）
        t0 = time.monotonic()
        await client.close()
        elapsed = time.monotonic() - t0
        assert elapsed < 2.0, f"关闭耗时 {elapsed:.2f}s（应 <2s）"
        assert len(app[stream_manager_key].groups) == 0
    asyncio.run(main())


# ---- 应用关闭时所有流都正常结束、订阅组任务都被取消 ----

def test_app_shutdown_ends_streams():
    async def main():
        engine = Engine(Config(), Clock())
        qs = QueryService(engine)
        app = create_app(engine, IngestService(engine, Admission()), qs, tick_period=1000.0)
        server = TestServer(app)
        client = TestClient(server)
        await server.start_server()
        resp = await client.get(
            "/v1/topk/stream?window=60s&k=10&by=url&interval=200ms")
        assert resp.status == 200
        await _read_block(resp)  # retry
        manager = app[stream_manager_key]
        assert len(manager.groups) == 1  # 流在进行、组存在
        # 关闭应用：on_shutdown 结束所有订阅组生产任务，aiohttp 取消各流任务（其
        # finally 调 leave 释放组）。client.close() 应干净返回（on_shutdown 不抛错）。
        await client.close()
        # 关闭后所有订阅组都被结束并释放
        assert len(manager.groups) == 0
    asyncio.run(main())
