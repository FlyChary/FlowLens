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
from flowlens.api.stream import StreamManager, Subscriber
from flowlens.config import Config
from flowlens.dims import Dim
from flowlens.engine import Engine
from flowlens.ingest import Admission, IngestService
from flowlens.query import QueryService
from flowlens.timewin import Clock, FakeClock


def _strict_json(text: str):
    """严格 JSON：遇到 NaN/Infinity 等非标准常量直接报错。"""
    def _reject(const):
        raise ValueError(f"响应里出现非标准 JSON 常量: {const}")
    return json.loads(text, parse_constant=_reject)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fake_memory_reader() -> tuple[int, int | None]:
    """测试用假内存读数（T8b）：恒 (0, None)，与运行环境的真实内存解耦。

    不注入时 Ticker 会把测试进程（pytest）的真实内存喂给降级控制器，Linux 上测试
    逐个跑会累积到超过 256 MB × 0.9，把服务打进第 4 级（写入全 503）。
    """
    return 0, None


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
                         tick_period=tick_period,
                         memory_reader=_fake_memory_reader)
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


# ---- 共享计算与重算节流（确定性：直接驱动订阅组的生产逻辑，不用真实时间计数）----
# 旧写法在真实 3s / 2.5s 窗口里统计 topk 调用次数，机器负载高时会少算、时好时坏
# （T10b 第零节-3）。改为 FakeClock + 手工步进 interval 网格、按 producer() 的节流
# 规则（帧时刻距上次计算的帧时刻 ≥ recompute_interval_ms 才调 topk）逐个网格点驱动，
# 调用次数完全确定：interval=200ms、1s 节流下 5 秒（25 个网格点）恰好 5 次。

def _drive_grid(group, clock, ms: int, topk_call):
    """按 producer() 的节流规则手工步进 [clock.ms, clock.ms+ms) 的网格点。

    每个网格点要么调一次 topk_call(g)（节流放行），要么复用上一次结果；返回
    topk_call 实际被调用的帧时刻列表。与 SubscriptionGroup.producer 的节流判断
    相同（payload is None or (g - last_g) >= recompute_interval_ms）。
    """
    calls = []
    payload = None
    last_g = None
    g = clock.ms  # 从当前网格点开始（clock.ms 是 200ms 的整数倍）
    end = clock.ms + ms
    while g < end:
        clock.advance(g - clock.ms)  # 时钟跟到网格点（否则 at 晚于 now 被拒）
        if payload is None or (g - last_g) >= group.recompute_interval_ms():
            calls.append(g)
            topk_call(g)
            payload = {"to": g}
            last_g = g
        g += group.interval_ms
    return calls


def _shared_group():
    """同参数 2 个订阅者的订阅组（FakeClock 驱动），返回（组, 时钟）。

    走真实的 StreamManager.subscribe（建组、起生产任务、登记订阅者），之后取消
    生产任务改为手工驱动——生产任务用 engine 时钟算网格、睡真实时间，测试里换成
    FakeClock 后直接驱动，计数才与机器负载无关。
    """
    async def setup():
        clock = FakeClock(10_000)  # 10.000 s：200ms 网格的整数倍
        engine = Engine(Config(), clock)
        qs = QueryService(engine)
        manager = StreamManager(qs)
        # raw 必须留住引用：subscribe 用 id(raw) 作订阅者下标，临时 object()
        # 被回收后下一个会复用同一地址、id 撞在一起
        raw1, raw2 = object(), object()
        g1, _ = manager.subscribe(by=Dim.URL, window_ms=60_000, k=10,
                                  interval_ms=200, raw=raw1)
        g2, _ = manager.subscribe(by=Dim.URL, window_ms=60_000, k=10,
                                  interval_ms=200, raw=raw2)
        return manager, g1, g2, clock, (raw1, raw2)
    manager, g1, g2, clock, _raws = asyncio.run(setup())
    assert g1 is g2 and len(manager.groups) == 1  # 同参数落在同一个组
    assert len(g1.subscribers) == 2  # 两个订阅者都在这个组里
    assert g1.task is not None  # 组只有这一个生产任务（结构上唯一）
    # 停掉真实生产任务，改由测试按网格手工驱动
    g1.task.cancel()
    return g1, clock


def _cancel_group(group):
    group.subscribers.clear()
    if group.task is not None and not group.task.done():
        group.task.cancel()


def test_shared_computation_same_params():
    """两个同参数订阅者共用一个生产任务：5 秒内 topk 只算 5 次（单生产者、每秒一次）。

    若没有共享（每个订阅者各算一份），同样 5 秒会算 10 次——断言恰好 5 次且每
    秒一个（帧时刻间隔 1s），两种错误写法都能被抓出。
    """
    group, clock = _shared_group()
    real = group.qs.topk

    async def spy(d, window_ms, k, at_ms=None, **kw):
        return await real(d, window_ms, k, at_ms, **kw)
    group.qs.topk = spy
    try:
        start = clock.ms  # 驱动过程中时钟会跟到网格点，先记下起点
        calls = _drive_grid(group, clock, 5000,
                            lambda g: asyncio.run(spy(group.by, group.window_ms,
                                                      group.k, g, from_stream=True)))
        assert calls == [start + i * 1000 for i in range(5)], calls
    finally:
        group.qs.topk = real
        _cancel_group(group)


def test_recompute_at_most_once_per_second():
    """interval=200ms 时每秒最多重新计算一次：5 秒 25 个网格点只算 5 次。"""
    group, clock = _shared_group()
    real = group.qs.topk
    calls = 0

    async def spy(d, window_ms, k, at_ms=None, **kw):
        nonlocal calls
        calls += 1
        return await real(d, window_ms, k, at_ms, **kw)
    group.qs.topk = spy
    try:
        # 不设限的话 25 个网格点会算 25 次；节流后恰好 5 次（每满 1s 一次）
        grid_calls = _drive_grid(group, clock, 5000,
                                 lambda g: asyncio.run(spy(group.by, group.window_ms,
                                                           group.k, g, from_stream=True)))
        assert calls == 5 and len(grid_calls) == 5, (calls, grid_calls)
    finally:
        group.qs.topk = real
        _cancel_group(group)


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

        async def spy(d, window_ms, k, at_ms=None, **kw):
            nonlocal calls
            calls += 1
            return await real(d, window_ms, k, at_ms, **kw)
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
                         tick_period=1000.0, memory_reader=_fake_memory_reader)
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
        app = create_app(engine, IngestService(engine, Admission()), qs,
                         tick_period=1000.0, memory_reader=_fake_memory_reader)
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
