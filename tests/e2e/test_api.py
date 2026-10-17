"""T6 e2e：启动真实服务器（真实 TCP 端口），跑通每个接口和每个错误码。

不依赖 pytest-asyncio（dev 依赖只有 pytest）：每个用例自己 asyncio.run 起一个
真实服务器、跑完断言再干净关闭，与 test_ingest / test_query 的 asyncio.run 模式一致。
时钟用 FakeClock 精确控制、不依赖墙钟，保证快照与回放断言确定。

服务器参数与生产 __main__.py 保持一致：auto_decompress=False（gzip 交给
decode_body 分块解压）、lingering_time=0（请求体没读完就返回响应时立即断开，
Content-Length 超 4 MB 的 413 才能立即发出，客户端不必把几 MB 传完）。
"""

import asyncio
import json
import logging
import socket
import time
import zlib

from aiohttp.test_utils import TestClient, TestServer

from flowlens.api.app import create_app
from flowlens.config import Config
from flowlens.engine import Engine
from flowlens.ingest import Admission, IngestService
from flowlens.query import QueryService
from flowlens.timewin import FakeClock

NOW = 1_735_689_600_000


def _gzip(data: bytes) -> bytes:
    c = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    return c.compress(data) + c.flush()


def _serve(run, tick_period: float = 1000.0):
    """起真实服务器，await run(client, engine, clock, app) 后干净关闭。

    tick_period 默认设很大，后台 Ticker 在用例期间不触发；需要快照刷新时用例自己
    engine.tick()，配合 FakeClock 保证确定性。
    """
    async def main():
        clock = FakeClock(NOW)
        engine = Engine(Config(), clock)
        app = create_app(engine, IngestService(engine, Admission()),
                         QueryService(engine), tick_period=tick_period)
        server = TestServer(app)
        client = TestClient(server)
        # TestClient.start_server 不转发参数，直接调 TestServer.start_server
        await server.start_server(auto_decompress=False, lingering_time=0)
        try:
            await run(client, engine, clock, app)
        finally:
            await client.close()
    asyncio.run(main())


def _evs(n, url, ts, user="u"):
    return [{"event_id": f"e{i}", "user_id": user, "url": url,
             "ts": ts, "bytes": 100} for i in range(n)]


def _strict_json(text: str):
    """严格 JSON 解析：遇到 NaN / Infinity 这类非标准常量直接报错。"""
    def _reject(const):
        raise ValueError(f"响应里出现非标准 JSON 常量: {const}")
    return json.loads(text, parse_constant=_reject)


async def _post(client, url, *a, **kw):
    """POST；连接被服务端关掉时（413/503 立即断连）换新连接重试。

    lingering_time=0 下，被拒绝的请求会让服务端立即关连接；aiohttp 客户端的连接
    池可能把这条已死的连接复用给下一条请求，表现为 ConnectionResetError——不是
    服务端行为，重试即可。
    """
    from aiohttp.client_exceptions import ClientOSError
    for _ in range(3):
        try:
            return await client.post(url, *a, **kw)
        except ClientOSError:
            await asyncio.sleep(0.05)
    raise RuntimeError("post 连续 3 次连接被重置")


async def _get(client, url, *a, **kw):
    from aiohttp.client_exceptions import ClientOSError
    for _ in range(3):
        try:
            return await client.get(url, *a, **kw)
        except ClientOSError:
            await asyncio.sleep(0.05)
    raise RuntimeError("get 连续 3 次连接被重置")


# ---- healthz / 基本写入与查询 ----

def test_healthz():
    async def run(client, engine, clock, app):
        r = await _get(client, "/healthz")
        assert r.status == 200
        body = await r.json()
        assert body["ok"] is True
        assert body["events_ingested"] == 0
        # 全部字段
        for f in ("memory_bytes", "degraded", "deduped", "late", "invalid",
                  "tick_errors", "last_tick_ms"):
            assert f in body, f"healthz 缺字段 {f}"
        assert isinstance(body["memory_bytes"], int)
        assert body["tick_errors"] == 0
        assert body["last_tick_ms"] >= 0
    _serve(run)


def test_ingest_then_topk_stats():
    async def run(client, engine, clock, app):
        evs = [{"event_id": f"e{i}", "user_id": f"u{i % 10}", "url": f"/x{i % 5}",
                "ts": NOW - 1000, "bytes": 100} for i in range(100)]
        r = await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
        assert r.status == 202
        body = await r.json()
        assert body["accepted"] == 100 and body["deduped"] == 0
        engine.tick()  # 刷新快照（默认窗口直接读快照）
        r = await _get(client, "/v1/topk", params={"window": "60s", "k": "10", "by": "url"})
        assert r.status == 200
        t = await r.json()
        assert t["by"] == "url" and t["k"] == 10 and t["window_ms"] == 60_000
        assert t["final"] is False and t["degraded"] is False
        assert len(t["items"]) >= 1
        assert set(t["items"][0]) == {"key", "count", "err"}
        assert t["items"][0]["key"] in {"/x0", "/x1", "/x2", "/x3", "/x4"}
        r = await _get(client, "/v1/stats", params={"window": "60s"})
        assert r.status == 200
        s = await r.json()
        assert s["window_ms"] == 60_000
        # 字段名与题目一致，且是整数
        assert s["approx_distinct_users"] > 0
        assert isinstance(s["approx_distinct_users"], int)
        assert "memory_bytes" in s and s["degraded"] is False
        assert "from" in s and "to" in s
    _serve(run)


def test_ingest_gzip():
    async def run(client, engine, clock, app):
        evs = _evs(20, "/z", NOW - 100)
        r = await _post(client, "/v1/ingest", data=_gzip(json.dumps(evs).encode()),
                              headers={"Content-Encoding": "gzip"})
        assert r.status == 202
        body = await r.json()
        assert body["accepted"] == 20
    _serve(run)


# ---- 写入错误码 ----

def test_ingest_error_codes():
    async def run(client, engine, clock, app):
        # 415 不支持的编码
        r = await _post(client, "/v1/ingest", data=b"[]",
                              headers={"Content-Encoding": "br"})
        assert r.status == 415 and "error" in _strict_json(await r.text())
        # 400 非法 JSON
        r = await _post(client, "/v1/ingest", data=b"{not json")
        assert r.status == 400 and "error" in _strict_json(await r.text())
        # 400 顶层不是数组
        r = await _post(client, "/v1/ingest", data=b'{"a": 1}')
        assert r.status == 400
        # 413 gzip 解压后超过 4 MB（压缩后很小，能过 RawBudget，
        # 走 decode_body 的分块解压上限）
        bomb = _gzip(b"\0" * (20 * 1024 * 1024))
        assert len(bomb) < 1024 * 1024  # 压缩后确实很小
        r = await _post(client, "/v1/ingest", data=bomb,
                              headers={"Content-Encoding": "gzip"})
        assert r.status == 413 and "error" in _strict_json(await r.text())
    _serve(run)


def test_content_length_over_4mb_rejected_without_reading():
    """Content-Length 超过 4 MB：服务端不读请求体，立即 413。

    用原始 socket 只发请求头、不发请求体：若服务端会读体，响应就会一直卡着；
    另加一个 spy 记录 IngestService 是否被调用（必须没被调用）。
    """
    calls = []

    async def main():
        clock = FakeClock(NOW)
        engine = Engine(Config(), clock)
        svc = IngestService(engine, Admission())
        real = svc.ingest

        async def spy(body, enc):
            calls.append(len(body))
            return await real(body, enc)

        svc.ingest = spy
        app = create_app(engine, svc, QueryService(engine), tick_period=1000.0)
        server = TestServer(app)
        client = TestClient(server)
        await server.start_server(auto_decompress=False, lingering_time=0)
        try:
            port = server.port

            def raw_headers_only():
                s = socket.create_connection(("127.0.0.1", port))
                s.settimeout(10)
                s.sendall(b"POST /v1/ingest HTTP/1.1\r\n"
                          b"Host: 127.0.0.1\r\n"
                          b"Content-Length: 5243000\r\n"  # 5 MB > 4 MB
                          b"\r\n")  # 注意：请求体一个字节都不发
                t0 = time.time()
                resp = b""
                while b"\r\n\r\n" not in resp:
                    part = s.recv(65536)
                    if not part:
                        break
                    resp += part
                return time.time() - t0, resp

            dt, resp = await asyncio.get_running_loop().run_in_executor(None, raw_headers_only)
            head, _, body = resp.partition(b"\r\n\r\n")
            assert b"413" in head.split(b" ")[1:2] or head.startswith(b"HTTP/1.1 413"), head
            err = _strict_json(body)
            assert "error" in err
            assert dt < 5.0  # 立即返回，没有等请求体
            assert calls == []  # 服务端没有读请求体，IngestService 从未被调用
        finally:
            await client.close()
    asyncio.run(main())


def test_chunked_body_over_4mb_rejected():
    async def run(client, engine, clock, app):
        # 分块传输（无 Content-Length），累计超过 4 MB → 413
        from aiohttp import payload
        big = b"y" * (5 * 1024 * 1024)
        r = await _post(client, "/v1/ingest",
                              data=payload.BytesPayload(big), chunked=True)
        assert r.status == 413 and "error" in _strict_json(await r.text())
        # 分块的小请求正常
        evs = _evs(5, "/c", NOW - 100)
        r = await _post(client, "/v1/ingest",
                              data=payload.BytesPayload(json.dumps(evs).encode()),
                              chunked=True)
        assert r.status == 202
        assert (await r.json())["accepted"] == 5
    _serve(run)


def test_raw_budget_full_503_and_retry_not_deduped():
    async def run(client, engine, clock, app):
        # 占满 RawBudget（8 MB），任何新请求都放不下 → 503
        budget = app["raw_budget"]
        assert budget.try_reserve(budget.limit)
        evs = _evs(5, "/p", NOW - 100)
        r = await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
        assert r.status == 503
        assert r.headers.get("Retry-After") == "1"
        assert "error" in _strict_json(await r.text())
        # 被拒绝的请求没写去重状态（CLAUDE.md 规则 2）：释放后原样重试
        # 应被正常接受、不判重——若写进了 Bloom，这里会被当成重复丢掉
        budget.release(budget.limit)
        r2 = await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
        assert r2.status == 202
        body = await r2.json()
        assert body["accepted"] == 5 and body["deduped"] == 0
    _serve(run)


# ---- topk / stats 参数校验 ----

def test_topk_param_errors():
    async def run(client, engine, clock, app):
        for params, code in [
            ({"window": "0s"}, 400),
            ({"window": "700s"}, 400),
            ({"window": "abc"}, 400),
            ({"k": "0"}, 400),
            ({"k": "101"}, 400),
            ({"by": "foo"}, 400),
            # at 只接受非负整数
            ({"at": "-1"}, 400),
            ({"at": "1.5"}, 400),
            ({"at": "abc"}, 400),
            # interval 格式错误 / 越界
            ({"interval": "abc"}, 400),
            ({"interval": "100"}, 400),       # 100ms < 200ms
            ({"interval": "100ms"}, 400),     # 同上
            ({"interval": "120s"}, 400),      # > 60s
            ({"interval": "-5s"}, 400),       # 负数
        ]:
            r = await _get(client, "/v1/topk", params=params)
            assert r.status == code, (params, r.status)
        # 合法 interval 写法都能过（默认窗口、无数据，200）
        for ok in ("500ms", "1s", "2", "60s", "200ms"):
            r = await _get(client, "/v1/topk", params={"interval": ok})
            assert r.status == 200, (ok, r.status)
        # at 在未来 → 400
        r = await _get(client, "/v1/topk", params={"at": str(NOW + 10_000)})
        assert r.status == 400
        # 区间早于保留期 600 秒 → 422 TooOld
        r = await _get(client, "/v1/topk",
                             params={"at": str(NOW - 700_000), "window": "60s"})
        assert r.status == 422
    _serve(run)


# ---- 404 / 405 / 500 的响应体 ----

def test_not_found_and_method_not_allowed_json():
    async def run(client, engine, clock, app):
        r = await _get(client, "/nope")
        assert r.status == 404
        body = _strict_json(await r.text())
        assert set(body) == {"error"}
        # 405：保留 Allow 响应头
        r = await _get(client, "/v1/ingest")
        assert r.status == 405
        assert r.headers.get("Allow") is not None
        body = _strict_json(await r.text())
        assert set(body) == {"error"}
        r = await _post(client, "/v1/topk", data=b"[]")
        assert r.status == 405 and "error" in _strict_json(await r.text())
    _serve(run)


def test_unexpected_error_500_with_log():
    async def run(client, engine, clock, app):
        import io
        # 让 stats 依赖抛一个意外异常 → 500 + {"error": "internal error"}
        qs = app["query_service"]
        real_stats = qs.stats

        async def boom(window_ms):
            raise RuntimeError("test: 故意抛的意外异常 boom-marker")
        qs.stats = boom
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        app_log = logging.getLogger("flowlens.api.app")
        old_level, old_handlers = app_log.level, app_log.handlers
        app_log.addHandler(handler)
        app_log.setLevel(logging.INFO)
        try:
            r = await _get(client, "/v1/stats", params={"window": "60s"})
            assert r.status == 500
            body = _strict_json(await r.text())
            assert set(body) == {"error"}
            assert body["error"] == "internal error"
        finally:
            qs.stats = real_stats
            app_log.removeHandler(handler)
            app_log.level, app_log.handlers = old_level, old_handlers
        # 日志里有完整异常信息（含异常消息与 traceback）
        logged = buf.getvalue()
        assert "boom-marker" in logged
        assert "Traceback" in logged
    _serve(run)


# ---- stats：字段名、空区间 p95 为 null、严格 JSON ----

def test_stats_empty_window_null_p95_strict_json():
    async def run(client, engine, clock, app):
        # 不写任何数据 → p95 为 NaN → 响应里必须是 null（不能出现 NaN 字面量）
        r = await _get(client, "/v1/stats", params={"window": "60s"})
        assert r.status == 200
        text = await r.text()
        assert "NaN" not in text and "Infinity" not in text
        s = _strict_json(text)  # 严格解析：出现 NaN/Infinity 会直接抛错
        assert s["approx_p95_bytes"] is None
        assert s["approx_distinct_users"] == 0
        # 字段名与题目一致
        assert set(s) == {"window_ms", "from", "to", "approx_distinct_users",
                          "approx_p95_bytes", "memory_bytes", "degraded"}
    _serve(run)


# ---- topk：count/err 整数化、带 at 回放 ----

def test_topk_count_integer_format():
    async def run(client, engine, clock, app):
        # 50 条同一 URL：count 应为整数 50
        evs = _evs(50, "/hot", NOW - 1000)
        r = await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
        assert r.status == 202
        engine.tick()
        r = await _get(client, "/v1/topk", params={"window": "60s", "k": "10", "by": "url"})
        assert r.status == 200
        t = _strict_json(await r.text())
        top = t["items"][0]
        assert top["key"] == "/hot"
        assert top["count"] == 50 and isinstance(top["count"], int)
        assert isinstance(top["err"], int)
    _serve(run)


def test_topk_with_at_replays():
    async def run(client, engine, clock, app):
        # 先写入“现在”的一批数据，再推进时钟 90 秒让它定稿进树，
        # 最后带 at 回放一个已终态的历史区间（to 早于 watermark）
        evs = _evs(50, "/hot", NOW - 1000)
        r = await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
        assert r.status == 202
        clock.advance(90_000)
        engine.tick()  # 推进环 + 定稿进树 + 刷快照
        # 回放 [NOW-60s, NOW)：to=NOW 早于 watermark(NOW+90s-60s)=NOW+30s → final
        r = await _get(client, "/v1/topk",
                             params={"window": "60s", "k": "10", "by": "url",
                                     "at": str(NOW)})
        assert r.status == 200
        t = await r.json()
        assert t["to"] == NOW and t["from"] == NOW - 60_000
        assert t["final"] is True  # 区间已定稿，结果终态
        assert "/hot" in [it["key"] for it in t["items"]]
    _serve(run)


# ---- 应用关闭时 Ticker 正常结束 ----

def test_ticker_stops_cleanly_on_shutdown():
    async def main():
        clock = FakeClock(NOW)
        engine = Engine(Config(), clock)
        app = create_app(engine, IngestService(engine, Admission()),
                         QueryService(engine), tick_period=0.05)
        server = TestServer(app)
        client = TestClient(server)
        await server.start_server(auto_decompress=False, lingering_time=0)
        try:
            # tick_period=0.05：等用例期间跑几拍，确认后台任务确实在运行
            await asyncio.sleep(0.2)
            assert app["ticker"].tick_count >= 1
        finally:
            # client.close() 触发 on_shutdown：停 Ticker、取消并等待任务
            await client.close()
        task = app["ticker_task"]
        # 关闭后任务已结束（cancel 让退出即时），且不是"没跑就没了"
        assert task.done()
        assert task.cancelled()  # 用例还没跑满一个周期，走的是取消路径
        assert app["ticker"].tick_count >= 1
    asyncio.run(main())
