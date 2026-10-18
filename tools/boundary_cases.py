"""边界用例（T10a）：题目要求的自测用例，两种形式共用同一套定义。

- tests/e2e/test_boundaries.py：进程内起真实服务器（aiohttp TestServer），pytest 运行；
- tools/selftest.py：对一个正在运行的服务地址（--url）逐项检查，打印表格。

两者都通过同一个 ``Target`` 接口访问服务（进程内用 TestClient、远端用
ClientSession），因此用例完全相同。每个用例是 ``async (Target) -> CaseResult``，
返回"期望 / 实际 / 通过与否"。
"""

import asyncio
import json
import socket
import time
import zlib

from dataclasses import dataclass

from flowgen import now_ms


def _gzip(data: bytes) -> bytes:
    c = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    return c.compress(data) + c.flush()


def _strict_json(text: str) -> dict:
    """严格 JSON：出现 NaN / Infinity 等非标准常量直接报错。"""
    def _reject(const):
        raise ValueError(f"响应里出现非标准 JSON 常量: {const}")
    return json.loads(text, parse_constant=_reject)


@dataclass
class CaseResult:
    name: str
    expected: str
    actual: str
    passed: bool


@dataclass
class Target:
    """访问服务的统一接口（进程内 TestClient / 远端 ClientSession）。

    client 需提供 ``.post(url, data=, headers=)`` 与 ``.get(url, params=,
    headers=)``，返回的对象带 ``.status`` / ``.headers`` / ``.text()`` /
    ``.json()`` / ``.content``；``open_stream`` 另封装 SSE 的长连接。
    host / port 供原始 socket 用例（Content-Length 超限不读体）用。
    """

    client: object
    host: str
    port: int
    base: str = ""          # 远端为 http://host:port；进程内为 ""（相对路径）

    def url(self, path: str) -> str:
        return self.base + path

    async def get(self, path: str, params: dict | None = None,
                  headers: dict | None = None):
        return await self.client.get(self.url(path), params=params, headers=headers)

    async def post_ingest(self, data: bytes, headers: dict | None = None,
                          **kw):
        return await self.client.post(self.url("/v1/ingest"), data=data,
                                      headers=headers, **kw)

    async def raw_ingest_headers_only(self, content_length: int,
                                      timeout: float = 10.0) -> tuple[float, str, str]:
        """只发请求头、不发请求体（Content-Length 声明超大）。

        返回 (耗时秒, 响应行, 响应体)。若服务端会去读请求体，响应会一直卡着直到
        超时——用"快速返回 413"来证明服务端没读体。
        """
        def _do():
            s = socket.create_connection((self.host, self.port), timeout=timeout)
            s.settimeout(timeout)
            s.sendall(b"POST /v1/ingest HTTP/1.1\r\n"
                      b"Host: 127.0.0.1\r\n"
                      b"Content-Type: application/json\r\n"
                      + f"Content-Length: {content_length}\r\n".encode()
                      + b"\r\n")  # 请求体一个字节都不发
            t0 = time.time()
            buf = b""
            while b"\r\n\r\n" not in buf:
                part = s.recv(65536)
                if not part:
                    break
                buf += part
            # 再读一点响应体（413 是立即返回的，有体）
            try:
                while True:
                    part = s.recv(65536)
                    if not part:
                        break
                    buf += part
                    if len(buf) > 4096:
                        break
            except socket.timeout:
                pass
            s.close()
            return time.time() - t0, buf.decode("latin-1")
        return await asyncio.get_running_loop().run_in_executor(None, _do)

    # ---- SSE ----
    async def open_stream(self, path: str, headers: dict | None = None):
        return await self.client.get(self.url(path), headers=headers or {})

    @staticmethod
    async def read_block(resp, timeout: float = 20.0) -> str | None:
        """读一块 SSE（到空行 \\n\\n 为止）；流结束返回 None。"""
        try:
            raw = await asyncio.wait_for(resp.content.readuntil(b"\n\n"), timeout=timeout)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, EOFError):
            return None
        return raw.decode()

    @staticmethod
    def parse_block(text: str) -> dict:
        out = {"kind": None, "id": None, "event": None, "data": ""}
        lines = [ln for ln in text.split("\n") if ln != ""]
        if not lines:
            return out
        if lines[0].startswith("retry:"):
            out["kind"] = "retry"
            out["data"] = lines[0][6:].strip()
            return out
        if lines[0].startswith(":"):
            out["kind"] = "ping"
            return out
        out["kind"] = "event"
        for ln in lines:
            if ln.startswith("id:"):
                out["id"] = ln[3:].strip()
            elif ln.startswith("event:"):
                out["event"] = ln[6:].strip()
            elif ln.startswith("data:"):
                out["data"] = ln[5:].lstrip()
        return out


def _ev(i, url, ts, user="u", eid=None):
    return {"event_id": eid if eid is not None else f"e{i}",
            "user_id": user, "url": url, "ts": ts, "bytes": 100}


async def run_case(name, expected, fn, *a, **kw) -> CaseResult:
    try:
        actual, ok = await fn(*a, **kw)
    except Exception as e:  # 用例本身抛异常 = 失败
        return CaseResult(name, expected, f"异常: {e!r}", False)
    return CaseResult(name, expected, actual, ok)


# ---- 用例定义：每个返回 (actual 描述, 是否通过) ----

async def c01_empty_array(t: Target):
    r = await t.post_ingest(b"[]")
    ok = r.status == 202
    b = await r.json()
    ok = ok and b.get("accepted") == 0 and b.get("deduped") == 0 \
        and b.get("late") == 0 and b.get("invalid") == 0
    return f"status={r.status}, {b}", ok


async def c02_bad_json(t: Target):
    r = await t.post_ingest(b"{not json")
    ok = r.status == 400 and "error" in _strict_json(await r.text())
    return f"status={r.status}", ok


async def c03_not_array(t: Target):
    r = await t.post_ingest(b'{"a": 1}')
    ok = r.status == 400 and "error" in _strict_json(await r.text())
    return f"status={r.status}", ok


async def c04_partial_invalid(t: Target):
    now = now_ms()
    evs = [
        _ev(0, "/ok", now - 100),
        {**_ev(1, "/ok", now - 100), "ts": "x"},  # ts 类型错
        {"event_id": "x", "url": "/ok", "ts": now - 100, "bytes": 1},  # user_id 缺
        _ev(2, "/ok", now - 100),
    ]
    import orjson
    r = await t.post_ingest(orjson.dumps(evs))
    b = await r.json()
    ok = r.status == 202 and b.get("invalid") == 2 and b.get("accepted") == 2
    return f"status={r.status}, {b}", ok


async def c05_intra_batch_dup(t: Target):
    now = now_ms()
    # 同一批里同一个 event_id 出现两次
    evs = [_ev(0, "/d", now - 100, eid="same"), _ev(1, "/d", now - 100, eid="same"),
           _ev(2, "/d", now - 100, eid="other")]
    import orjson
    r = await t.post_ingest(orjson.dumps(evs))
    b = await r.json()
    ok = r.status == 202 and b.get("deduped") == 1 and b.get("accepted") == 2
    return f"status={r.status}, {b}", ok


async def c06_cross_batch_dup(t: Target):
    now = now_ms()
    evs = [_ev(0, "/c", now - 100, eid="x1"), _ev(1, "/c", now - 100, eid="x2")]
    import orjson
    body = orjson.dumps(evs)
    r1 = await t.post_ingest(body)
    b1 = await r1.json()
    r2 = await t.post_ingest(body)  # 原样重发
    b2 = await r2.json()
    ok = r1.status == 202 and r2.status == 202 \
        and b1.get("deduped") == 0 and b1.get("accepted") == 2 \
        and b2.get("deduped") == 2 and b2.get("accepted") == 0
    return f"1st={b1} 2nd={b2}", ok


async def c07_late_event(t: Target):
    now = now_ms()
    evs = [_ev(0, "/l", now - 90_000, eid=f"late-{now}")]  # 90s 前 > 60s 迟到上限
    import orjson
    r = await t.post_ingest(orjson.dumps(evs))
    b = await r.json()
    ok = r.status == 202 and b.get("late") == 1 and b.get("accepted") == 0
    return f"status={r.status}, {b}", ok


async def c08_future_event(t: Target):
    now = now_ms()
    evs = [_ev(0, "/f", now + 20_000, eid=f"fut-{now}")]  # 20s 后 > 5s 未来容差
    import orjson
    r = await t.post_ingest(orjson.dumps(evs))
    b = await r.json()
    ok = r.status == 202 and b.get("invalid") == 1 and b.get("accepted") == 0
    return f"status={r.status}, {b}", ok


async def c09_gzip(t: Target):
    now = now_ms()
    evs = [_ev(i, "/gz", now - 100, eid=f"gz-{now}-{i}") for i in range(20)]
    import orjson
    r = await t.post_ingest(_gzip(orjson.dumps(evs)),
                            headers={"Content-Encoding": "gzip"})
    b = await r.json()
    ok = r.status == 202 and b.get("accepted") == 20
    return f"status={r.status}, {b}", ok


async def c10_gzip_bomb(t: Target):
    bomb = _gzip(b"\0" * (20 * 1024 * 1024))
    r = await t.post_ingest(bomb, headers={"Content-Encoding": "gzip"})
    ok = r.status == 413 and "error" in _strict_json(await r.text())
    return f"status={r.status}", ok


async def c11_content_length_over_4mb(t: Target):
    # 只发头、不发体；服务端应快速 413（没去读 5 MB 的体）
    dt, resp = await t.raw_ingest_headers_only(5 * 1024 * 1024 + 120, timeout=10)
    head, _, body = resp.partition("\r\n\r\n")
    is413 = head.startswith("HTTP/1.1 413") or " 413 " in head
    ok = is413 and dt < 5.0
    return f"status={'413' if is413 else head.splitlines()[0]}, dt={dt:.2f}s", ok


async def c12_unsupported_encoding(t: Target):
    r = await t.post_ingest(b"[]", headers={"Content-Encoding": "br"})
    ok = r.status == 415 and "error" in _strict_json(await r.text())
    return f"status={r.status}", ok


async def c13_at_before_retention(t: Target):
    # at 早于保留期（now − 600s − 迟到上限 60s = now − 660s）→ 422
    at = now_ms() - 700_000
    r = await t.get("/v1/topk", params={"at": str(at), "window": "60s"})
    ok = r.status == 422 and "error" in _strict_json(await r.text())
    return f"status={r.status}", ok


async def c14_param_out_of_range(t: Target):
    ok_all = True
    outs = []
    for params in ({"k": "0"}, {"k": "101"}, {"window": "700s"}, {"by": "foo"}):
        r = await t.get("/v1/topk", params=params)
        ok = r.status == 400
        ok_all = ok_all and ok
        outs.append(f"{params}={r.status}")
    return " ".join(outs), ok_all


async def c15_post_to_topk_405(t: Target):
    r = await t.client.post(t.url("/v1/topk"), data=b"[]")
    body = _strict_json(await r.text())
    ok = r.status == 405 and "error" in body
    return f"status={r.status}", ok


async def c16_not_found_404(t: Target):
    r = await t.get("/definitely-not-a-path")
    body = _strict_json(await r.text())
    ok = r.status == 404 and "error" in body
    return f"status={r.status}", ok


async def c17_stats_empty_p95_null(t: Target):
    # 用一个 600 秒前的 60 秒窗口（起点 = now−660s，恰在保留期下界）——对"刚启动
    # 不久"的服务这段区间必无数据；对新测试服务（FakeClock、没写过数据）任意区间
    # 都无数据。两条路径下 p95 都应为 null（而非 NaN 字面量）。
    at = now_ms() - 600_000
    r = await t.get("/v1/stats", params={"window": "60s", "at": str(at)})
    text = await r.text()
    s = _strict_json(text)
    ok = r.status == 200 and s.get("approx_p95_bytes") is None \
        and "NaN" not in text and "Infinity" not in text
    return f"status={r.status}, p95={s.get('approx_p95_bytes')}", ok


async def c18_sse_frames_and_reconnect(t: Target):
    import orjson
    now = now_ms()
    evs = [_ev(i, f"/s{i % 5}", now - 100, eid=f"sse-{now}-{i}") for i in range(50)]
    await t.post_ingest(orjson.dumps(evs))
    url = "/v1/topk/stream?window=60s&k=10&by=url&interval=500ms"
    resp = await t.open_stream(url)
    try:
        # 第一块 retry: 3000
        b = t.parse_block(await t.read_block(resp))
        assert b["kind"] == "retry" and b["data"] == "3000", b
        # 读 3 个数据帧，记下 id
        ids = []
        for _ in range(3):
            f = t.parse_block(await t.read_block(resp))
            assert f["kind"] == "event" and f["event"] == "topk", f
            _strict_json(f["data"])  # data 是合法 JSON
            ids.append(int(f["id"]))
        last = ids[-1]
    finally:
        resp.close()
    # 带 Last-Event-ID 重连：已见帧不重发（≥ last+500）、落在 500ms 网格上、
    # 之后 id 连续（+500）。不要求恰好 last+500：若第一个连接断开后订阅组已被
    # 释放，重连会开新组、新生产者从"当前网格点"起产帧，首帧是 ≥ last+500 的
    # 下一个网格点（负载高时重连可能跨过一格）——"id 连续、不重发已见帧"仍是
    # 硬要求，首帧精确等于 last+interval 只在"旧组还活着"时成立。
    resp2 = await t.open_stream(url, headers={"Last-Event-ID": str(last)})
    try:
        await t.read_block(resp2)  # retry
        got = []
        t0 = time.monotonic()
        while len(got) < 3 and time.monotonic() - t0 < 10:
            f = t.parse_block(await t.read_block(resp2))
            if f["kind"] == "event":
                got.append(int(f["id"]))
        ok = (len(got) >= 3
              and got[0] >= last + 500 and got[0] % 500 == 0
              and got[0] <= last + 500 * 4
              and all(b - a == 500 for a, b in zip(got, got[1:])))
        return f"first={ids} reconnect_first={got}", ok
    finally:
        resp2.close()


CASES: list[tuple[str, str, object]] = [
    ("空数组", "202，各计数 0", c01_empty_array),
    ("非法 JSON", "400", c02_bad_json),
    ("顶层不是数组", "400", c03_not_array),
    ("部分事件非法", "202，invalid=2，其余 2 条被接受", c04_partial_invalid),
    ("批内重复", "deduped=1，accepted=2", c05_intra_batch_dup),
    ("跨批重复", "第二次全部计为 deduped", c06_cross_batch_dup),
    ("迟到事件", "late=1", c07_late_event),
    ("未来事件", "计入 invalid", c08_future_event),
    ("gzip 请求体", "正常写入 20 条", c09_gzip),
    ("解压炸弹", "413", c10_gzip_bomb),
    ("Content-Length 超 4 MB", "413，且服务端不读请求体", c11_content_length_over_4mb),
    ("不支持的 Content-Encoding", "415", c12_unsupported_encoding),
    ("at 早于保留期", "422", c13_at_before_retention),
    ("参数越界 k=0/101、window=700s、by=foo", "400", c14_param_out_of_range),
    ("对 /v1/topk 发 POST", "405，响应体是 JSON", c15_post_to_topk_405),
    ("不存在的路径", "404，响应体是 JSON", c16_not_found_404),
    ("区间无数据 /v1/stats", "approx_p95_bytes 为 null，严格 JSON", c17_stats_empty_p95_null),
    ("SSE 帧 + Last-Event-ID 重连", "帧格式正确；重连后 id 连续", c18_sse_frames_and_reconnect),
]


async def run_all(t: Target) -> list[CaseResult]:
    """逐项跑全部用例，返回 CaseResult 列表。"""
    results = []
    for name, expected, fn in CASES:
        results.append(await run_case(name, expected, fn, t))
    return results
