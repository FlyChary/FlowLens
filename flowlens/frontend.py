"""前端进程（P2d-1 第四节）：SO_REUSEPORT 共享 8080 端口，把写入请求解析后发给引擎。

前端不碰任何统计状态：POST /v1/ingest 在本进程解压、按解压后字节预留、parse_body
解析，再把紧凑的 WireBatch 经写入通道（ipc）发给引擎；引擎去重、写桶、回 ACK。
其他一切请求（topk / stats / healthz / SSE / 不存在的路径 / 错误方法）原样转发给
引擎的内部 HTTP——HTTP 语义只在引擎一处定义。

前端进程由引擎用 spawn 拉起（supervisor.FrontSupervisor），不继承引擎的大块统计
内存；它自己 import 完成处理所需的模块（ingest 的纯函数、parse_body、supervisor
的共享视图、engine.IngestResult 这个纯数据类），不 import 引擎的统计结构。
"""

import asyncio
import itertools
import logging
import signal

from aiohttp import ClientSession, ClientTimeout, UnixConnector, web

from .api.app import (_error_middleware, _ingest_expect, _json_response,
                      _read_raw_body, degrade_controller_key, raw_budget_key)
from .engine import IngestResult
from .errors import (BadRequest, FlowLensError, Overloaded, PayloadTooLarge,
                     TooOld, UnsupportedEncoding)
from .ingest import MAX_PENDING_BYTES, decode_body
from .ipc import MSG_ACK, MSG_BATCH, encode_parsed, read_msg, send_msg
from .parse_pool import parse_body
from .supervisor import DegradeView, SlotAdmission

log = logging.getLogger("flowlens.frontend")


def _error_from_ack(msg) -> FlowLensError:
    """按 status 把引擎的错误负载 (req_id, "error", status, message, retry_after)
    还原成对应的异常；前端的错误中间件再转成与单进程完全一致的 HTTP 响应。"""
    status, message, retry_after = msg[2], msg[3], msg[4]
    if status == 400:
        return BadRequest(message)
    if status == 413:
        return PayloadTooLarge(message)
    if status == 415:
        return UnsupportedEncoding(message)
    if status == 422:
        return TooOld(message)
    # 503 与未知状态都按 Overloaded 处理（引擎写入路径只会产生这五类）
    return Overloaded(message, retry_after=retry_after if retry_after else 1)


class EngineConnection:
    """前端到引擎写入通道的一条连接：请求按 req_id 关联，ACK 回来唤醒对应 future。

    多个并发写入共享这条连接，靠 req_id 区分；发送用锁串行化，保证一条消息的
    头与体连续写出（不与其他消息交错）。连接断开时，所有在途请求以 Overloaded
    失败（引擎侧尚未去重，调用方重试安全），并回调 on_lost 让前端主协程退出。
    """

    def __init__(self, reader, writer, on_lost):
        self._r, self._w, self._on_lost = reader, writer, on_lost
        self._ids = itertools.count(1)
        self._waiting: dict[int, asyncio.Future] = {}
        self._send_lock = asyncio.Lock()
        self._task = asyncio.create_task(self._read_loop())

    async def submit(self, parsed) -> IngestResult:
        """把一个 ParsedBatch 发给引擎并等待写入结果（IngestResult）。"""
        req_id = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._waiting[req_id] = fut
        try:
            async with self._send_lock:   # 一条消息的头和体必须连续写出
                await send_msg(self._w, MSG_BATCH, encode_parsed(req_id, parsed))
            return await fut
        finally:
            self._waiting.pop(req_id, None)

    async def _read_loop(self):
        try:
            while True:
                mtype, msg = await read_msg(self._r)
                if mtype != MSG_ACK:
                    continue
                fut = self._waiting.get(msg[0])
                if fut is None or fut.done():
                    continue
                if msg[1] == "ok":
                    # ACK 的 ok 负载 (req_id, "ok", accepted, deduped, late, invalid)
                    # —— IngestResult 字段顺序 (accepted, deduped, late, invalid)，已按实际定义确认
                    fut.set_result(IngestResult(*msg[2:6]))
                else:
                    fut.set_exception(_error_from_ack(msg))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            for fut in self._waiting.values():
                if not fut.done():
                    fut.set_exception(Overloaded("引擎连接中断，请重试", retry_after=1))
            self._on_lost()


class FrontIngestService:
    """前端本地的写入处理。顺序与 IngestService.ingest 完全一致：
    解压 → 按解压后字节预留 → 解析 → 发给引擎 → finally 释放。

    额度（admission）是前端本地的 SlotAdmission，同时在共享数组里镜像占用与拒绝；
    正常/收紧两个上限按 shared.tight 标志**每次请求时**选择（第 3 级起收紧，由引擎
    翻转 tight 标志，见 supervisor.FrontsBudgetView）。
    """

    def __init__(self, conn: EngineConnection, admission: "SlotAdmission",
                 shared, normal_limit: int, tight_limit: int):
        self.conn = conn
        self.admission = admission
        self._s = shared
        self._normal = normal_limit
        self._tight = tight_limit

    async def ingest(self, body: bytes, content_encoding: str | None) -> IngestResult:
        # 按 shared.tight 选当前上限（第 3 级起收紧到 tight）
        self.admission.limit = self._tight if self._s.tight.value else self._normal
        raw = decode_body(body, content_encoding)
        n = len(raw)
        if not self.admission.try_reserve(n):
            # 与 IngestService.ingest 的两种错误信息保持一致
            if self.admission.limit < MAX_PENDING_BYTES:
                mb = self.admission.limit // (1024 * 1024)
                raise Overloaded(
                    f"降级期间单个请求的上限暂时为 {mb} MB，请稍后重试", retry_after=1)
            raise Overloaded(retry_after=1)
        try:
            parsed = parse_body(raw)   # P2d-2 改为分块、块间让出
            return await self.conn.submit(parsed)
        finally:
            self.admission.release(n)


def per_front_limit(total: int, n: int, floor: int) -> int:
    """总额度按前端数平均，但每个前端的正常额度不能低于单请求上限 floor，
    否则一个合法的大请求（最大 floor 字节）永远预留不到。"""
    return max(total // n, floor)


front_ingest_key = web.AppKey("front_ingest", FrontIngestService)
upstream_key = web.AppKey("upstream", ClientSession)

# 转发时保留的请求/响应头（其余丢弃）
_FWD_REQ = {"accept", "last-event-id", "content-type", "content-length", "content-encoding"}
_FWD_RESP = {"content-type", "retry-after", "cache-control", "x-accel-buffering"}


async def _front_ingest(request: web.Request) -> web.Response:
    """与 app.py 的 _ingest 逐行对应，只是写入服务换成 FrontIngestService。"""
    if request.app[degrade_controller_key].reject_ingest:
        raise Overloaded("降级期间暂停写入", retry_after=1)
    body = await _read_raw_body(request, request.app[raw_budget_key])
    r = await request.app[front_ingest_key].ingest(
        body, request.headers.get("Content-Encoding"))
    return _json_response({"accepted": r.accepted, "deduped": r.deduped,
                           "late": r.late, "invalid": r.invalid}, status=202)


async def _proxy(request: web.Request) -> web.StreamResponse:
    """POST /v1/ingest 以外的一切请求（含不存在的路径、错误的方法）原样交给引擎，
    HTTP 语义只在引擎一处定义。

    需要验证项的实测结论（aiohttp 3.14）：路径匹配但方法不匹配时，路由会继续尝试
    后面的资源——所以 GET /v1/ingest、PUT /v1/topk、任意 404 都落到本处理器转发给
    引擎，由引擎返回 405/404；前端**不需要**为这些路径显式注册 405/404 处理器
    （端到端用例会再验证一次）。
    """
    headers = {k: v for k, v in request.headers.items() if k.lower() in _FWD_REQ}
    data = await request.read() if request.can_read_body else None
    sess = request.app[upstream_key]
    try:
        up_cm = sess.request(request.method, "http://engine" + request.rel_url.path_qs,
                             headers=headers, data=data, allow_redirects=False)
        async with up_cm as up:
            resp = web.StreamResponse(status=up.status, reason=up.reason)
            for k, v in up.headers.items():
                if k.lower() in _FWD_RESP:
                    resp.headers[k] = v
            if up.content_length is not None:
                resp.content_length = up.content_length
            await resp.prepare(request)
            async for chunk in up.content.iter_any():   # SSE：边收边发
                await resp.write(chunk)
            await resp.write_eof()
            return resp
    except (ConnectionError, OSError):
        raise Overloaded("引擎不可用，请重试", retry_after=1)


def create_front_app(conn, shared, slot, n_fronts, query_sock: str,
                     adm_normal: int, adm_tight: int,
                     raw_normal: int, raw_tight: int) -> web.Application:
    """组装一个前端进程的 HTTP 应用。

    raw_budget 放 SlotAdmission（_read_raw_body / _ingest_expect 只依赖这个键）、
    degrade 放 DegradeView；额度镜像到共享数组的第 slot 槽。
    """
    app = web.Application(client_max_size=0, middlewares=[_error_middleware])
    app[raw_budget_key] = SlotAdmission(raw_normal, shared.raw_pending,
                                        shared.raw_rejected, slot)
    app[degrade_controller_key] = DegradeView(shared)
    app[front_ingest_key] = FrontIngestService(
        conn, SlotAdmission(adm_normal, shared.pending, shared.ingest_rejected, slot),
        shared, adm_normal, adm_tight)
    app.router.add_post("/v1/ingest", _front_ingest, expect_handler=_ingest_expect)
    # catch-all：除 POST /v1/ingest 外的一切（含错误方法、不存在路径）转发给引擎
    app.router.add_route("*", "/{tail:.*}", _proxy)

    async def _open(app):
        app[upstream_key] = ClientSession(
            connector=UnixConnector(path=query_sock), auto_decompress=False,
            timeout=ClientTimeout(total=None))   # SSE 不能有总超时
    async def _close(app):
        await app[upstream_key].close()
    app.on_startup.append(_open)
    app.on_cleanup.append(_close)
    return app


def front_main(slot, host, port, ingest_sock, query_sock, shared,
               adm_normal, adm_tight, raw_normal, raw_tight, lingering):
    """spawn 的目标函数（模块级、可 pickle）：起前端 HTTP 应用、连引擎、等信号退出。

    每前端的额度（正常/收紧、在途/原始体）由引擎在 FrontSupervisor 里用
    per_front_limit 算好后作为参数传入（spawn 子进程不继承主进程内存，但
    sharedctypes 与基本类型可以 pickle 传递）。
    """
    logging.basicConfig(level=logging.INFO,
                        format=f"%(asctime)s %(levelname)s front{slot} %(name)s %(message)s")
    asyncio.run(_front_run(slot, host, port, ingest_sock, query_sock, shared,
                           adm_normal, adm_tight, raw_normal, raw_tight, lingering))


async def _front_run(slot, host, port, ingest_sock, query_sock, shared,
                     adm_normal, adm_tight, raw_normal, raw_tight, lingering) -> None:
    stop = asyncio.Event()
    # 引擎先启动前端、再分配内存和监听，所以这里要重试连接（最多约 30 秒）
    reader = writer = None
    for _ in range(300):
        try:
            reader, writer = await asyncio.open_unix_connection(ingest_sock)
            break
        except (FileNotFoundError, ConnectionRefusedError):
            await asyncio.sleep(0.1)
    if reader is None:
        raise SystemExit(f"front{slot}: 无法连接引擎写入通道 {ingest_sock}")
    conn = EngineConnection(reader, writer, on_lost=stop.set)
    app = create_front_app(conn, shared, slot, 1, query_sock,
                           adm_normal, adm_tight, raw_normal, raw_tight)
    # auto_decompress=False 与 lingering_time 的含义与 __main__.py 中的注释相同，
    # 必须同样设置（见那里的注释：aiohttp 3.14+ 不自动解压、413/503 后读体再关连接）
    runner = web.AppRunner(app, auto_decompress=False, lingering_time=lingering)
    await runner.setup()
    await web.TCPSite(runner, host, port, reuse_port=True).start()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    log.info("前端 %d 就绪（%s:%d，SO_REUSEPORT）", slot, host, port)
    await stop.wait()
    await runner.cleanup()
