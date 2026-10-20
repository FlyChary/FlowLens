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
import time

from aiohttp import ClientSession, ClientTimeout, UnixConnector, web

from .api.app import (_error_middleware, _ingest_expect, _json_response,
                      _read_raw_body, degrade_controller_key,
                      raw_budget_key, stats_body)
from .api.params import QueryParams
from .api.stream import topk_data
from .config import Config
from .engine import IngestResult
from .errors import (BadRequest, FlowLensError, Overloaded, PayloadTooLarge,
                     TooOld, UnsupportedEncoding)
from .ingest import MAX_PENDING_BYTES, decode_body
from .ipc import MSG_ACK, MSG_BATCH, MSG_SNAPSHOT, encode_parsed, read_msg, send_msg
from .parse_pool import parse_body
from .query import (is_snapshot_query, stats_from_snapshot,
                    topk_from_snapshot)
from .supervisor import DegradeView, RawSlotAdmission, SlotAdmission
from .timewin import Clock

log = logging.getLogger("flowlens.frontend")

# 快照保留的 Top-K 条数（Config 默认值；引擎与前端同版本启动，值一致）
_TOP_N = Config().snapshot_top_n
# 解析 at 参数用的时钟（判"at 在未来"），前端进程各一个、无状态
_clock = Clock()


class FrontState:
    """前端本地的只读副本（P2d-2 二.4）：引擎广播来的最新快照及其接收时刻。

    快照是引擎"每秒整体替换、只读不改"的对象（engine.Snapshot 冻结 dataclass），
    复制一份给前端是安全的。回答查询用的 degraded / sample_weight / memory_bytes
    直接从共享内存读（引擎在每次快照回调里写，见 hub.make_on_snapshot）。
    """

    def __init__(self, shared):
        self.shared = shared
        self.snapshot = None            # 最新快照（Snapshot）
        self.snapshot_recv_ms = 0.0     # 单调钟毫秒，收到快照的时刻

    def fresh_snapshot(self):
        """快照存在且在最近 3 秒内收到，才用于回答；否则转发给引擎（由引擎决定）。

        引擎被暂停/卡住（不再产生新快照）时，前端的本地副本会过期——此时转发
        给引擎，由引擎按自己的实时状态回答或拒绝，避免前端拿过期快照继续答。
        """
        if self.snapshot is None:
            return None
        if time.monotonic() * 1000 - self.snapshot_recv_ms > 3000:
            return None
        return self.snapshot


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

    除 ACK 外还接收快照广播（P2d-2 二.4）：MSG_SNAPSHOT 帧整体存入 FrontState
    （state.snapshot），供前端的默认窗口查询本地回答。
    """

    def __init__(self, reader, writer, on_lost, state: "FrontState | None" = None):
        self._r, self._w, self._on_lost = reader, writer, on_lost
        self._state = state
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
                if mtype == MSG_SNAPSHOT:
                    if self._state is not None:
                        self._state.snapshot = msg
                        self._state.snapshot_recv_ms = time.monotonic() * 1000
                    continue
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


front_ingest_key = web.AppKey("front_ingest", FrontIngestService)
upstream_key = web.AppKey("upstream", ClientSession)
front_state_key = web.AppKey("front_state", FrontState)

# 转发时保留的请求/响应头（其余丢弃）
_FWD_REQ = {"accept", "last-event-id", "content-type", "content-length", "content-encoding"}
# allow：引擎 405 响应的 Allow 头（允许的方法列表），规则要求对外头部与单进程
# 模式一致，必须透传；其余响应一般没有这个头
_FWD_RESP = {"allow", "cache-control", "content-type", "retry-after",
             "x-accel-buffering"}


async def _front_ingest(request: web.Request) -> web.Response:
    """与 app.py 的 _ingest 逐行对应，只是写入服务换成 FrontIngestService。"""
    if request.app[degrade_controller_key].reject_ingest:
        raise Overloaded("降级期间暂停写入", retry_after=1)
    body = await _read_raw_body(request, request.app[raw_budget_key])
    r = await request.app[front_ingest_key].ingest(
        body, request.headers.get("Content-Encoding"))
    return _json_response({"accepted": r.accepted, "deduped": r.deduped,
                           "late": r.late, "invalid": r.invalid}, status=202)


# ---- 默认窗口查询：本地快照副本直接回答（P2d-2 三）----

async def _front_topk(request: web.Request) -> web.StreamResponse:
    """默认窗口 topk：本地快照新鲜（3 秒内收到）时直接回答，否则转发引擎。

    参数解析与校验与引擎的 _topk 同一份 QueryParams.from_request：非法参数
    在这里就得到与引擎一致的 400（在转发之前）。引擎的第二道校验
    （QueryService._check_window / _check_k）范围与 params 层相同（window
    1~600 秒、k 1~100），对通过 from_request 的默认窗口查询不会触发；引擎的
    "第 1 级暂停回放"检查在快照分支**之后**，默认窗口在引擎上同样不受影响
    （以实际代码为准核对过，行为与引擎逐字节一致）。
    """
    st = request.app[front_state_key]
    snap = st.fresh_snapshot()
    if snap is not None:
        qp = QueryParams.from_request(request, _clock.now_ms())
        if is_snapshot_query(qp.at_ms, qp.window_ms, qp.k, _TOP_N):
            ans = topk_from_snapshot(snap, qp.by, qp.k, qp.window_ms)
            sh = st.shared
            return _json_response(
                topk_data(ans, degraded=bool(sh.degraded.value),
                          sample_weight=sh.sample_weight.value))
    return await _proxy(request)


async def _front_stats(request: web.Request) -> web.StreamResponse:
    """默认窗口 stats：本地快照新鲜时直接回答（memory_bytes / degraded 从
    共享内存读，引擎在每次快照回调里写的），否则转发引擎。"""
    st = request.app[front_state_key]
    snap = st.fresh_snapshot()
    if snap is not None:
        qp = QueryParams.from_request(request, _clock.now_ms(),
                                      need_k=False, need_by=False)
        if is_snapshot_query(qp.at_ms, qp.window_ms, None, _TOP_N):
            sh = st.shared
            return _json_response(
                stats_body(stats_from_snapshot(snap, qp.window_ms),
                           sh.mem_bytes.value, bool(sh.degraded.value)))
    return await _proxy(request)


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
                     raw_normal: int, raw_tight: int,
                     state: "FrontState | None" = None) -> web.Application:
    """组装一个前端进程的 HTTP 应用。

    raw_budget 放 RawSlotAdmission（_read_raw_body / _ingest_expect 只依赖这个键）、
    degrade 放 DegradeView；额度镜像到共享数组的第 slot 槽。

    参考代码骨架在这里放的是普通 SlotAdmission（limit 恒为 raw_normal），但第 4.3
    节又定义了收紧口径 raw_tight、单进程模式下第 3 级会把 raw_budget.limit 从
    RAW_BUDGET_LIMIT 收紧到 TIGHT_RAW_BUDGET_LIMIT——骨架没用上 raw_tight 参数，
    会让"第 3 级收紧原始体额度"在前端模式不生效。这里改用 RawSlotAdmission：
    limit 随共享 tight 标志动态（现读现选），_ingest_expect 与 _read_raw_body
    每次使用都读 limit，天然生效（汇报与 P2-notes 中说明）。
    """
    app = web.Application(client_max_size=0, middlewares=[_error_middleware])
    app[raw_budget_key] = RawSlotAdmission(raw_normal, raw_tight,
                                           shared.raw_pending, shared.raw_rejected,
                                           slot, shared)
    app[degrade_controller_key] = DegradeView(shared)
    app[front_ingest_key] = FrontIngestService(
        conn, SlotAdmission(adm_normal, shared.pending, shared.ingest_rejected, slot),
        shared, adm_normal, adm_tight)
    # 本地快照副本（P2d-2 三）：默认窗口查询用它直接回答。state 由调用方传入时
    # 复用（_front_run 在创建 EngineConnection 之前建好它，读循环要往里存快照），
    # 测试等直接构造时在这里新建
    app[front_state_key] = state if state is not None else FrontState(shared)
    app.router.add_post("/v1/ingest", _front_ingest, expect_handler=_ingest_expect)
    # 默认窗口查询在 catch-all 之前注册：本地快照新鲜时直接回答，否则落到下面转发
    app.router.add_get("/v1/topk", _front_topk)
    app.router.add_get("/v1/stats", _front_stats)
    # catch-all：除上面已注册的路径外的一切（含错误方法、不存在路径）转发给引擎
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
    state = FrontState(shared)   # 本地快照副本：读循环存快照、默认窗口查询读快照
    conn = EngineConnection(reader, writer, on_lost=stop.set, state=state)
    app = create_front_app(conn, shared, slot, 1, query_sock,
                           adm_normal, adm_tight, raw_normal, raw_tight,
                           state=state)
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
