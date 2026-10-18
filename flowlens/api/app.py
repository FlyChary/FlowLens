"""aiohttp 应用：路由、统一错误中间件、后台 Ticker。

路由（T6）：
  POST /v1/ingest                     写入一批事件（支持 gzip）
  GET  /v1/topk?window&k&by&at        Top-K 回放（默认窗口读快照）
  GET  /v1/stats?window               去重人数与 p95
  GET  /healthz                       健康与计数
SSE（/v1/topk/stream）在 T7 实现。

原始请求体的内存保护（RawBudget）：在途预留只管解压之后的数据，但"正在读取或
持有的原始请求体"本身也占内存，由 app 层再管一道：
- 有 Content-Length：超过 4 MB 直接 413，不读请求体；否则先预留 Content-Length，
  预留失败 503 + Retry-After: 1；
- 无 Content-Length（分块传输）：先预留 4 MB，边读边累计，超过 4 MB 立即 413；
- 预留在**读完请求体**时就释放（`_read_raw_body` 的 finally），不是等整个请求处理
  完。这样是安全的：读完请求体后紧接着就在 IngestService 里按**解压后字节数**另做
  一份在途预留（ingest.py 的 Admission），两份预留之间没有让出事件循环（读体与
  预留是同一个协程里连续执行），不存在"两份都没预留"的空窗；而"进入处理阶段的
  请求数量"本身已由在途预留（按解压后字节）限住，原始体的额度在读取完的那一刻
  已经不再被占用，可以立即释放给下一个请求。
RawBudget 总上限 8 MB，即同一时刻所有在途请求体之和的上限。

Expect: 100-continue：curl 等客户端上传大请求体时会先发 Expect 头等服务端同意
再发请求体。/v1/ingest 注册了自定义 expect_handler：Content-Length 超过 4 MB 时
直接回 413、RawBudget 预留不下时回 503 + Retry-After: 1，两种情况都不发
100 Continue，客户端根本不会发送请求体，连接上也没有没读完的字节。判断与真正的
预留分离：expect_handler 只判断（能否预留），返回最终响应时 handler 不再执行、
_read_raw_body 也就不会运行，无需传状态；返回 None 发 100 后，真正预留在
_read_raw_body 里 try_reserve、finally release，预算借还是单一 owner，不会双重预留。
"""

import asyncio
import logging
from typing import Callable

import orjson
from aiohttp import hdrs, web
from aiohttp.http import HttpVersion11

from ..degrade import DegradeController
from ..engine import Engine
from ..errors import FlowLensError, Overloaded, PayloadTooLarge
from ..ingest import Admission, IngestService
from ..memstat import memory_bytes, memory_limit_bytes
from ..model import clear_norm_cache
from ..query import QueryService
from ..ticker import Ticker
from . import debug
from .params import QueryParams
from .stream import (DEFAULT_HEARTBEAT_SEC, DEFAULT_MAX_GROUPS,
                     DEFAULT_MAX_SUBSCRIBERS, DEFAULT_MIN_RECOMPUTE_SEC,
                     DEFAULT_WRITE_TIMEOUT_SEC, StreamManager, compute_next_tid,
                     pump, topk_data)

log = logging.getLogger(__name__)

# 单个请求体（原始、压缩后）上限：超过直接 413
MAX_RAW_BODY_BYTES = 4 * 1024 * 1024
# RawBudget 总上限：所有正在读取/持有的原始请求体字节数之和
RAW_BUDGET_LIMIT = 8 * 1024 * 1024
# 第 3 级降级（收紧内存）时的上限：Admission 由 4 MB 降到 2 MB，RawBudget 由 8 MB 降到 4 MB
TIGHT_ADMISSION_LIMIT = 2 * 1024 * 1024
TIGHT_RAW_BUDGET_LIMIT = 4 * 1024 * 1024
# 分块读取的块大小
_READ_CHUNK = 256 * 1024

# app 上的键统一用 web.AppKey（带类型标注，消除 NotAppKeyWarning）。
# AppKey 按身份而非 (name, type) 比较，所以必须在模块级定义一次，处处复用同一对象。
ingest_service_key = web.AppKey("ingest_service", IngestService)
query_service_key = web.AppKey("query_service", QueryService)
raw_budget_key = web.AppKey("raw_budget", Admission)
ticker_key = web.AppKey("ticker", Ticker)
ticker_task_key = web.AppKey("ticker_task", asyncio.Task)
stream_manager_key = web.AppKey("stream_manager", StreamManager)
degrade_controller_key = web.AppKey("degrade_controller", DegradeController)
memory_reader_key = web.AppKey("memory_reader", Callable)


def _default_memory_reader() -> tuple[int, int | None]:
    """默认内存读数：memstat（cgroup / statm / Windows WorkingSet）。"""
    return memory_bytes(), memory_limit_bytes()


def create_app(engine: Engine, ingest_service: IngestService,
               query_service: QueryService, *, tick_period: float = 1.0,
               memory_reader: Callable[[], tuple[int, int | None]] | None = None
               ) -> web.Application:
    # client_max_size=0（不限制）：4 MB 上限由 _read_raw_body 自己执行，
    # 否则 aiohttp 会在 handler 之前以自己的 413 拒绝，RawBudget 预留就没机会了
    app = web.Application(client_max_size=0, middlewares=[_error_middleware])
    app[ingest_service_key] = ingest_service
    app[query_service_key] = query_service
    # RawBudget 由 app 持有（create_app 的参数形式保持不变）
    app[raw_budget_key] = Admission(limit=RAW_BUDGET_LIMIT)
    # 降级控制器（T8）：挂在 app 上，并与 engine（采样权重）和 query_service
    # （暂停回放）共享同一个实例——否则 engine 的采样与 query 的暂停各看各的级别，
    # 而"占用率触发第 1 级"这类只有 app 层才喂得进来的状态两边会不一致。
    controller = DegradeController()
    engine.controller = controller
    query_service.controller = controller
    app[degrade_controller_key] = controller
    # 内存读数（T8b）：Ticker 喂控制器的值与 /healthz、/v1/stats 的 memory_bytes /
    # memory_limit_bytes 都统一从这一个 reader 读。默认是 memstat 的两个函数；
    # 测试注入假读数（如恒 (0, None)），让测试与运行环境的真实内存解耦。
    app[memory_reader_key] = memory_reader if memory_reader is not None else _default_memory_reader
    # 级别变化副作用（收紧/恢复预算、进入第 3 级清缓存）：只在实际变化时回调一次
    controller.on_change = _make_degrade_change_hook(app)
    # Expect: 100-continue 的超大请求在这一步拒绝，客户端不会发送请求体（见 _ingest_expect）
    app.router.add_post("/v1/ingest", _ingest, expect_handler=_ingest_expect)
    app.router.add_get("/v1/topk", _topk)
    app.router.add_get("/v1/stats", _stats)
    app.router.add_get("/healthz", _healthz)
    app.router.add_get("/v1/topk/stream", _stream)
    ticker = Ticker(engine, period=tick_period, controller=controller,
                    admission=ingest_service.admission,
                    memory_reader=app[memory_reader_key])
    app[ticker_key] = ticker
    # SSE 订阅管理器：参数相同的订阅者共用计算，组数/连接数/计算频率都有上限（T7）
    manager = StreamManager(
        query_service,
        max_groups=DEFAULT_MAX_GROUPS,
        max_subscribers=DEFAULT_MAX_SUBSCRIBERS,
        min_recompute=DEFAULT_MIN_RECOMPUTE_SEC,
        heartbeat_period=DEFAULT_HEARTBEAT_SEC,
        write_timeout=DEFAULT_WRITE_TIMEOUT_SEC,
    )
    app[stream_manager_key] = manager
    app.on_startup.append(_start_ticker)
    app.on_shutdown.append(_stop_ticker)
    app.on_shutdown.append(_stop_stream)
    # 调试接口（B1）：仅 FLOWLENS_DEBUG=1 时注册，未设置时访问 /debug/* 返回 404
    if debug.is_enabled():
        debug.register(app)
    return app


# ---- JSON 响应：统一用 orjson 序列化（默认把 NaN/Infinity 输出为 null），
# ---- 保证所有 JSON 响应都能被严格解析器解析（不出现 NaN / Infinity 字面量）
def _json_response(data, *, status: int = 200) -> web.Response:
    return web.Response(status=status, content_type="application/json",
                        body=orjson.dumps(data))


def _make_degrade_change_hook(app: web.Application):
    """降级控制器级别变化时的副作用（收紧/恢复预算、进入第 3 级清一次缓存）。

    做成工厂、闭包捕获 app，而不是把 app 传进控制器——控制器保持纯逻辑、不依赖
    aiohttp。只在实际变化时调用一次，所以"进入第 3 级清缓存"确实只在进入那一刻发生。
    """
    admission = app[ingest_service_key].admission
    raw_budget = app[raw_budget_key]
    query_service = app[query_service_key]
    normal_adm, tight_adm = admission.limit, TIGHT_ADMISSION_LIMIT
    normal_raw, tight_raw = raw_budget.limit, TIGHT_RAW_BUDGET_LIMIT

    def on_change(old: int, new: int) -> None:
        if new >= 3:
            if old < 3:
                # 进入第 3 级：收紧两个预算，并清一次结果缓存与 URL 归一化缓存
                admission.limit = tight_adm
                raw_budget.limit = tight_raw
                query_service.clear_cache()
                clear_norm_cache()
        elif new < 3:
            if old >= 3:
                # 退出第 3 级：恢复预算（缓存不清，重新填充）
                admission.limit = normal_adm
                raw_budget.limit = normal_raw
        # 第 1/2/4 级没有需要延迟执行的副作用（暂停回放/采样/拒写都是读属性即得）

    return on_change


def _fmt_num(x: float):
    """整数输出整数，否则保留两位小数。"""
    return int(x) if float(x).is_integer() else round(float(x), 2)


# ---- 中间件：只在这里把异常转成 HTTP 响应，各接口只抛异常 ----
# 必须加 @web.middleware（新式，__middleware_version__=1）：aiohttp 3.14+ 对新式中间件
# 按 request 调用 m(request, handler=handler)，并把 request.app 设成当前应用；
# 缺了标记会被当旧式，调用时把 app 绑到第一个形参（request）上，request.app 就没了。
@web.middleware
async def _error_middleware(request: web.Request, handler):
    try:
        return await handler(request)
    except FlowLensError as e:
        resp = _json_response({"error": e.message}, status=e.status)
        if getattr(e, "retry_after", None):
            resp.headers["Retry-After"] = str(e.retry_after)
        return resp
    except web.HTTPException as e:
        # aiohttp 自己的 HTTP 异常（404 / 405 / 413 等）：保持状态码与原有响应头
        # （例如 405 的 Allow），只把响应体换成 {"error": ...}
        resp = _json_response({"error": e.reason}, status=e.status)
        for k, v in e.headers.items():
            resp.headers[k] = v
        resp.headers["Content-Type"] = "application/json"
        return resp
    except Exception:
        # 未预期的异常：记完整日志，返回 500
        log.exception("处理请求 %s %s 出未预期异常", request.method, request.path)
        return _json_response({"error": "internal error"}, status=500)


# ---- Expect: 100-continue 的拒绝路径（超大请求在发请求体之前拒绝）----

# 显式发 100 Continue 的报文（照 aiohttp 默认实现；返回 None 时 aiohttp 不会代发）
_HUNDRED_CONTINUE = b"HTTP/1.1 100 Continue\r\n\r\n"


async def _ingest_expect(request: web.Request) -> web.StreamResponse | None:
    """/v1/ingest 的 expect_handler：在客户端发送请求体之前就做出拒绝决定。

    curl 等客户端上传大请求体时会先发送 `Expect: 100-continue` 头等服务端同意再
    发体。在这一步拒绝，客户端根本不会发送请求体，连接上也不会有没读完的字节，
    避免 lingering_time=0 下"关连接时收缓冲区还有数据 → 客户端收到 RST 而非 413"。

    返回值约定（aiohttp）：返回 StreamResponse 则直接作为最终响应、handler 不再
    执行；返回 None 则表示"同意"，需要自己显式发 100 Continue（aiohttp 不代发）。
    本函数在错误中间件之前执行，所以必须自己构造 JSON 响应、不能靠抛异常。

    与真正预留的分工：这里只做**只读判断**（can_reserve），不预留。
    - 判断不过 → 返回 413/503，handler 不执行、_read_raw_body 不运行，无需释放；
    - 判断通过 → 发 100，随后 _read_raw_body 里 try_reserve 真正预留、finally 释放。
    预算的借还始终只有 _read_raw_body 一个 owner，不会双重预留，也不会"判了但没
    释放"。判断与 _read_raw_body 的 4MB/预算检查逐条一致，故结论不会分叉。
    """
    expect = request.headers.get(hdrs.EXPECT, "")
    if request.version == HttpVersion11 and expect.lower() == "100-continue":
        # 第 4 级降级：在客户端发送请求体之前就直接 503（与 _ingest 的拒写口径一致）
        if request.app[degrade_controller_key].reject_ingest:
            resp = _json_response({"error": "降级期间暂停写入"}, status=503)
            resp.headers["Retry-After"] = "1"
            return resp
        cl = request.content_length
        if cl is not None and cl > MAX_RAW_BODY_BYTES:
            # 超大：直接 413，客户端不会发送请求体
            return _json_response(
                {"error": f"请求体超过 {MAX_RAW_BODY_BYTES} 字节"}, status=413)
        budget: Admission = request.app[raw_budget_key]
        # 分块传输（cl 为 None）在 expect 阶段无法判断预留量，一律先放行发 100，
        # 由 _read_raw_body 边读边核对 4MB 与预算。
        if cl is not None and not budget.can_reserve(cl):
            resp = _json_response({"error": "overloaded"}, status=503)
            resp.headers["Retry-After"] = "1"
            return resp
        # 通过：显式发 100（照默认实现；发完重置 output_size，100 不计入响应体）
        await request.writer.write(_HUNDRED_CONTINUE)
        request.writer.output_size = 0
    return None


# ---- 请求体读取（RawBudget 预留 + 4 MB 上限）----

async def _read_raw_body(request: web.Request, budget: Admission) -> bytes:
    """带原始请求体预算的读取；预留成功才继续，失败抛 Overloaded。

    预留与释放在本函数内闭环（预留成功后一定 release），调用方不用管：
    - Content-Length 超过 4 MB：不读请求体，直接 413（服务端不接收超大请求）
    - 有 Content-Length：预留该字节数，失败 503；成功后整体读取
    - 分块传输（无 Content-Length）：先预留 4 MB，分块读取，累计超过 4 MB 立即 413
    """
    cl = request.content_length
    if cl is None:
        # 分块传输：按单请求上限预留，边读边核对
        if not budget.try_reserve(MAX_RAW_BODY_BYTES):
            raise Overloaded(retry_after=1)
        try:
            body = bytearray()
            while True:
                chunk = await request.content.read(_READ_CHUNK)
                if not chunk:
                    return bytes(body)
                body.extend(chunk)
                if len(body) > MAX_RAW_BODY_BYTES:
                    raise PayloadTooLarge(f"请求体超过 {MAX_RAW_BODY_BYTES} 字节")
                await asyncio.sleep(0)  # 大请求分块读取时让出事件循环
        finally:
            budget.release(MAX_RAW_BODY_BYTES)
    if cl > MAX_RAW_BODY_BYTES:
        # 不读请求体直接拒绝：服务端不接收超大请求，客户端无需把几 MB 传完
        raise PayloadTooLarge(f"请求体超过 {MAX_RAW_BODY_BYTES} 字节")
    if not budget.try_reserve(cl):
        raise Overloaded(retry_after=1)
    try:
        return await request.read()
    finally:
        budget.release(cl)


# ---- 路由 ----

async def _ingest(request: web.Request) -> web.Response:
    svc = request.app[ingest_service_key]
    # 第 4 级降级：在读取请求体之前直接拒绝。此时还没写入任何去重状态，
    # 调用方重试是安全的（CLAUDE.md 规则 2）。
    if request.app[degrade_controller_key].reject_ingest:
        raise Overloaded("降级期间暂停写入", retry_after=1)
    body = await _read_raw_body(request, request.app[raw_budget_key])
    result = await svc.ingest(body, request.headers.get("Content-Encoding"))
    return _json_response({
        "accepted": result.accepted,
        "deduped": result.deduped,
        "late": result.late,
        "invalid": result.invalid,
    }, status=202)


async def _topk(request: web.Request) -> web.Response:
    ctrl = request.app[degrade_controller_key]
    now_ms = request.app[query_service_key].engine.clock.now_ms()
    qp = QueryParams.from_request(request, now_ms)
    ans = await request.app[query_service_key].topk(qp.by, qp.window_ms, qp.k, qp.at_ms)
    return _json_response(topk_data(ans, degraded=ctrl.degraded,
                                    sample_weight=ctrl.sample_weight))


async def _stats(request: web.Request) -> web.Response:
    ctrl = request.app[degrade_controller_key]
    now_ms = request.app[query_service_key].engine.clock.now_ms()
    qp = QueryParams.from_request(request, now_ms, need_k=False, need_by=False)
    s = await request.app[query_service_key].stats(qp.window_ms)
    mem_bytes, _ = request.app[memory_reader_key]()
    return _json_response({
        "window_ms": s.window_ms,
        "from": s.from_ms,
        "to": s.to_ms,
        "approx_distinct_users": int(round(s.distinct_users)),
        # 区间内没有数据时 p95 是 NaN：orjson 默认输出 null（不出现 NaN 字面量）
        "approx_p95_bytes": _fmt_num(s.p95_bytes) if s.p95_bytes == s.p95_bytes else None,
        "memory_bytes": mem_bytes,
        "degraded": ctrl.degraded,
    })


async def _healthz(request: web.Request) -> web.Response:
    c = request.app[query_service_key].engine.counters
    t = request.app[ticker_key]
    mgr = request.app[stream_manager_key]
    ctrl = request.app[degrade_controller_key]
    mem_bytes, mem_limit = request.app[memory_reader_key]()
    return _json_response({
        "ok": True,
        "events_ingested": c.events_ingested,
        "memory_bytes": mem_bytes,
        "memory_limit_bytes": mem_limit,  # 无上限时为 null
        "degraded": ctrl.degraded,
        "degrade": ctrl.status(),
        "deduped": c.deduped,
        "late": c.late,
        "invalid": c.invalid,
        "tick_errors": t.error_count,
        "last_tick_ms": t.last_tick_duration_ms,
        "sse": mgr.stats(),
    })


# ---- SSE：GET /v1/topk/stream ----

async def _stream(request: web.Request) -> web.StreamResponse:
    """SSE 实时 Top-K 流：参数相同者共用计算，断线可按 Last-Event-ID 补帧。"""
    engine = request.app[query_service_key].engine
    now_ms = engine.clock.now_ms()
    # 解析并校验 window/k/by/interval（非法参数走 400）
    qp = QueryParams.from_request(request, now_ms)
    manager = request.app[stream_manager_key]
    # 上限检查放在发出任何字节之前：超限走 503 + Retry-After 的普通 JSON 响应
    manager.check_limits()
    by, window_ms, k, interval_ms = qp.by, qp.window_ms, qp.k, qp.interval_ms

    last_id = request.headers.get(hdrs.LAST_EVENT_ID)
    # 订阅者要交付的起始帧时刻（Last-Event-ID 重连时从此补发，否则从实时帧开始）
    next_tid = compute_next_tid(last_id, now_ms, interval_ms, window_ms,
                                engine.cfg.retention_sec * 1000)

    resp = web.StreamResponse()
    resp.headers["Content-Type"] = "text/event-stream"
    resp.headers["Cache-Control"] = "no-cache"
    group, sub = manager.subscribe(by=by, window_ms=window_ms, k=k,
                                   interval_ms=interval_ms, raw=request)
    try:
        await resp.prepare(request)
        await resp.write(b"retry: 3000\n\n")
        # pump 用单调游标 next_tid：先把 [next_tid, 首帧) 逐帧补发（断线重连，优先用
        # 组缓存、不重算），再无缝跟随实时帧——id 连续、严格递增、不重复、不重叠。
        # 新订阅者（没有 Last-Event-ID）若组里已有最近一帧，pump 会先立即补发它。
        await pump(resp, sub, group, next_tid=next_tid, immediate=last_id is None,
                   heartbeat=manager.heartbeat_period,
                   write_timeout=manager.write_timeout,
                   compute_lock=manager.compute_lock)
    except asyncio.CancelledError:
        raise  # 客户端断开 / 应用关闭：清理后照常取消（CancelledError 不被下面捕获）
    except Exception:
        # 写入超时（写满缓冲后 5 秒没写出去）或连接被重置：结束这个订阅者
        log.info("SSE 流结束（写入超时或连接断开）")
    finally:
        manager.leave((by, window_ms, k, interval_ms), raw=request)
    return resp


# ---- 后台 ticker 生命周期 ----

async def _start_ticker(app: web.Application) -> None:
    app[ticker_task_key] = asyncio.create_task(app[ticker_key].run())


async def _stop_ticker(app: web.Application) -> None:
    ticker, task = app[ticker_key], app[ticker_task_key]
    ticker.stop()
    if not task.done():
        task.cancel()  # 等待窗口最多一个周期，取消让退出即时
    await asyncio.gather(task, return_exceptions=True)


async def _stop_stream(app: web.Application) -> None:
    # 应用关闭：结束所有 SSE 订阅组的生产任务（流本身的连接由 aiohttp 关连接时取消）
    await app[stream_manager_key].close()


def make_engine(clock=None) -> tuple[Engine, IngestService, QueryService]:
    """按默认 Config 组装 engine + 服务（__main__ 用）。"""
    from ..config import Config
    from ..timewin import Clock
    engine = Engine(Config(), clock or Clock())
    return engine, IngestService(engine, Admission()), QueryService(engine)
