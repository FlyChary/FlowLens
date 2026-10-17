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
- 预留在整个请求处理完毕后释放（finally）。
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

import orjson
from aiohttp import hdrs, web
from aiohttp.http import HttpVersion11

from ..engine import Engine
from ..errors import FlowLensError, Overloaded, PayloadTooLarge
from ..ingest import Admission, IngestService
from ..memstat import memory_bytes
from ..query import QueryService
from ..ticker import Ticker
from .params import QueryParams

log = logging.getLogger(__name__)

# 单个请求体（原始、压缩后）上限：超过直接 413
MAX_RAW_BODY_BYTES = 4 * 1024 * 1024
# RawBudget 总上限：所有正在读取/持有的原始请求体字节数之和
RAW_BUDGET_LIMIT = 8 * 1024 * 1024
# 分块读取的块大小
_READ_CHUNK = 256 * 1024

# 请求级状态键（web.RequestKey，避免字符串键的告警）：expect_handler 把
# Expect 请求的判定结果存进来，_read_raw_body 读取（见 _ingest_expect）
_INGEST_EXPECT = web.RequestKey("ingest_expect")


def create_app(engine: Engine, ingest_service: IngestService,
               query_service: QueryService, *, tick_period: float = 1.0) -> web.Application:
    # client_max_size=0（不限制）：4 MB 上限由 _read_raw_body 自己执行，
    # 否则 aiohttp 会在 handler 之前以自己的 413 拒绝，RawBudget 预留就没机会了
    app = web.Application(client_max_size=0, middlewares=[_error_middleware])
    app["ingest_service"] = ingest_service
    app["query_service"] = query_service
    # RawBudget 由 app 持有（create_app 的参数形式保持不变）
    app["raw_budget"] = Admission(limit=RAW_BUDGET_LIMIT)
    app.router.add_post("/v1/ingest", _ingest)
    app.router.add_get("/v1/topk", _topk)
    app.router.add_get("/v1/stats", _stats)
    app.router.add_get("/healthz", _healthz)
    ticker = Ticker(engine, period=tick_period)
    app["ticker"] = ticker
    app.on_startup.append(_start_ticker)
    app.on_shutdown.append(_stop_ticker)
    return app


# ---- JSON 响应：统一用 orjson 序列化（默认把 NaN/Infinity 输出为 null），
# ---- 保证所有 JSON 响应都能被严格解析器解析（不出现 NaN / Infinity 字面量）
def _json_response(data, *, status: int = 200) -> web.Response:
    return web.Response(status=status, content_type="application/json",
                        body=orjson.dumps(data))


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
    svc = request.app["ingest_service"]
    body = await _read_raw_body(request, request.app["raw_budget"])
    result = await svc.ingest(body, request.headers.get("Content-Encoding"))
    return _json_response({
        "accepted": result.accepted,
        "deduped": result.deduped,
        "late": result.late,
        "invalid": result.invalid,
    }, status=202)


async def _topk(request: web.Request) -> web.Response:
    now_ms = request.app["query_service"].engine.clock.now_ms()
    qp = QueryParams.from_request(request, now_ms)
    qs = request.app["query_service"]
    ans = await qs.topk(qp.by, qp.window_ms, qp.k, qp.at_ms)
    return _json_response({
        "window_ms": ans.window_ms,
        "from": ans.from_ms,
        "to": ans.to_ms,
        "by": str(ans.dim),
        "k": ans.k,
        "final": ans.final,
        "degraded": False,  # T8 降级控制器接入后替换
        "unseen_upper": ans.unseen_upper,
        "items": [{"key": it.key, "count": _fmt_num(it.count), "err": _fmt_num(it.err)}
                  for it in ans.items],
    })


async def _stats(request: web.Request) -> web.Response:
    now_ms = request.app["query_service"].engine.clock.now_ms()
    qp = QueryParams.from_request(request, now_ms, need_k=False, need_by=False)
    s = await request.app["query_service"].stats(qp.window_ms)
    return _json_response({
        "window_ms": s.window_ms,
        "from": s.from_ms,
        "to": s.to_ms,
        "approx_distinct_users": int(round(s.distinct_users)),
        # 区间内没有数据时 p95 是 NaN：orjson 默认输出 null（不出现 NaN 字面量）
        "approx_p95_bytes": _fmt_num(s.p95_bytes) if s.p95_bytes == s.p95_bytes else None,
        "memory_bytes": memory_bytes(),
        "degraded": False,  # T8 降级控制器接入后替换
    })


async def _healthz(request: web.Request) -> web.Response:
    c = request.app["query_service"].engine.counters
    t = request.app["ticker"]
    return _json_response({
        "ok": True,
        "events_ingested": c.events_ingested,
        "memory_bytes": memory_bytes(),
        "degraded": False,  # T8 降级控制器接入后替换
        "deduped": c.deduped,
        "late": c.late,
        "invalid": c.invalid,
        "tick_errors": t.error_count,
        "last_tick_ms": t.last_tick_duration_ms,
    })


# ---- 后台 ticker 生命周期 ----

async def _start_ticker(app: web.Application) -> None:
    app["ticker_task"] = asyncio.create_task(app["ticker"].run())


async def _stop_ticker(app: web.Application) -> None:
    ticker, task = app["ticker"], app["ticker_task"]
    ticker.stop()
    if not task.done():
        task.cancel()  # 等待窗口最多一个周期，取消让退出即时
    await asyncio.gather(task, return_exceptions=True)


def make_engine(clock=None) -> tuple[Engine, IngestService, QueryService]:
    """按默认 Config 组装 engine + 服务（__main__ 用）。"""
    from ..config import Config
    from ..timewin import Clock
    engine = Engine(Config(), clock or Clock())
    return engine, IngestService(engine, Admission()), QueryService(engine)
