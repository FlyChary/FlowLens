"""启动入口：$env:PYTHONHASHSEED="0"; python -m flowlens --host 0.0.0.0 --port 8080。

单进程模式（前端数 0，默认）：与 P2d-1 之前完全相同——TCPSite 监听 8080、本进程解析。
前端模式（前端数 > 0，仅 Linux）：先 spawn 出 n 个前端进程（SO_REUSEPORT 共享 8080），
引擎不监听 TCP，只起 Unix 套接字的内部 HTTP（给前端转发查询）+ 写入通道（前端把
解析结果发给引擎）；降级级别与额度经共享内存交换（见 flowlens/supervisor.py）。
"""

import argparse
import asyncio
import logging
import os
import signal
import tempfile

from aiohttp import web

from .api.app import (create_app, degrade_controller_key, make_engine,
                      memory_reader_key)
from .hub import SnapshotHub, make_on_snapshot
from .ipc import MSG_ACK, MSG_BATCH, decode_to_parsed, read_msg, send_msg
from .intern import url_intern, user_intern
from .errors import FlowLensError
from .supervisor import FrontSupervisor, resolve_front_count
from .timewin import Clock

log = logging.getLogger("flowlens")


async def serve_front(reader, writer, svc, hub=None) -> None:
    """写入通道的服务端（P2d-1 5.2）：每个前端一条连接、一个协程。

    读取 BATCH → decode_to_parsed（只对不重复字符串驻留）→ svc.write_parsed（与单进程
    同一套切块写引擎）→ 回 ACK。同一连接上的请求按顺序处理即可（写入本来就被信号量
    串行化）。引擎写入路径只会产生 FlowLensError（400/413/415/422/503），转成
    (req_id, "error", status, message, retry_after) 回给前端还原成同样的异常。

    快照广播（P2d-2 二）：开始读取循环前 hub.add(writer)（新连接立即拿到当前快照），
    退出（finally）hub.discard(writer)。hub 为 None（单进程/测试）时跳过。
    """
    iu = url_intern.intern if url_intern is not None else None
    ir = user_intern.intern if user_intern is not None else None
    if hub is not None:
        hub.add(writer)
    try:
        while True:
            try:
                mtype, w = await read_msg(reader)
            except (asyncio.IncompleteReadError, ConnectionError):
                return
            if mtype != MSG_BATCH:
                continue
            try:
                r = await svc.write_parsed(decode_to_parsed(w, iu, ir))
                ack = (w.req_id, "ok", r.accepted, r.deduped, r.late, r.invalid)
            except FlowLensError as e:
                ack = (w.req_id, "error", e.status, e.message,
                       getattr(e, "retry_after", None))
            await send_msg(writer, MSG_ACK, ack)
    finally:
        if hub is not None:
            hub.discard(writer)
        writer.close()


def main() -> None:
    # 日志输出到标准输出，带时间，级别 INFO（T7）
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    parser = argparse.ArgumentParser(description="FlowLens 实时流量分析服务")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址（默认 0.0.0.0）")
    parser.add_argument("--port", type=int, default=8080, help="监听端口（默认 8080）")
    args = parser.parse_args()

    # CLAUDE.md 规则 5：字符串哈希依赖固定 PYTHONHASHSEED（任意非 random 的具体值），
    # 否则重启前后哈希不一致，落盘数据将来无法合并
    seed = os.environ.get("PYTHONHASHSEED")
    if not seed or seed == "random":
        log.warning("PYTHONHASHSEED 未固定（或为 random），哈希结果每次启动不同，"
                    "落盘数据将来无法合并。请固定为具体值（推荐 0）。")

    lingering = float(os.environ.get("FLOWLENS_LINGERING_TIME", "1"))
    n_fronts, why = resolve_front_count()
    log.info("前端数 %d（%s）", n_fronts, why)

    if n_fronts > 0:
        _run_with_fronts(args, n_fronts, lingering)
    else:
        # 单进程模式：与 P2d-1 之前完全相同（不再使用 ParsePool）
        _run_single(args, lingering)


def _run_single(args, lingering: float) -> None:
    engine, ingest_service, query_service = make_engine(Clock())
    app = create_app(engine, ingest_service, query_service,
                     memory_reader=_env_memory_reader)

    async def run():
        # auto_decompress=False：不在传输层自动解压 gzip 请求体。请求体保持压缩，
        # 由 ingest.decode_body 分块解压并做 4 MB 限制与解压炸弹防护；否则 aiohttp
        # 3.14+ 会先解压、Content-Encoding 头仍在，decode_body 二次 gunzip 会报错。
        # lingering_time：请求体没读完就返回响应时，服务端继续把剩余体读完的最长
        # 时间（秒）。生产定为 1（见 T6c 实测）：413/503 响应仍立即发出，lingering
        # 只影响"关连接前是否读完在途请求体"，定为 1 时读完才关（干净 FIN，客户端
        # 稳定收到 413 而非 RST）。详见 P2d-1 前的 __main__.py 注释。
        runner = web.AppRunner(app, auto_decompress=False, lingering_time=lingering)
        await runner.setup()
        site = web.TCPSite(runner, args.host, args.port)
        await site.start()
        log.info("FlowLens 监听 %s:%d", args.host, args.port)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _on_signal(signum: int) -> None:
            log.info("收到信号 %s，停止服务", signal.Signals(signum).name)
            stop.set()

        registered: list[signal.Signals] = []
        if hasattr(signal, "SIGTERM"):
            for _sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(_sig, _on_signal, _sig)
                    registered.append(_sig)
                except (NotImplementedError, RuntimeError, ValueError):
                    log.warning("无法注册信号处理器 %s（平台不支持或非主线程），"
                                "Ctrl+C 仍可停止", signal.Signals(_sig).name)
        try:
            await stop.wait()
        finally:
            for _sig in registered:
                try:
                    loop.remove_signal_handler(_sig)
                except (NotImplementedError, RuntimeError, ValueError):
                    pass
            await runner.cleanup()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("收到 Ctrl+C（KeyboardInterrupt），停止服务")


def _env_memory_reader() -> tuple[int, int | None]:
    """内存读数（P2d-1 六）：默认读真实内存；测试用环境变量
    FLOWLENS_TEST_MEM（格式 "mem,limit"，如 "251658240,268435456"）强制返回值，
    让真实子进程可控地进入指定降级级别（子进程里没法像进程内 e2e 那样注入
    memory_reader）。数值非法时忽略、退回真实读数。单进程与前端模式都生效。"""
    from .api.app import _default_memory_reader
    raw = os.environ.get("FLOWLENS_TEST_MEM", "").strip()
    if raw:
        try:
            mem_s, limit_s = (x.strip() for x in raw.split(","))
            return int(mem_s), (int(limit_s) if limit_s else None)
        except ValueError:
            log.warning("FLOWLENS_TEST_MEM 格式非法（应为 mem,limit），忽略")
    return _default_memory_reader()


def _run_with_fronts(args, n_fronts: int, lingering: float) -> None:
    """前端模式：先起前端（spawn），再建引擎、起 Unix 内部 HTTP 与写入通道。"""
    from .ingest import MAX_PENDING_BYTES, MAX_DECOMPRESSED_BYTES
    from .api.app import (MAX_RAW_BODY_BYTES, RAW_BUDGET_LIMIT,
                          TIGHT_ADMISSION_LIMIT, TIGHT_RAW_BUDGET_LIMIT)

    tmpdir = tempfile.mkdtemp(prefix="flowlens-")
    ingest_sock = os.path.join(tmpdir, "ingest.sock")
    query_sock = os.path.join(tmpdir, "query.sock")
    # 每前端额度（4.3）：正常 = 合计 ÷ n（有下限），收紧 = 收紧合计 ÷ n（不设下限）
    sup = FrontSupervisor(
        n_fronts, args.host, args.port, ingest_sock, query_sock, lingering,
        adm_total=MAX_PENDING_BYTES, adm_tight_total=TIGHT_ADMISSION_LIMIT,
        raw_total=RAW_BUDGET_LIMIT, raw_tight_total=TIGHT_RAW_BUDGET_LIMIT,
        adm_floor=MAX_DECOMPRESSED_BYTES, raw_floor=MAX_RAW_BODY_BYTES)
    log.info("前端额度合计：在途 正常 %d / 收紧 %d；原始体 正常 %d / 收紧 %d"
             "（每前端 在途 %d / %d，原始体 %d / %d）",
             sup.shared.adm_total, sup.shared.adm_tight_total,
             sup.shared.raw_total, sup.shared.raw_tight_total,
             sup.adm_normal, sup.adm_tight, sup.raw_normal, sup.raw_tight)
    sup.start_all()                       # 先启动前端（spawn，不继承大块内存）
    engine, ingest_service, query_service = make_engine(Clock())
    # memory_reader=_env_memory_reader：支持 FLOWLENS_TEST_MEM 注入（测试进第 4 级用）
    app = create_app(engine, ingest_service, query_service, fronts=sup.shared,
                     memory_reader=_env_memory_reader)
    # 快照广播（P2d-2 二）：每次替换快照后，把快照与降级/采样/内存状态发给所有前端
    hub = SnapshotHub()
    engine.on_snapshot = make_on_snapshot(hub, sup.shared, app[degrade_controller_key],
                                          app[memory_reader_key])

    async def run():
        runner = web.AppRunner(app, auto_decompress=False, lingering_time=lingering)
        await runner.setup()
        # 前端模式：只起 Unix 内部 HTTP（给前端转发查询），不监听 TCP
        await web.UnixSite(runner, query_sock).start()
        server = await asyncio.start_unix_server(
            lambda r, w: serve_front(r, w, ingest_service, hub), path=ingest_sock)
        monitor_task = asyncio.create_task(sup.monitor())
        log.info("FlowLens 前端模式就绪：%d 个前端、内部 HTTP %s、写入通道 %s",
                 n_fronts, query_sock, ingest_sock)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _on_signal(signum: int) -> None:
            log.info("收到信号 %s，停止服务", signal.Signals(signum).name)
            stop.set()

        registered: list[signal.Signals] = []
        if hasattr(signal, "SIGTERM"):
            for _sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(_sig, _on_signal, _sig)
                    registered.append(_sig)
                except (NotImplementedError, RuntimeError, ValueError):
                    log.warning("无法注册信号处理器 %s（平台不支持或非主线程），"
                                "Ctrl+C 仍可停止", signal.Signals(_sig).name)
        try:
            await stop.wait()
        finally:
            for _sig in registered:
                try:
                    loop.remove_signal_handler(_sig)
                except (NotImplementedError, RuntimeError, ValueError):
                    pass
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
            engine.on_snapshot = None   # 停止快照回调（不再广播）
            await hub.close()
            sup.stop()
            server.close()
            await server.wait_closed()
            await runner.cleanup()
            # 删除临时套接字目录
            try:
                os.rmdir(tmpdir)
            except OSError:
                import shutil
                shutil.rmtree(tmpdir, ignore_errors=True)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("收到 Ctrl+C（KeyboardInterrupt），停止服务")


if __name__ == "__main__":
    main()
