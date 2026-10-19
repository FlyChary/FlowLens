"""启动入口：$env:PYTHONHASHSEED="0"; python -m flowlens --host 0.0.0.0 --port 8080。"""

import argparse
import asyncio
import logging
import os
import signal

from aiohttp import web

from .api.app import create_app, make_engine
from .parse_pool import ParsePool, resolve_worker_count
from .timewin import Clock

log = logging.getLogger("flowlens")


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

    # 解析进程池（P1，用户决定三）：在创建引擎**之前**启动并预热。spawn 上下文不继承
    # 主进程内存，但子进程要在主进程分配大块统计内存（Ring/Tree/Bloom）之前"真的"存在，
    # 所以先 ParsePool（内部提交空任务迫使 n 个子进程启动）再 make_engine。核数不足
    # （resolve_worker_count < 1）或 FLOWLENS_PARSE_WORKERS=0 时为 None，退回单进程路径。
    pool = None
    n_workers = resolve_worker_count()
    if n_workers > 0:
        pool = ParsePool(n_workers)
        log.info("解析进程池已启动：%d 个子进程（哈希自检通过）", n_workers)

    engine, ingest_service, query_service = make_engine(Clock(), parse_pool=pool)
    app = create_app(engine, ingest_service, query_service,
                     parse_pool=pool)

    async def run():
        # auto_decompress=False：不在传输层自动解压 gzip 请求体。请求体保持压缩，
        # 由 ingest.decode_body 分块解压并做 4 MB 限制与解压炸弹防护；否则 aiohttp
        # 3.14+ 会先解压、Content-Encoding 头仍在，decode_body 二次 gunzip 会报错。
        # lingering_time：请求体没读完就返回响应时，服务端继续把剩余体读完的最长
        # 时间（秒），期间不发响应、读完（或超时）才关连接。
        # 生产定为 1（见 T6c 实测与 docs/PROGRESS.md 决策）：
        #   - 413/503 响应仍在 finish_response 里立即发出，客户端拿 413 不靠 lingering；
        #   - lingering 只影响"关连接前是否读完在途请求体"。定为 0 时，对"不发
        #     Expect 直接灌 5 MB 体"的客户端，服务端发完 413 就立即关连接，而客户端
        #     还在发送 → 接收缓冲区有未读数据 → 内核发 RST（而非 FIN）→ 客户端偶发
        #     收到"连接被重置"而非 413（实测 aiohttp 客户端 2/40）。定为 1 时服务端
        #     会把剩余体读完再关连接（干净 FIN），20/20 稳定收到 413，且 413 仍是
        #     毫秒级返回、没有可感知的 1 秒延迟（drain 发生在响应发出之后）。
        # 发 Expect: 100-continue 的客户端（curl 默认、aiohttp expect100）不受影响：
        # 超大请求在发请求体之前就被 expect_handler 拒绝，根本没有在途体。
        # 可用 FLOWLENS_LINGERING_TIME 覆盖以便做对比实验（默认 1）。
        lingering = float(os.environ.get("FLOWLENS_LINGERING_TIME", "1"))
        runner = web.AppRunner(app, auto_decompress=False, lingering_time=lingering)
        await runner.setup()
        site = web.TCPSite(runner, args.host, args.port)
        await site.start()
        log.info("FlowLens 监听 %s:%d", args.host, args.port)
        # 优雅停机：等待一个停止事件，而不是 asyncio.Event().wait() 永远等下去。
        # 在支持 loop.add_signal_handler 的平台（POSIX）上，为 SIGTERM 与 SIGINT
        # 注册处理器，收到即置位停止事件；随后照常 runner.cleanup()，并记录收到的
        # 信号（即退出原因）。这样 docker stop 的 SIGTERM 也能干净退出（退出码 0）。
        #
        # Windows 不支持 add_signal_handler（会抛 NotImplementedError），保留 Ctrl+C
        # （KeyboardInterrupt）路径，不能因此报错——注册失败就退回那条路径。
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
        # Windows：add_signal_handler 不可用，Ctrl+C 走这条路径
        log.info("收到 Ctrl+C（KeyboardInterrupt），停止服务")


if __name__ == "__main__":
    main()
