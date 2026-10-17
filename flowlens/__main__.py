"""启动入口：$env:PYTHONHASHSEED="0"; python -m flowlens，监听 :8080。"""

import asyncio
import os

from aiohttp import web

from .api.app import create_app, make_engine
from .timewin import Clock


def main() -> None:
    # CLAUDE.md 规则 5：字符串哈希依赖固定 PYTHONHASHSEED（任意非 random 的具体值），
    # 否则重启前后哈希不一致，落盘数据将来无法合并
    seed = os.environ.get("PYTHONHASHSEED")
    if not seed or seed == "random":
        print("警告：PYTHONHASHSEED 未固定（或为 random），哈希结果每次启动不同，"
              "落盘数据将来无法合并。请固定为具体值（推荐 0）。")

    engine, ingest_service, query_service = make_engine(Clock())
    app = create_app(engine, ingest_service, query_service)

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
        site = web.TCPSite(runner, "0.0.0.0", 8080)
        await site.start()
        print("FlowLens 监听 :8080")
        # 保持运行直到收到中断
        try:
            await asyncio.Event().wait()
        finally:
            await runner.cleanup()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("已停止")


if __name__ == "__main__":
    main()
