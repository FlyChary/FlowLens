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
        # lingering_time=0：请求体没读完就返回响应时立即断开，不再"等收完剩余请求体"。
        # Content-Length 超 4 MB 的 413 必须立即发出，客户端不必把几 MB 传完。
        runner = web.AppRunner(app, auto_decompress=False, lingering_time=0)
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
