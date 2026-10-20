"""P2a 二.6：纯接收的 echo 服务——隔离出压测端的最大发送能力。

收任意 POST /v1/ingest，读完 body 立即返回 202（不解析、不计费）。
只测"压测端发得有多快"，不测服务。用法：
  python tools/exp/echo_server.py --port 9999
"""
import argparse
import asyncio

import aiohttp
from aiohttp import web


async def _ingest(request: web.Request) -> web.Response:
    body = await request.read()
    return web.Response(status=202, text=f'{{"accepted": {len(body)}}}')


async def _healthz(request: web.Request) -> web.Response:
    return web.Response(text='{"ok": true}')


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9999)
    args = p.parse_args()
    app = web.Application(client_max_size=64 * 1024 * 1024)
    app.router.add_post("/v1/ingest", _ingest)
    app.router.add_get("/healthz", _healthz)
    runner = web.AppRunner(app, access_log=None)
    asyncio.run(_serve(runner, args.port))


async def _serve(runner, port: int) -> None:
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    print(f"echo server on :{port}", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    main()
