"""T10a 边界用例（pytest 形式）：进程内启动真实服务器，跑 tools/boundary_cases
的同一套用例。远端形式（对正在运行的服务）见 tools/selftest.py。

引擎用**真实时钟**（与 test_stream.py 一致）：用例里的迟到/未来/at 都按真实
"现在"构造，FakeClock（固定 2024 年）会与用例里的 now_ms() 脱节。
"""

import asyncio

from aiohttp.test_utils import TestClient, TestServer

import boundary_cases as bc
from flowlens.api.app import create_app
from flowlens.config import Config
from flowlens.engine import Engine
from flowlens.ingest import Admission, IngestService
from flowlens.query import QueryService
from flowlens.timewin import Clock


def _fake_memory_reader() -> tuple[int, int | None]:
    return 0, None  # 与运行环境真实内存解耦（T8b）


async def _serve_and_run(run):
    """真实时钟服务器，await run(client, server) 后干净关闭。"""
    engine = Engine(Config(), Clock())
    qs = QueryService(engine)
    app = create_app(engine, IngestService(engine, Admission()), qs,
                     tick_period=1000.0, memory_reader=_fake_memory_reader)
    server = TestServer(app)
    client = TestClient(server)
    # 与生产一致：auto_decompress=False（br 等不支持的编码要能到达 decode_body
    # 返回 415，而不是在服务端传输层因解码失败直接 400）；lingering_time=1
    await server.start_server(auto_decompress=False, lingering_time=1)
    try:
        t = bc.Target(client=client, host="127.0.0.1", port=server.port, base="")
        await run(t)
    finally:
        await client.close()


def test_all_boundary_cases():
    """19 条边界用例（与 tools/selftest.py 同一套 CASES，F2 加 NDJSON）。"""

    async def run(t):
        results = await bc.run_all(t)
        for r in results:
            assert r.passed, f"{r.name}: 期望 {r.expected}，实际 {r.actual}"

    asyncio.run(_serve_and_run(run))
