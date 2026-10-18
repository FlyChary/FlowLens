"""experiments 冒烟测试（slow）：进程内起一个真实服务，跑几个缩短版场景，
验证结果目录与 meta.json / timeline.jsonl / summary.json 的格式（T10b 第二节第 4 点）。

不跑完整场景（S1 6 级×20s、S3 5 分钟等），只用很低的速率与几秒的时长，几秒钟跑完，
重点验证"结果目录结构 + 各文件字段"对得上 docs/EXPERIMENTS_FORMAT.md。
"""

import asyncio
import json
import os
import sys

import pytest
from aiohttp.test_utils import TestServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "tools"))

import experiments as ex  # noqa: E402
from flowlens.api.app import create_app  # noqa: E402
from flowlens.config import Config  # noqa: E402
from flowlens.engine import Engine  # noqa: E402
from flowlens.ingest import Admission, IngestService  # noqa: E402
from flowlens.query import QueryService  # noqa: E402
from flowlens.timewin import Clock  # noqa: E402


def _fake_memory_reader() -> tuple[int, int | None]:
    """固定读数：工作集 100 MB、上限 256 MB（确定、不依赖运行环境真实内存）。"""
    return 100 * 1024 * 1024, 256 * 1024 * 1024


def _run_scenario(scenario: str, outdir: str) -> dict:
    """在独立事件循环里：建服务 → 跑一个缩短版场景 → 关服务，返回 summary。"""
    import numpy as np

    async def _one():
        engine = Engine(Config(), Clock())
        qs = QueryService(engine)
        app = create_app(engine, IngestService(engine, Admission()), qs,
                         tick_period=1.0, memory_reader=_fake_memory_reader)
        server = TestServer(app)
        await server.start_server()
        port = server.make_url("").port
        svc = ex.Service("local", port, proc=None)
        rng = np.random.default_rng(0)
        try:
            if scenario == "S1":
                return await ex.run_s1(svc, outdir, gzip=False,
                                       rates=[3000], level_sec=4, batch=500)
            elif scenario == "S5":
                return await ex.run_s5(svc, outdir, base_rate=3000,
                                       overload_sec=4, observe_sec=3, batch=500)
            else:
                raise ValueError(scenario)
        finally:
            await server.close()
    return asyncio.run(_one())


def _read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(ln) for ln in f if ln.strip()]


@pytest.mark.slow
def test_experiments_result_format(tmp_path):
    # S1（plain）：验证 meta.json / timeline.jsonl / summary.json
    d1 = tmp_path / "S1_plain"
    d1.mkdir()
    ex.write_meta(str(d1), scenario="S1", target="local", port=0,
                  cpu_alloc={"service": "0", "loadgen": "1-6", "aux": "7"},
                  params={"gzip": False, "rates": [3000], "level_sec": 4})
    s1 = _run_scenario("S1", str(d1))

    meta = json.loads((d1 / "meta.json").read_text(encoding="utf-8"))
    assert meta["scenario"] == "S1"
    assert set(meta) >= {"scenario", "git_commit", "machine", "service", "params"}
    assert meta["machine"]["cpus"] > 0

    tl = _read_jsonl(d1 / "timeline.jsonl")
    assert tl, "timeline.jsonl 为空"
    for row in tl:
        assert set(row) >= {"t", "sent", "accepted", "rejected", "errors",
                            "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "healthz"}, row
    # 至少有一些秒带上了 healthz（工作集与降级级别）
    assert any(r["healthz"] is not None for r in tl)
    hz = next(r["healthz"] for r in tl if r["healthz"] is not None)
    assert "memory_bytes" in hz and "memory_current_bytes" in hz
    assert "level" in hz["degrade"]

    s1 = json.loads((d1 / "summary.json").read_text(encoding="utf-8"))
    assert s1["scenario"] == "S1"
    assert "levels" in s1 and "max_sustainable" in s1
    assert "memory" in s1
    ws = s1["memory"]["working_set"]
    assert set(ws) == {"start_mb", "peak_mb", "end_mb"}
    assert s1["memory"]["peak_degrade_level"] == 0  # 3000/s 不降级


@pytest.mark.slow
def test_experiments_s5_degrade_seq(tmp_path):
    d5 = tmp_path / "S5"
    d5.mkdir()
    ex.write_meta(str(d5), scenario="S5", target="local", port=0,
                  cpu_alloc={"service": "0"}, params={"base_rate": 3000})
    _run_scenario("S5", str(d5))
    s5 = json.loads((d5 / "summary.json").read_text(encoding="utf-8"))
    assert s5["scenario"] == "S5"
    assert "overload_rate" in s5 and "recover_seconds" in s5
    assert isinstance(s5["degrade_level_seq"], list)
    assert all(isinstance(x, list) and len(x) == 2 for x in s5["degrade_level_seq"])
    tl = _read_jsonl(d5 / "timeline.jsonl")
    assert tl and all("healthz" in r for r in tl)
