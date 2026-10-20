"""T10a 工具自身的测试：生成器、离线精确解、发送器。

可复用逻辑（flowgen / exact / loadgen / boundary_cases）都写成可导入模块，
命令行入口只做参数解析——因此这里直接 import 它们测试，不起 subprocess。
tools/ 不在包路径里，这里显式加进 sys.path。
"""

import asyncio
import json
import sys
from pathlib import Path

import numpy as np
import orjson
import pytest

_TOOLS = Path(__file__).resolve().parent.parent / "tools"
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

from flowgen import (NORMAL, LATE, FUTURE, INVALID, ArchiveWriter, EventGen,
                     finalize_archive, key_of, load_archive, raw_url, user_of)
from flowlens.model import normalize_url
from exact import ExactSolution, compare, LATENESS_MS
import loadgen
import boundary_cases as bc


# ---------------- 一、事件生成 ----------------

def test_generation_deterministic_same_seed():
    """同一 (seed, 分布, 参数) 两次生成完全相同。"""
    def gen(mode, seed):
        g = EventGen(mode=mode, seed=seed, p_late=0.05, p_future=0.02,
                     p_cross=0.03, p_dup=0.02, p_invalid=0.01)
        out = []
        for b in range(5):
            for e in g.batch(1000, sent_ms=1_750_000_000_000 + b * 200):
                out.append((e.seq, e.url_idx, e.user_idx, e.ts, e.nbytes,
                            e.etype, e.invalid_kind))
        return out
    for mode in ("长尾", "均匀", "随机"):
        assert gen(mode, 42) == gen(mode, 42)
    assert gen("长尾", 42) != gen("长尾", 43)


def test_distribution_shape_longtail_vs_uniform():
    """长尾（Zipf s=1.1）前 10 个 URL 占比明显高于均匀。"""
    def top10_share(mode):
        g = EventGen(mode=mode, seed=11)
        cnt = {}
        for _ in range(20):
            for e in g.batch(5000, sent_ms=1_750_000_000_000):
                if e.etype == NORMAL:
                    cnt[e.url_idx] = cnt.get(e.url_idx, 0) + 1
        tot = sum(cnt.values())
        top = sum(v for _, v in sorted(cnt.items(), key=lambda kv: -kv[1])[:10])
        return top / tot
    assert top10_share("长尾") > 0.30
    assert top10_share("均匀") < 0.06


def test_random_mode_no_duplicates():
    """全随机：每个事件的 URL 与用户编号都不重复（跨批也不重）。"""
    g = EventGen(mode="随机", seed=5)
    urls, users = [], []
    for _ in range(20):
        for e in g.batch(500, sent_ms=1_750_000_000_000):
            if e.etype in (NORMAL, LATE, FUTURE):
                urls.append(e.url_idx)
                users.append(e.user_idx)
    assert len(set(urls)) == len(urls)
    assert len(set(users)) == len(users)


def test_raw_url_normalizes_to_key():
    """对大量随机样本，normalize_url(raw_url(key_of(i))) == key_of(i)。

    T10b：key_of 现在约一半是含 {id} 段的模板，raw_url 会把 {id} 替换成随机值、
    并按比例加 scheme://host、query、fragment——所有变化形式归一化后都还原成
    key_of(i)。断言 id 模板确实出现了（特性真被覆盖）、且两种模板都验证到。
    """
    g = EventGen(mode="长尾", seed=99)
    evs = []
    for _ in range(20):
        evs += g.batch(1000, sent_ms=1_750_000_000_000)
    checked = 0
    saw_id_key = False       # 至少一条 key 模板含 {id}
    saw_scheme_host = False  # 至少一条 raw_url 带了 scheme://host
    for e in evs:
        if e.etype in (NORMAL, LATE, FUTURE):
            k = key_of(e.url_idx)
            if "{id}" in k:
                saw_id_key = True
            d = g.raw(e)
            if d["url"].startswith("https://"):
                saw_scheme_host = True
            assert normalize_url(d["url"]) == k, d["url"]
            checked += 1
    assert checked > 3000
    assert saw_id_key, "没有覆盖到含 {id} 段的 key 模板"
    assert saw_scheme_host, "没有覆盖到带 scheme://host 的 raw_url"


def test_id_key_templates_diverse():
    """含 {id} 的模板确实用了多个不同路径，且约一半编号启用 {id} 模板。"""
    # 两个模板都应出现（i % len 轮转）
    keys = [key_of(i) for i in range(100)]
    id_keys = [k for k in keys if "{id}" in k]
    assert len(id_keys) > 40, f"含 {{id}} 模板比例偏低: {len(id_keys)}/100"
    prefixes = {k.split("/x")[0] for k in id_keys}
    assert len(prefixes) >= 2, f"含 {{id}} 的模板路径不够多样: {prefixes}"
    # key_of 是编号的注入映射：不同编号 → 不同 key
    assert len(set(keys)) == len(keys)


def test_ts_avoids_decision_boundaries():
    """迟到 61.5~119.5s（避 60±0.5）、未来 5.5~30s（避 5±0.5）、正常 0~3s。"""
    sent = 1_750_000_000_000
    g = EventGen(mode="均匀", seed=1, p_late=1.0)
    evs = g.batch(3000, sent_ms=sent)
    back = [sent - e.ts for e in evs]
    assert min(back) >= 61_500 and max(back) <= 119_500

    g2 = EventGen(mode="均匀", seed=1, p_future=1.0)
    evs2 = g2.batch(3000, sent_ms=sent)
    fwd = [e.ts - sent for e in evs2]
    assert min(fwd) >= 5_500 and max(fwd) <= 30_000

    g3 = EventGen(mode="均匀", seed=1)
    evs3 = [e for e in g3.batch(5000, sent_ms=sent) if e.etype == NORMAL]
    assert evs3
    assert all(0 <= sent - e.ts <= 3_000 for e in evs3)


# ---------------- 二、存档 ----------------

def test_archive_write_read_roundtrip(tmp_path):
    """存档写入与读取结果一致（多 shard）。"""
    total = 0
    for shard in range(2):
        g = EventGen(mode="长尾", seed=3, p_late=0.05, p_future=0.02,
                     p_invalid=0.01, seq_offset=shard * 10 ** 8)
        w = ArchiveWriter(tmp_path, shard=shard, flush_every=3000)
        for b in range(4):
            evs = g.batch(2000, sent_ms=1_750_000_000_000 + b * 150)
            w.add(evs)
            total += len(evs)
        w.close()
    finalize_archive(tmp_path, base_meta={"seed": 3, "mode": "长尾"})
    data = load_archive(tmp_path)
    assert len(data) == total
    # 跨 shard 的 event_id 不冲突
    assert len(set(data.event_id.tolist())) == len(data)
    assert set(data.etype.tolist()) <= {NORMAL, LATE, FUTURE, INVALID}


# ---------------- 三、离线精确解（对照最朴素实现）----------------

def _naive_accept(batches):
    """最朴素的逐条重放：去非法/未来、按 sent_ms 判迟到、event_id 去重保留首次。"""
    seen = set()
    acc = []
    for evs in batches:
        for e in evs:
            if e.etype == INVALID or e.etype == FUTURE:
                continue
            if e.ts < e.sent_ms - LATENESS_MS:
                continue
            if e.seq in seen:
                continue
            seen.add(e.seq)
            acc.append((e.ts, e.url_idx, e.user_idx, e.nbytes))
    acc.sort(key=lambda t: t[0])
    return acc


def _make_solution(tmp_path, seed=9):
    g = EventGen(mode="长尾", seed=seed, p_late=0.1, p_future=0.05,
                 p_cross=0.1, p_dup=0.1, p_invalid=0.1)
    batches = [g.batch(1500, sent_ms=1_750_000_000_000 + b * 120) for b in range(12)]
    w = ArchiveWriter(tmp_path, shard=0, flush_every=500)
    for evs in batches:
        w.add(evs)
    w.close()
    finalize_archive(tmp_path, base_meta={"seed": seed})
    return batches, ExactSolution.from_dir(tmp_path)


def test_exact_matches_naive(tmp_path):
    """topk（含并列字典序）、distinct_users、quantile_bytes、count_of 与朴素实现逐项一致。"""
    from collections import Counter
    batches, sol = _make_solution(tmp_path)
    acc = _naive_accept(batches)
    assert len(acc) == len(sol)
    assert [a[0] for a in acc] == sol.ts.tolist()  # 排序一致

    rng = np.random.default_rng(0)
    lo0, hi0 = int(sol.ts.min()), int(sol.ts.max())
    for _ in range(40):
        f = int(rng.integers(lo0, hi0))
        to = int(rng.integers(f + 1, hi0 + 1))
        u, s, bs = Counter(), Counter(), []
        for ts, ui, usi, b in acc:
            if f <= ts < to:
                u[ui] += 1
                s[usi] += 1
                bs.append(b)
        # topk（次数降序、并列按 key 字典序）
        exp_u = sorted(((c, key_of(k)) for k, c in u.items()),
                       key=lambda p: (-p[0], p[1]))[:7]
        assert sol.topk("url", f, to, 7) == [(k, c) for c, k in exp_u], (f, to)
        exp_s = sorted(((c, user_of(k)) for k, c in s.items()),
                       key=lambda p: (-p[0], p[1]))[:5]
        assert sol.topk("user", f, to, 5) == [(k, c) for c, k in exp_s], (f, to)
        # 去重人数
        assert sol.distinct_users(f, to) == len(s)
        # 分位数（秩 floor(q(n-1))+1）
        bs.sort()
        for q in (0.0, 0.5, 0.95, 1.0):
            R = int(np.floor(q * (len(bs) - 1))) + 1 if bs else 0
            exp = bs[R - 1] if bs else float("nan")
            got = sol.quantile_bytes(f, to, q)
            if bs:
                assert got == exp
            else:
                assert got != got  # nan
        # 单 key 次数
        for k2 in list(u)[:3]:
            assert sol.count_of("url", f, to, key_of(k2)) == u[k2]


def test_exact_half_open_boundaries():
    """to 不含（等于 to 的行排除）、from 含（等于 from 的行计入）。"""
    sol = ExactSolution(
        ts=np.array([100, 200, 300, 400]),
        url_idx=np.array([1, 2, 2, 3]),
        user_idx=np.array([9, 8, 7, 6]),
        nbytes=np.array([10, 20, 30, 40]),
        sent_ms=np.array([100, 200, 300, 400]),
    )
    # key 由 key_of 生成（含 {id} 模板），不能硬编码 /api/item/x{i}
    k1, k2, k3 = key_of(1), key_of(2), key_of(3)
    assert sol.topk("url", 200, 300, 5) == [(k2, 1)]  # 200 含、300 不含
    # (0,500)：x1=1、x2=2、x3=1；x2 次数最高在前，x1 与 x3 次数并列、按 key 字典序
    exp = sorted([(1, k1), (2, k2), (1, k3)], key=lambda p: (-p[0], p[1]))
    assert sol.topk("url", 0, 500, 5) == [(k, c) for c, k in exp]
    assert sol.distinct_users(0, 500) == 4
    assert sol.quantile_bytes(0, 500, 1.0) == 40.0
    assert sol.quantile_bytes(600, 700, 0.5) != sol.quantile_bytes(600, 700, 0.5)


def test_compare_tie_rule():
    """召回口径：返回 key 的真实次数 ≥ 精确第 k 名即算命中（并列不算错）。"""
    exact = [("a", 10), ("b", 10), ("c", 10), ("d", 9)]
    svc = [{"key": "a", "count": 10.4}, {"key": "b", "count": 9.9},
           {"key": "d", "count": 9.2}, {"key": "x", "count": 8.0}]
    r = compare(svc, exact, k=4)
    # 第 4 名真实次数 = 9；a(10)、b(10)、d(9 并列) 命中，x(0) 不命中 → 3/4
    assert r["recall"] == 0.75
    assert r["in_bounds"] is True
    # in_bounds 失败情形（真实次数落在 [lower, upper] 外）
    r2 = compare([{"key": "a", "count": 10.0, "lower": 0, "upper": 9.5}],
                 exact, k=4)
    assert r2["in_bounds"] is False


def test_compare_empty_service_answer():
    """服务返回空 top-k：精确非空时召回应为 0（不是 1.0），两皆空才是 1.0。

    T10b 发现：降级期服务返回空响应，若按"分母为 0 记 1.0"会被误判成完美精度。
    """
    exact = [("a", 10), ("b", 9), ("c", 8)]
    r = compare([], exact, k=10)
    assert r["recall"] == 0.0
    assert r["in_bounds"] is True  # 没有返回 key，界判空平凡成立
    # 精确答案也为空（区间无数据）：全命中
    r2 = compare([], [], k=10)
    assert r2["recall"] == 1.0


# ---------------- 四、发送器（对测试里启动的服务）----------------

class _InProcQueue:
    """worker 用的进程内队列（替 mp.Queue，避免起子进程）。"""

    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)


def _cfg(**over):
    base = dict(url="http://127.0.0.1:0", workers=1, conc=1, rate=500,
                batch=100, duration=2.0, gzip=False, mode="长尾", seed=0,
                archive=None, max_retries=3, p_late=0.0, p_future=0.0,
                p_cross=0.0, p_dup=0.0, p_invalid=0.0, request_timeout_s=5.0,
                # P2a 快速模式（--fast）：与 argparse 的接口一致（默认关闭）
                fast=False, fast_templates=32)
    base.update(over)
    return base


def test_loadgen_503_retry_and_archive(tmp_path):
    """503 后按 Retry-After 重发、重发成功（202）的批次被存档。"""
    from aiohttp.test_utils import TestServer
    from aiohttp import web

    state = {"hits": 0, "flaky": 2}  # 前 2 次请求返回 503

    async def handler(request):
        state["hits"] += 1
        if state["hits"] <= state["flaky"]:
            resp = web.Response(status=503,
                                body=b'{"error":"overloaded"}',
                                content_type="application/json")
            resp.headers["Retry-After"] = "1"
            return resp
        body = await request.read()
        orjson.loads(body)  # 确认真的收到了请求体
        return web.Response(status=202,
                            body=b'{"accepted":1}', content_type="application/json")

    async def run():
        app = web.Application()
        app.router.add_post("/v1/ingest", handler)
        server = TestServer(app)
        await server.start_server()
        cfg = _cfg(url=f"http://127.0.0.1:{server.port}",
                   archive=str(tmp_path / "arch"), batch=100, rate=1000,
                   duration=2.0, conc=1)
        q = _InProcQueue()
        await loadgen._worker_async(cfg, 0, q)
        await server.close()
        return q, state["hits"]

    q, hits = asyncio.run(run())
    done = [m[1] for m in q.items if m[0] == "done"]
    assert len(done) == 1 and "error" not in done[0], done
    assert done[0]["retries"] >= 1, f"应发生过 503 重试：{done}"
    assert hits >= 3, "至少：2 次 503 + 1 次 202"
    # 重发成功的批次被存档
    finalize_archive(tmp_path / "arch", base_meta={"seed": 0, "mode": "长尾"})
    data = load_archive(tmp_path / "arch")
    assert len(data) > 0, "被接受的批次应落盘"
    # 存档里应有跨批重复被去重前的行（p_cross=0 时无重复；这里只验证非空且合法）
    assert set(data.etype.tolist()) <= {NORMAL, LATE, FUTURE, INVALID}


def test_loadgen_second_and_summary_json_shape():
    """每秒输出的 JSON 与汇总 JSON 格式正确（可被严格解析、字段齐全）。"""
    lat = [10.0, 20.0, 30.0]
    line = loadgen.second_line(1234, {"sent": 100, "accepted": 90,
                                      "rejected": 5, "errors": 5}, lat)
    d = json.loads(line)
    assert d["type"] == "second" and d["t"] == 1234
    for f in ("sent", "accepted", "rejected", "errors",
              "lat_p50_ms", "lat_p95_ms", "lat_p99_ms"):
        assert f in d, f
    assert d["sent"] == 100 and d["accepted"] == 90

    per_sec = {10: {"sent": 100, "accepted": 90, "rejected": 5, "errors": 5}}
    done = [{"wid": 0, "retries": 2, "cpu_s": 1.0, "wall_s": 2.0}]
    s = loadgen.summary_dict(_cfg(), per_sec, done, wall=2.0, t0_ms=1,
                             all_lat=lat, cpu_s=1.5)
    assert s["type"] == "summary"
    for f in ("sent", "accepted", "rejected", "errors", "accept_rate",
              "accepted_events_rate", "retries", "lat_p50_ms", "lat_p99_ms",
              "client_cpu_pct", "start_ms", "workers_done"):
        assert f in s, f
    assert s["sent"] == 100 and s["retries"] == 2 and s["workers_done"] == 1
    # 空列表的百分位为 None（而非报错）
    assert loadgen.pctl([], 50) is None


# ---------------- 五、边界用例的 Target 抽象（进程内）----------------

def test_boundary_cases_run_all():
    """boundary_cases.run_all 在进程内服务器上全部通过（与 e2e 共用同一套用例）。"""
    from aiohttp.test_utils import TestClient, TestServer
    from flowlens.api.app import create_app
    from flowlens.config import Config
    from flowlens.engine import Engine
    from flowlens.ingest import Admission, IngestService
    from flowlens.query import QueryService
    from flowlens.timewin import Clock

    def fake_mem():
        return 0, None

    async def run():
        engine = Engine(Config(), Clock())
        app = create_app(engine, IngestService(engine, Admission()),
                         QueryService(engine), tick_period=1000.0,
                         memory_reader=fake_mem)
        server = TestServer(app)
        client = TestClient(server)
        # 与生产一致：auto_decompress=False（否则 br/不支持的编码在服务端传输层就 400，
        # 到不了 decode_body 的 415）；lingering_time=1
        await server.start_server(auto_decompress=False, lingering_time=1)
        try:
            t = bc.Target(client=client, host="127.0.0.1", port=server.port)
            return await bc.run_all(t)
        finally:
            await client.close()

    results = asyncio.run(run())
    failed = [r for r in results if not r.passed]
    assert not failed, [f"{r.name}: {r.actual}" for r in failed]
    assert len(results) >= 18
