"""C1（T9a）内存账本：满载模拟 + 按组件统计实际内存占用 + 节省方案测算。

只做测量与分析，不修改 flowlens/ 下任何代码。用法（项目根目录）：

    $env:PYTHONHASHSEED="0"
    python tools/mem_report.py --mini                 # 70 秒小流量自测（先跑这个）
    python tools/mem_report.py --mode 长尾 --duration 660 --eps 30000
    python tools/mem_report.py --mode 随机  --duration 660 --eps 30000
    python tools/mem_report.py --peak                # 4 MB 请求写入路径峰值增量
    python tools/mem_report.py --recall              # 秒桶名单 512 vs 256 的召回对照
    （--json 路径 可把结果另存为 JSON）

满载模拟（第一、三节用）
    FakeClock 驱动 Engine，按 flowgen 的分布生成事件，以 5000 条/请求 的节奏走完整
    真实写入路径（orjson 序列化 → IngestService.ingest：解码/预留/解析/校验/URL
    归一化 → engine.ingest），每个模拟秒结束调一次 engine.tick()。默认 duration=660
    秒（11 分钟）：线段树保留期（600 秒）填满、活跃环（66 秒）也满。最后 120 秒内
    每 2 秒做一次默认窗口 topk（user 维、带 at → 回放），让查询结果缓存处于工作态。

记账方法（C1.md 第一节第 2、3 条）
    一个按对象 id 去重的深度遍历（gc.get_referents）：同一对象只计一次；numpy 数组
    按 nbytes；对每个组件分别做有向遍历、共享同一个 seen 集合，先到达者记谁的账
    （跨组件共享对象不重复计数）。名单里的 key 字符串单列一行（C_KEYS），并按维度
    统计"内容相同但对象不同"的重复份数与浪费字节（= 写入端驻留的潜在节省上界）。
    用 tracemalloc 总量与进程常驻集（RSS）做交叉核对，报告无法归类的剩余。

方案测算（第二、三节用；只分析、不实施、不改 flowlens/）
    1. 字符串驻留：由 key 字符串盘点算出"当前字节 / 驻留后字节 / 驻留表开销 / 净节省"
       （url 与 user 分列；url 已被归一化缓存驻留、user 未驻留），并用微基准估写入
       耗时影响。
    2. 叶子/节点 HLL p=14→12：按统计出的实例数折算。
    3. 去重 Bloom 位数减半：理论误判率。
    4. 秒桶名单容量 512→256：同一事件序列喂两个引擎，固定区间（最后 60 秒）用
       tools/exact.py 的 topk 与 compare 对比召回（k=100 与 k=512 两档）。
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import math
import os
import sys
import time
import tracemalloc
from collections import defaultdict
from pathlib import Path

import numpy as np

_ROOT = Path(__file__).resolve().parent.parent
# 必须把项目根放在 sys.path 最前：脚本模式下 CWD 不在 path 上，而 conda 环境的
# editable install 指向主仓库（D:\dhy\code\flowlens）——不显式插根，flowlens 会
# 解析到主线工作区，测的就不是本支线（lane-c）的代码。
sys.path.insert(0, str(_ROOT))
if str(_ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(_ROOT / "tools"))

from flowgen import EventGen, key_of, user_of           # noqa: E402
import orjson                                          # noqa: E402

from flowlens.bounds import top_k                      # noqa: E402
from flowlens.config import Config                     # noqa: E402
from flowlens.dims import ALL_DIMS, Dim                # noqa: E402
from flowlens.engine import Engine                     # noqa: E402
from flowlens import model as _model                   # noqa: E402
from flowlens.ingest import Admission, IngestService   # noqa: E402
from flowlens.model import EventBatch                  # noqa: E402
from flowlens.query import QueryService                # noqa: E402
from flowlens.timewin import FakeClock, split_range    # noqa: E402

if os.environ.get("PYTHONHASHSEED") != "0":
    print("警告：PYTHONHASHSEED 未固定为 0，哈希相关数字与正常服务不一致", file=sys.stderr)

BASE_NOW_MS = 1_760_000_000_000  # 固定模拟起点，保证可复现
SUB_BATCH = 5000                 # 每请求事件数（与 IngestService 分块、loadgen 一致；
                                 # 单个请求解压后必须 < 4 MB）

# ---- 组件名（账本行的组件列）----
C_BLOOM = "去重 Bloom filter"
C_RING_CMS = "活跃环·秒桶 Count-Min"
C_RING_LIST = "活跃环·秒桶名单（堆+索引表）"
C_RING_HLL = "活跃环·秒桶 HLL"
C_RING_DD = "活跃环·秒桶 DDSketch"
C_RING_MISC = "活跃环·其他结构"
C_TREE_CMS = "线段树·各层累加器（CMS）"
C_TREE_CAND = "线段树·构建中候选堆"
C_TREE_FROZEN = "线段树·叶子与节点（名单数组+keys）"
C_TREE_HLL = "线段树·叶子/节点 HLL"
C_TREE_DD = "线段树·叶子/节点 DDSketch"
C_TREE_MISC = "线段树·其他结构"
C_SNAP = "快照"
C_QCACHE = "查询结果缓存"
C_NORM = "URL 归一化缓存"
C_KEYS = "名单 key 字符串（单列）"
C_MISC = "引擎及其他（配置/时钟/控制器等）"
C_UNACCOUNTED = "无法归类的剩余（RSS−对象图）"

# DESIGN.md 第 5 节估算（字节），用于逐项对比
DESIGN_MB = {
    "Python 运行时与依赖库": 45,
    "活跃环": 34,
    "线段树": 30,
    "字符串表上限": 13,
    "去重 Bloom": 34,
    "在途写入（上限 10 万条）": 20,
    "查询临时内存与快照": 20,
}


# ---------------- 记账：按对象 id 去重的深度遍历 ----------------

import types as _types

def _probe_fn():
    pass
_FUNC = type(_probe_fn)                  # 普通函数
_BUILTIN_FUNC = type(len)                # builtin 函数/方法（type(globals) 同此）
_MODULE = type(sys)                      # 模块
_METHOD = _types.MethodType              # 绑定方法：不展开！否则经 __self__ 会"向上"
                                         # 走到宿主对象（如 ring.finalize → engine），
                                         # 把还没轮到遍历的组件提前归错账
# 注意：list/dict/tuple/str 等内置容器的 __module__ 也是 "builtins"，
# 不能用模块名判断，只能按"对象本身是不是函数/方法/模块/类型"判断。
_SKIP_TYPES = (_FUNC, _BUILTIN_FUNC, _MODULE, _METHOD, type,
               property, classmethod, staticmethod)


def _skip_expand(obj) -> bool:
    """不再向引用展开的对象：标量、numpy 数组（按 nbytes 计）、类，以及
    函数/方法/模块等解释器内部对象（展开只会带进源码/代码对象等无意义的东西）。"""
    t = type(obj)
    if t in (str, bytes, bytearray, int, float, bool, complex, type(None)):
        return True
    if isinstance(obj, np.ndarray):
        return True
    if t in _SKIP_TYPES:
        return True
    m = t.__module__ or ""
    return m.startswith("_") or m in ("abc", "enum", "functools",
                                      "contextlib", "warnings")


class MemWalker:
    """按对象 id 去重的深度遍历，按"组件"归账。

    - 每个对象（按 id）只计一次；先到达的组件记账（多组件共享同一 seen）。
    - numpy 数组按 nbytes 计、不展开（避免把内部缓冲重复计入）。
    - id 在 keystring_ids 里的字符串一律计入 C_KEYS 行（名单 key 字符串单列），
      无论它从哪个组件可达。

    用法：先 walk 大组件（Bloom、环、树、快照、缓存…），最后 walk([engine])，
    把散件（配置/时钟/控制器/容器对象）归入 C_MISC。
    """

    def __init__(self, keystring_ids: set[int]):
        self.seen: set[int] = set()
        self.keystring_ids = keystring_ids
        self.n: dict[str, int] = defaultdict(int)
        self.b: dict[str, int] = defaultdict(int)

    def walk(self, roots, comp: str) -> None:
        stack = list(roots)
        while stack:
            obj = stack.pop()
            i = id(obj)
            if i in self.seen:
                continue
            self.seen.add(i)
            sz = obj.nbytes if isinstance(obj, np.ndarray) else sys.getsizeof(obj)
            if isinstance(obj, str) and i in self.keystring_ids:
                self.n[C_KEYS] += 1
                self.b[C_KEYS] += sz
            else:
                self.n[comp] += 1
                self.b[comp] += sz
            if _skip_expand(obj):
                continue
            try:
                refs = gc.get_referents(obj)
            except Exception:
                continue
            stack.extend(refs)

    def total(self) -> int:
        return sum(self.b.values())


def _keystring_ids(engine: Engine) -> set[int]:
    """名单里存 key 字符串的全部位置：秒桶堆、候选堆、叶子/节点 keys、快照。"""
    ids: set[int] = set()

    def add(s):
        if isinstance(s, str):
            ids.add(id(s))

    for b in engine.ring.slots:
        for d in ALL_DIMS:
            for v in b.dims[d].hh.top.q.vals:
                add(v[0])
    for tree in engine.trees:
        for lv in tree.levels:
            for s in lv.values():
                for k in s.keys:
                    add(k)
        for b in tree.builders:
            if b is not None:
                for v in b.cand.vals:
                    add(v[0])
    if engine.snapshot is not None:
        for d in ALL_DIMS:
            for r in engine.snapshot.tops[d]:
                add(r.key)
    return ids


def _count_strings(engine: Engine) -> dict[str, dict[str, set[int]]]:
    """按 维度 → 值 → {字符串对象 id} 收集名单里的 key 字符串（含快照），
    用于驻留节省测算与重复份数统计。"""
    out: dict[str, dict[str, set[int]]] = {"url": {}, "user": {}}

    def add(dim: str, s):
        if isinstance(s, str):
            out[dim].setdefault(s, set()).add(id(s))

    for b in engine.ring.slots:
        for v in b.dims[Dim.URL].hh.top.q.vals:
            add("url", v[0])
        for v in b.dims[Dim.USER].hh.top.q.vals:
            add("user", v[0])
    for di, tree in enumerate(engine.trees):
        dim = ALL_DIMS[di]
        tag = "url" if dim == Dim.URL else "user"
        for lv in tree.levels:
            for s in lv.values():
                for k in s.keys:
                    add(tag, k)
        for b in tree.builders:
            if b is not None:
                for v in b.cand.vals:
                    add(tag, v[0])
    if engine.snapshot is not None:
        for di in ALL_DIMS:
            tag = "url" if ALL_DIMS[di] == Dim.URL else "user"
            for r in engine.snapshot.tops[di]:
                add(tag, r.key)
    return out


# 驻留表每条目开销估算：dict 表项均摊 + 值指针（CPython 3.11 量级，估算用）
INTERN_ENTRY_COST = 56


def _intern_savings(inv: dict[str, dict[str, set[int]]]) -> dict:
    """写入端驻留的节省测算（静态上界：假设名单里全部位置都换成驻留对象）。

    interned_bytes = 每个不同值保留一个规范对象；table_cost = 驻留表自身开销；
    saved = current − interned − table（可为负：表比省下的还贵）。
    """
    out = {}
    for dim, by_val in inv.items():
        cur_bytes = 0
        cur_objs = 0
        for val, ids in by_val.items():
            sz = sys.getsizeof(val)
            cur_bytes += sz * len(ids)
            cur_objs += len(ids)
        n_uniq = len(by_val)
        interned_bytes = sum(sys.getsizeof(v) for v in by_val)
        table_cost = n_uniq * INTERN_ENTRY_COST
        out[dim] = {
            "distinct_values": n_uniq,
            "current_objects": cur_objs,
            "dup_objects": cur_objs - n_uniq,
            "dup_waste_bytes": cur_bytes - interned_bytes,
            "interned_bytes": interned_bytes,
            "table_cost": table_cost,
            "saved_bytes": cur_bytes - interned_bytes - table_cost,
        }
    return out


def _intern_write_cost(eps: int, pool: list[str]) -> dict:
    """写入端驻留的耗时影响微基准：每请求 5000 条做一次
    batch.urls = [table.get(u) for u in batch.urls] 式的替换，测其耗时并外推到每秒。"""
    tab: dict[str, str] = {s: s for s in pool}

    def one_batch(urls: list[str]) -> list[str]:
        get = tab.get
        return [u if (r := get(u)) is None else r for u in urls]

    urls = pool[:SUB_BATCH]
    n = max(1, eps // SUB_BATCH)
    t0 = time.perf_counter()
    for _ in range(n):
        one_batch(urls)
    dt = time.perf_counter() - t0
    return {"per_request_ms": dt / n * 1000, "per_sim_sec_ms": dt * 1000,
            "pool_size": len(tab)}


def build_ledger(engine: Engine, qsvc: QueryService) -> dict:
    """对满载引擎做一次完整记账，返回账本 dict。"""
    kset = _keystring_ids(engine)
    w = MemWalker(kset)
    # 有向遍历（共享 seen，先到达者记账）
    w.walk([engine.bloom], C_BLOOM)
    ring = engine.ring
    for b in ring.slots:
        for d in ALL_DIMS:
            db = b.dims[d]
            w.walk([db.hh.cms], C_RING_CMS)
            w.walk([db.hh.top], C_RING_LIST)
            st = db.stats
            if st.hll is not None:
                w.walk([st.hll], C_RING_HLL)
            if st.dd is not None:
                w.walk([st.dd], C_RING_DD)
    w.walk([ring], C_RING_MISC)
    for tree in engine.trees:
        for b in tree.builders:
            if b is None:
                continue
            w.walk([b.cms], C_TREE_CMS)
            w.walk([b.cand], C_TREE_CAND)
            if b.stats is not None:
                if b.stats.hll is not None:
                    w.walk([b.stats.hll], C_TREE_HLL)
                if b.stats.dd is not None:
                    w.walk([b.stats.dd], C_TREE_DD)
        for lv in tree.levels:
            for s in lv.values():
                if s.stats is not None:
                    if s.stats.hll is not None:
                        w.walk([s.stats.hll], C_TREE_HLL)
                    if s.stats.dd is not None:
                        w.walk([s.stats.dd], C_TREE_DD)
                w.walk([s], C_TREE_FROZEN)
        w.walk([tree], C_TREE_MISC)
    w.walk([engine.snapshot], C_SNAP)
    w.walk([qsvc._cache], C_QCACHE)
    w.walk([_model._norm_cache], C_NORM)
    w.walk([engine], C_MISC)
    return {"rows": {k: {"n": w.n.get(k, 0), "b": w.b.get(k, 0)} for k in w.b},
            "walked_bytes": w.total()}


def _rss_bytes() -> int:
    try:
        with open("/proc/self/statm", encoding="utf-8") as f:
            return int(f.read().split()[1]) * 4096
    except OSError:
        try:
            from flowlens import memstat
            return memstat.memory_bytes()
        except Exception:
            return 0


# ---------------- 满载模拟 ----------------

def _drive(engine: Engine, gen: EventGen, eps: int, t0: int, t1: int) -> None:
    """单事件循环里逐秒：生成 → 5000 条/请求 → IngestService.ingest → tick。"""

    async def _run():
        ing = IngestService(engine, Admission())
        clock = engine.clock
        n_sub = (eps + SUB_BATCH - 1) // SUB_BATCH
        for s in range(t0, t1):
            clock.ms = BASE_NOW_MS + s * 1000
            now = clock.ms
            for _ in range(n_sub):
                evs = gen.batch(SUB_BATCH, now)
                body = orjson.dumps([gen.raw(e) for e in evs])
                await ing.ingest(body, "identity")
            engine.tick()

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()


def _rss_probe() -> int:
    return _rss_bytes()


def _op_peak(mode: str, seed: int, eps: int, window_s: int) -> dict:
    """单独一个引擎、开 tracemalloc 跑 window_s 秒，测"活跃操作的临时峰值"。

    与主模拟分开：主模拟要干净的稳态 RSS（容器真正看到的），tracemalloc 本身
    会抬高 RSS 且只关心"一次写入/定稿/查询期间的瞬时临时对象"，两者口径不同。
    """
    engine = Engine(Config(), FakeClock(BASE_NOW_MS))
    qsvc = QueryService(engine)
    gen = EventGen(mode=mode, seed=seed)
    tracemalloc.start()
    tracemalloc.reset_peak()

    async def _run():
        ing = IngestService(engine, Admission())
        clock = engine.clock
        n_sub = (eps + SUB_BATCH - 1) // SUB_BATCH
        for s in range(window_s):
            clock.ms = BASE_NOW_MS + s * 1000
            now = clock.ms
            for _ in range(n_sub):
                evs = gen.batch(SUB_BATCH, now)
                body = orjson.dumps([gen.raw(e) for e in evs])
                await ing.ingest(body, "identity")
            engine.tick()
            if s % 2 == 0:  # 顺带做几次默认窗口查询
                await qsvc.topk(Dim.USER, 60_000, 10, at_ms=now)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"window_s": window_s, "live_bytes": cur, "peak_bytes": peak}


def run(mode: str, seed: int, eps: int, duration_s: int,
        with_queries: bool = True, tm_window_s: int = 30) -> dict:
    # 基线：本进程 import numpy/orjson/flowlens 后的 RSS（= "Python 运行时与依赖"）
    baseline_rss = _rss_probe()
    t0 = time.perf_counter()
    engine = Engine(Config(), FakeClock(BASE_NOW_MS))
    qsvc = QueryService(engine)
    gen = EventGen(mode=mode, seed=seed)
    # 干净驱动全程（不开 tracemalloc）：测的是容器真正看到的稳态 RSS
    _drive(engine, gen, eps, 0, duration_s)

    if with_queries:
        async def _q():
            for s in range(max(0, duration_s - 120), duration_s, 2):
                await qsvc.topk(Dim.USER, 60_000, 10, at_ms=BASE_NOW_MS + s * 1000)
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_q())
        finally:
            loop.close()
    sim_s = time.perf_counter() - t0

    gc.collect()
    time.sleep(2)   # 等分配器把可归还的页还掉，稳态 RSS 才不虚高
    rss = _rss_bytes()
    ledger = build_ledger(engine, qsvc)
    inv = _count_strings(engine)
    # 模拟工具自身开销（EventGen 的 _sent 历史等），不计入服务端内存
    gw = MemWalker(set())
    gw.walk([gen], "sim-harness")
    op_peak = _op_peak(mode, seed, eps, tm_window_s)

    # 概要实例计数（p=12 折算用）
    hll_tree = dd_tree = 0
    for tree in engine.trees:
        for lv in tree.levels:
            for s in lv.values():
                if s.stats is not None:
                    hll_tree += 1 if s.stats.hll is not None else 0
                    dd_tree += 1 if s.stats.dd is not None else 0
        for b in tree.builders:
            if b is not None and b.stats is not None:
                hll_tree += 1 if b.stats.hll is not None else 0
                dd_tree += 1 if b.stats.dd is not None else 0
    hll_ring = sum(1 for b in engine.ring.slots
                   if b.dims[Dim.USER].stats.hll is not None)

    walked = ledger["walked_bytes"]
    delta_rss = max(0, rss - baseline_rss)
    # 驻留写入耗时的微基准池：取当前名单里出现过的全部 user 值
    user_pool = list(inv["user"].keys())[:200_000]
    return {
        "mode": mode, "seed": seed, "eps": eps, "duration_s": duration_s,
        "events": eps * duration_s, "sim_wall_s": sim_s,
        "rows": ledger["rows"], "walked_bytes": walked,
        "crosscheck": {
            "walked_bytes": walked,
            "rss_bytes": rss,
            "baseline_rss_bytes": baseline_rss,
            "delta_rss_bytes": delta_rss,
            "unaccounted_delta_rss_minus_walked": max(0, delta_rss - walked),
            "sim_harness_bytes": gw.total(),
            # 单独引擎、开 tracemalloc 的活跃操作临时峰值（写入+定稿+查询）
            "op_peak": op_peak,
        },
        "counts": {
            "hll_p14_tree": hll_tree, "hll_p14_ring": hll_ring, "dd_tree": dd_tree,
            "bloom_bits": engine.bloom.bits,
            "bloom_inserted": sum(g.n for g in engine.bloom.gens),
            "norm_cache_entries": len(_model._norm_cache),
        },
        "intern": _intern_savings(inv),
        "intern_write_cost": _intern_write_cost(eps, user_pool),
        "hll_p12": {
            "tree_instances": hll_tree, "ring_instances": hll_ring,
            "save_tree_bytes": hll_tree * ((1 << 14) - (1 << 12)),
            "save_ring_bytes": hll_ring * ((1 << 14) - (1 << 12)),
            "se_p14_pct": 3.7 / math.sqrt(1 << 14) * 100,
            "se_p12_pct": 3.7 / math.sqrt(1 << 12) * 100,
        },
        "bloom_halved": _bloom_halved(engine.cfg),
        "design_mb": DESIGN_MB,
    }


# ---------------- 方案 3：Bloom 位数减半 ----------------

def _bloom_halved(cfg) -> dict:
    c = cfg.bloom
    need = -c.per_gen * math.log(c.fp_rate) / math.log(2) ** 2
    bits = max(6, math.ceil(math.log2(need)))
    k = min(16, max(1, round((1 << bits) / c.per_gen * math.log(2))))
    m = 1 << bits
    m2, k2 = m // 2, max(1, k // 2)

    def fp(n, m_, k_):
        return (1 - math.exp(-k_ * n / m_)) ** k_

    return {
        "cur": {"m": m, "k": k,
                "fp_at_capacity": fp(c.per_gen, m, k),
                "fp_at_150pct": fp(int(c.per_gen * 1.5), m, k)},
        "half": {"m": m2, "k": k2,
                 "fp_at_capacity": fp(c.per_gen, m2, k2),
                 "fp_at_150pct": fp(int(c.per_gen * 1.5), m2, k2)},
    }


# ---------------- 方案 4：秒桶名单容量 512→256 的召回影响 ----------------

def _recall_two_engines(mode: str, seed: int, eps: int, dur: int) -> dict:
    """同一事件序列喂 cap=512 与 cap=256 两个引擎，固定区间（最后 60 秒、url 维）
    用 tools/exact.py 的 topk/compare 对比召回（k=100 与 k=512 两档）。

    事件全为 NORMAL、ts 均匀取在"当前秒"内（now−[1,1000) ms）：全部被接受，
    且每批恰好落在第 s 秒；固定区间 [now−60s, now) 恰好 60 个整秒。精确解直接
    按编号计数（ts 升序 = 生成顺序，无重复）。"""
    from exact import ExactSolution, compare

    cfgA = Config()                                   # heap_cap = 512
    cfgB = Config(heap_cap=256)                       # 只改秒桶名单容量
    engA = Engine(cfgA, FakeClock(BASE_NOW_MS))
    engB = Engine(cfgB, FakeClock(BASE_NOW_MS))
    gen = EventGen(mode=mode, seed=seed)

    ts_all: list[int] = []
    u_all: list[int] = []
    t0 = time.perf_counter()
    for s in range(dur):
        now = BASE_NOW_MS + s * 1000
        engA.clock.ms = engB.clock.ms = now
        evs = gen.batch(eps, now)
        ts_arr = (now - gen.rng.integers(1, 1000, len(evs))).astype(np.int64)
        if dur - 60 <= s < dur:
            ts_all.extend(ts_arr.tolist())
            u_all.extend(e.url_idx for e in evs)
        batch = EventBatch(
            [f"e{e.seq}" for e in evs],
            [user_of(e.user_idx) for e in evs],
            [key_of(e.url_idx) for e in evs],
            ts_arr, np.array([e.nbytes for e in evs], np.int64), 0)
        engA.ingest(batch)
        engB.ingest(batch)
        engA.tick()
        engB.tick()
    sim_s = time.perf_counter() - t0

    to_ms = BASE_NOW_MS + dur * 1000
    from_ms = to_ms - 60_000
    ts_arr = np.array(ts_all, np.int64)
    order = np.argsort(ts_arr, kind="stable")
    sol = ExactSolution(ts=ts_arr[order], url_idx=np.array(u_all, np.int64)[order],
                        user_idx=np.zeros(len(ts_arr), np.int64),
                        nbytes=np.zeros(len(ts_arr), np.int64),
                        sent_ms=ts_arr[order])
    exact512 = sol.topk("url", from_ms, to_ms, 512)

    def svc_topk(eng, k):
        parts = eng.collect_parts(Dim.URL, split_range(from_ms, to_ms))
        ans = top_k(parts, k)
        return [{"key": r.key, "count": r.count, "lower": r.lower, "upper": r.upper}
                for r in ans.items], ans.unseen_upper

    out = {"mode": mode, "interval": [from_ms, to_ms], "sim_s": sim_s,
           "exact_top10": [(k_, c) for k_, c in exact512[:10]],
           "exact_top512_size": len(exact512)}
    # 随机流量下 Top-K 大面积并列，召回口径（并列不算错）会掩盖差异；
    # 另给"原始重叠率"：返回 key 集合与精确 top-k 集合的交集 / k
    for tag, eng, cap in (("A_cap512", engA, 512), ("B_cap256", engB, 256)):
        r100, uu100 = svc_topk(eng, 100)
        r512, uu512 = svc_topk(eng, 512)
        got100 = {x["key"] for x in r100}
        got512 = {x["key"] for x in r512}
        out[tag] = {
            "k100": compare(r100, exact512[:100], 100),
            "k512": compare(r512, exact512, 512),
            "raw_overlap_k100": len(got100 & {k_ for k_, _ in exact512[:100]}) / 100,
            "raw_overlap_k512": len(got512 & {k_ for k_, _ in exact512}) / 512,
            "unseen_upper_k512": uu512,
        }
    return out


# ---------------- 4 MB 写入路径峰值 ----------------

def peak_4mb() -> dict:
    """模拟一个解压后 ~4 MB 的请求从解析到写入的 tracemalloc 峰值增量。"""
    import gzip
    rng = np.random.default_rng(0)
    w = 1.0 / np.arange(1, 20_001) ** 1.1
    n = 40_000  # ≈ 4 MB JSON（单请求上限：解压后 4 MB）
    uidx = rng.choice(20_000, n, p=w / w.sum())
    suidx = rng.integers(0, 200_000, n)
    nbytes = np.clip(rng.lognormal(math.log(10_000) - 1.6449, 1.0, n), 0, 2 ** 31 - 1)
    now = BASE_NOW_MS
    ts = now - rng.integers(0, 2_000, n)
    rows = [{"event_id": f"e{i}", "user_id": user_of(int(s)), "url": key_of(int(u)),
             "ts": int(t), "bytes": int(b)}
            for i, (u, s, t, b) in enumerate(zip(uidx, suidx, ts, nbytes))]
    body = orjson.dumps(rows)
    out = {"n_events": n, "json_bytes": len(body)}
    for name, raw in (("identity", body), ("gzip", gzip.compress(body))):
        engine = Engine(Config(), FakeClock(now))
        ing = IngestService(engine, Admission())
        tracemalloc.start()
        tracemalloc.reset_peak()
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(ing.ingest(raw, name))
        finally:
            loop.close()
        cur, pk = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        out[name] = {"raw_bytes": len(raw),
                     "peak_delta_bytes": pk, "live_after_bytes": cur}
    return out


# ---------------- 报告 ----------------

def _mb(x: float) -> str:
    return f"{x / 1048576:.2f}"


def print_report(r: dict) -> None:
    print(f"\n=== 内存账本（{r['mode']}，{r['eps']}/秒 × {r['duration_s']} 秒，"
          f"事件 {r['events']:,}，模拟墙钟 {r['sim_wall_s']:.1f} s）===")
    rows = r["rows"]
    total = sum(v["b"] for v in rows.values())
    print(f"\n| 组件 | 对象数 | 字节(MB) | 占比 |")
    print(f"|---|---:|---:|---:|")
    for k in sorted(rows, key=lambda k: -rows[k]["b"]):
        v = rows[k]
        pct = 100 * v["b"] / total if total else 0
        print(f"| {k} | {v['n']:,} | {_mb(v['b'])} | {pct:.1f}% |")
    print(f"| 合计（对象图） | | {_mb(total)} | 100% |")
    c = r["crosscheck"]
    op = c["op_peak"]
    print(f"\n交叉核对：对象图（引擎+查询+缓存，存活数据）{_mb(c['walked_bytes'])} MB；"
          f"稳态 RSS 增量 {_mb(c['delta_rss_bytes'])} MB"
          f"（基线解释器+依赖 {_mb(c['baseline_rss_bytes'])} MB，gc+2s 后读数）；"
          f"RSS增量−对象图 = {_mb(c['unaccounted_delta_rss_minus_walked'])} MB"
          f"（分配器保留的已释放页+碎片，非存活数据）；"
          f"活跃操作临时峰值（tracemalloc，单独 {op['window_s']} s 窗口）"
          f"存活 {_mb(op['live_bytes'])} / 峰值 {_mb(op['peak_bytes'])} MB；"
          f"模拟工具自身 {_mb(c['sim_harness_bytes'])} MB（不计入服务端）")
    ct = r["counts"]
    print(f"HLL 实例（p=14）：线段树 {ct['hll_p14_tree']}、活跃环 {ct['hll_p14_ring']}；"
          f"线段树 DDSketch {ct['dd_tree']}；Bloom bits={ct['bloom_bits']}、"
          f"窗口内插入 {ct['bloom_inserted']:,}；归一化缓存 {ct['norm_cache_entries']:,} 条")
    print("\n--- 方案测算 ---")
    for dim, v in r["intern"].items():
        print(f"驻留[{dim}]：不同值 {v['distinct_values']:,}、对象 {v['current_objects']:,}、"
              f"重复 {v['dup_objects']:,} 份（浪费 {_mb(v['dup_waste_bytes'])} MB）；"
              f"驻留后 {_mb(v['interned_bytes'])} + 表 {_mb(v['table_cost'])} MB；"
              f"净省 {_mb(v['saved_bytes'])} MB")
    wc = r["intern_write_cost"]
    print(f"驻留写入耗时（微基准）：每请求 {wc['per_request_ms']:.2f} ms，"
          f"每模拟秒 {wc['per_sim_sec_ms']:.1f} ms（池 {wc['pool_size']:,}）")
    h = r["hll_p12"]
    print(f"HLL p=14→12：线段树 {h['tree_instances']} 实例省 {_mb(h['save_tree_bytes'])} MB"
          f"（活跃环 {h['ring_instances']} 实例另省 {_mb(h['save_ring_bytes'])} MB）；"
          f"标准误差 {h['se_p14_pct']:.2f}% → {h['se_p12_pct']:.2f}%")
    bl = r["bloom_halved"]
    print(f"Bloom 减半：m {bl['cur']['m']:,}→{bl['half']['m']:,}、"
          f"k {bl['cur']['k']}→{bl['half']['k']}；满容量误判率 "
          f"{bl['cur']['fp_at_capacity']:.4%}→{bl['half']['fp_at_capacity']:.4%}、"
          f"150% 容量 {bl['cur']['fp_at_150pct']:.4%}→{bl['half']['fp_at_150pct']:.4%}")
    print("\n--- 与 DESIGN.md 第 5 节估算对比 ---")
    print("| 设计估算 | MB | 本账本对应项 |")
    print("|---|---:|---|")
    for name, mb in r["design_mb"].items():
        print(f"| {name} | {mb} | （见报告正文对照） |")


def main() -> None:
    p = argparse.ArgumentParser(description="C1 内存账本")
    p.add_argument("--mode", choices=["长尾", "随机"], default="长尾")
    p.add_argument("--eps", type=int, default=30_000)
    p.add_argument("--duration", type=int, default=660)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--no-queries", action="store_true")
    p.add_argument("--peak", action="store_true")
    p.add_argument("--recall", action="store_true")
    p.add_argument("--recall-duration", type=int, default=120)
    p.add_argument("--mini", action="store_true")
    p.add_argument("--json", type=str, default=None)
    args = p.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    if args.peak:
        import json
        print(json.dumps(peak_4mb(), ensure_ascii=False, indent=2))
        return
    if args.recall:
        import json
        out = _recall_two_engines(args.mode, args.seed, args.eps,
                                  args.recall_duration)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return
    if args.mini:
        args.duration = 70
        args.eps = min(args.eps, 5_000)
    r = run(args.mode, args.seed, args.eps, args.duration,
            with_queries=not args.no_queries)
    print_report(r)
    if args.json:
        import json
        Path(args.json).write_text(json.dumps(r, ensure_ascii=False, indent=1),
                                   encoding="utf-8")
        print(f"\n结果已写入 {args.json}")


if __name__ == "__main__":
    main()
