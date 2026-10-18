"""调试接口：GET /debug/structure（仅在 FLOWLENS_DEBUG=1 时注册）。

供控制台"内部结构"视图只读展示活跃环与线段树的当前状态。

- 只读：现读现算，不修改任何统计状态。
- 时间划分直接调用 engine._plan（真实查询唯一的"区间 → 环秒/树/边缘"划分入口），
  页面展示的拆分与真实查询一致，不另写一套划分逻辑。
- 事件数统一取自 url 维度叶子的 DDSketch 总计数（dd.n）：两个维度处理的是同一批
  事件，某秒的事件数在两个维度相同；而只有 url 维度的概要里存了计数（user 维度
  是 HLL，不存计数）。所以无论 by 取哪个维度，事件数都从 url 维叶子取。
"""

import os

import orjson
from aiohttp import web

from ..dims import Dim
from ..timewin import sec, split_range
from .params import _parse_at, _parse_by, _parse_window

# 每个秒桶展示的前几个 key
_TOP_N = 3


def is_enabled() -> bool:
    """FLOWLENS_DEBUG=1 时才启用调试接口。"""
    return os.environ.get("FLOWLENS_DEBUG") == "1"


def register(app: web.Application) -> None:
    """注册调试路由（create_app 在 FLOWLENS_DEBUG=1 时调用）。"""
    app.router.add_get("/debug/structure", _debug_structure)


# ---- 只读快照：各部分独立成纯函数，便于测试与基准测量 ----

def _dim_summary(hh, cap: int) -> dict:
    """一个维度秒桶的名单摘要：长度 / 容量 / 缺席上界 u / 前几个 key 及计数。

    全部走 HeavyHitters/TopK 的公开接口（len(top)、top.top(n)、u()）与公开配置
    cfg.heap_cap，不读底层堆的私有字段。
    """
    n = len(hh.top)
    top = [[e.key, e.count] for e in hh.top.top(_TOP_N)] if n else []
    return {"len": n, "cap": cap, "u": hh.u(), "top": top}


def _ring_buckets(engine) -> list:
    """活跃环里每个有数据的秒桶：秒号、事件数、两维名单摘要。"""
    cap = engine.cfg.heap_cap
    out = []
    for b in engine.ring.slots:
        if b.sec is None:
            continue
        events = b.dims[Dim.URL].stats.dd.n
        if events == 0:
            continue
        out.append({
            "sec": b.sec,
            "events": events,
            "dims": {
                "url": _dim_summary(b.dims[Dim.URL].hh, cap),
                "user": _dim_summary(b.dims[Dim.USER].hh, cap),
            },
        })
    out.sort(key=lambda x: x["sec"])
    return out


def _node_events(node, sec_events: dict) -> int:
    """节点事件数：带 DDSketch 的节点直接读总计数，否则按覆盖范围内叶子求和。"""
    if node.stats is not None and node.stats.dd is not None:
        return node.stats.dd.n
    return sum(sec_events.get(s, 0) for s in range(node.start, node.end))


def _tree_summary(engine, d: Dim) -> dict:
    """by 指定维度的线段树：每个已冻结节点与每层构建中节点的只读摘要。"""
    tree = engine.trees[d]
    # 每秒事件数取自 url 维叶子（见模块 docstring：两维同秒事件数相同）
    sec_events: dict[int, int] = {}
    for s, leaf in engine.trees[Dim.URL].levels[0].items():
        if leaf.stats is not None and leaf.stats.dd is not None:
            sec_events[s] = leaf.stats.dd.n
    nodes = []
    for k, lvl in enumerate(tree.levels):
        for start in sorted(lvl):
            node = lvl[start]
            nodes.append({
                "level": k, "start": node.start, "end": node.end,
                "items": len(node.keys), "u": node.u,
                "events": _node_events(node, sec_events),
            })
    builders = []
    for k in range(1, tree.cfg.levels + 1):
        b = tree.builders[k]
        if b is None or b.start is None:
            continue
        # 已并入的秒数：从节点起点到最新定稿秒（不超过节点跨度）
        newest = tree.newest if tree.newest is not None else b.start - 1
        filled = min(newest + 1, b.start + b.span) - b.start
        builders.append({"level": b.level, "start": b.start, "span": b.span,
                         "seconds": filled, "cand": len(b.cand)})
    return {"by": str(d), "newest": tree.newest,
            "retention_range": tree.frozen_range(),
            "nodes": nodes, "builders": builders}


def _plan_summary(engine, d: Dim, at_ms: int, window_ms: int) -> dict:
    """按真实查询的同一区间调用 engine._plan，返回拆分结果（与真实查询一致）。"""
    from_ms, to_ms = at_ms - window_ms, at_ms
    plan = engine._plan(d, split_range(from_ms, to_ms))
    tree_nodes = []
    if plan.history is not None:
        s0, e0 = plan.history
        cover = engine._cover(d, s0, e0, stats_only=False)
        tree_nodes = [{"level": n.level, "start": n.start, "end": n.end} for n in cover]
    return {
        "from_ms": from_ms, "to_ms": to_ms,
        "ring_secs": [b.sec for b in plan.ring_buckets],
        "tree_nodes": tree_nodes,
        "edges": [{"sec": e.sec, "frac": e.frac, "in_ring": e.in_ring} for e in plan.edges],
    }


def build_structure(engine, d: Dim, at_ms: int | None = None,
                    window_ms: int = 60_000) -> dict:
    """组装 /debug/structure 的只读数据（纯函数，便于测试与基准测量）。

    提供 at_ms（查询结束时刻，epoch 毫秒）时，额外返回该 [at-window, at) 区间的
    拆分结果；window_ms 为该查询的窗口长度（毫秒），缺省 60 秒。
    """
    now_ms = engine.clock.now_ms()
    tree = engine.trees[d]
    data: dict = {
        "now_ms": now_ms,
        "now_sec": sec(now_ms),
        "ring_next": engine.ring.next,
        "latest_finalized_sec": tree.newest,
        "retention_sec": engine.cfg.retention_sec,
        "retention_range": tree.frozen_range(),
        "ring": _ring_buckets(engine),
        "tree": _tree_summary(engine, d),
    }
    if at_ms is not None:
        data["plan"] = _plan_summary(engine, d, at_ms, window_ms)
    return data


async def _debug_structure(request: web.Request) -> web.Response:
    # 延迟导入：避免 app.py 顶部 import debug 时产生循环导入
    from .app import query_service_key
    engine = request.app[query_service_key].engine
    q = request.query
    d = _parse_by(q.get("by", "url"))
    at_ms = None
    window_ms = 60_000
    at_raw = q.get("at")
    if at_raw is not None and at_raw.strip():
        window_ms = _parse_window(q.get("window", "60s"))
        at_ms = _parse_at(at_raw, engine.clock.now_ms())
    data = build_structure(engine, d, at_ms, window_ms)
    return web.Response(content_type="application/json", body=orjson.dumps(data))
