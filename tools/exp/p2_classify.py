"""P2a 主进程 py-spy 采样分类。

读 p2_profile.sh 的产物目录（raw_t.txt + window.txt + h0/h1.json），把主进程的每条
采样按 P2a.md 的类别归类，报告每类占主进程的百分比（= 占全部采样），并按
"类别占比 × 主进程每条 μs"换算成每条 μs（主进程每条 μs = 采样窗口内主进程 CPU 秒
÷ 接受条数，取自 /proc 与 /healthz，与 P1b 口径一致）。

输入格式（py-spy record -t --format raw，一行一条聚合样本）：
  thread (tid): 线程名;帧1 (file:line);帧2 (file:line);...  计数
  栈自底向上（runpy 在外层），计数是该聚合栈出现的采样次数。

类别（判定为整条栈自底向上的第一个命中，顺序即优先级）：
  gc            —— GC 线程（py-spy 线程名 gc）
  pool          —— 进程池通信：concurrent.futures / multiprocessing / queue / threading
                    帧；或在写入入口里等解析结果（无写入核心帧、只有 asyncio 等待帧）
  tick          —— 定稿与快照：engine 的 advance/refresh_snapshot*/_dim_tops/collect_*、
                    _on_finalize、ring.advance/_finalize_through、segtree、bounds、ticker 工作帧
  write.*       —— 写入引擎：ingest/_ingest_core 入口下细分
                    bloom（dedup）、classify（timewin）、hash（hashing）、secgroup
                    （按秒分组的 numpy 帧）、dims（sketch/* 与 ring.slot）、intern、
                    decompress、other
  parse.*       —— 解压 / JSON 解析 / 校验与归一化 / 哈希（单进程路径 B 才有）
  http          —— aiohttp 协议与请求体读取、api 路由、响应构造（含 orjson.dumps）
  loop          —— asyncio 事件循环、selector、回调调度
  other         —— 其余
同时按线程统计采样占比（判断 GIL 争抢），--debug 打印未分类栈样例。

用法：python3 p2_classify.py <profile_dir> [--debug]
"""
import json
import sys
from pathlib import Path

# 帧标记：(文件名子串, 函数名集合或 None=该文件全部)
WRITE_ENTRY = [
    ("engine.py", {"ingest", "ingest_parsed", "_ingest_core"}),
    ("ingest.py", {"ingest", "_ingest_inproc", "_ingest_pooled"}),
]
TICK_WORK = [
    ("engine.py", {"advance", "refresh_snapshot", "refresh_snapshot_async",
                   "_snapshot_range", "_dim_tops", "_snapshot_stats", "_build_snapshot",
                   "_on_finalize", "collect_parts", "collect_stats", "_plan", "_cover"}),
    ("segtree.py", None),
    ("bounds.py", None),
    ("ring.py", {"advance", "_finalize_through"}),
    ("ticker.py", None),
    ("degrade.py", {"update"}),
]
POOL_MODS = {"concurrent", "multiprocessing", "queue", "threading",
             "concurrent.futures", "multiprocessing.context", "multiprocessing.process",
             "multiprocessing.util", "multiprocessing.reduction",
             "concurrent.futures.process", "concurrent.futures.thread",
             "concurrent.futures._base", "concurrent.futures.queue"}
WRITE_CORE_MARKERS = [  # "写入核心帧"：真正的写引擎计算
    ("dedup.py", None), ("hashing.py", None), ("intern.py", None),
    ("timewin.py", None), ("heavy.py", None), ("cms.py", None), ("topk.py", None),
    ("stats.py", None), ("hll.py", None), ("ddsketch.py", None), ("sketch", None),
    ("ring.py", {"slot"}),
    ("engine.py", {"ingest", "ingest_parsed", "_ingest_core"}),
]
AIOHTTP_MODS = {"aiohttp", "aiohttp.http_parser", "aiohttp.web_protocol",
                "aiohttp.web_app", "aiohttp.web_request", "aiohttp.payload",
                "aiohttp.streams", "aiohttp.http_writer", "aiohttp.helpers",
                "aiohttp.base_protocol", "aiohttp.resolver", "aiohttp.abc",
                "aiohttp.web_middlewares", "aiohttp.http_websocket",
                "aiohttp.web_response", "aiohttp.web_urldispatcher"}
ASYNCIO_MODS = {"asyncio", "asyncio.base_events", "asyncio.events",
                "asyncio.coroutines", "asyncio.futures", "asyncio.tasks",
                "asyncio.selector_events", "asyncio.proactor_events",
                "asyncio.sslproto", "asyncio.runners", "socket", "selectors",
                "concurrent.futures._base"}


def _base(fn: str) -> str:
    fn = fn.split(" (", 1)[0] if "(" in fn else fn
    return fn.rsplit("/", 1)[-1]


def _mod(fn: str) -> str:
    """近似模块名：site-packages/flowlens/sketch/heavy.py → flowlens.sketch.heavy；
    标准库 concurrent/futures/process.py → concurrent.futures.process。"""
    fn = fn.split(" (", 1)[0]
    if not fn:
        return ""
    if "site-packages" in fn:
        parts = fn.split("site-packages/", 1)[1].rsplit("/", 1)
        return parts[0].replace("/", ".") if len(parts) > 1 else ""
    if "/" in fn:
        return fn.rsplit("/", 1)[0].replace("/", ".")
    return ""


def _has(frames, marks) -> bool:
    for name, fn in frames:
        b = _base(fn)
        for mb, names in marks:
            if mb in b and (names is None or name in names):
                return True
    return False


def _has_mod(frames, mods) -> bool:
    for _name, fn in frames:
        if _mod(fn) in mods:
            return True
    return False


def classify(frames: list, thread_name: str) -> str:
    """一条采样（frames = [(函数名, 文件), ...] 自底向上）→ 类别。"""
    if thread_name == "gc":
        return "gc"
    # 进程池通信（仅池路径）
    if _has_mod(frames, POOL_MODS):
        return "pool"
    write_entry = _has(frames, WRITE_ENTRY)
    # 单进程路径（_ingest_inproc）里 model.py / orjson.loads 是"解析"，
    # 不是池等待——判定 pool 等待前必须先排除（否则 B 组的解析栈全被误吞）
    in_pooled_path = _has(frames, [("ingest.py", {"_ingest_pooled"})] +
                          [("engine.py", {"ingest_parsed"})])
    if write_entry and in_pooled_path and not _has(frames, WRITE_CORE_MARKERS):
        # 在写入入口里但没有写入核心帧：等解析结果 / 块间让出 → 池通信（相关等待）
        if _has_mod(frames, ASYNCIO_MODS) or \
           any(_base(fn) in ("futures.py", "runners.py") for _n, fn in frames):
            return "pool"
    # tick：定稿与快照
    if _has(frames, TICK_WORK):
        return "tick"
    # 解析/校验/归一化/哈希（单进程路径 B；池路径 A 主进程无这些帧）。
    # 必须在写入判定之前：单进程路径的解析与写入在同一条调用链上，
    # 快照落在 validate 段时栈里有 model.py、没有 sketch 帧
    if _has(frames, [("model.py", None)]):
        return "parse.validate"
    if any(_base(fn) in ("zlib.py", "gzip.py") for _n, fn in frames):
        return "parse.decompress"
    for n, fn in frames:
        if n in ("loads",) and "orjson" in fn:
            return "parse.json"
    # 真正的 key 哈希（单进程路径 engine.ingest 算三列）。bloom/CMS 的派生
    # （derive/mix64/double_hash）不算 parse.hash——它们会先被 write.bloom/dims
    # 或 tick 规则捕获（各自的 dedup.py/sketch/segtree 帧在栈里）
    if _has(frames, [("hashing.py", {"key_hashes", "key_hash"})]):
        return "parse.hash"
    # 写入引擎
    if write_entry:
        if _has(frames, [("dedup.py", None)]):
            return "write.bloom"
        if _has(frames, [("timewin.py", None)]):
            return "write.classify"
        if _has(frames, [("intern.py", None)]):
            return "write.intern"
        if _has(frames, [("sketch", None), ("ring.py", {"slot"})] +
                [("heavy.py", None), ("cms.py", None), ("topk.py", None),
                 ("stats.py", None), ("hll.py", None), ("ddsketch.py", None)]):
            return "write.dims"
        # key_hashes（engine.ingest 下的三列哈希；bloom/CMS 的 derive/mix64
        # 已被 write.bloom/dims 先捕获）
        if _has(frames, [("hashing.py", {"key_hashes", "key_hash"})]):
            return "write.hash"
        # 按秒分组：_ingest_core 之下的 numpy 帧（argsort/flatnonzero/掩码/切片）
        for _n, fn in frames:
            if "numpy" in fn or "multiarray" in fn:
                return "write.secgroup"
        if any(_base(fn) in ("zlib.py", "gzip.py") for _n, fn in frames):
            return "write.decompress"
        return "write.other"
    # 解析/校验/归一化/哈希（单进程路径 B）
    if _has(frames, [("model.py", None)]):
        return "parse.validate"
    if any(_base(fn) in ("zlib.py", "gzip.py") for _n, fn in frames):
        return "parse.decompress"
    for n, fn in frames:
        if n in ("loads",) and "orjson" in fn:
            return "parse.json"
    if _has(frames, [("hashing.py", None)]):
        return "parse.hash"
    # HTTP：aiohttp / api 路由 / 响应
    if _has_mod(frames, AIOHTTP_MODS):
        return "http"
    if _has(frames, [("app.py", None), ("errors.py", None), ("debug.py", None)]):
        return "http"
    if any(n in ("dumps",) and "orjson" in fn for n, fn in frames):
        return "http"  # 响应序列化
    # 事件循环
    if _has_mod(frames, ASYNCIO_MODS):
        return "loop"
    return "other"


def parse_raw(path: Path) -> list:
    """py-spy -t raw 格式 → [(tid, 线程名, [(函数,文件)...], 计数), ...]。"""
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        # "thread (tid): Name;f1 (file:line);f2 (file:line)  13"
        if not line.startswith("thread"):
            continue
        head, _, rest = line.partition(": ")
        tid = head.split("(")[1].split(")")[0]
        # 末尾是空白分隔的计数
        body, _, cnt_s = rest.rpartition(" ")
        cnt = int(cnt_s)
        parts = body.split(";")
        thread_name = parts[0]
        frames = []
        for p in parts[1:]:
            p = p.strip()
            if "(" in p and p.endswith(")"):
                fn = p[p.rfind("(") + 1:-1]
                name = p[:p.rfind("(") - 1].strip()
            else:
                name, fn = p, ""
            frames.append((name, fn))
        out.append((tid, thread_name, frames, cnt))
    return out


def main() -> None:
    d = Path(sys.argv[1])
    debug = "--debug" in sys.argv
    samples = parse_raw(d / "raw_t.txt")
    total = sum(c for *_x, c in samples)
    if total == 0:
        sys.exit("raw_t.txt 没有解析出任何采样；前 500 字符："
                 + (d / "raw_t.txt").read_text(encoding="utf-8")[:500])

    # 窗口 CPU 与接受条数
    label = rate = "?"
    cpu_s = acc = None
    try:
        label, rate, u0, s0, u1, s1, clkt = \
            (d / "window.txt").read_text().split()
        cpu_s = ((int(u1) - int(u0)) + (int(s1) - int(s0))) / int(clkt)
    except Exception:
        pass
    try:
        h0 = json.loads((d / "h0.json").read_text())
        h1 = json.loads((d / "h1.json").read_text())
        acc = h1.get("events_ingested", 0) - h0.get("events_ingested", 0)
    except Exception:
        pass
    us_ev = cpu_s / acc * 1e6 if (cpu_s and acc) else None

    cnt: dict[str, int] = {}
    by_thread: dict[tuple, int] = {}
    uncl: list = []
    for tid, tname, frames, c in samples:
        cat = classify(frames, tname)
        cnt[cat] = cnt.get(cat, 0) + c
        by_thread[(tid, tname)] = by_thread.get((tid, tname), 0) + c
        if debug and cat == "other":
            uncl.append((c, frames))

    print(f"== {label} rate={rate} samples={total} "
          f"main_cpu_s={cpu_s and round(cpu_s, 3)} acc={acc} "
          f"us/ev={us_ev and round(us_ev, 2)} ==")
    order = ["http", "loop", "pool",
             "parse.decompress", "parse.json", "parse.validate", "parse.hash",
             "write.bloom", "write.classify", "write.hash", "write.secgroup",
             "write.dims", "write.intern", "write.decompress", "write.other",
             "tick", "gc", "other"]
    keys = order + sorted(k for k in cnt if k not in order)
    print(f"{'类别':<20}{'占比%':>8}{'每条μs':>10}")
    for k in keys:
        if k not in cnt:
            continue
        pct = cnt[k] / total * 100
        us = pct / 100 * us_ev if us_ev else None
        print(f"{k:<20}{pct:>8.1f}{(round(us, 2) if us is not None else '-'):>10}")
    print("-- 线程 --")
    for (tid, tname), n in sorted(by_thread.items(), key=lambda kv: -kv[1]):
        print(f"tid={tid} name={tname:<24} {n / total * 100:>6.1f}%  ({n})")
    if debug and uncl:
        uncl.sort(key=lambda x: -x[0])
        print(f"-- 未分类样例（{len(uncl)} 种栈，前 5 条）--")
        for c, fr in uncl[:5]:
            print(f"  ×{c}", " <- ".join(f"{n}({fn.rsplit('/', 1)[-1]})"
                                          for n, fn in fr[-8:]))
    print("SUMMARY " + " ".join(
        f"{k}:{round(v / total * 100, 1)}pct" for k, v in
        sorted(cnt.items(), key=lambda kv: -kv[1])))


if __name__ == "__main__":
    main()
