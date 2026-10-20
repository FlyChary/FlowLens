"""P2a 三：解析与归一化方案的实际收益（只做基准，不修改服务）。

比较对象（每批 5000 条，flowgen 生成请求体）：
  A 现有：orjson 解析为 dict 列表 + flowlens.model.validate（含归一化）
  B msgspec：Event 结构体（Struct, gc=False）+ msgspec.json.Decoder(list[Event])
     解析，再用列表推导取出各列、做 validate 中剩余的检查（长度、范围、截断）
     与归一化（复用 flowlens.model 的函数，不复制逻辑）

再单独测一项：
  C URL 归一化的"ID 段替换"改用一个预编译正则一次 sub 完成，对比现有实现的耗时；
    并用 tests/test_model.py 的参考实现（改前 _normalize_url 的逐字拷贝）做等价性
    检查，报告是否存在不一致的输入（只报告，不修改服务）。

计时口径：每项跑若干轮（总事件数约 100 万），取中位数；每条 μs = 轮耗时 ÷ 批条数。
内存口径（tracemalloc，单独一轮、不叠加计时）：解析一个 5000 条批次的峰值（含
解析出的中间对象，解析返回后不释放——与"解析完持有到写入完成"的真实持有期一致）。

归一化缓存在 A/B/C 之间共享（与真实服务一致：长尾流量下缓存是热的）；
"长尾"与"全随机"两种请求体分别测（全随机下缓存几乎不命中）。

用法：python tools/bench_parse.py [--batch 5000] [--rounds 200] [--fuzz N]
"""
import argparse
import statistics
import sys
import time
import tracemalloc
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

import msgspec
import numpy as np
import orjson

from flowlens.model import (EVENT_ID_MAX_BYTES, INT64_MAX, KEY_MAX_BYTES,
                            _ID_SEG, _MULTI_SLASH, _SCHEME_HOST,
                            _normalize_url_impl, _strip_query_fragment,
                            _truncate_utf8, normalize_url, validate)
from flowgen import EventGen

# 参考实现（tests/test_model.py 里改前 _normalize_url 的逐字拷贝）
TESTS = ROOT / "tests"
sys.path.insert(0, str(TESTS))
import test_model  # noqa: E402  提供 ref_normalize_url 与 _T11B_EDGE


# ---- B：msgspec 类型化解析 ----

class Ev(msgspec.Struct, gc=False):
    """与请求体字段一一对应（msgspec 按类型强校验，非 int 直接解码失败）。"""

    event_id: str
    user_id: str
    url: str
    ts: int
    bytes: int


_DEC = msgspec.json.Decoder(list[Ev])


def parse_msgspec(body: bytes) -> tuple:
    """msgspec 路径：结构体解码 + 列表推导取列 + validate 的剩余检查 + 归一化。

    返回与 validate 的 EventBatch 同形的五列（ids, users, urls, ts, nbytes）。
    解码失败（某条字段类型错）时抛异常——基准数据全部合法，不会发生；
    真实服务的容错（逐条计 invalid）需要额外 try 逻辑，本报告在结论里说明。
    """
    evs = _DEC.decode(body)
    eids = [e.event_id for e in evs]
    uids = [e.user_id for e in evs]
    urls = [e.url for e in evs]
    tss = [e.ts for e in evs]
    byts = [e.bytes for e in evs]
    # validate 的剩余检查：event_id 长度（非 ASCII 按字节）、ts/bytes 的 int64 范围
    for eid, t, b in zip(eids, tss, byts):
        if len(eid) > EVENT_ID_MAX_BYTES or (
                not eid.isascii() and len(eid.encode("utf-8")) > EVENT_ID_MAX_BYTES):
            continue  # 基准数据合法，这里仅为保持检查逻辑完整
        if t <= 0 or t > INT64_MAX or b < 0 or b > INT64_MAX:
            continue
    users = [u if (u.isascii() and len(u) <= KEY_MAX_BYTES) else _truncate_utf8(u)
             for u in uids]
    return (eids, users, [normalize_url(u) for u in urls],
            np.array(tss, dtype=np.int64), np.array(byts, dtype=np.int64))


def parse_orjson(body: bytes) -> tuple:
    """现有路径：orjson 解析 + validate（含归一化），返回 EventBatch。"""
    return validate(orjson.loads(body))


# ---- C：正则一次 sub 的 ID 段替换 ----

# 与 _ID_SEG 同义的单段模式。整段边界：每个非根段前恰好一个 "/"（多斜杠已归一、
# 尾斜杠已去掉），后面是 "/" 或串尾。s 恒以 "/" 开头，第 0 段（根，空串）前没有
# "/"，天然不参与匹配——与现有 range(1, len(parts)) 的"从第 1 段起判"一致。
_SEG_ALT = (r"[0-9]+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
            r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{16,}")
_ID_SEG_RE = __import__("re").compile(r"/(" + _SEG_ALT + r")(?=/|$)")


def _id_replace_regex(s: str) -> str:
    """C 方案：ID 段替换 = 一次预编译正则 sub（s 为已剥 query/fragment 的路径）。"""
    return _ID_SEG_RE.sub("/{id}", s)


def _id_replace_current(s: str) -> str:
    """现有实现：split + 逐段 _ID_SEG.fullmatch（_normalize_url_impl 的 ID 段部分）。"""
    parts = s.split("/")
    fullmatch = _ID_SEG.fullmatch
    for i in range(1, len(parts)):
        if fullmatch(parts[i]) is not None:
            parts[i] = "{id}"
    return "/".join(parts)


def time_fn(fn, rounds: int) -> float:
    """fn() 跑 rounds 次的总秒数（分 5 组取组耗时中位数 × 轮数，抗偶发抖动）。
    fn 自己决定用哪个 body（闭包轮换）。"""
    g = max(1, rounds // 5)
    per = []
    for _ in range(5):
        t0 = time.perf_counter()
        for _ in range(g):
            fn()
        per.append((time.perf_counter() - t0) / g)
    return statistics.median(per) * rounds


def mem_peak(fn, body: bytes) -> float:
    """解析一个批次的 tracemalloc 峰值（字节；不含基线）。"""
    tracemalloc.start()
    tracemalloc.reset_peak()
    out = fn(body)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    del out
    return peak


def make_bodies(mode: str, batch: int, n_batches: int) -> list[bytes]:
    gen = EventGen(mode=mode, seed=7)
    bodies = []
    for _ in range(n_batches):
        evs = gen.batch(batch)
        bodies.append(gen.raw_batch(evs))
    return bodies


def main() -> None:
    ap = argparse.ArgumentParser(description="P2a 解析/归一化基准")
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--rounds", type=int, default=200, help="每模式每方案的轮数")
    ap.add_argument("--fuzz", type=int, default=20_000, help="正则等价性模糊测试条数")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    b = args.batch
    rounds = args.rounds
    total = b * rounds
    print(f"== 解析对比（每批 {b} 条，每方案 {rounds} 轮 ≈ {total / 1e4:.0f} 万条）==")
    results = {}
    for mode in ("长尾", "随机"):
        bodies = make_bodies(mode, b, 8)  # 8 个不同批次轮换，避免只压同一请求
        state = {"i": 0}
        def fn_o():
            state["i"] += 1
            parse_orjson(bodies[state["i"] % 8])
        def fn_m():
            state["i"] += 1
            parse_msgspec(bodies[state["i"] % 8])
        t_o = time_fn(fn_o, rounds)
        t_m = time_fn(fn_m, rounds)
        # 内存峰值（tracemalloc 单独跑，取 8 个批次的中位）
        po = statistics.median(mem_peak(parse_orjson, x) for x in bodies)
        pm = statistics.median(mem_peak(parse_msgspec, x) for x in bodies)
        results[mode] = (t_o, t_m, po, pm)
        print(f"[{mode}] orjson+validate {t_o / b / rounds * 1e6:7.2f} μs/条  "
              f"峰值 {po / 1048576:7.2f} MiB | "
              f"msgspec {t_m / b / rounds * 1e6:7.2f} μs/条  峰值 {pm / 1048576:7.2f} MiB | "
              f"msgspec 快 {t_o / t_m:.2f}×  省 {(po - pm) / 1048576:+.2f} MiB")

    # ---- C：ID 段替换 正则一次 sub（只隔离"ID 段替换"这一步，不做整管线）----
    print(f"\n== ID 段替换：现有逐段 fullmatch vs 预编译正则一次 sub（每批 {b} 条）==")
    for mode in ("长尾", "随机"):
        bodies = make_bodies(mode, b, 8)
        urls = orjson.loads(bodies[0])
        # 已剥 query/fragment 的路径（ID 替换在这一步作用的对象）
        paths = [_strip_query_fragment(e["url"]) for e in urls]
        t0 = time.perf_counter()
        for _ in range(rounds):
            for p in paths:
                _id_replace_current(p)
        t_old = time.perf_counter() - t0
        t0 = time.perf_counter()
        for _ in range(rounds):
            for p in paths:
                _id_replace_regex(p)
        t_new = time.perf_counter() - t0
        n = b * rounds
        print(f"[{mode}] 现有逐段 {t_old / n * 1e6:7.2f} μs/条 | 正则一次 sub "
              f"{t_new / n * 1e6:7.2f} μs/条 | sub 快 {t_old / t_new:.2f}×")

    # 等价性：正则方案 vs 参考实现（tests/test_model.ref_normalize_url）
    print(f"\n== 等价性检查（正则 sub 方案 vs 改前参考实现，模糊 {args.fuzz} 条）==")
    import random
    import string
    rng = random.Random(20261020)
    alpha = string.ascii_letters + string.digits + "-._~%:()!$&'*,=;@[]^`{|}"
    hexa = "0123456789abcdefABCDEF"
    def id_like() -> str:
        return "".join(rng.choice(hexa) for _ in range(
            rng.choice((8, 15, 16, 17, 32, 36))))
    def rand_seg():
        r = rng.random()
        if r < 0.25:
            return "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 20)))
        if r < 0.4:
            return id_like()
        if r < 0.5:
            return "".join(rng.choice("0123456789") for _ in range(8)) + "-" + \
                   "".join(rng.choice(hexa) for _ in range(4)) + "-" + \
                   "".join(rng.choice(hexa) for _ in range(4)) + "-" + \
                   "".join(rng.choice(hexa) for _ in range(4)) + "-" + \
                   "".join(rng.choice(hexa) for _ in range(12))
        return "".join(rng.choice(alpha) for _ in range(rng.randint(1, 12)))
    # 等价性：C 方案**完整管线**（与 _normalize_url_impl 逐步相同，只把 ID 段替换
    # 换成一次正则 sub）vs 改前参考实现 test_model.ref__normalize_url。两者都喂
    # "已剥 query/fragment 的路径 head"，与真实调用口径一致。
    print(f"\n== 等价性检查（正则 sub 完整管线 vs 改前参考实现，模糊 {args.fuzz} 条）==")

    def c_pipeline(head: str) -> str:
        """与 _normalize_url_impl 同管线，ID 段替换用 _id_replace_regex（一次 sub）。"""
        has_slashes = "//" in head
        s = _SCHEME_HOST.sub("", head, count=1) if has_slashes else head
        q, h = s.find("?"), s.find("#")          # head 已剥，通常 no-op
        if q == -1 or (h != -1 and h < q):
            q = h
        if q != -1:
            s = s[:q]
        if has_slashes:
            s = _MULTI_SLASH.sub("/", s)
        if not s:
            s = "/"
        elif s[0] != "/":
            s = "/" + s
        if len(s) > 1:
            s = s.rstrip("/")
        s = _id_replace_regex(s)                 # ← 唯一的差别
        b_ = s.encode("utf-8")
        if len(b_) > KEY_MAX_BYTES:
            s = b_[:KEY_MAX_BYTES].decode("utf-8", "ignore")
            if len(s) > 1 and s.endswith("/"):
                s = s.rstrip("/")
        return s

    bad = 0
    checked = 0
    cases = list(test_model._T11B_EDGE)
    for i in range(args.fuzz):
        if i % 4 == 0:
            s = "".join(rng.choice("abcABC0123456789-/") for _ in range(rng.randint(0, 30)))
        elif i % 4 == 1:
            s = "/" + "/".join(rand_seg() for _ in range(rng.randint(1, 6)))
        elif i % 4 == 2:
            s = "https://api.example.com/" + \
                "/".join(rand_seg() for _ in range(rng.randint(1, 5))) + \
                (f"?x={id_like()}&p=3" if rng.random() < 0.5 else "")
        else:
            s = "/" + "/".join(rand_seg() for _ in range(rng.randint(1, 5))) + \
                (f"#f{rng.randint(0, 999)}" if rng.random() < 0.3 else "")
        cases.append(s)
    for raw in cases:
        head = _strip_query_fragment(raw)
        got = c_pipeline(head)
        want = test_model.ref__normalize_url(head)
        checked += 1
        if got != want:
            bad += 1
            if bad <= 5:
                print(f"  不一致: {raw!r} → 正则={got!r} 参考={want!r}")
    print(f"检查 {checked} 条，不一致 {bad} 条")
    print("SUMMARY", {m: {"orjson_us": round(t_o / b / rounds * 1e6, 2),
                          "msgspec_us": round(t_m / b / rounds * 1e6, 2),
                          "orjson_peak_mib": round(po / 1048576, 2),
                          "msgspec_peak_mib": round(pm / 1048576, 2)}
                      for m, (t_o, t_m, po, pm) in results.items()})


if __name__ == "__main__":
    main()
