"""P2b 三：tick（定稿 + 快照）的剖析（只诊断，不修改 flowlens/）。

用 FakeClock 驱动一个真实 Engine，按长尾与全随机两种分布、每秒 5 万 / 12 万 / 20 万
条写入（直接调用 engine.ingest，每个模拟秒调一次 advance + refresh_snapshot），在稳态
（活跃环与线段树填满）下测量每 tick 的 advance（定稿）与 refresh_snapshot（快照）耗时，
并细分到子步骤：
- 定稿：叶子摘要（Tree._freeze_leaf）、Count-Min 加入各层累加器（_Builder.accept）、
  节点完成（_Builder.freeze）；
- 快照：组装时间片（collect_parts）、Top-K 计算（含合并候选，bounds.top_k）、
  HLL 合并（collect_stats USER）、DDSketch 合并（collect_stats URL）。

子步骤用**运行时包一层计时器**的方式采集（本工具只包不改，冻结源码不变；与
T11a 的 profile_server 同法）。报告每项在三种速率下的每 tick 中位耗时。

用法：python tools/bench_tick.py [--rates 50000,120000,200000]
       [--modes 长尾,随机] [--warmup 70] [--measure 40]
"""
import argparse
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

import numpy as np

import flowlens.engine as eng_mod
from flowlens.config import Config
from flowlens.dims import ALL_DIMS, Dim
from flowlens.engine import Engine
from flowlens.model import EventBatch
from flowlens.timewin import FakeClock

POOL_N = 300_000  # 预生成的唯一 key 池大小（全随机每秒用 rate 条连续切片）


def make_pools():
    urls = [f"/api/item/x{i}" for i in range(POOL_N)]
    users = [f"u{i}" for i in range(POOL_N)]
    return urls, users


def make_second_batch(mode: str, rate: int, sec_ms: int, urls, users,
                      rng: np.random.Generator) -> EventBatch:
    """生成"模拟第 sec_ms 秒"的 rate 条事件，ts 全落在 [sec_ms−500, sec_ms)（近期、
    会被接受）。长尾：url Zipf(20000)、user 均匀(200000)；全随机：每秒用 rate 条连续
    唯一切片（该秒内全不重复）。"""
    n = rate
    if mode == "随机":
        off = (sec_ms // 1000) * rate % (POOL_N - rate)
        uidx = np.arange(off, off + rate) % POOL_N
        sidx = np.arange(off + 7, off + 7 + rate) % POOL_N
    else:
        # 与 flowgen 长尾一致：url Zipf(1.1)/20000、user Zipf(0.9)/200000
        w = 1.0 / np.arange(1, 20_001) ** 1.1
        w = w / w.sum()
        uidx = rng.choice(20_000, size=n, p=w)
        wu = 1.0 / np.arange(1, 200_001) ** 0.9
        wu = wu / wu.sum()
        sidx = rng.choice(200_000, size=n, p=wu)
    ts = sec_ms - rng.integers(0, 500, n)
    nbytes = rng.lognormal(9.0, 0.5, n).astype(np.int64)
    base_seq = (sec_ms // 1000) * rate  # 该秒的 id 序号基准（跨秒不重复）
    ids = [f"e{base_seq + j}" for j in range(n)]
    return EventBatch(ids, [users[i] for i in sidx.tolist()],
                      [urls[i] for i in uidx.tolist()],
                      ts.astype(np.int64), nbytes, 0)


class SubTimer:
    """累计各子步骤的墙钟秒数；per_tick(n) 换算成"每 tick 的 ms"。"""

    def __init__(self):
        self.acc: dict[str, float] = {}
        self.calls: dict[str, int] = {}

    def reset(self):
        self.acc.clear()
        self.calls.clear()

    def record(self, tag: str, dt: float):
        self.acc[tag] = self.acc.get(tag, 0.0) + dt
        self.calls[tag] = self.calls.get(tag, 0) + 1

    def wrap(self, fn, tag: str):
        rec = self.record

        def timed(*a, **k):
            t0 = time.perf_counter()
            r = fn(*a, **k)
            rec(tag, time.perf_counter() - t0)
            return r
        return timed

    def per_tick(self, ticks: int) -> dict:
        return {k: v / max(1, ticks) * 1000.0 for k, v in self.acc.items()}


def install_timers(engine: Engine, t: SubTimer) -> None:
    """把 tick 涉及的子步骤包上计时器（本工具只包不改，冻结源码不变）。
    定稿类由 advance 触发、快照类由 refresh_snapshot 触发，二者不重叠，可按 tag 归因。"""
    # 定稿：advance = ring.advance(触发回调→add_leaf) + bloom.advance
    engine.ring.advance = t.wrap(engine.ring.advance, "定稿/ring")
    engine.bloom.advance = t.wrap(engine.bloom.advance, "定稿/bloom")
    for tree in engine.trees:
        tree.add_leaf = t.wrap(tree.add_leaf, "定稿/add_leaf两维")
        tree._freeze_leaf = t.wrap(tree._freeze_leaf, "定稿/叶子摘要")
        for b in tree.builders[1:]:
            b.accept = t.wrap(b.accept, "定稿/CMS入层")
            b.freeze = t.wrap(b.freeze, "定稿/节点完成")
    # 快照：collect_parts + top_k + collect_stats(两维)
    engine.collect_parts = t.wrap(engine.collect_parts, "快照/组装时间片")
    eng_mod.top_k = t.wrap(eng_mod.top_k, "快照/TopK(合并候选+计算)")
    orig_stats = engine.collect_stats

    def stats_wrapped(d, r):
        tag = "快照/HLL合并" if d == Dim.USER else "快照/DDSketch合并"
        t0 = time.perf_counter()
        r_ = orig_stats(d, r)
        t.record(tag, time.perf_counter() - t0)
        return r_
    engine.collect_stats = stats_wrapped


TAGS = ("定稿/ring", "定稿/bloom", "定稿/add_leaf两维", "定稿/叶子摘要",
        "定稿/CMS入层", "定稿/节点完成",
        "快照/组装时间片", "快照/TopK(合并候选+计算)",
        "快照/HLL合并", "快照/DDSketch合并")


def run_rate(mode: str, rate: int, warmup: int, measure: int, urls, users) -> dict:
    clock = FakeClock(1_735_689_600_000)
    engine = Engine(Config(), clock)
    t = SubTimer()
    install_timers(engine, t)
    rng = np.random.default_rng(0)
    chunk = 5000
    start_sec = clock.ms // 1000
    adv_list: list[float] = []
    snap_list: list[float] = []
    window_started = False
    for sec in range(start_sec + 1, start_sec + 1 + warmup + measure):
        clock.ms = sec * 1000
        base = sec * 1000
        b = make_second_batch(mode, rate, base, urls, users, rng)
        for i in range(0, rate, chunk):
            sub = EventBatch(b.ids[i:i + chunk], b.users[i:i + chunk],
                             b.urls[i:i + chunk], b.ts[i:i + chunk],
                             b.nbytes[i:i + chunk], 0)
            engine.ingest(sub)
        measuring = sec > start_sec + warmup
        if measuring and not window_started:
            t.reset()  # 测量窗口开始时清一次，之后子步骤累计整个窗口
            window_started = True
        t0 = time.perf_counter()
        engine.advance()
        adv = time.perf_counter() - t0
        t0 = time.perf_counter()
        engine.refresh_snapshot()
        snap = time.perf_counter() - t0
        if measuring:
            adv_list.append(adv)
            snap_list.append(snap)
    per = t.per_tick(len(adv_list))
    return {
        "adv_med": statistics.median(adv_list) * 1000.0,
        "snap_med": statistics.median(snap_list) * 1000.0,
        "per_tick": per,
        "n": len(adv_list),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="P2b tick 剖析（只诊断）")
    ap.add_argument("--rates", default="50000,120000,200000")
    ap.add_argument("--modes", default="长尾,随机")
    ap.add_argument("--warmup", type=int, default=70)
    ap.add_argument("--measure", type=int, default=40)
    args = ap.parse_args()
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    rates = [int(x) for x in args.rates.split(",") if x]
    modes = args.modes.split(",")
    urls, users = make_pools()

    for mode in modes:
        print(f"\n==== 分布：{mode}（warmup {args.warmup}s，测量 {args.measure}s，"
              f"每 tick 中位 ms）====")
        for rate in rates:
            r = run_rate(mode, rate, args.warmup, args.measure, urls, users)
            print(f"\n  [{mode} {rate//1000}k/s]  advance(定稿) {r['adv_med']:7.3f} ms | "
                  f"refresh_snapshot(快照) {r['snap_med']:7.3f} ms | "
                  f"合计 {r['adv_med'] + r['snap_med']:7.3f} ms  (n={r['n']})")
            for tag in TAGS:
                print(f"      {tag:<16} {r['per_tick'].get(tag, 0.0):8.3f} ms/tick")


if __name__ == "__main__":
    main()
