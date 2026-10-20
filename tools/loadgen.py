"""压测发送器（T10a）：多进程、令牌桶速率控制、503 重试、每秒 JSON 统计、存档。

- ``--workers N`` 个进程，每个进程内 ``--conc C`` 个并发连接（aiohttp 客户端，
  ``trust_env=False`` 避免走 WSL 自动代理）；
- 速率：``--rate`` 总速率（事件/秒）平均分给各进程；生成端按"理想发送时刻"
  发批（第 k 批在 start + 累计条数 / 每进程速率 时刻发出），发送端 C 条并发
  连接消化批次队列（有界队列，背压防内存无界）；
- 请求体在发送前即时生成（时间戳取发送时刻），orjson 序列化，可选 gzip（级别 1）；
- 503 按 Retry-After 等待后重发同一批（同样的 event_id），重试次数
  ``--max-retries`` 可配；连接错误也重试（短退避）；
- 每秒向标准输出打印一行 JSON（本秒发起的批次事件数：发送/接受 202/拒绝 503/
  其他错误，与请求延迟 p50/p95/p99——按"发起时刻"归秒），结束打印一行汇总
  （含客户端 CPU 占用，判断压测工具是否成瓶颈）。T10b 与控制台读这些行
  （type=second / summary）。

存档（``--archive DIR``）：只存被服务接受（202）的批次，重发成功时按重发那一
次的时刻落盘；各 worker 按 shard 写各自的 npz，主进程收尾合并 meta.json。
worker 的随机种子 = --seed + worker 号，event_id 序号区间互不相交
（worker × SEQ_STRIDE）。

快速模式（``--fast``，P2a）：发送前不再逐条生成与序列化——每个 worker 启动时
预生成 ``--fast-templates N``（默认 32）个完整请求体模板（按 --mode 指定的分布），
模板里的 ts 与 event_id 序号写成占位串，每次发送用 bytes.replace 把两个占位换成
当前墙钟毫秒与该进程递增的序号（批内所有事件同一 ts；event_id 全局不重复）。
占位宽度与替换值宽度相同，请求体大小逐字节不变。快速模式**不支持** --archive
与精确解对比（仍用原模式）；迟到/未来/跨批/批内重复/非法比例参数在快速模式
下无效（模板全是正常事件，验证口径：服务应 deduped/late/invalid 全为 0）。

用法示例：
  python tools/loadgen.py --url http://127.0.0.1:8080 --workers 4 --conc 8 \
      --rate 20000 --batch 5000 --duration 60 --gzip --archive out \
      --mode 长尾 --seed 0
  python tools/loadgen.py --url http://127.0.0.1:8080 --workers 8 --conc 8 \
      --rate 200000 --batch 5000 --duration 30 --fast --mode 长尾 --seed 0
"""

import argparse
import asyncio
import json
import multiprocessing as mp
import sys
import time
import zlib

import aiohttp
import numpy as np
import orjson

from flowgen import (ArchiveWriter, SEQ_STRIDE, EventGen, finalize_archive,
                     now_ms)

GZIP_CLEVEL = 1          # 压缩级别 1：减少客户端 CPU
BATCH_QUEUE_CAP = 8      # 进程内待发送批次的缓冲上限（背压，防内存无界）


def gzip_bytes(data: bytes) -> bytes:
    c = zlib.compressobj(GZIP_CLEVEL, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    return c.compress(data) + c.flush()


# ---- 快速模式（--fast，P2a）：模板预生成 + bytes.replace ----

# ts 占位：13 位（当前 epoch 毫秒恒为 13 位，到 2286 年不变）
TS_PLACEHOLDER = "9999999999999"
# event_id 序号占位：12 位。两个占位**不能互为子串**（否则先后 replace 互相破坏），
# 故用不同的数字序列（9 与 8）。自然流量中出现整段 12 位相同数字的概率 ~1e-12/处，
# 生成模板时再用 count 校验占位只出现在预期位置。
SEQ_PLACEHOLDER = "888888888888"


def _make_fast_templates(gen: EventGen, batch: int, n: int) -> tuple[list[bytes], int]:
    """预生成 n 个完整请求体模板（按 --mode 分布），返回 (模板, 批内下标宽度 w)。

    模板与现有模式同分布（同一套 key 规则、query/fragment、bytes），但生成方式是
    直接在 dict 里写占位符再 orjson.dumps——**O(n)**。不能先 raw_batch 再逐条
    bytes.replace(...,1)：replace 从字符串开头线性扫描，n 条事件是 O(n²)（
    32×10000 条约需几分钟，worker 启动卡死、整轮压测 0 发送）。

    只有两个字段是占位：
      - ts：每条都是 13 位 TS_PLACEHOLDER（发送时替换为当前毫秒，批内同一 ts）
      - event_id："e" + 12 位 SEQ_PLACEHOLDER + 批内下标（w 位补零）
        发送时把 SEQ_PLACEHOLDER 替换为本批基准序号 B（按批递增 10^w），
        事件 j 的 id 即 f"e{B}{j:0{w}d}"——批内、跨批、跨 worker（B 起点互不相交）
        全局不重复。
    占位与真实值等宽，两次 bytes.replace 后请求体大小逐字节不变。
    生成后 count 校验占位只出现在预期位置（自然内容撞上占位串则断言失败）。
    """
    w = max(1, len(str(batch - 1)))          # 批内下标的位数
    sp = SEQ_PLACEHOLDER.encode()
    tp = TS_PLACEHOLDER.encode()
    out = []
    for _ in range(n):
        evs = gen.batch(batch)
        d = []
        for j, e in enumerate(evs):
            # gen.raw(e) 照常消耗 rng（URL 的 query/fragment 等），但 id/ts 用占位
            row = gen.raw(e)
            row["event_id"] = "e" + SEQ_PLACEHOLDER + str(j).zfill(w)
            row["ts"] = int(TS_PLACEHOLDER)
            d.append(row)
        body = orjson.dumps(d)
        # 占位校验：SEQ/TS 占位各恰好每条一次。自然内容（url 随机数字等）撞上
        # 占位串时 count 会超预期，替换会损坏其他字段——断言失败、重新生成
        assert body.count(sp) == batch, \
            f"SEQ 占位出现 {body.count(sp)} 次（应为 {batch}），模板与随机内容冲突"
        assert body.count(tp) == batch, \
            f"TS 占位出现 {body.count(tp)} 次（应为 {batch}），模板与随机内容冲突"
        out.append(body)
    return out, w


def _fast_fill(body: bytes, base: int, w: int, ts_ms: int) -> bytes:
    """发送前填充模板：SEQ 占位 → 本批基准序号（等宽），TS 占位 → 当前毫秒。

    填充后事件 j 的 event_id = f"e{base + j:0{12 + w}d}" 的十进制展开
    （= 12 位基准 + w 位批内下标）：base 按批递增 10**w，worker 间起点
    相差 SEQ_STRIDE（10**8），全局不重复。
    """
    filled = body.replace(SEQ_PLACEHOLDER.encode(), str(base).zfill(12).encode())
    return filled.replace(TS_PLACEHOLDER.encode(), str(ts_ms).encode())


def pctl(values, p: float) -> float | None:
    """百分位（线性插值）；空列表返回 None。"""
    if not values:
        return None
    return float(np.percentile(values, p))


class _SecondBucket:
    """一秒的统计（按批次的"发起时刻"归秒）。"""

    __slots__ = ("sent", "accepted", "rejected", "errors", "lat")

    def __init__(self) -> None:
        self.sent = 0
        self.accepted = 0
        self.rejected = 0
        self.errors = 0
        self.lat: list[float] = []   # 请求延迟（毫秒）


async def _send_once(session: aiohttp.ClientSession, url: str, body: bytes,
                     headers: dict) -> tuple[int, dict, float]:
    """发一次请求，返回 (状态码, 响应头, 延迟毫秒)。"""
    t0 = time.perf_counter()
    async with session.post(url, data=body, headers=headers) as r:
        await r.read()
        return r.status, dict(r.headers), (time.perf_counter() - t0) * 1000.0


async def _send_batch(session, url: str, body: bytes, headers: dict,
                      n_ev: int, max_retries: int, retries_seen: list[int]
                      ) -> tuple[str, list[float]]:
    """发一批（含 503 重试）。返回 (outcome, 每次尝试的延迟毫秒)。

    outcome 为 'accepted'（最终 202）/ 'rejected'（重试耗尽仍 503）/
    'error'（连接错误或 4xx）。503 按 Retry-After 等待后重发同一批；连接错误
    短退避重试。计数不在此处累加——由 collector 按"完成时刻"归秒、每批只计一次
    （见 _worker_async 的 collector），避免"响应跨秒到达、写进已发送的旧桶"导致
    accepted 少计。
    """
    lats: list[float] = []
    for attempt in range(max_retries + 1):
        try:
            status, hdrs, dt_ms = await _send_once(session, url, body, headers)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            if attempt >= max_retries:
                return "error", lats
            await asyncio.sleep(0.1)
            continue
        lats.append(dt_ms)
        if status == 202:
            return "accepted", lats
        if status == 503:
            retries_seen[0] += 1
            if attempt >= max_retries:
                return "rejected", lats
            wait = hdrs.get("Retry-After")
            await asyncio.sleep(float(wait) if wait and wait.isdigit() else 1.0)
            continue
        # 其他状态码（4xx/5xx）：该批不可用，重试同批没有意义
        return "error", lats
    return "rejected", lats


async def _worker_async(cfg: dict, wid: int, mp_queue) -> None:
    """一个 worker 进程：生成 → 有界队列 → C 条并发连接发送 → 统计 → 存档。

    生成由主协程串行驱动（同一 seed 的序列确定性、按理想时刻发批）；发送由 C
    个 sender 任务消化队列（每条连接一个独立 ClientSession）。队列有界
    （BATCH_QUEUE_CAP），发送跟不上时生成端自然被背压。
    """
    url = cfg["url"].rstrip("/") + "/v1/ingest"
    rate_pw = cfg["rate"] / cfg["workers"]          # 每进程速率
    gen = EventGen(
        mode=cfg["mode"], seed=cfg["seed"] + wid,
        p_late=cfg["p_late"], p_future=cfg["p_future"], p_cross=cfg["p_cross"],
        p_dup=cfg["p_dup"], p_invalid=cfg["p_invalid"],
        seq_offset=wid * SEQ_STRIDE,
    )
    arch = ArchiveWriter(cfg["archive"], shard=wid) if cfg["archive"] else None
    headers = {"Content-Encoding": "gzip"} if cfg["gzip"] else {}
    retries = [0]
    archived_ev = [0]
    fast = cfg["fast"]

    # 快速模式：启动时预生成请求体模板（长尾/均匀分布），之后每批只做
    # bytes.replace（ts + event_id 序号），不再生成事件与序列化
    fast_templates: list[bytes] = []
    fast_w = 1
    fast_base = 0
    if fast:
        fast_templates, fast_w = _make_fast_templates(gen, cfg["batch"],
                                                       cfg["fast_templates"])
        # worker 间序号起点互不相交（SEQ_STRIDE 间隔），批内下标占 w 位
        fast_base = wid * SEQ_STRIDE + 1

    sessions = [aiohttp.ClientSession(
        trust_env=False,
        timeout=aiohttp.ClientTimeout(total=cfg["request_timeout_s"]),
        connector=aiohttp.TCPConnector(limit=1, ttl_dns_cache=300),
    ) for _ in range(cfg["conc"])]

    lane: asyncio.Queue = asyncio.Queue(maxsize=BATCH_QUEUE_CAP)
    # 完成结果队列：每个批次完成时放一条 (n_ev, outcome, lats)，collector 按
    # "完成时刻"归秒、每批只计一次——避免"响应跨秒到达、写进已发送给主进程的旧桶"
    # 导致 accepted 少计（T10a WSL 实测 2 万/秒时曾少计 ~86%）。
    done_q: asyncio.Queue = asyncio.Queue()

    async def sender(si: int) -> None:
        session = sessions[si]
        while True:
            item = await lane.get()
            if item is None:  # 结束标记
                return
            evs, body = item
            n_ev = cfg["batch"] if evs is None else len(evs)
            outcome, lats = await _send_batch(session, url, body, headers,
                                              n_ev, cfg["max_retries"], retries)
            if outcome == "accepted" and arch is not None:
                # 存档按"这一次成功发送"的时刻落盘（重发成功时用重发时刻）。
                # 快速模式不允许 --archive（main 里已拒绝），evs 恒非 None 时到这里
                t_ms = now_ms()
                for e in evs:
                    e.sent_ms = t_ms
                arch.add(evs)
                archived_ev[0] += len(evs)
            await done_q.put((n_ev, outcome, lats))

    async def collector() -> None:
        """汇总完成结果：每批按"完成时刻"归秒、只计一次；收到结束标记后输出逐秒统计。"""
        per_sec: dict[int, _SecondBucket] = {}
        while True:
            item = await done_q.get()
            if item is None:  # 结束标记（所有 sender 结束后由主协程放）
                break
            n_ev, outcome, lats = item
            b = per_sec.setdefault(now_ms() // 1000, _SecondBucket())
            b.sent += n_ev
            if outcome == "accepted":
                b.accepted += n_ev
            elif outcome == "rejected":
                b.rejected += n_ev
            else:
                b.errors += n_ev
            b.lat.extend(lats)
        for sec in sorted(per_sec):
            bucket = per_sec[sec]
            mp_queue.put(("second", sec,
                          {"sent": bucket.sent, "accepted": bucket.accepted,
                           "rejected": bucket.rejected, "errors": bucket.errors},
                          list(bucket.lat)))

    senders = [asyncio.create_task(sender(i)) for i in range(cfg["conc"])]
    collector_task = asyncio.create_task(collector())

    # deadline 在模板生成**之后**才起算：模板预生成是启动开销（快速模式 32×10000
    # 条约 10 s），不应吃掉发送窗口——否则短 --duration 下生成完 deadline 已过、
    # 一条都发不出（实测 workers_done 正常但 sent=0、CPU 烧满在生成上）
    t_start = time.monotonic()
    deadline = t_start + cfg["duration"]
    next_send_at = t_start
    sent_batches = 0
    cpu_send_t0 = time.process_time()   # 发送窗口 CPU 计时起点（排除模板生成）

    fast_w10 = 10 ** fast_w
    fast_n = len(fast_templates)
    try:
        while True:
            now = time.monotonic()
            if now > deadline:
                break
            if now < next_send_at:
                await asyncio.sleep(min(next_send_at - now, 0.2))
                continue
            if fast:
                # 快速模式：模板填充（等宽替换，请求体大小不变）
                body = _fast_fill(fast_templates[sent_batches % fast_n],
                                  fast_base, fast_w, now_ms())
                if cfg["gzip"]:
                    body = gzip_bytes(body)
                fast_base += fast_w10
                sent_batches += 1
                await lane.put((None, body))
            else:
                evs = gen.batch(cfg["batch"], now_ms())
                body = gen.raw_batch(evs)
                if cfg["gzip"]:
                    body = gzip_bytes(body)
                sent_batches += 1
                await lane.put((evs, body))
            next_send_at += cfg["batch"] / rate_pw
    finally:
        # 收尾：等所有批次发完（结果进 done_q），放结束标记让 collector 汇总后退出
        for _ in senders:
            await lane.put(None)
        await asyncio.gather(*senders, return_exceptions=True)
        await done_q.put(None)
        await collector_task
        for s in sessions:
            await s.close()
        if arch is not None:
            arch.close()
        wall = time.monotonic() - t_start
        mp_queue.put(("done", {"wid": wid, "sent_batches": sent_batches,
                               "archived": archived_ev[0], "retries": retries[0],
                               "wall_s": wall, "cpu_s": time.process_time(),
                               "cpu_send_s": time.process_time() - cpu_send_t0}))


def _merge_seconds(per_sec: dict, sec: int, stats: dict, lat: list) -> None:
    b = per_sec.setdefault(sec, {"sent": 0, "accepted": 0, "rejected": 0,
                                 "errors": 0, "lat": []})
    for k in ("sent", "accepted", "rejected", "errors"):
        b[k] += stats[k]
    b["lat"].extend(lat)


def second_line(sec: int, stats: dict, lat: list) -> str:
    """一秒的统计 → 一行 JSON（T10b / 控制台读这些行）。"""
    return json.dumps({
        "type": "second", "t": sec,
        "sent": stats["sent"], "accepted": stats["accepted"],
        "rejected": stats["rejected"], "errors": stats["errors"],
        "lat_p50_ms": pctl(lat, 50), "lat_p95_ms": pctl(lat, 95),
        "lat_p99_ms": pctl(lat, 99),
    }, ensure_ascii=False)


def summary_dict(cfg: dict, per_sec: dict, done: list, wall: float,
                 t0_ms: int, all_lat: list, cpu_s: float,
                 cpu_send_s: float = 0.0) -> dict:
    """汇总 → 一行 JSON 的 dict。"""
    tot = {k: sum(b[k] for b in per_sec.values()) for k in
           ("sent", "accepted", "rejected", "errors")}
    crashed = [d.get("error") for d in done if "error" in d]
    s = {
        "type": "summary",
        "config": {k: cfg.get(k) for k in
                   ("url", "workers", "conc", "rate", "batch", "duration",
                    "gzip", "mode", "seed", "archive", "max_retries")},
        "wall_s": round(wall, 2),
        "sent": tot["sent"], "accepted": tot["accepted"],
        "rejected": tot["rejected"], "errors": tot["errors"],
        "accept_rate": round(tot["accepted"] / tot["sent"], 4) if tot["sent"] else 0.0,
        "accepted_events_rate": round(tot["accepted"] / wall, 1) if wall else 0.0,
        "retries": sum(d.get("retries", 0) for d in done),
        "lat_p50_ms": pctl(all_lat, 50), "lat_p95_ms": pctl(all_lat, 95),
        "lat_p99_ms": pctl(all_lat, 99),
        "client_cpu_pct": round(cpu_s / wall * 100.0, 1) if wall else 0.0,
        # 发送窗口 CPU 口径（排除模板预生成等启动开销）：worker 的 cpu_send_s
        # 之和 ÷ 发送窗口 wall。每条 μs = 该 CPU 秒 ÷ sent——P2a 报告压测端
        # 每条事件 CPU 时间用这个
        "client_cpu_send_pct": round(cpu_send_s / wall * 100.0, 1) if wall else 0.0,
        "client_cpu_us_per_ev": round(cpu_send_s / tot["sent"] * 1e6, 2)
                               if (cpu_send_s and tot["sent"]) else None,
        "start_ms": t0_ms,
        "workers_done": len(done),
    }
    if crashed:
        s["worker_errors"] = crashed
    return s


def _drain_queue(mp_queue, per_sec: dict, done: list, timeout: float) -> None:
    try:
        msg = mp_queue.get(timeout=timeout)
    except Exception:
        return
    if msg[0] == "second":
        _, sec, stats, lat = msg
        _merge_seconds(per_sec, sec, stats, lat)
    else:  # ("done", {...})
        done.append(msg[1])


def main() -> None:
    # Windows 控制台/管道默认 GBK，中文参数（--mode 长尾）会编码失败或乱码：
    # 统一强制 stdout 为 UTF-8（读输出方也要按 UTF-8 读）
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(description="FlowLens 压测发送器")
    p.add_argument("--url", default="http://127.0.0.1:8080", help="服务地址")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--conc", type=int, default=8, help="每进程并发连接数")
    p.add_argument("--rate", type=int, default=20_000, help="总速率（事件/秒）")
    p.add_argument("--batch", type=int, default=5000, help="每批事件数")
    p.add_argument("--duration", type=float, default=60.0, help="持续秒数")
    p.add_argument("--gzip", action="store_true", help="请求体 gzip 压缩（级别 1）")
    p.add_argument("--mode", choices=["长尾", "均匀", "随机"], default="长尾")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--archive", default=None, help="存档目录（只存 202 接受的批次）")
    p.add_argument("--fast", action="store_true",
                   help="快速模式：预生成请求体模板 + bytes.replace，"
                        "发送开销降到最低（P2a）。不支持 --archive 与精确解对比；"
                        "所有事件都是正常事件（迟到/未来/重复/非法比例为 0）")
    p.add_argument("--fast-templates", type=int, default=32,
                   help="快速模式每个 worker 预生成的模板数（默认 32）")
    p.add_argument("--max-retries", type=int, default=5, help="503 后的最大重试次数")
    p.add_argument("--p-late", type=float, default=0.0)
    p.add_argument("--p-future", type=float, default=0.0)
    p.add_argument("--p-cross", type=float, default=0.0)
    p.add_argument("--p-dup", type=float, default=0.0)
    p.add_argument("--p-invalid", type=float, default=0.0)
    p.add_argument("--request-timeout-s", type=float, default=10.0)
    args = p.parse_args()
    if args.fast:
        if args.archive:
            p.error("--fast 不支持 --archive（存档与精确解对比用原模式）")
        if args.mode == "随机":
            p.error("--fast 不支持 --mode 随机（模板复用固定 key 集，"
                    "只有长尾/均匀有吞吐测试意义）")
        if args.fast_templates < 1:
            p.error("--fast-templates 至少 1")

    start_wall = time.monotonic()
    t0_ms = now_ms()
    cfg = vars(args)
    mp_queue = mp.Queue()
    # spawn 上下文：子进程不继承父进程状态（Windows 上本就是 spawn）
    ctx = mp.get_context("spawn")
    procs = []
    for wid in range(args.workers):
        pr = ctx.Process(target=_worker_target, args=(cfg, wid, mp_queue), daemon=True)
        pr.start()
        procs.append(pr)

    per_sec: dict[int, dict] = {}
    done: list[dict] = []
    deadline = start_wall + args.duration + 30
    while len(done) < args.workers:
        if time.monotonic() > deadline:
            break
        if all(pr.exitcode is not None for pr in procs):
            # 进程全退（含崩溃）：排空剩余消息后退出
            while True:
                _drain_queue(mp_queue, per_sec, done, 0.05)
                if all(pr.exitcode is not None for pr in procs) and not mp_queue:
                    break
            break
        _drain_queue(mp_queue, per_sec, done, 0.5)
    # 排空收尾消息（队列空且 0.5 秒内无新消息即认为排空）
    import queue as _queue
    while True:
        try:
            msg = mp_queue.get(timeout=0.5)
        except _queue.Empty:
            break
        if msg[0] == "second":
            _merge_seconds(per_sec, msg[1], msg[2], msg[3])
        else:
            done.append(msg[1])

    # 逐秒打印（按秒排序；lat 合并后算百分位）
    all_lat: list[float] = []
    for sec in sorted(per_sec):
        b = per_sec[sec]
        lat = b.pop("lat")
        all_lat.extend(lat)
        print(second_line(sec, b, lat), flush=True)

    wall = time.monotonic() - start_wall
    cpu_s = sum(d.get("cpu_s", 0.0) for d in done) + time.process_time()
    cpu_send_s = sum(d.get("cpu_send_s", 0.0) for d in done)
    summary = summary_dict(cfg, per_sec, done, wall, t0_ms, all_lat, cpu_s,
                           cpu_send_s)
    if args.archive:
        meta = {
            "key_rule": 1,
            "seed": args.seed, "mode": args.mode,
            "workers": args.workers, "conc": args.conc, "rate": args.rate,
            "batch": args.batch, "duration": args.duration, "gzip": args.gzip,
            "max_retries": args.max_retries,
            "p_late": args.p_late, "p_future": args.p_future,
            "p_cross": args.p_cross, "p_dup": args.p_dup,
            "p_invalid": args.p_invalid,
            "start_ms": t0_ms, "end_ms": now_ms(),
        }
        finalize_archive(args.archive, base_meta=meta)
        summary["archive"] = args.archive
        summary["archived_events"] = sum(d.get("archived", 0) for d in done)
    print(json.dumps(summary, ensure_ascii=False), flush=True)

    for pr in procs:
        pr.join(timeout=5)
        if pr.is_alive():
            pr.terminate()
    if any("error" in d for d in done):
        raise SystemExit(1)


def _worker_target(cfg: dict, wid: int, mp_queue) -> None:
    """spawn 子进程的入口（必须可被 pickle：模块顶层函数）。"""
    try:
        asyncio.run(_worker_async(cfg, wid, mp_queue))
    except Exception as e:  # 崩溃信息回传主进程，不留哑进程
        try:
            mp_queue.put(("done", {"wid": wid, "error": repr(e),
                                   "cpu_s": 0.0, "wall_s": 0.0}))
        except Exception:
            pass


if __name__ == "__main__":
    main()
