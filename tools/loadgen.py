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

用法示例：
  python tools/loadgen.py --url http://127.0.0.1:8080 --workers 4 --conc 8 \
      --rate 20000 --batch 5000 --duration 60 --gzip --archive out \
      --mode 长尾 --seed 0
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

from flowgen import (ArchiveWriter, SEQ_STRIDE, EventGen, finalize_archive,
                     now_ms)

GZIP_CLEVEL = 1          # 压缩级别 1：减少客户端 CPU
BATCH_QUEUE_CAP = 8      # 进程内待发送批次的缓冲上限（背压，防内存无界）


def gzip_bytes(data: bytes) -> bytes:
    c = zlib.compressobj(GZIP_CLEVEL, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    return c.compress(data) + c.flush()


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
    deadline = time.monotonic() + cfg["duration"]
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
            outcome, lats = await _send_batch(session, url, body, headers,
                                              len(evs), cfg["max_retries"], retries)
            if outcome == "accepted" and arch is not None:
                # 存档按"这一次成功发送"的时刻落盘（重发成功时用重发时刻）
                t_ms = now_ms()
                for e in evs:
                    e.sent_ms = t_ms
                arch.add(evs)
                archived_ev[0] += len(evs)
            await done_q.put((len(evs), outcome, lats))

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

    t_start = time.monotonic()
    next_send_at = t_start
    sent_batches = 0

    try:
        while True:
            now = time.monotonic()
            if now > deadline:
                break
            if now < next_send_at:
                await asyncio.sleep(min(next_send_at - now, 0.2))
                continue
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
                               "wall_s": wall, "cpu_s": time.process_time()}))


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
                 t0_ms: int, all_lat: list, cpu_s: float) -> dict:
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
    p.add_argument("--max-retries", type=int, default=5, help="503 后的最大重试次数")
    p.add_argument("--p-late", type=float, default=0.0)
    p.add_argument("--p-future", type=float, default=0.0)
    p.add_argument("--p-cross", type=float, default=0.0)
    p.add_argument("--p-dup", type=float, default=0.0)
    p.add_argument("--p-invalid", type=float, default=0.0)
    p.add_argument("--request-timeout-s", type=float, default=10.0)
    args = p.parse_args()

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
    summary = summary_dict(cfg, per_sec, done, wall, t0_ms, all_lat, cpu_s)
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
