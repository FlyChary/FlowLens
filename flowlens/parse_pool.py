"""解析进程池（P1）：把逐条处理里的"解析"部分搬到多个子进程，主进程只留单写者部分。

为什么要搬：T11a 剖析显示每条事件里约 9.5 μs 花在"解压、load_array、validate（含归一化）、
三个 key 的哈希"上，这部分彼此独立、只读，可以并行；主进程只剩 classify、Bloom 去重、按秒
分组、写入秒桶、tick（约 3.5 μs）——这些**必须**单写者、无锁，不能并行。

分工（用户决定一）：**gzip 解压留在主进程**。在途额度按"解压后字节数"预留，而解压后大小
只有解压完才知道；若把解压放进子进程，主进程就没法在交出去之前完成预留，"先预留、再解析、
再去重"的顺序会被打破。所以：主进程 decode_body（解压）→ 预留 → 把**解压后的字节**交给子
进程 → 子进程只做 load_array、validate（含归一化）、三列 64 位哈希 → 拿回 ParsedBatch →
主进程写入引擎 → finally 释放预留。load_array 抛的 BadRequest 原样传回，预留照常在 finally
释放（被拒/报错的请求都没写去重，重试安全）。

子进程只 import 完成解析所需的模块（orjson、flowlens.model、flowlens.hashing），**不 import
aiohttp 与引擎**（见 flowlens/model.py：只依赖 numpy/orjson/errors/intern）。

用 multiprocessing 的 **spawn** 上下文（不 fork）：在 __main__ 里、创建引擎**之前**启动并
预热，让子进程在主进程分配大块统计内存之前就存在（用户决定三），避免子进程继承主进程的大块
内存（fork 会 COW 继承，spawn 不会）。
"""

import os
import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import numpy as np

from .hashing import key_hash, key_hashes
from .model import load_array, validate

log = logging.getLogger(__name__)

# 哈希自检用的固定字符串：主进程算好期望值传给子进程，子进程启动时重算，不一致就报错退出
# （防止 PYTHONHASHSEED 不一致导致两边 hash() 不同——那样去重/分桶会错乱且极难排查）。
_SELFTEST_STR = "flowlens-selftest"


@dataclass
class ParsedBatch:
    """子进程解析后回传的小对象（必须可 pickle）：只有数组与字符串列表，无对象引用。

    三列 64 位哈希与 ts/nbytes/urls/users 逐行对齐（长度相同）；event_id 本身不需要回传
    ——主进程只用到它的哈希（去重 + 采样），不引用字符串。invalid 是 validate 剔除的条数。
    """

    ts: np.ndarray      # 毫秒，int64
    nbytes: np.ndarray  # 响应字节数，int64
    h_id: np.ndarray    # event_id 的 64 位哈希，uint64
    h_url: np.ndarray   # url 的 64 位哈希，uint64
    h_user: np.ndarray  # user 的 64 位哈希，uint64
    urls: list          # 归一化后的 url 字符串（与上述数组对齐）
    users: list         # user_id 字符串（与上述数组对齐）
    invalid: int = 0    # 被 validate 剔除的不合法条数

    def __len__(self) -> int:
        return len(self.ts)

    def chunk(self, i: int, j: int) -> "ParsedBatch":
        """取 [i, j) 的子集（写入引擎按块切分用）。子集的 invalid 为 0——invalid 计数
        在整批层面已由主进程汇总一次，切块只用来控制单块同步耗时，不重复计 invalid。

        注意：子块传给 engine.ingest_parsed 时只带本块的 valid 事件与哈希；整批的
        invalid 由 IngestService 在第一个块上单独加回（见 ingest.py），避免丢失。
        """
        sl = slice(i, j)
        return ParsedBatch(
            ts=self.ts[sl], nbytes=self.nbytes[sl],
            h_id=self.h_id[sl], h_url=self.h_url[sl], h_user=self.h_user[sl],
            urls=self.urls[i:j], users=self.users[i:j], invalid=0,
        )


def _selfcheck(expected: int) -> None:
    """子进程 initializer：重算固定串的哈希，与主进程传入的期望值比较，不一致就报错退出。

    用 SystemExit 而非异常：让进程池干净地标记为 broken（BrokenProcessPool），主进程据此
    重建并给当前请求 503。
    """
    got = key_hash(_SELFTEST_STR)
    if got != expected:
        raise SystemExit(f"解析子进程哈希自检失败（期望 {expected:#x}，实得 {got:#x}）："
                         "PYTHONHASHSEED 不一致，两边 hash() 不同，必须固定后再启动")


def _worker_noop() -> None:
    """预热用：一个模块级 no-op，spawn 上下文里必须可 pickle（lambda 不行）。"""
    return None


def parse_body(raw: bytes) -> ParsedBatch:
    """子进程任务函数：输入**解压后的**字节，做 load_array + validate + 三列哈希。

    异常（BadRequest / PayloadTooLarge / UnsupportedEncoding 等）原样抛回主进程，由
    IngestService 转成对应 HTTP 状态码。本函数不碰任何主进程的统计状态（纯读、纯计算）。
    """
    arr = load_array(raw)
    batch = validate(arr)
    return ParsedBatch(
        ts=batch.ts,
        nbytes=batch.nbytes,
        h_id=key_hashes(batch.ids),
        h_url=key_hashes(batch.urls),
        h_user=key_hashes(batch.users),
        urls=batch.urls,
        users=batch.users,
        invalid=batch.invalid,
    )


def _cgroup_cpu_quota() -> float | None:
    """读 cgroup 的 CPU 配额（核数）：v2 的 cpu.max，v1 的 cfs_quota/period；读不到返回 None。

    Windows 上没有 /sys/fs/cgroup，直接返回 None（用 os.cpu_count()）。
    """
    # cgroup v2：/sys/fs/cgroup/cpu.max = "<max|数值> <period_ns>"
    try:
        with open("/sys/fs/cgroup/cpu.max", encoding="utf-8") as f:
            parts = f.read().split()
        if parts and parts[0] != "max" and float(parts[1]) > 0:
            return float(parts[0]) / float(parts[1])
    except (OSError, IndexError, ValueError):
        pass
    # cgroup v1：cpu.cfs_quota_us / cpu.cfs_period_us（配额为 -1 表示无限制）
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", encoding="utf-8") as f:
            quota = float(f.read().strip())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us", encoding="utf-8") as f:
            period = float(f.read().strip())
        if quota > 0 and period > 0:
            return quota / period
    except (OSError, ValueError):
        pass
    return None


def available_cpus() -> int:
    """实际可用的逻辑核数（用户决定二）。

    os.cpu_count() 在 docker --cpuset-cpus 限制核数时仍返回宿主机全部核数，所以再与
    os.sched_getaffinity（Windows 没有，退回 os.cpu_count()）和 cgroup CPU 配额取较小值。
    """
    n = os.cpu_count() or 1
    try:
        n = min(n, len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):
        pass
    quota = _cgroup_cpu_quota()
    if quota is not None and quota > 0:
        n = min(n, max(1, int(quota)))
    return n


def resolve_worker_count() -> int:
    """解析进程数：min(3, 可用核数 − 1)，小于 1 时不启用（返回 0）。

    FLOWLENS_PARSE_WORKERS 环境变量可覆盖（0 表示禁用）；覆盖值也夹在 [0, 3]。
    """
    raw = os.environ.get("FLOWLENS_PARSE_WORKERS", "").strip()
    if raw:
        try:
            v = int(raw)
        except ValueError:
            return 0
        return max(0, min(3, v))
    return max(0, min(3, available_cpus() - 1))


class ParsePool:
    """管理解析子进程池：创建（spawn + 哈希自检 initializer）、预热、提交、崩溃重建、关闭。

    在 __main__ 里、创建引擎**之前**实例化，并靠预热迫使 n 个子进程立即启动（用户决定三）：
    ProcessPoolExecutor 默认"用到才建"子进程，若等第一个请求才建，那时主进程已分配了大块
    统计内存，spawn 虽不继承但仍希望子进程尽早独立存在。
    """

    def __init__(self, num_workers: int):
        if num_workers < 1:
            raise ValueError("ParsePool 需要至少 1 个 worker")
        self.num_workers = num_workers
        self._ctx = mp.get_context("spawn")
        self._expected = key_hash(_SELFTEST_STR)  # 主进程算期望值（自检基准）
        self._executor: ProcessPoolExecutor | None = None
        self._build()

    def _build(self) -> None:
        """创建进程池并预热：提交 num_workers 个 no-op 并等待，迫使子进程全部启动。"""
        self._executor = ProcessPoolExecutor(
            max_workers=self.num_workers,
            mp_context=self._ctx,
            initializer=_selfcheck,
            initargs=(self._expected,),
        )
        futs = [self._executor.submit(_worker_noop) for _ in range(self.num_workers)]
        try:
            for f in futs:
                f.result()  # 触发 initializer（哈希自检）与首次任务，失败在此抛出
        except BaseException:
            # 自检失败 / 子进程崩溃：关掉半成品池再向上抛，避免泄漏管理线程与残留进程
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None
            raise

    @property
    def available(self) -> bool:
        return self.num_workers > 0

    def submit(self, raw: bytes):
        """把解压后的字节交给子进程解析，返回 concurrent.futures.Future（结果是 ParsedBatch）。

        调用方用 asyncio.wrap_future 把它桥接成 await 的 future；子进程抛的异常会经 result()
        原样传出。
        """
        if self._executor is None:
            raise RuntimeError("ParsePool 已关闭")
        return self._executor.submit(parse_body, raw)

    def rebuild(self) -> None:
        """子进程崩溃（BrokenProcessPool 等）后重建整个进程池并重新预热。"""
        log.warning("解析进程池重建（之前的子进程崩溃）")
        try:
            if self._executor is not None:
                self._executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        self._build()

    def close(self) -> None:
        """关闭进程池（应用关闭 / SIGTERM 时调用），不残留子进程。"""
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None
