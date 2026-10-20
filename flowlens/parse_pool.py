"""解析的纯计算部分（P1 起，P2d-1 起只保留纯函数）。

本模块原是"解析进程池"（P1）：把逐条处理里的"解析"部分搬到多个子进程。P2d-1 起
**进程池被前端进程架构取代**（前端进程自己解析、经写入通道把 ParsedBatch 发给
引擎；ParsePool / FLOWLENS_PARSE_WORKERS 已删除，见 docs/lanes/P2-notes.md P2d-1）。
保留下来的都是**纯计算 / 纯数据**，前端进程与测试直接 import：

- ParsedBatch：解析结果的列式数据（三列 64 位哈希与 ts/nbytes/urls/users 逐行对齐）
- parse_body：解压后字节 → load_array + validate + 三列哈希（前端进程的执行体）
- available_cpus：实际可用逻辑核数（前端数计算 resolve_front_count 用）

哈希自检的历史说明：解析子进程时代，主进程把固定串的期望哈希传给子进程自检
（防 PYTHONHASHSEED 不一致导致两边 hash() 不同）。前端进程与引擎同环境启动、
PYTHONHASHSEED 在 __main__ 启动时统一检查告警，自检逻辑随之删除。
"""

import os
from dataclasses import dataclass

import numpy as np

from .hashing import key_hashes
from .model import load_array, validate


@dataclass
class ParsedBatch:
    """解析结果的小对象（必须可 pickle）：只有数组与字符串列表，无对象引用。

    前端进程 parse_body 产出后经写入通道发给引擎（ipc.WireBatch 在传输前把字符串列
    去重）。三列 64 位哈希与 ts/nbytes/urls/users 逐行对齐（长度相同）；event_id 本身
    不需要回传——引擎只用到它的哈希（去重 + 采样），不引用字符串。invalid 是被 validate
    剔除的不合法条数。
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


def parse_body(raw: bytes) -> ParsedBatch:
    """纯计算部分（前端进程的执行体）：输入**解压后的**字节，做
    load_array + validate + 三列 64 位哈希。

    异常（BadRequest / PayloadTooLarge / UnsupportedEncoding 等）原样抛出，由调用方
    （前端的错误中间件 / IngestService）转成对应 HTTP 状态码。本函数不碰任何统计状态
    （纯读、纯计算），前端进程与引擎进程都可直接 import 使用。
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


