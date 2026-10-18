"""事件生成与存档（T10a）：确定性事件序列、发送载荷、整数列存档。

设计要点：

- 确定性：同一 (seed, 分布, 参数) 生成完全相同的事件序列。每个事件（含原始 URL
  的 query/fragment 变化）都从一个由 seed 派生的 numpy 随机流按固定顺序取数，
  重放可复现。
- URL / 用户都用整数编号表示：key_of(i) 是归一化后的 URL，raw_url(key, rng)
  在 key 基础上随机加 query 与 fragment——归一化后必须等于 key（压测数据因此
  既测 URL 归一化，又让离线精确解与服务的 key 对得上：精确解按编号分组，
  服务按归一化后的 key 分组，两者是同一个划分）。
- 时间戳避开判定边界：正常事件 ts 在发送时刻往前 0~3 秒（带乱序）；迟到事件
  61.5~119.5 秒（避开 60 秒 ± 0.5 秒）；未来事件 5.5~30 秒后（避开 5 秒
  ± 0.5 秒）。这样"服务按收到时刻判定、精确解按发送时刻判定"的两种口径结论
  一致（收-发延迟远小于边界余量）。
- 存档只存整数列（npz）：event_id、ts、url 编号、user 编号、bytes、sent_ms
  （该批的发送时刻）、事件类型。字符串可由编号按 KEY_RULE_VERSION 的规则重建，
  存档体积与事件数成线性、与字符串长度无关。
- event_id 全局唯一：多 worker 并发时每个 worker 用不相交的序号区间
  （seq_offset = worker × SEQ_STRIDE），互不撞 id。

本模块只做"生成与存取"，不依赖 aiohttp；发送器在 tools/loadgen.py。
"""

import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import orjson

# 存档格式与 key 生成规则版本（meta.json 记录；读取方按版本解释）
KEY_RULE_VERSION = 1
META_NAME = "meta.json"

# 事件类型（存档 etype 列的取值）
NORMAL, LATE, FUTURE, INVALID = 0, 1, 2, 3

# 迟到/未来/正常时间戳的取值范围（毫秒），已避开 ±0.5 秒判定边界
NORMAL_BACK_MIN_MS, NORMAL_BACK_MAX_MS = 0, 3_000
LATE_BACK_MIN_MS, LATE_BACK_MAX_MS = 61_500, 119_500
FUTURE_FWD_MIN_MS, FUTURE_FWD_MAX_MS = 5_500, 30_000

# bytes 的对数正态参数：p95 ≈ 10_000 字节（mu = ln(10000) − 1.645·sigma、sigma = 1）
_BYTES_MU = math.log(10_000) - 1.6449
_BYTES_SIGMA = 1.0
# int32 上限（存档 bytes 列是 int32）
_BYTES_MAX = 2_000_000_000

# 各分布的池大小
N_URL = 20_000
N_USER = 200_000

# 多 worker 的 event_id 序号区间宽度（每个 worker 的 seq 落在
# [worker×SEQ_STRIDE, (worker+1)×SEQ_STRIDE)）
SEQ_STRIDE = 10 ** 8


def key_of(i: int) -> str:
    """编号 i 的归一化 URL。

    末尾段 ``x{i}`` 以字母开头，不命中 {id} 归一化规则（纯数字/UUID/长十六进制），
    归一化后就是它本身；raw_url 在它基础上只加 query 与 fragment（归一化整体剥掉），
    所以 normalize_url(raw_url(key)) == key。
    """
    return f"/api/item/x{i}"


def user_of(i: int) -> str:
    """编号 i 的用户 ID（确定性编号映射，不参与 URL 归一化）。"""
    return f"u{i}"


def raw_url(key: str, rng: np.random.Generator) -> str:
    """在归一化 key 基础上加随机变化，保证 normalize_url(raw) == key。

    变化只取 normalize_url 会整体剥掉的部分：query（? 之后）与 fragment（# 之后）。
    query 的参数值用纯数字 / UUID / 长十六进制段（这些段的归一化规则在压测里
    也被覆盖，且不影响归一化结果——它们在 ? 之后，整段被剥掉）。
    """
    s = key
    if rng.random() < 0.8:
        s += f"?ref={_id_segment(rng)}&page={rng.integers(0, 50)}"
    if rng.random() < 0.3:
        s += f"#a{rng.integers(0, 10 ** 6)}"
    return s


def _id_segment(rng: np.random.Generator) -> str:
    """一个纯数字/UUID/长十六进制段（随机取一种），作 query 参数值。"""
    kind = int(rng.integers(0, 3))
    if kind == 0:  # 纯数字
        return str(int(rng.integers(1, 10 ** 12)))
    if kind == 1:  # UUID：32 位十六进制按 8-4-4-4-12 分组
        d = rng.integers(0, 16, 32)
        g = "".join(str(int(x)) if x < 10 else "abcdef"[x - 10] for x in d)
        return f"{g[0:8]}-{g[8:12]}-{g[12:16]}-{g[16:20]}-{g[20:32]}"
    # 长十六进制（32 位，≥16 即命中归一化规则）
    d = rng.integers(0, 16, 32)
    return "".join(str(int(x)) if x < 10 else "abcdef"[x - 10] for x in d)


def bytes_of(rng: np.random.Generator) -> int:
    """对数正态的响应字节数（p95 ≈ 10 KB），int32 范围内。"""
    return int(min(rng.lognormal(_BYTES_MU, _BYTES_SIGMA), _BYTES_MAX))


def now_ms() -> int:
    """当前墙钟毫秒（生成与存档的时间基准）。"""
    return int(time.time() * 1000)


@dataclass
class Event:
    """一个逻辑事件（发送载荷与存档共用）。

    seq 全局唯一，event_id = f"e{seq}"。etype 为 NORMAL / LATE / FUTURE /
    INVALID；INVALID 事件按 invalid_kind 损坏一个字段（缺字段或类型错），
    其余字段保留真实编号（存档落这些编号，精确解按 etype 剔除它们）。
    sent_ms 是所属批次的发送时刻（墙钟毫秒），精确解按它判定迟到。
    """

    seq: int
    url_idx: int
    user_idx: int
    ts: int
    nbytes: int
    etype: int = NORMAL
    sent_ms: int = 0
    invalid_kind: int = 0


class EventGen:
    """确定性事件序列生成器。

    mode（分布）：
      "长尾"   —— URL 从 n_url 个按 Zipf(s=1.1) 抽，用户从 n_user 个按 Zipf(s=0.9) 抽；
      "均匀"   —— URL / 用户均匀抽取；
      "随机"   —— 每个事件的 URL 与用户编号单调递增、互不重复。

    比例参数（0~1，按事件独立判定，优先级 迟到 → 未来 → 跨批重复 → 批内重复 → 正常）：
      p_late   —— 迟到（ts 在发送时刻前 61.5~119.5 秒）
      p_future —— 未来（ts 在发送时刻后 5.5~30 秒）
      p_cross  —— 跨批重复（重发之前发过的事件：同一 event_id、同一原始 ts）
      p_dup    —— 批内重复（重发本批已分配的事件：同一 event_id、同一原始 ts）
      p_invalid—— 非法（缺字段或类型错，服务逐条判非法、只计数）

    seq_offset：多 worker 时每个 worker 用不相交的序号区间（worker × SEQ_STRIDE），
    event_id 全局唯一。
    """

    def __init__(self, *, mode: str, seed: int,
                 p_late: float = 0.0, p_future: float = 0.0,
                 p_cross: float = 0.0, p_dup: float = 0.0,
                 p_invalid: float = 0.0,
                 n_url: int = N_URL, n_user: int = N_USER,
                 seq_offset: int = 0):
        if mode not in ("长尾", "均匀", "随机"):
            raise ValueError(f"未知分布: {mode!r}")
        for name, v in (("p_late", p_late), ("p_future", p_future),
                        ("p_cross", p_cross), ("p_dup", p_dup),
                        ("p_invalid", p_invalid)):
            if not 0.0 <= v <= 1.0:
                raise ValueError(f"{name} 应在 0~1，收到 {v}")
        self.mode = mode
        self.seed = int(seed)
        self.rng = np.random.default_rng(seed)
        self.p_late, self.p_future = p_late, p_future
        self.p_cross, self.p_dup, self.p_invalid = p_cross, p_dup, p_invalid
        self.n_url, self.n_user = n_url, n_user
        self._seq = int(seq_offset)
        self._sent: list[list[Event]] = []   # 已发送的批次（跨批重复从中取）
        if mode == "长尾":
            self._w_url = 1.0 / np.arange(1, n_url + 1) ** 1.1
            self._w_url /= self._w_url.sum()
            self._w_user = 1.0 / np.arange(1, n_user + 1) ** 0.9
            self._w_user /= self._w_user.sum()
        else:
            self._w_url = self._w_user = None

    def _draw(self, n: int) -> list[tuple[int, int]]:
        """按分布抽 n 个 (url 编号, user 编号)（"随机" mode 由 batch 覆盖）。"""
        r = self.rng
        idx_u = r.choice(self.n_url, size=n, p=self._w_url) if self._w_url is not None \
            else r.integers(0, self.n_url, n)
        idx_s = r.choice(self.n_user, size=n, p=self._w_user) if self._w_user is not None \
            else r.integers(0, self.n_user, n)
        return list(zip(idx_u.tolist(), idx_s.tolist()))

    def _ts(self, sent_ms: int, etype: int) -> int:
        r = self.rng
        if etype == LATE:
            return sent_ms - int(r.integers(LATE_BACK_MIN_MS, LATE_BACK_MAX_MS + 1))
        if etype == FUTURE:
            return sent_ms + int(r.integers(FUTURE_FWD_MIN_MS, FUTURE_FWD_MAX_MS + 1))
        return sent_ms - int(r.integers(NORMAL_BACK_MIN_MS, NORMAL_BACK_MAX_MS + 1))

    def batch(self, n: int, sent_ms: int | None = None) -> list[Event]:
        """生成一批 n 条事件（sent_ms 缺省取当前墙钟毫秒）。"""
        sent_ms = int(sent_ms if sent_ms is not None else now_ms())
        r = self.rng
        draws = self._draw(n)
        evs: list[Event] = []
        fresh: list[Event] = []       # 本批新分配的事件（批内重复从中取）
        kinds = r.random(n)
        k_late = self.p_late
        k_future = k_late + self.p_future
        k_cross = k_future + self.p_cross
        k_dup = k_cross + self.p_dup
        k_invalid = k_dup + self.p_invalid
        for j in range(n):
            seq = self._seq
            self._seq += 1
            k = kinds[j]
            # 先定类型；重复/非法事件复制已有事件的编号与 ts
            if k < k_late:
                etype = LATE
            elif k < k_future:
                etype = FUTURE
            elif k < k_cross and self._sent:
                src = self._sent[-1][int(r.integers(len(self._sent[-1])))]
                ev = Event(seq, src.url_idx, src.user_idx, src.ts, src.nbytes,
                           NORMAL, sent_ms)
                ev.seq = src.seq          # 同一逻辑事件 → 同一 event_id
                evs.append(ev)
                continue
            elif k < k_dup and fresh:
                src = fresh[int(r.integers(len(fresh)))]
                ev = Event(seq, src.url_idx, src.user_idx, src.ts, src.nbytes,
                           NORMAL, sent_ms)
                ev.seq = src.seq
                evs.append(ev)
                continue
            elif k < k_invalid:
                etype = INVALID
            else:
                etype = NORMAL
            u_idx, s_idx = draws[j]
            if self.mode == "随机" and etype in (NORMAL, LATE, FUTURE):
                # 全随机：编号单调递增、互不重复（用全局 seq 保证跨批也不重）
                u_idx = self.n_url + seq
                s_idx = self.n_user + seq
            ev = Event(seq, int(u_idx), int(s_idx), self._ts(sent_ms, etype),
                       bytes_of(r), etype, sent_ms)
            if etype == INVALID:
                ev.invalid_kind = int(r.integers(0, 6))
            evs.append(ev)
            if etype in (NORMAL, LATE, FUTURE):
                fresh.append(ev)
        self._sent.append(evs)
        if len(self._sent) > 64:  # 只留最近的批（跨批重复窗口有限）
            self._sent = self._sent[-32:]
        return evs

    def raw(self, ev: Event) -> dict:
        """事件 → 发送用的 JSON dict（INVALID 按 invalid_kind 损坏一个字段）。"""
        if ev.etype == INVALID:
            return _invalid_dict(ev)
        return {
            "event_id": f"e{ev.seq}",
            "user_id": user_of(ev.user_idx),
            "url": raw_url(key_of(ev.url_idx), self.rng),
            "ts": ev.ts,
            "bytes": ev.nbytes,
        }

    def raw_batch(self, evs: list[Event]) -> bytes:
        """一批事件 → JSON 数组字节（orjson 序列化，发送前即时生成）。"""
        return orjson.dumps([self.raw(e) for e in evs])

    def as_row(self, ev: Event) -> tuple:
        """事件 → 存档整数行。"""
        return (ev.seq, ev.ts, ev.url_idx, ev.user_idx, ev.nbytes,
                ev.sent_ms, ev.etype)


def _invalid_dict(ev: Event) -> dict:
    """按 invalid_kind 损坏一个事件（缺字段/类型错），服务应逐条判非法。"""
    base = {
        "event_id": f"e{ev.seq}",
        "user_id": user_of(ev.user_idx),
        "url": key_of(ev.url_idx),
        "ts": ev.ts,
        "bytes": ev.nbytes,
    }
    kind = ev.invalid_kind % 6
    if kind == 0:
        del base["event_id"]
    elif kind == 1:
        del base["user_id"]
    elif kind == 2:
        del base["url"]
    elif kind == 3:
        base["ts"] = "not-an-int"
    elif kind == 4:
        base["bytes"] = -1
    else:
        base["event_id"] = 12345  # 类型错（非字符串）
    return base


# ---------------- 存档 ----------------

class ArchiveWriter:
    """多 worker 安全的整数列存档写入器。

    每个 worker 一个实例（shard = worker 号）：npz 文件名带 shard 前缀，互不
    冲突；close() 时把本 shard 的文件清单写进 meta_shard_{shard}.json，由
    协调进程合并成 meta.json（loadgen 主进程收尾做）。只存被服务接受（202）
    的批次——被 503 拒绝的批次不落盘，重发成功时按重发那一次的 sent_ms 落盘。
    """

    def __init__(self, dir: Path | str, shard: int = 0, *,
                 flush_every: int = 300_000, meta: dict | None = None):
        self.dir = Path(dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.shard = int(shard)
        self.flush_every = flush_every
        self.meta = meta if meta is not None else {}
        self.seq = 0
        self.total = 0
        self._rows: list = []
        self._first_sent: int | None = None
        self._last_sent: int | None = None

    def add(self, events: list[Event]) -> None:
        """写入一个被接受的批次（整批，含 INVALID 行——精确解按 etype 剔除）。

        调用方保证同一批次内事件按发送顺序排列；多批次按完成顺序到达。这样
        "保留第一次出现"的跨批/批内重复在文件里都是首次在前（与服务的到达
        顺序一致，重复只发生在同一 worker 内）。
        """
        if not events:
            return
        rows = [e.as_row() if hasattr(e, "as_row") else (
            e.seq, e.ts, e.url_idx, e.user_idx, e.nbytes, e.sent_ms, e.etype)
            for e in events]
        sent = [e.sent_ms for e in events]
        self._first_sent = min(sent) if self._first_sent is None \
            else min(self._first_sent, min(sent))
        self._last_sent = max(self._last_sent or 0, max(sent))
        self._rows.extend(rows)
        self.total += len(rows)
        if len(self._rows) >= self.flush_every:
            self._flush()

    def _flush(self) -> None:
        if not self._rows:
            return
        a = np.asarray(self._rows, dtype=np.int64)
        name = f"events_{self.shard:02d}_{self.seq:05d}.npz"
        np.savez_compressed(
            self.dir / name,
            event_id=a[:, 0], ts=a[:, 1],
            url_idx=a[:, 2].astype(np.int32), user_idx=a[:, 3].astype(np.int32),
            nbytes=a[:, 4].astype(np.int32),
            sent_ms=a[:, 5], etype=a[:, 6].astype(np.int8),
        )
        self.meta.setdefault("files", []).append(
            {"file": name, "n": int(len(a)),
             "first_sent": int(self._first_sent or 0),
             "last_sent": int(self._last_sent or 0)})
        self._rows = []
        self._first_sent = None
        self._last_sent = None
        self.seq += 1

    def close(self) -> None:
        """收尾：落最后一个文件，写本 shard 的 meta 片段（协调进程合并）。"""
        self._flush()
        frag = dict(self.meta)
        frag["shard"] = self.shard
        frag["total"] = self.total
        (self.dir / f"meta_shard_{self.shard:02d}.json").write_text(
            orjson.dumps(frag).decode(), encoding="utf-8")


def finalize_archive(dir: Path | str, *, base_meta: dict) -> dict:
    """协调进程收尾：把各 shard 的 meta 片段合并成 meta.json，返回它。

    文件清单按 (shard, seq) 排序（= 各 worker 的完成顺序），保证 load_archive
    拼接后"跨批重复的第一次出现在前"。
    """
    dir = Path(dir)
    files = []
    shards = sorted(p.name for p in dir.glob("meta_shard_*.json"))
    for name in shards:
        frag = json.loads((dir / name).read_text(encoding="utf-8"))
        files.extend(frag.get("files", []))
    # npz 文件名自带 shard 与 seq（events_{shard:02d}_{seq:05d}.npz），按名字排即
    # 按 (shard, seq) 排
    files.sort(key=lambda f: f["file"])
    meta = dict(base_meta)
    meta["files"] = files
    (dir / META_NAME).write_text(orjson.dumps(meta).decode(), encoding="utf-8")
    return meta


def read_meta(dir: Path | str) -> dict:
    """读存档的 meta.json（协调进程合并后的）。"""
    return json.loads((Path(dir) / META_NAME).read_text(encoding="utf-8"))


def load_archive(dir: Path | str) -> "ArchiveData":
    """读回整个存档（全部 npz 按 meta 顺序拼接）。

    拼接顺序 = 各 shard 的完成顺序（worker 内的到达顺序），跨批重复的"第一次
    出现"在前——精确解按 event_id 去重（保留第一次出现）时直接按此序即可。
    """
    dir = Path(dir)
    meta = read_meta(dir)
    ids, ts, u, s, b, sent, et = [], [], [], [], [], [], []
    for f in meta["files"]:
        z = np.load(dir / f["file"])
        ids.append(z["event_id"]); ts.append(z["ts"]); u.append(z["url_idx"])
        s.append(z["user_idx"]); b.append(z["nbytes"]); sent.append(z["sent_ms"])
        et.append(z["etype"])
    return ArchiveData(
        meta=meta,
        event_id=np.concatenate(ids).astype(np.int64),
        ts=np.concatenate(ts).astype(np.int64),
        url_idx=np.concatenate(u).astype(np.int64),
        user_idx=np.concatenate(s).astype(np.int64),
        nbytes=np.concatenate(b).astype(np.int64),
        sent_ms=np.concatenate(sent).astype(np.int64),
        etype=np.concatenate(et).astype(np.int8),
    )


@dataclass
class ArchiveData:
    """读回的存档（numpy 整数列；event_id 存的是 event_id 的序号部分）。"""

    meta: dict
    event_id: np.ndarray
    ts: np.ndarray
    url_idx: np.ndarray
    user_idx: np.ndarray
    nbytes: np.ndarray
    sent_ms: np.ndarray
    etype: np.ndarray

    def __len__(self) -> int:
        return len(self.ts)

    @staticmethod
    def key_str(idx, dim: str = "url") -> str:
        """编号（int 或 numpy 标量）→ key 字符串，与生成规则同版本。"""
        return key_of(int(idx)) if dim == "url" else user_of(int(idx))
