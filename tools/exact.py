"""离线精确解（T10a）：读 loadgen 的存档，按服务规则求出"被接受的事件集合"，
对任意毫秒区间 [from_ms, to_ms) 给出精确的 Top-K、去重人数、p95 分位数，
并提供与服务结果对比的辅助函数。

"被接受的事件集合"的口径（与写入路径一致）：
1. 去掉非法事件（etype=INVALID：缺字段/类型错，服务逐条只计 invalid）与未来事件
   （etype=FUTURE：服务计入 invalid）；
2. 按 **sent_ms**（该批的发送时刻）判定迟到：ts < sent_ms − 60 s 去掉。服务按
   "收到时刻"判定，loadgen 的时间戳已避开 ±0.5 s 边界，两种口径结论一致；
3. 按 event_id 去重，保留第一次出现（存档顺序 = 服务到达顺序：同一 worker 内按
   发送顺序，跨批重复的"第一次"在前；跨 worker 的 event_id 互不冲突）。

注意与服务的顺序一致：迟到/未来判断在去重之前（engine.ingest 先 classify 再
bloom），所以迟到的重复事件不会写进"已见"集合、不影响后到的同 id 事件。

分位数口径（quantile_bytes）：按秩取值，与 DDSketch 的 `quantile` 完全同一套
秩定义——目标秩 R = ⌊q·(n−1)⌋ + 1（1 起算），答案 = 升序第 R 个值（numpy 的
`np.quantile(x, q, method="lower")`）。DDSketch 在同一秩上返回的是桶代表值
（相对误差 ≤ α），精确解返回该秩的真实值：两者取的是"同一个位置"，比较的是
位置相同而取值精度不同。
"""

import argparse
import sys
from dataclasses import dataclass

import numpy as np

from flowgen import (INVALID, FUTURE, ArchiveData, load_archive, key_of, user_of)

# 与 timewin.LATENESS_MS 一致（离线精确解独立于服务进程，这里复制常量并注释来源）
LATENESS_MS = 60_000


@dataclass
class ExactSolution:
    """存档 → 被接受事件集合（按 ts 排序）→ 区间查询。"""

    # 被接受的事件列（服务规则筛选 + 去重后，按 ts 升序）
    ts: np.ndarray
    url_idx: np.ndarray
    user_idx: np.ndarray
    nbytes: np.ndarray
    sent_ms: np.ndarray          # 仅保留，便于核对；查询不用

    @classmethod
    def from_archive(cls, data: ArchiveData) -> "ExactSolution":
        ok = (data.etype != INVALID) & (data.etype != FUTURE)
        # 按发送时刻判迟到（见模块 docstring 的口径说明）
        ok &= ~(data.ts < data.sent_ms - LATENESS_MS)
        # 按 event_id 去重，保留第一次出现（存档序 = 服务到达序）。
        # np.unique 的 return_index 给出"过滤后子数组"里每个 id 首次出现的位置，
        # 用 flatnonzero(ok) 映射回全量行的下标。
        _, first = np.unique(data.event_id[ok], return_index=True)
        rows = np.flatnonzero(ok)[first]
        order = np.argsort(data.ts[rows], kind="stable")
        rows = rows[order]
        return cls(
            ts=data.ts[rows],
            url_idx=data.url_idx[rows],
            user_idx=data.user_idx[rows],
            nbytes=data.nbytes[rows],
            sent_ms=data.sent_ms[rows],
        )

    @classmethod
    def from_dir(cls, dir) -> "ExactSolution":
        return cls.from_archive(load_archive(dir))

    def _span(self, from_ms: int, to_ms: int) -> tuple[int, int]:
        # 半开区间 [from, to)：下界取"第一个 ≥ from"，上界取"第一个 ≥ to"（to 不含，
        # 等于 to 的行必须被排除——用 side="left" 而非 "right"）
        return (int(np.searchsorted(self.ts, from_ms, side="left")),
                int(np.searchsorted(self.ts, to_ms, side="left")))

    def topk(self, by: str, from_ms: int, to_ms: int, k: int) -> list[tuple[str, int]]:
        """精确前 k 名（次数降序，并列按 key 字典序——与服务一致）。"""
        if by not in ("url", "user"):
            raise ValueError(f"by 只能是 url 或 user，收到 {by!r}")
        i0, i1 = self._span(from_ms, to_ms)
        idx = (self.url_idx if by == "url" else self.user_idx)[i0:i1]
        if len(idx) == 0:
            return []
        _, inv = np.unique(idx, return_inverse=True)
        counts = np.bincount(inv)
        maker = key_of if by == "url" else user_of
        keys = [maker(int(i)) for i in np.unique(idx)]
        pairs = list(zip(counts.tolist(), keys))
        pairs.sort(key=lambda p: (-p[0], p[1]))
        return [(key, int(c)) for c, key in pairs[:k]]

    def count_of(self, by: str, from_ms: int, to_ms: int, key: str) -> int:
        """某个 key 在区间内的精确次数。"""
        i0, i1 = self._span(from_ms, to_ms)
        idx = (self.url_idx if by == "url" else self.user_idx)[i0:i1]
        # 编号与字符串的映射是双射（key_of / user_of），反解编号
        prefix = "/api/item/x" if by == "url" else "u"
        num = key[len(prefix):]
        if not num.isdigit():
            return 0
        return int(np.sum(idx == int(num)))

    def distinct_users(self, from_ms: int, to_ms: int) -> int:
        i0, i1 = self._span(from_ms, to_ms)
        return int(len(np.unique(self.user_idx[i0:i1])))

    def quantile_bytes(self, from_ms: int, to_ms: int, q: float) -> float:
        """精确分位数（秩定义见模块 docstring；区间无数据返回 nan）。"""
        if not (0 <= q <= 1):
            raise ValueError(f"q 应在 0~1，收到 {q}")
        i0, i1 = self._span(from_ms, to_ms)
        b = self.nbytes[i0:i1]
        if len(b) == 0:
            return float("nan")
        return float(np.quantile(b, q, method="lower"))

    def __len__(self) -> int:
        return len(self.ts)


def compare(service_items: list[dict], exact_items: list[tuple[str, int]],
            k: int | None = None) -> dict:
    """把服务的答案与精确答案对比（T10a）。

    service_items: 服务的 topk 结果，每项 {'key', 'count', 'lower', 'upper'}
                   （lower/upper 缺省按 count 自身处理，即只算误差）。
    exact_items:   精确 top-k，[(key, 真实次数), ...]（降序）。
    k:             名义 k（缺省取 len(service_items)）。

    返回：
      recall          —— 召回。口径（与 PROGRESS.md 已定决策一致）：返回的 key 的
                         **真实次数**不小于精确答案第 k 名的次数即算命中（并列不算
                         错）。分母是服务返回的条数。
      mean_rel_err / max_rel_err —— 每个返回 key 的计数相对误差 |count−真实|/真实，
                         均值与最大值。
      in_bounds       —— 每个返回 key 的真实次数是否都落在其 [lower, upper] 内。
    """
    k = k if k is not None else len(service_items)
    exact_counts = dict(exact_items)
    # 精确第 k 名的次数（不足 k 个 key 时取最小的那个真实次数）
    if exact_items:
        kth = exact_counts[exact_items[min(k, len(exact_items)) - 1][0]]
    else:
        kth = 0
    hits = 0
    rel_errs = []
    in_bounds = True
    for it in service_items:
        key = it["key"]
        true = exact_counts.get(key, 0)
        # 命中：真实次数 ≥ 精确第 k 名的次数（并列不算错）。kth == 0（精确答案
        # 为空）时任何返回的 key 都不算命中。
        if kth > 0 and true >= kth:
            hits += 1
        if true > 0:
            rel_errs.append(abs(it["count"] - true) / true)
        if "lower" in it or "upper" in it:
            lo = it.get("lower", it["count"])
            hi = it.get("upper", it["count"])
            if not (lo <= true <= hi):
                in_bounds = False
    n = len(service_items)
    return {
        "k": k,
        "recall": hits / n if n else 1.0,
        "mean_rel_err": float(np.mean(rel_errs)) if rel_errs else 0.0,
        "max_rel_err": float(np.max(rel_errs)) if rel_errs else 0.0,
        "in_bounds": in_bounds,
    }


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(description="离线精确解：对存档的任意毫秒区间求精确答案")
    p.add_argument("archive", help="存档目录（loadgen --archive 的输出）")
    p.add_argument("--from-ms", type=int, default=None, help="区间起点（毫秒，含）")
    p.add_argument("--to-ms", type=int, default=None, help="区间终点（毫秒，不含）")
    p.add_argument("--last", type=float, default=None,
                   help="区间长度（秒）：to = 存档最新 sent_ms，from = to − last×1000")
    p.add_argument("--by", choices=["url", "user"], default="url")
    p.add_argument("--k", type=int, default=10)
    args = p.parse_args()

    if args.last is not None:
        data = load_archive(args.archive)
        to = int(data.sent_ms.max())
        frm = to - int(args.last * 1000)
    else:
        if args.from_ms is None or args.to_ms is None:
            p.error("需要 --from-ms 与 --to-ms，或 --last")
        frm, to = args.from_ms, args.to_ms

    sol = ExactSolution.from_dir(args.archive)
    print(f"区间 [{frm}, {to})，被接受事件 {len(sol)} 条")
    print(f"Top-{args.k}（by={args.by}）：")
    for key, c in sol.topk(args.by, frm, to, args.k):
        print(f"  {c:>8}  {key}")
    print(f"去重用户数（精确）：{sol.distinct_users(frm, to)}")
    p95 = sol.quantile_bytes(frm, to, 0.95)
    print(f"p95 字节（精确，秩 ⌊0.95(n−1)⌋+1）：{p95 if p95 == p95 else 'nan（区间无数据）'}")


if __name__ == "__main__":
    main()
