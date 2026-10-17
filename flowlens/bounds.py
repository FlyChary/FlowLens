"""把若干个时间片（秒桶、线段树节点）的名单合并成一个区间的 Top-K。

每个 key 都有次数的 [下界, 上界]：按点估计选出前 K 个，只对它们用 Count-Min 收紧
上界；其余候选不需要精算，因为上界够不到第 K 大下界的候选不可能进前 K。
全部计算向量化完成。
"""

import heapq
from dataclasses import dataclass, field
from typing import Callable, Optional

import numpy as np

EMPTY_U64 = np.empty(0, dtype=np.uint64)
EMPTY_F = np.empty(0, dtype=np.float64)


@dataclass
class Part:
    """参与合并的一个时间片。

    hashes/lower/upper/keys 一一对应：名单内 key 的真实次数在 [lower, upper] 内。
    u：不在名单里的 key次数最多多少，在这个时间片内的真实次数不超过 u。
    frac：1 表示整段在查询区间内；(0,1) 表示被区间切开的边缘秒，按比例折算。
    refine：可选，批量给出名单外 key 的更紧上界（活跃秒桶可以查 Count-Min）。
    """

    hashes: np.ndarray = field(default_factory=lambda: EMPTY_U64)
    lower: np.ndarray = field(default_factory=lambda: EMPTY_F)
    upper: np.ndarray = field(default_factory=lambda: EMPTY_F)
    keys: list = field(default_factory=list)
    u: float = 0.0
    frac: float = 1.0
    refine: Optional[Callable[[np.ndarray], np.ndarray]] = None


@dataclass(frozen=True)
class Result:
    hash: int
    key: str
    count: float  # 点估计，表示合并累加出来的代表值
    lower: float
    upper: float

    @property
    def err(self) -> float:
        # upper − lower：保证的误差上界（真实值在区间内，所以点估计偏差不超过区间宽）
        return self.upper - self.lower


@dataclass(frozen=True)
class Answer:
    items: list # Result 列表（已排序、已截到 k 个）
    unseen_upper: float  # 任何不在候选里的 key，次数都不超过它


def top_k(parts: list[Part], k: int) -> Answer:
    """合并各时间片，返回点估计最大的 k 个 key（次数降序，相同时按 key 升序）。"""
    u_all = float(sum(p.u for p in parts))
    if k <= 0 or not any(len(p.hashes) for p in parts):
        return Answer([], u_all)
    H = np.concatenate([p.hashes for p in parts])
    full = [p.frac >= 1 for p in parts]
    L = np.concatenate([p.lower if f else np.zeros(len(p.hashes)) for p, f in zip(parts, full)])
    Up = np.concatenate([p.upper for p in parts])
    P = np.concatenate([p.upper if f else p.frac * p.upper for p, f in zip(parts, full)])
    Upart = np.concatenate([np.full(len(p.hashes), float(p.u)) for p in parts])
    keys_all = [k_ for p in parts for k_ in p.keys]

    # 第 2 步：按 hash 跨片累加
    uniq, first, inv = np.unique(H, return_index=True, return_inverse=True)
    lower = np.bincount(inv, weights=L)
    upper = np.bincount(inv, weights=Up) + (u_all - np.bincount(inv, weights=Upart))
    point = np.bincount(inv, weights=P)

    # 第 3 步：精确选出最终要返回的 k 个候选（而不是"点估计前 k 含全部并列"的整批），
    # 只对它们 refine 并排序。
    #   a. 点估计严格大于第 k 大的候选全部入选（数量一定 < k）；
    #   b. 剩余名额从点估计等于第 k 大的并列组里，按 key 字典序取最小的若干个，
    #      用 heapq.nsmallest 而非整批排序（接近均匀 / 全随机流量下这一组可能有
    #      上万个成员，整批 refine + 排序会把收益全抵消）；
    #   c. 只对最终选出的这 k 个 refine，再按 (-点估计, key) 排序。
    # 这与原实现"点估计降序、相同时按 key 升序取前 k"完全一致：第 k 大点估计 cut 处，
    # 先取 point > cut 的全部（都排在 cut 之前），cut 之上按 key 升序补满到 k。
    # 正确性：refine 只降上界、不改点估计；被 refine 剪掉的候选点估计 < 第 k 大下界，
    # 而至少 k 个候选点估计 ≥ 下界 ≥ 该门槛且精化后上界 ≥ 点估计，必保留，故被剪者
    # 本就不在前 k，结果不变。
    if len(uniq) >= k:
        cut = float(np.partition(point, len(point) - k)[len(point) - k])
        gt = np.flatnonzero(point > cut)  # 严格大于第 k 大的，全入选（数量必 < k）
        ties = np.flatnonzero(point == cut)
        # 并列组里取 key 字典序最小的 k - len(gt) 个；索引作第二键，只为打破
        # "key 相同但哈希不同"的平局，不影响与"按 key 升序"的一致性
        picked = heapq.nsmallest(k - len(gt), ((keys_all[first[i]], i) for i in ties))
        sel_idx = np.concatenate([gt, np.fromiter((i for _, i in picked), dtype=gt.dtype)])
    else:
        sel_idx = np.arange(len(uniq))  # 候选不足 k 个，全部入选
    for p in parts:  # 用 Count-Min 收紧选中候选缺席部分的上界
        if p.refine is None:
            continue
        absent = sel_idx[~np.isin(uniq[sel_idx], p.hashes)]
        if len(absent):
            r = np.minimum(p.refine(uniq[absent]).astype(np.float64), p.u)
            upper[absent] -= p.u - r

    # 第 4 步：组装结果，只对选出的 ≤ k 个排序（次数降序，相同时 key 升序）取前 k
    res = [Result(int(uniq[i]), keys_all[first[i]], float(point[i]), float(lower[i]), float(upper[i])) for i in sel_idx]
    res.sort(key=lambda r: (-r.count, r.key))
    return Answer(res[:k], u_all)


def for_keys(parts: list[Part], hashes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """一批 key 在这些时间片上的 [下界, 上界]（向量化）。"""
    lo = np.zeros(len(hashes))
    hi = np.zeros(len(hashes))
    for p in parts:
        # 每片名单按 hash 排序（每片做一次）
        order = np.argsort(p.hashes) 
        sh = p.hashes[order]
        # 二分找每个查询 key 在片内名单的位置
        pos = np.searchsorted(sh, hashes).clip(max=max(len(sh) - 1, 0))
        found = (sh[pos] == hashes) if len(sh) else np.zeros(len(hashes), dtype=bool)
        j = order[pos[found]]
         # 在名单：累加上界
        hi[found] += p.upper[j]
        # 整秒才累加下界
        if p.frac >= 1:
            lo[found] += p.lower[j]
        # 缺席：用 u（能 refine 就收紧上界）
        miss = ~found
        if miss.any():
            u = np.full(int(miss.sum()), float(p.u))
            if p.refine is not None:
                u = np.minimum(u, p.refine(hashes[miss]))
            hi[miss] += u
    return lo, hi


def for_key(parts: list[Part], h: int) -> tuple[float, float]:
    """某个 key 的 [下界, 上界]。"""
    lo, hi = for_keys(parts, np.array([h], dtype=np.uint64))
    return float(lo[0]), float(hi[0])
