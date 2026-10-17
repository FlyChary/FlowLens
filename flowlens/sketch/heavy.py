import numpy as np

from ..bounds import Part
from .cms import CMS
from .topk import TopK


class HeavyHitters:
    """Count-Min（记次数）+ TopK（记名单）。

    每条事件都写 Count-Min（包括已在名单里的 key），被挤出的 key 回来时历史次数还在。
    """

    def __init__(self, width: int, depth: int, capacity: int):
        self.cms = CMS(width, depth)
        self.top = TopK(capacity)

    def observe(self, hashes: np.ndarray, keys: list, weights: np.ndarray) -> None:
        """记录一批出现。同一 key 可以出现多次，这里先聚合再写入。
            hashes用来合并去重，以及使用cms
        """
        if len(hashes) == 0:
            return
        uniq, first, inv = np.unique(hashes, return_index=True, return_inverse=True)
        # 按照inv分组求和，把一批数据里重复的 key 合并，并把它们的权重加起来
        c = np.bincount(inv, weights=weights).astype(np.uint64)
        # 返回估计上界
        est = self.cms.add(uniq, c)
        if self.top.q.full():
            # 名单已满时，估计值不超过堆顶、又不在名单里的 key 一定进不了名单（堆顶只增不减），
            # 先向量化筛掉，只对剩下的少数 key 走逐个更新。
            keep = (est > self.top.min()) | np.isin(uniq, self.top.hash_array())
            uniq, first, est, c = uniq[keep], first[keep], est[keep], c[keep]
        observe = self.top.observe
        for h, i, e, ci in zip(uniq.tolist(), first.tolist(), est.tolist(), c.tolist()):
            observe(h, keys[i], e, ci)

    def u(self) -> int:
        """不在名单里的 key 的次数上界。"""
        return self.top.min()

    def refine(self, hashes: np.ndarray) -> np.ndarray:
        """名单外 key 的更紧上界：min(堆顶, Count-Min 估计)。"""
        return np.minimum(self.cms.estimate(hashes), self.u())

    def bounds(self, h: int) -> tuple[int, int]:
        """ 某个hash值，单 key 上下界 """
        e = self.top.lookup(h)
        if e is not None:
            return e.lower, e.count
        return 0, int(self.refine(np.array([h], dtype=np.uint64))[0])

    def part(self, frac: float = 1.0) -> Part:
        """把当前名单转成可参与合并的时间片。数组是副本；refine 引用本结构的 Count-Min，
        必须在本结构被修改之前用完（在同一次事件循环回调里完成合并即可）。
        frac: 整秒 1.0, 边缘秒传 (0,1)之间的值
        """
        # 秒桶离开环、进入查询世界的接口，把名单快照成 Part，表示一段时间的摘要。
        # 直接读候选堆的 keys/prios/vals，不再逐个建 Entry：
        #   hashes：key 的 64 位哈希（q.keys），用于合并与跨片对齐
        #   upper：该 key 真实次数的上界（q.prios，即 count）
        #   lower：上界 − 入堆时继承的不确定部分（count − base）
        #   keys：原始字符串（URL 或 user_id，q.vals 第一项），出结果展示用
        q = self.top.q
        keys, prios, vals = q.keys, q.prios, q.vals
        n = len(keys)
        return Part(
            hashes=np.fromiter(keys, dtype=np.uint64, count=n),
            lower=np.fromiter((p - base for p, (_, base) in zip(prios, vals)), dtype=np.float64, count=n),
            upper=np.fromiter(prios, dtype=np.float64, count=n),
            keys=[v[0] for v in vals], u=self.u(), frac=frac, refine=self.refine,
        )

    def clear(self) -> None:
        self.cms.reset()
        self.top.clear()
