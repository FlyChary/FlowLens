from dataclasses import dataclass

from .heap import IndexedMinHeap


@dataclass(frozen=True)
class Entry:
    """名单里的一个 key。

    count：上界。入堆时取 Count-Min 估计值（不小于真实值），之后每次出现精确累加。
    base ：入堆时继承的不确定部分。下界 = count − base，即入堆以来精确数到的次数。
    """

    hash: int
    key: str
    count: int
    base: int # 入堆前在CMS里之前攒下次数

    @property
    def lower(self) -> int:
        """ 
        [lower, count] 夹住真实次数，写入时只是被维护（observe 的 bump/offer
        保证它始终成立）；查询时才有用武之地：
        1. 剪枝——"第 K 大的下界当门槛，上界够不着就扔"，安全性全靠它
        2. 报错——err = upper − lower 是保证的误差上界（Result.err）；
        3. 单 key 查询——for_keys/bounds 返回的区间保证含真实值。

        没有这个性质，Top-K 的剪枝会丢真热点 
        """
        return self.count - self.base


class TopK:
    """固定容量的热点名单，底层是带索引的小顶堆，堆顶是当前计数最小的 key。

    缺席上界：堆满以后，不在堆里的 key 真实次数一定不超过堆顶 min()。
    理由：它每次出现时估计值都没超过当时的堆顶；被挤出时它的计数等于当时的堆顶；
    而堆顶只增不减（计数只增，替换时新值大于旧堆顶）。堆没满时，所有出现过的 key 都在堆里。
    """

    def __init__(self, capacity: int):
        self.q = IndexedMinHeap(capacity)

    def __len__(self) -> int:
        return len(self.q)

    def min(self) -> int:
        """缺席上界：堆没满时为 0。"""
        return self.q.min_prio() if self.q.full() else 0

    def observe(self, h: int, key: str, est: int, c: int) -> None:
        """记录 key 出现 c 次；est 是同一次 Count-Min 更新后的估计值。"""
        # 增加堆中值，调整位置
        if self.q.bump(h, c):
            return
        # 若不在名单，让堆判断是否进入
        self.q.offer(h, est, (key, max(est - c, 0)))

    def hash_array(self):
        """名单内全部 key 的哈希（numpy 数组）。用于复查CMS"""
        import numpy as np

        return np.fromiter(self.q.keys, dtype=np.uint64, count=len(self.q.keys))

    def lookup(self, h: int):
        # 查单个 key，返回它的 Entry（含上下界）
        got = self.q.get(h)
        if got is None:
            return None
        prio, (key, base) = got
        return Entry(h, key, prio, base)

    def entries(self) -> list[Entry]:
        return [Entry(h, key, p, base) for h, p, (key, base) in self.q.items()]

    def top(self, n: int) -> list[Entry]:
        """计数最大的 n 个：次数降序，相同时按 key 字典序。"""
        es = self.entries()
        es.sort(key=lambda e: (-e.count, e.key))
        return es[:n]

    def clear(self) -> None:
        self.q.clear()
