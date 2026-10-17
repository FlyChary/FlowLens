"""带索引的固定容量小顶堆：按 prio 排序、按 key 查找。TopK 与线段树候选堆共用。

key 是 64 位整数哈希，prio 是整数，val 是任意业务数据。
"""


class IndexedMinHeap:
    # __slots__：省掉每个实例的 __dict__，降内存
    __slots__ = ("cap", "keys", "prios", "vals", "idx")

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity 必须为正")
        self.cap = capacity
        self.keys: list[int] = [] # key（64 位哈希）
        self.prios: list[int] = [] # 优先级（次数）
        self.vals: list = [] # 任意业务数据
        self.idx: dict[int, int] = {} # 字典：key → 在堆里的位置

    def __len__(self) -> int:
        return len(self.keys)

    def full(self) -> bool:
        return len(self.keys) >= self.cap

    def min_prio(self) -> int:
        """堆顶优先级；空堆返回 0。"""
        return self.prios[0] if self.keys else 0

    def __contains__(self, key: int) -> bool:
        return key in self.idx

    def get(self, key: int):
        """返回 (prio, val)；不存在时返回 None。"""
        i = self.idx.get(key)
        return None if i is None else (self.prios[i], self.vals[i])

    def bump(self, key: int, delta: int) -> bool:
        """bump：key 已经在堆里，增加次数
        若 key 存在，优先级加 delta 并下沉，返回 True。热路径只查一次索引。
        """
        i = self.idx.get(key)
        if i is None:
            return False
        self.prios[i] += delta
        self._down(i)
        return True

    def update(self, key: int, prio: int, val) -> None:
        """修改已有元素的优先级和数据。"""
        i = self.idx[key]
        old = self.prios[i]
        self.prios[i], self.vals[i] = prio, val
        self._up(i) if prio < old else self._down(i)

    def push(self, key: int, prio: int, val) -> None:
        if self.full() or key in self.idx:
            raise ValueError("堆已满或 key 已存在")
        # 追加到末尾，idx 记下位置，_up 上浮
        self.keys.append(key)
        self.prios.append(prio)
        self.vals.append(val)
        self.idx[key] = len(self.keys) - 1
        self._up(len(self.keys) - 1)

    def replace_min(self, key: int, prio: int, val):
        """用新元素替换堆顶，返回被替换掉的 (key, prio, val)。"""
        old = (self.keys[0], self.prios[0], self.vals[0])
        del self.idx[old[0]]
        self.keys[0], self.prios[0], self.vals[0] = key, prio, val
        self.idx[key] = 0
        self._down(0)
        return old

    def offer(self, key: int, prio: int, val) -> bool:
        """不在堆中的 key：未满就插入，满了且 prio 大于堆顶就替换。返回是否进入堆。"""
        if not self.full():
            self.push(key, prio, val)
            return True
        if prio > self.prios[0]:
            self.replace_min(key, prio, val)
            return True
        return False

    def reprioritize(self, new_prios) -> None:
        """按当前 keys 的顺序整体替换优先级，然后重建堆。"""
        self.prios = [int(p) for p in new_prios]
        for i in range(len(self.keys) // 2 - 1, -1, -1):
            self._down(i)

    def items(self):
        """(key, prio, val) 三元组，无序。"""
        return zip(self.keys, self.prios, self.vals)

    def clear(self) -> None:
        self.keys.clear()
        self.prios.clear()
        self.vals.clear()
        self.idx.clear()

    def _swap(self, i: int, j: int) -> None:
        k, p, v = self.keys, self.prios, self.vals
        k[i], k[j] = k[j], k[i]
        p[i], p[j] = p[j], p[i]
        v[i], v[j] = v[j], v[i]
        self.idx[k[i]] = i
        self.idx[k[j]] = j

    def _up(self, i: int) -> None:
        p = self.prios
        while i > 0:
            parent = (i - 1) >> 1
            if p[i] >= p[parent]:
                return
            self._swap(i, parent)
            i = parent

    def _down(self, i: int) -> None:
        p, n = self.prios, len(self.prios)
        while True:
            left = 2 * i + 1
            if left >= n:
                return
            m = left
            if left + 1 < n and p[left + 1] < p[left]:
                m = left + 1
            if p[m] >= p[i]:
                return
            self._swap(i, m)
            i = m
