"""有上限的字符串驻留表（T9b 支线 C3）。

相同内容的字符串返回同一个对象：写入路径上每个事件都会带着 user_id 与归一化
后的 URL 进秒桶、叶子、节点，重复内容若各自一份对象，长尾流量下会存很多份
（C1 账本：长尾下 user 侧 ~7 MB、lane-c 旧口径 url 侧 ~9 MB 都是重复对象）。

策略与 model._norm_cache 一致：上限为模块常量（不进 config.py），满了**整体
清空**重建——不做 LRU（淘汰逻辑的 CPU 开销与实现复杂度不值这个收益），清空
瞬间的代价是丢一批驻留项、后续命中重新驻留。全随机（对抗性）流量下每个值都
不重复，表只涨不清命中——整体清空把驻留表自身的内存钉死在 cap 以内，这也是
"对抗性流量下内存有界"的一环。

驻留只影响"同一内容是不是同一个对象"，不改字符串的内容与哈希：同一内容
hash 值本来就相同，去重、分桶、Top-K 语义不变。

实验变量（C3）：上限可用环境变量覆盖（FLOWLENS_INTERN_USER_CAP /
FLOWLENS_INTERN_URL_CAP），设 0 表示关闭驻留——用于"加不加驻留"的对照实验。
默认值就是下面的模块常量，生产不设这两个变量。
"""

import os


class Intern:
    """有上限的驻留表：intern(s) 相同内容返回同一对象，满了整体清空。"""

    def __init__(self, cap: int):
        if cap <= 0:
            raise ValueError("cap 必须为正")
        self.cap = cap
        self._table: dict[str, str] = {}

    def intern(self, s: str) -> str:
        """返回与 s 同内容的驻留对象；表满时先整体清空再写入。"""
        hit = self._table.get(s)
        if hit is not None:
            return hit
        if len(self._table) >= self.cap:
            self._table.clear()
        self._table[s] = s
        return s

    def clear(self) -> None:
        """清空驻留表（降级第 3 级收紧内存时可与归一化缓存一起清）。"""
        self._table.clear()

    def __len__(self) -> int:
        return len(self._table)


# 驻留上限（模块常量，不进 config.py）：
# user 上限 8192（C1 建议：长尾不同 user ~2448，随机 3.7 万——整体清空策略下随机
# 流量的驻留开销被限制在 <0.5 MB）；URL 上限 32768（归一化结果的去重对象来源，
# 长尾不同归一化结果 ~1847，留出 10 倍以上余量）。
USER_CAP = 8192
URL_CAP = 32768


def _effective_cap(env_name: str, default: int) -> int:
    """从环境变量读上限（实验变量）：缺省/非法时退回默认常量。"""
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _make_intern(env_name: str, default: int) -> Intern | None:
    cap = _effective_cap(env_name, default)
    return Intern(cap) if cap > 0 else None


# 进程级单例：user 驻留（engine.ingest 用）与 url 驻留（model.normalize_url 用）。
# cap <= 0 时为 None（驻留关闭），调用方跳过驻留、行为与未启用时一致。
user_intern: Intern | None = _make_intern("FLOWLENS_INTERN_USER_CAP", USER_CAP)
url_intern: Intern | None = _make_intern("FLOWLENS_INTERN_URL_CAP", URL_CAP)
