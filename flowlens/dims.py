"""统计维度。各处用"按维度下标的列表"代替 url、user 两套重复代码。"""

from enum import IntEnum


class Dim(IntEnum):
    URL = 0
    USER = 1

    @classmethod
    def parse(cls, s: str) -> "Dim":
        """解析查询参数 by。"""
        try:
            return cls[s.upper()]
        except KeyError:
            raise ValueError(f"by 只能是 url 或 user，收到 {s!r}") from None

    def __str__(self) -> str:
        return self.name.lower()


ALL_DIMS = (Dim.URL, Dim.USER)
