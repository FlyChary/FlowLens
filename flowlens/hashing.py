"""全项目共用的 64 位哈希。

每个 key 只算一次 key_hash，其余所有哈希值都通过廉价的整数运算派生出来：
- Count-Min 各行位置、Bloom 位置：先用 derive 以不同种子重新打散；
- HLL 寄存器编号与 TopK 索引：直接用原值。

key_hash 基于 Python 内置的字符串哈希（SipHash，C 实现，结果缓存在字符串对象上，很快）。
它默认每次启动随机化，部署时必须固定 PYTHONHASHSEED（Dockerfile 已设置），
否则重启前后的哈希不一致，将来落盘的数据无法合并。
"""

import numpy as np

MASK64 = (1 << 64) - 1
U64 = np.uint64

SEED_CMS = 0x9E3779B97F4A7C15
SEED_BLOOM = 0xC2B2AE3D27D4EB4F


def key_hash(s: str) -> int:
    """把hash变成无符号 64 位，超出位置为零"""
    return hash(s) & MASK64


def key_hashes(keys) -> np.ndarray:
    """一批字符串的哈希，返回 uint64 数组。"""
    return np.fromiter((hash(str(k)) & MASK64 for k in keys), dtype=U64, count=len(keys))


def mix64(x: np.ndarray) -> np.ndarray:
    """
    通过已有的哈希值再算一次哈希
    splitmix64 终混（向量化，uint64 乘法按 2^64 自然回绕）。
    改动输入的任意一位，输出平均有一半位会翻转
    """
    x = x ^ (x >> U64(30))
    x = x * U64(0xBF58476D1CE4E5B9)
    x = x ^ (x >> U64(27))
    x = x * U64(0x94D049BB133111EB)
    return x ^ (x >> U64(31))


def derive(h: np.ndarray, seed: int) -> np.ndarray:
    """ 指定seed下，根据已有哈希值，派生哈希值 """
    return mix64(h ^ U64(seed))


def double_hash(h: np.ndarray, seed: int):
    """
    bloom需要k个位置，Count-Min需要d行，返回两个hash值，可以派生出多个值
    返回 (h1, h2)，第 i 个位置取 h1 + i·h2；h2 为奇数。
    """
    h1 = derive(h, seed)
    h2 = ((h1 >> U64(32)) | (h1 << U64(32))) | U64(1)
    return h1, h2
