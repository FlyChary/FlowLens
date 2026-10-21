"""P2b 四：py-spy raw 剖析的分类汇总（只读 raw_t.txt，报告各类别占比）。

用法：python tools/exp/p2b_pyspy_cat.py <raw_t.txt 路径>
按 P2a（docs/lanes/P2-notes §1.1）的口径把每个采样按**栈顶函数所在的模块**归类，
重点报告 Bloom（flowlens/dedup.py）占比，用于对照 P2a 的 22.2% 看 P2b 优化后
Bloom 占比下降了多少。
"""
import re
import sys
from pathlib import Path

# 栈顶帧 → 类别。判定顺序：先具体后宽泛。frame 形如 "name (file:line)"。
FRM_RE = re.compile(r"([^\s(]+) \(([^:)]+):(\d+)\)")


def top_frame(line: str) -> str:
    """取线程名之后的第一个帧（栈顶）。行形如 'thread (N): name1;f1 (a.py:1);f2 (b.py:2) COUNT'。"""
    body = line.split(":", 1)[1]  # 去掉 "thread (N)"
    # 帧之间以 "; " 分隔，取最后一段前再取最后一个帧？——py-spy raw 的栈是
    # 从外层到栈顶的顺序，栈顶是最后一段（; 分隔的末帧）。
    segs = [s for s in body.split(";") if s.strip() and not s.strip().isdigit()]
    for s in reversed(segs):
        m = FRM_RE.search(s.strip())
        if m:
            return m.group(1) + "|" + m.group(2)
    return ""


def categorize(frame: str) -> str:
    name, path = frame.split("|")
    p = path
    if "flowlens/dedup.py" in p:
        return "Bloom(去重)"
    if "flowlens/sketch/" in p:
        return "写入·各维度(sketch)"
    if "flowlens/ring.py" in p:
        return "写入·ring"
    if "flowlens/segtree.py" in p:
        return "定稿/快照(tree)"
    if "flowlens/bounds.py" in p:
        return "定稿/快照(bounds)"
    if "flowlens/engine.py" in p:
        return "写入/快照(engine)"
    if "flowlens/parse_pool.py" in p or "multiprocessing/" in p or "concurrent/" in p \
            or "futures/" in p:
        return "进程池通信"
    if "flowlens/hashing.py" in p:
        return "哈希(hashing)"
    if "flowlens/intern.py" in p:
        return "intern"
    if "aiohttp/" in p or "flowlens/api/" in p or "flowlens/ingest.py" in p:
        return "HTTP/接入"
    if "asyncio/" in p or "selectors" in p:
        return "事件循环"
    if "flowlens/model.py" in p:
        return "解析/归一化(model)"
    if "orjson" in p:
        return "解析(orjson)"
    return "其他"


def main() -> None:
    raw = Path(sys.argv[1])
    counts: dict[str, int] = {}
    total = 0
    for line in raw.open(encoding="utf-8", errors="replace"):
        if not line.startswith("thread"):
            continue
        m = re.match(r"thread \((\d+)\): (.*\S)\s+(\d+)$", line.strip())
        if not m:
            continue
        n = int(m.group(3))
        fr = top_frame(m.group(2))
        cat = categorize(fr) if fr else "未知"
        counts[cat] = counts.get(cat, 0) + n
        total += n
    if not total:
        print("（无采样）")
        return
    print(f"总采样 {total}")
    print(f"{'类别':<22}{'占比':>8}{'采样':>8}")
    for cat, c in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"{cat:<22}{c / total * 100:>7.1f}%{c:>8}")


if __name__ == "__main__":
    main()
