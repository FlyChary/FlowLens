#!/bin/bash
# P2d-3 第二节后两段：二.2（块 10000 vs 5000，各 120 s）→ 二.3（事件循环探针 240 s）。
# 顺序执行（CPU 独占，不能并行）。在 WSL 里跑：bash tools/exp/p2d3_sec2b.sh
set -u
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
mkdir -p results/p2d3

echo "### [P2d-3 二.2] 块 10000（默认） ###"
$PY tools/exp/p2d3_query_probe.py --image flowlens:p2d3 \
  --rates 200000 --duration 120 --chunk-events 10000 \
  --fronts 3 --max-pending-bytes 16777216 --raw-budget-bytes 16777216 \
  --out results/p2d3/q22_chunk10000.json --label p2d3-q22-c10000 || echo "[ERR] c10000"

echo "### [P2d-3 二.2] 块 5000 ###"
$PY tools/exp/p2d3_query_probe.py --image flowlens:p2d3 \
  --rates 200000 --duration 120 --chunk-events 5000 \
  --fronts 3 --max-pending-bytes 16777216 --raw-budget-bytes 16777216 \
  --out results/p2d3/q22_chunk5000.json --label p2d3-q22-c5000 || echo "[ERR] c5000"

echo "### [P2d-3 二.3] 事件循环探针（20 万/秒 ×240 s，FLOWLENS_DEBUG=1） ###"
$PY tools/exp/p2d3_loop_probe.py --image flowlens:p2d3 --duration 240 \
  --fronts 3 --max-pending-bytes 16777216 --raw-budget-bytes 16777216 \
  --out results/p2d3/q23.json --label p2d3-q23 || echo "[ERR] q23"

echo "### [P2d-3 二.2/二.3] DONE ###"
