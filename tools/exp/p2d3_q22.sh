#!/bin/bash
# P2d-3 第二.2：写入块大小对回放查询的影响。
# 20 万/秒写入，FLOWLENS_POOLED_CHUNK_EVENTS=10000（默认）与 5000 各跑 120 s，
# 每档新容器。复用二.1 的查询探针（四类查询 + SSE + 引擎/前端 CPU）。
# 在 WSL 里跑：bash tools/exp/p2d3_q22.sh
set -u
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
mkdir -p results/p2d3
echo "### [P2d-3 二.2] 块大小 10000 vs 5000（20 万/秒 ×120s） ###"
$PY tools/exp/p2d3_query_probe.py --image flowlens:p2d3 \
  --rates 200000 --duration 120 --chunk-events 10000 \
  --fronts 3 --max-pending-bytes 16777216 --raw-budget-bytes 16777216 \
  --out results/p2d3/q22_chunk10000.json --label p2d3-q22-c10000
$PY tools/exp/p2d3_query_probe.py --image flowlens:p2d3 \
  --rates 200000 --duration 120 --chunk-events 5000 \
  --fronts 3 --max-pending-bytes 16777216 --raw-budget-bytes 16777216 \
  --out results/p2d3/q22_chunk5000.json --label p2d3-q22-c5000
echo "### [P2d-3 二.2] DONE ###"
