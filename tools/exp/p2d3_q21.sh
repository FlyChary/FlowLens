#!/bin/bash
# P2d-3 第二.1：不同写入速率下四类查询的 p50/p95/p99。
# 写入 3/10/14/20 万每秒、每档 120 s、每档新容器（512m、3 前端、两额度 16 MB、快速长尾）。
# 在 WSL 里跑（服务镜像已构建）：
#   bash tools/exp/p2d3_q21.sh
set -u
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
mkdir -p results/p2d3
echo "### [P2d-3 二.1] 四类查询 × 4 写入速率 ###"
$PY tools/exp/p2d3_query_probe.py --image flowlens:p2d3 \
  --rates 30000,100000,140000,200000 --duration 120 \
  --fronts 3 \
  --max-pending-bytes 16777216 --raw-budget-bytes 16777216 \
  --out results/p2d3/q21.json --label p2d3-q21
echo "### [P2d-3 二.1] DONE ###"
