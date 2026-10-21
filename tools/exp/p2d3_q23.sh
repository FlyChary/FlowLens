#!/bin/bash
# P2d-3 第二.3：20 万/秒写入时引擎事件循环延迟 + 归因（FLOWLENS_DEBUG=1，
# 探针每分钟报告 + tick 分解 + 写入块耗时，解析对齐后归因"写入块/定稿/快照/其他"）。
# 在 WSL 里跑：bash tools/exp/p2d3_q23.sh
set -u
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
mkdir -p results/p2d3
echo "### [P2d-3 二.3] 事件循环探针（20 万/秒 ×240 s，FLOWLENS_DEBUG=1） ###"
$PY tools/exp/p2d3_loop_probe.py --image flowlens:p2d3 --duration 240 \
  --fronts 3 --max-pending-bytes 16777216 --raw-budget-bytes 16777216 \
  --out results/p2d3/q23.json --label p2d3-q23
echo "### [P2d-3 二.3] DONE ###"
