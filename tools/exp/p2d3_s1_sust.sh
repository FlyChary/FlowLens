#!/bin/bash
# P2d-3 第一节：512 MB、3 前端、两额度合计 16 MB、快速压测长尾、
# 20 万/秒持续 10 分钟 + 停发观察 60 秒（SUST 场景）。
# 每分钟一行：工作集峰值、memory.current 峰值、最高降级、接受率、引擎 CPU%、
# 引擎每条 μs、前端合计 CPU%；全程合计见 summary.json。
# 在 WSL 里跑：bash tools/exp/p2d3_s1_sust.sh
set -u
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
mkdir -p results/p2d3
echo "### [P2d-3 一] SUST 20万/秒 × 600s + 停发 60s ###"
$PY tools/experiments.py --scenarios SUST --port 12345 --image flowlens:p2d3 --no-build \
  --fast --svc-mem 512m --svc-cpus 0,2,4,6 --load-cpus 8-15 --load-workers 8 \
  --sust-rate 200000 --sust-sec 600 --batch 5000 \
  --results results/p2d3 \
  --expect-fronts 3 \
  --extra-env FLOWLENS_FRONTENDS=3 \
  --extra-env FLOWLENS_MAX_PENDING_BYTES=16777216 \
  --extra-env FLOWLENS_RAW_BUDGET_BYTES=16777216
echo "### [P2d-3 一] DONE ###"
