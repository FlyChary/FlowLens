#!/bin/bash
# P2d-3 第三、四节：精度（S3，原压测模式长尾 3 万/秒 ×5 分钟 + 精确解 200 区间）
# 与攻击流量（S4，全随机 3 万/秒 ×10 分钟）。配置同第一节：
# 512m、3 前端、两额度合计 16 MB。原模式不支持 --fast。
# 在 WSL 里跑：bash tools/exp/p2d3_sec34.sh
set -u
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
mkdir -p results/p2d3
echo "### [P2d-3 三] S3 精度（原模式长尾 3 万/秒 ×5 分钟） ###"
$PY tools/experiments.py --scenarios S3 --port 12346 --image flowlens:p2d3 --no-build \
  --svc-mem 512m --svc-cpus 0,2,4,6 --load-cpus 8-15 --load-workers 8 \
  --mode 长尾 --results results/p2d3 \
  --expect-fronts 3 \
  --extra-env FLOWLENS_FRONTENDS=3 \
  --extra-env FLOWLENS_MAX_PENDING_BYTES=16777216 \
  --extra-env FLOWLENS_RAW_BUDGET_BYTES=16777216
echo "### [P2d-3 三] DONE ###"
echo "### [P2d-3 四] S4 攻击流量（全随机 3 万/秒 ×10 分钟） ###"
$PY tools/experiments.py --scenarios S4 --port 12347 --image flowlens:p2d3 --no-build \
  --svc-mem 512m --svc-cpus 0,2,4,6 --load-cpus 8-15 --load-workers 8 \
  --results results/p2d3 \
  --expect-fronts 3 \
  --extra-env FLOWLENS_FRONTENDS=3 \
  --extra-env FLOWLENS_MAX_PENDING_BYTES=16777216 \
  --extra-env FLOWLENS_RAW_BUDGET_BYTES=16777216
echo "### [P2d-3 四] DONE ###"
