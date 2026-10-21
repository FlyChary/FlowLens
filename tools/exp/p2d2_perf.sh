#!/bin/bash
# P2d-2 第七节性能检查：S2 14万/20万 ×120s（512MB、3前端、两额度各16MB）+ s1 20万×20s。
# 在 WSL 里跑（服务容器 + 压测都在宿主核上）：bash tools/exp/p2d2_perf.sh
set -e
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
mkdir -p results/p2d2
echo "### S2 @140000 ###"
$PY tools/experiments.py --scenarios S2 --port 12345 --image flowlens:p2d2 --no-build \
  --fast --s2-rate 140000 --svc-mem 512m --svc-cpus 0,2,4,6 --load-cpus 8-15 \
  --load-workers 8 --results results \
  --extra-env FLOWLENS_FRONTENDS=3 \
  --extra-env FLOWLENS_MAX_PENDING_BYTES=16777216 \
  --extra-env FLOWLENS_RAW_BUDGET_BYTES=16777216
echo "### S2 @200000 ###"
$PY tools/experiments.py --scenarios S2 --port 12345 --image flowlens:p2d2 --no-build \
  --fast --s2-rate 200000 --svc-mem 512m --svc-cpus 0,2,4,6 --load-cpus 8-15 \
  --load-workers 8 --results results \
  --extra-env FLOWLENS_FRONTENDS=3 \
  --extra-env FLOWLENS_MAX_PENDING_BYTES=16777216 \
  --extra-env FLOWLENS_RAW_BUDGET_BYTES=16777216
echo "### s1 @200000 x20s ###"
$PY tools/s1_measure.py --image flowlens:p2d2 --mem 512m --svc-cp 0,2,4,6 \
  --load-cp 8-15 --load-workers 8 --rates 200000 --level-sec 20 --batch 5000 \
  --fronts 3 --max-pending-bytes 16777216 --raw-budget-bytes 16777216 --fast \
  --out results/p2d2/s1_fronts3.json --label p2d2-fronts3
echo "### ALL DONE ###"
