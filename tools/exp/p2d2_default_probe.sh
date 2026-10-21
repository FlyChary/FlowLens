#!/bin/bash
# P2d-2 第七节：起 512m/3 前端服务，跑 p2d2_default_probe.py（自己起 loadgen 20 万/s、
# 用 healthz 增量确认写入到位后测默认窗口 topk/stats 的 p50/p95/p99）。
set -u
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
PORT=12345
OUT=/mnt/d/dhy/code/flowlens-c/results/p2d2
mkdir -p $OUT
NAME="p2d2-probe-$$"
docker rm -f $NAME >/dev/null 2>&1
docker run -d --name $NAME --memory=512m --memory-swap=512m --cpuset-cpus=0,2,4,6 \
  -e PYTHONHASHSEED=0 -e FLOWLENS_FRONTENDS=3 \
  -e FLOWLENS_MAX_PENDING_BYTES=16777216 -e FLOWLENS_RAW_BUDGET_BYTES=16777216 \
  -p $PORT:8080 flowlens:p2d2
ok=0
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:$PORT/healthz >/dev/null 2>&1; then ok=1; break; fi
  sleep 0.5
done
[ $ok = 1 ] || { echo "service not ready"; docker rm -f $NAME >/dev/null 2>&1; exit 1; }
sleep 2
$PY tools/exp/p2d2_default_probe.py --port $PORT --duration 40 | tee $OUT/probe_result.txt
docker rm -f $NAME >/dev/null 2>&1
echo "=== DONE ==="
