#!/bin/bash
# P2d-3：重建镜像（含二.3 探针）。test stage 跑 Python 3.12 全量快测试，
# app stage 出服务镜像 flowlens:p2d3。在 WSL 里跑：
#   bash tools/exp/p2d3_rebuild.sh > results/p2d3/build_status.log 2>&1
set -u
cd /mnt/d/dhy/code/flowlens-c
mkdir -p results/p2d3
echo "### BUILD-START $(date +%H:%M:%S)"
docker build --target test -t flowlens:p2d3-test . > results/p2d3/build_test.log 2>&1 \
  || { echo "### TEST-STAGE-FAILED $(date +%H:%M:%S)"; tail -30 results/p2d3/build_test.log; exit 1; }
echo "### TEST-STAGE-OK $(date +%H:%M:%S)"
docker build -t flowlens:p2d3 . > results/p2d3/build_app.log 2>&1 \
  || { echo "### APP-FAILED $(date +%H:%M:%S)"; tail -30 results/p2d3/build_app.log; exit 1; }
echo "### APP-OK $(date +%H:%M:%S)"
