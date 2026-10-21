#!/bin/bash
# M1 第一节第 3 步：正式镜像 flowlens:merged 在两种配置下跑 tools/selftest.py 边界用例，
# 全部用例必须通过。两种配置都**不设置 FLOWLENS_FRONTENDS**（自动判断）、**不加
# --cpuset-cpus**（与评测方最可能的方式一致），只改内存上限：
#   a. 256 MB → 日志"前端数 0（内存上限 256 MB < 384 MB）"
#   b. 512 MB → 日志"前端数 3（自动）"（Linux、核 ≥ 2、内存上限 ≥ 384 MB）
# 结果写 results/m1/selftest_256.txt / selftest_512.txt。
# 在 WSL 里跑：bash tools/exp/m1_selftest.sh
set -u
cd /mnt/d/dhy/code/flowlens
PY=~/.venv-p2/bin/python
mkdir -p results/m1

run_selftest() {
  local tag=$1; shift
  local name="m1-selftest-$tag-$$"
  local port=$1; shift
  docker rm -f "$name" >/dev/null 2>&1
  docker run -d --name "$name" -p $port:8080 \
    --memory=$1 --memory-swap=$1 \
    -e PYTHONHASHSEED=0 \
    flowlens:merged >/dev/null
  local ok=0
  for i in $(seq 1 60); do
    if curl -sf --noproxy '*' "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then ok=1; break; fi
    sleep 0.5
  done
  [ $ok = 1 ] || { echo "  [FAIL] $tag 服务未就绪"; docker logs "$name" 2>&1 | tail -20; docker rm -f "$name" >/dev/null; return 1; }
  echo "  [前端数核对 $tag]"; docker logs "$name" 2>&1 | grep "前端数" | tail -1 | sed 's/^/  /'
  echo "  [selftest $tag] 跑边界用例 …"
  $PY tools/selftest.py --url "http://127.0.0.1:$port" > "results/m1/selftest_$tag.txt" 2>&1
  local rc=$?
  tail -1 "results/m1/selftest_$tag.txt" | sed 's/^/  /'
  docker rm -f "$name" >/dev/null
  return $rc
}

echo "### [M1 一.3a] selftest：256 MB、不设置 FLOWLENS_FRONTENDS（自动→0 前端）"
run_selftest m1_256 12350 256m && echo "  [PASS] m1_256" || echo "  [FAIL] m1_256"
echo "### [M1 一.3b] selftest：512 MB、不设置 FLOWLENS_FRONTENDS（自动→3 前端）"
run_selftest m1_512 12351 512m && echo "  [PASS] m1_512" || echo "  [FAIL] m1_512"
echo "### [M1 一.3] DONE ###"
