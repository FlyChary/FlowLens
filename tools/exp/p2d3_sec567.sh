#!/bin/bash
# P2d-3 第五、六、七节：
#   五 256 MB、不设置 FLOWLENS_FRONTENDS（自动判断）：S1 爬坡 4/6/8/10/12/14 万
#       （每档 20 s，快速压测）+ 8 万/秒 ×10 分钟长测
#   六 --cpuset-cpus=0、512 MB、不设置 FLOWLENS_FRONTENDS：S1 4/6 万两档
#   七 tools/selftest.py 边界用例，分别在第一节配置（512m/3 前端）与第五节
#       配置（256m/自动→0 前端）下运行，全部用例必须通过
# 在 WSL 里跑：bash tools/exp/p2d3_sec567.sh
set -u
cd /mnt/d/dhy/code/flowlens-c
PY=~/.venv-p2/bin/python
mkdir -p results/p2d3

echo "### [P2d-3 五.2] 256 MB 自动判断 S1 爬坡（快速，4~14 万） ###"
# 指令要求跑满 6 档（每档 20 s），不用 --stop-below 提前停
$PY tools/s1_measure.py --image flowlens:p2d3 --auto-fronts --mem 256m \
  --svc-cp 0,2,4,6 --load-cp 8-15 --load-workers 8 \
  --rates 40000,60000,80000,100000,120000,140000 --level-sec 20 \
  --fast --out results/p2d3/s5_s1.json --label s5_auto256
echo "### [P2d-3 五.2] DONE ###"

echo "### [P2d-3 五.3] 256 MB 自动判断 8 万/秒 ×10 分钟长测 ###"
$PY tools/experiments.py --scenarios SUST --port 12348 --image flowlens:p2d3 --no-build \
  --fast --svc-mem 256m --svc-cpus 0,2,4,6 --load-cpus 8-15 --load-workers 8 \
  --sust-rate 80000 --sust-sec 600 --batch 5000 \
  --results results/p2d3 --expect-fronts 0
echo "### [P2d-3 五.3] DONE ###"

echo "### [P2d-3 六] 单核 512 MB 自动判断 S1（4/6 万） ###"
$PY tools/s1_measure.py --image flowlens:p2d3 --auto-fronts --mem 512m \
  --svc-cp 0 --load-cp 8-15 --load-workers 8 \
  --rates 40000,60000 --level-sec 20 \
  --fast --out results/p2d3/s6_s1.json --label s6_auto512_1core
echo "### [P2d-3 六] DONE ###"

# ---- 七：边界用例（selftest 与边界 e2e 同一套用例，全过即 exit 0） ----
run_selftest() {
  local tag=$1; shift
  local name="p2d3-selftest-$tag-$$"
  local port=12349
  docker run -d --name "$name" -p $port:8080 "$@" flowlens:p2d3 >/dev/null
  local ok=0
  for i in $(seq 1 60); do
    if curl -sf "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then ok=1; break; fi
    sleep 0.5
  done
  [ $ok = 1 ] || { echo "  [FAIL] $tag 服务未就绪"; docker logs "$name" 2>&1 | tail -20; docker rm -f "$name" >/dev/null; return 1; }
  # 核对前端数（第零节第 2 条）
  docker logs "$name" 2>&1 | grep "前端数" | tail -1 | sed "s/^/  /"
  echo "  [selftest $tag] 跑边界用例 …"
  $PY tools/selftest.py --url "http://127.0.0.1:$port" > "results/p2d3/selftest_$tag.txt" 2>&1
  local rc=$?
  tail -1 "results/p2d3/selftest_$tag.txt" | sed "s/^/  /"
  docker rm -f "$name" >/dev/null
  return $rc
}
echo "### [P2d-3 七.1] selftest：第一节配置（512m、3 前端、两额度 16 MB） ###"
run_selftest sec512 \
  --memory=512m --memory-swap=512m --cpuset-cpus=0,2,4,6 \
  -e PYTHONHASHSEED=0 -e FLOWLENS_FRONTENDS=3 \
  -e FLOWLENS_MAX_PENDING_BYTES=16777216 -e FLOWLENS_RAW_BUDGET_BYTES=16777216 \
  && echo "  [PASS] sec512" || echo "  [FAIL] sec512"
echo "### [P2d-3 七.2] selftest：第五节配置（256m、不设置 FLOWLENS_FRONTENDS→0 前端） ###"
run_selftest sec256 \
  --memory=256m --memory-swap=256m --cpuset-cpus=0,2,4,6 \
  -e PYTHONHASHSEED=0 \
  && echo "  [PASS] sec256" || echo "  [FAIL] sec256"
echo "### [P2d-3 七] DONE ###"
