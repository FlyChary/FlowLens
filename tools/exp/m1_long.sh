#!/bin/bash
# M1 第二.2：长时间运行（评测配置 256 MB、自动单进程、**不加 --cpuset-cpus**，
# 压测进程 taskset 固定核；每次实验从日志确认前端数 = 0）。
#   1. 长尾，每秒 5 万条，10 分钟（SUST 快速压测）
#   2. 全随机攻击流量，每秒 3 万条，10 分钟（S4 原压测模式，不重复 key）
# 每分钟一行：工作集峰值、最高降级级别、接受率（experiments.py SUST/S4 自带 per_minute）。
# 脱离会话运行、逐分钟写盘（experiments.py 逐秒写 timeline.jsonl，summary 结束写）。
# 在 WSL 里跑（建议 nohup）：
#   nohup bash tools/exp/m1_long.sh > results/m1/long.log 2>&1 &
set -u
cd /mnt/d/dhy/code/flowlens
PY=~/.venv-p2/bin/python
mkdir -p results/m1

echo "### [M1 二.2-1] SUST 长尾 5 万/秒 × 600 s（快速压测、256m、自动前端） $(date +%H:%M:%S) ###"
$PY tools/experiments.py --scenarios SUST --port 12352 --image flowlens:merged --no-build \
  --fast --svc-mem 256m --no-cpuset --load-cpus 8-15 --load-workers 8 \
  --sust-rate 50000 --sust-sec 600 --batch 5000 \
  --results results/m1 --expect-fronts 0
echo "### [M1 二.2-1] DONE $(date +%H:%M:%S) ###"

echo "### [M1 二.2-2] S4 全随机攻击流量 3 万/秒 × 600 s（原压测模式、256m、自动前端） $(date +%H:%M:%S) ###"
$PY tools/experiments.py --scenarios S4 --port 12353 --image flowlens:merged --no-build \
  --svc-mem 256m --no-cpuset --load-cpus 8-15 --load-workers 8 \
  --results results/m1 --expect-fronts 0
echo "### [M1 二.2-2] DONE $(date +%H:%M:%S) ###"

echo "### [M1 二.2] ALL DONE $(date +%H:%M:%S) ###"
