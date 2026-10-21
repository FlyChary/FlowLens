#!/bin/bash
# M1 第二节：评测配置（256 MB、自动单进程、**不加 --cpuset-cpus**，压测进程 taskset
# 固定核；每次实验从日志确认前端数 = 0）的补测。
#   2.1 查询延迟随写入速率（0/3/5/8 万每秒，每档 120 s，m1_q21.py 每档新容器）
#   2.2 长时间运行：长尾 5 万/秒 ×10 分钟（SUST 快速）+ 全随机 3 万/秒 ×10 分钟（S4 原模式）
#   2.3 精度：长尾 3 万/秒 ×5 分钟（S3 原模式，存档 + 精确解 200 区间）
# 长实验逐档/逐分钟写盘（m1_q21.py 每档增量 JSON；experiments.py 逐秒 timeline + 结束 summary）。
# 必须由"保持 WSL VM 存活的前台命令"驱动（如 Claude 的后台 Bash），单独 nohup 会被回收。
#   python 用 ~/.venv-p2/bin/python（与 P2d 系列同环境）。
set -u
cd /mnt/d/dhy/code/flowlens
PY=~/.venv-p2/bin/python
mkdir -p results/m1

echo "### [M1 二.1] 查询延迟随写入速率（0/3/5/8 万，每档 120 s） $(date +%H:%M:%S) ###"
$PY tools/exp/m1_q21.py --image flowlens:merged \
  --rates 0,30000,50000,80000 --duration 120 \
  --out results/m1/q21.json --label m1-q21
echo "### [M1 二.1] DONE $(date +%H:%M:%S) ###"

echo "### [M1 二.2-1] SUST 长尾 5 万/秒 × 600 s $(date +%H:%M:%S) ###"
$PY tools/experiments.py --scenarios SUST --port 12352 --image flowlens:merged --no-build \
  --fast --svc-mem 256m --no-cpuset --load-cpus 8-15 --load-workers 8 \
  --sust-rate 50000 --sust-sec 600 --batch 5000 \
  --results results/m1 --expect-fronts 0
echo "### [M1 二.2-1] DONE $(date +%H:%M:%S) ###"

echo "### [M1 二.2-2] S4 全随机攻击流量 3 万/秒 × 600 s $(date +%H:%M:%S) ###"
$PY tools/experiments.py --scenarios S4 --port 12353 --image flowlens:merged --no-build \
  --svc-mem 256m --no-cpuset --load-cpus 8-15 --load-workers 8 \
  --results results/m1 --expect-fronts 0
echo "### [M1 二.2-2] DONE $(date +%H:%M:%S) ###"

echo "### [M1 二.3] S3 精度 长尾 3 万/秒 × 300 s（原模式，存档 + 精确解） $(date +%H:%M:%S) ###"
$PY tools/experiments.py --scenarios S3 --port 12354 --image flowlens:merged --no-build \
  --svc-mem 256m --no-cpuset --load-cpus 8-15 --load-workers 8 \
  --mode 长尾 --results results/m1 --expect-fronts 0
echo "### [M1 二.3] DONE $(date +%H:%M:%S) ###"

echo "### [M1 第二节] ALL DONE $(date +%H:%M:%S) ###"
