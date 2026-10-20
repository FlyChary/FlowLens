#!/usr/bin/env bash
# P2b 四：对修改后的主进程 py-spy 剖析 30 秒，确认 Bloom 占比下降（对照 P2a 的 22.2%）。
# 与 P2a 的 p2_profile.sh 同口径（SYS_PTRACE、不加 --subprocesses、不加 --nonblocking），
# 只是把 loadgen 换成 P2a 快速模式（--fast），并用 p2b_pyspy_cat.py 分类汇总。
#
# 用法（WSL 宿主，需要 py-spy 的 root，密码从 P2_PASSWORD 管道传入）：
#   P2_PASSWORD=<wsl sudo 密码> bash p2b_pyspy.sh <image> <rate>
#   例：P2_PASSWORD=xxx bash p2b_pyspy.sh flowlens:p2b-after 120000
# 结果：~/p2prof/<image tag>-<rate>/raw_t.txt 与 cat 汇总行。
set -u
IMG="${1:?image 必填}"; RATE="${2:?rate 必填}"
WARMUP=${WARMUP:-30}; DUR=${DUR:-30}; TAIL=${TAIL:-10}
VENV="$HOME/.venv-p2"
LABEL="$(echo "$IMG" | tr '/:' '__')"
OUTDIR="$HOME/p2prof/p2b_$LABEL_$RATE"
SVC_CP=${SVC_CP:-0,2,4,6}; LOAD_CP=${LOAD_CP:-8-15}; LOAD_WORKERS=${LOAD_WORKERS:-8}
BATCH=5000
MEM=${MEM:-512m}
mkdir -p "$OUTDIR"
NAME="p2b-pyspy-$$"
PORT=$((19100 + RANDOM % 900))
URL="http://127.0.0.1:$PORT"
cd /mnt/d/dhy/code/flowlens-c

docker rm -f "$NAME" >/dev/null 2>&1
docker run -d --name "$NAME" --cap-add SYS_PTRACE \
  --memory="$MEM" --memory-swap="$MEM" --cpuset-cpus="$SVC_CP" \
  -e PYTHONHASHSEED=0 -e FLOWLENS_PARSE_WORKERS=3 \
  -e FLOWLENS_MAX_PENDING_BYTES=16777216 -e FLOWLENS_RAW_BUDGET_BYTES=16777216 \
  -p "$PORT:8080" "$IMG" >/dev/null
ready=0
for i in $(seq 1 40); do curl -sf "$URL/healthz" >/dev/null 2>&1 && { ready=1; break; }; sleep 0.5; done
[ "$ready" = 1 ] || { docker logs "$NAME" | tail -30; docker rm -f "$NAME"; echo "SETUP_FAIL"; exit 1; }
MPID=$(docker inspect -f '{{.State.Pid}}' "$NAME")
echo "[$LABEL/$RATE] main_pid=$MPID port=$PORT"

nohup taskset -c "$LOAD_CP" /home/user/flowlens-venv/bin/python tools/loadgen.py \
  --url "$URL" --rate "$RATE" --mode 长尾 --workers "$LOAD_WORKERS" --conc 8 \
  --batch "$BATCH" --duration $((WARMUP + DUR + TAIL)) --fast --seed 42 \
  > "$OUTDIR/loadgen.log" 2>&1 &
LGPID=$!
sleep "$WARMUP"
CLKTCK=$(getconf CLK_TCK)
curl -sf "$URL/healthz" > "$OUTDIR/h1.json"
read -r U1 S1 _ < <(sed 's/.*) //' "/proc/$MPID/stat" | awk '{print $12, $13}')
echo "$LABEL $RATE $U1 $S1 $CLKTCK" > "$OUTDIR/window.txt"
echo "$P2_PASSWORD" | sudo -S -p "" "$VENV/bin/py-spy" record -t --pid "$MPID" \
  --duration "$DUR" --rate 200 --format raw -o "$OUTDIR/raw_t.txt" 2> "$OUTDIR/pyspy.err"
wait "$LGPID" || true
tail -1 "$OUTDIR/loadgen.log" > "$OUTDIR/loadgen_summary.json"
/home/user/flowlens-venv/bin/python tools/exp/p2b_pyspy_cat.py "$OUTDIR/raw_t.txt" > "$OUTDIR/cat.txt" 2>&1
echo "[$LABEL/$RATE] 分类汇总："
cat "$OUTDIR/cat.txt"
docker rm -f "$NAME" >/dev/null 2>&1
echo "[$LABEL/$RATE] DONE -> $OUTDIR"
