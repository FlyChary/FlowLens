#!/usr/bin/env bash
# P2b 四：对修改后的主进程 py-spy 剖析 30 秒（py-spy 在**容器内**以 root 运行，
# 免去 WSL 宿主 sudo 密码）。与 P2a 的 p2_profile.sh 同口径：SYS_PTRACE、
# 不加 --subprocesses、不加 --nonblocking、warmup 30s 后采样 30s。
# 差别：py-spy 二进制从挂载的 WSL venv（/home/user/.venv-p2）读入容器、以 --user root
# 运行，容器内主进程 pid=1。loadgen 仍在 WSL 宿主（taskset 8-15、--fast 快速模式）。
#
# 用法（WSL 宿主）：
#   bash p2b_pyspy_incontainer.sh <image> <rate>
# 结果：~/p2prof/p2b_<img>_<rate>/raw_t.txt + cat 汇总 + 主进程 CPU 窗口。
set -u
IMG="${1:?image 必填}"; RATE="${2:?rate 必填}"
WARMUP=${WARMUP:-30}; DUR=${DUR:-30}; TAIL=${TAIL:-10}
VENV="/home/user/.venv-p2"
LABEL="$(echo "$IMG" | tr '/:' '__')"
OUTDIR="$HOME/p2prof/p2b_${LABEL}_${RATE}"
SVC_CP=${SVC_CP:-0,2,4,6}; LOAD_CP=${LOAD_CP:-8-15}; LOAD_WORKERS=${LOAD_WORKERS:-8}
BATCH=5000; MEM=${MEM:-512m}
mkdir -p "$OUTDIR"
NAME="p2b-pyspy-in-$$"
PORT=$((19100 + RANDOM % 900))
URL="http://127.0.0.1:$PORT"
cd /mnt/d/dhy/code/flowlens-c

docker rm -f "$NAME" >/dev/null 2>&1
docker run -d --name "$NAME" --cap-add SYS_PTRACE \
  -v "$VENV":/venv-ro -v "$OUTDIR":/out \
  --memory="$MEM" --memory-swap="$MEM" --cpuset-cpus="$SVC_CP" \
  -e PYTHONHASHSEED=0 -e FLOWLENS_FRONTENDS=3 \
  -e FLOWLENS_MAX_PENDING_BYTES=16777216 -e FLOWLENS_RAW_BUDGET_BYTES=16777216 \
  -p "$PORT:8080" "$IMG" >/dev/null
ready=0
for i in $(seq 1 40); do curl -sf "$URL/healthz" >/dev/null 2>&1 && { ready=1; break; }; sleep 0.5; done
[ "$ready" = 1 ] || { docker logs "$NAME" | tail -30; docker rm -f "$NAME"; echo "SETUP_FAIL"; exit 1; }
# 容器内主进程 pid（恒为 1，py-spy 在容器内以 root 采）
echo "[$LABEL/$RATE] 容器内主进程 pid=1 port=$PORT"

nohup taskset -c "$LOAD_CP" /home/user/flowlens-venv/bin/python tools/loadgen.py \
  --url "$URL" --rate "$RATE" --mode 长尾 --workers "$LOAD_WORKERS" --conc 8 \
  --batch "$BATCH" --duration $((WARMUP + DUR + TAIL)) --fast --seed 42 \
  > "$OUTDIR/loadgen.log" 2>&1 &
LGPID=$!
sleep "$WARMUP"
# 窗口起：主进程 CPU（宿主 /proc，容器主进程宿主 PID）
HPID=$(docker inspect -f '{{.State.Pid}}' "$NAME")
CLKTCK=$(getconf CLK_TCK)
read -r U1 S1 _ < <(sed 's/.*) //' "/proc/$HPID/stat" | awk '{print $12, $13}')
curl -sf "$URL/healthz" > "$OUTDIR/h1.json"
echo "$U1 $S1 $CLKTCK" > "$OUTDIR/window.txt"
# py-spy 在容器内以 root 采样主进程（pid 1），写 /out/raw_t.txt（=宿主 $OUTDIR/raw_t.txt）
docker exec --user root "$NAME" /venv-ro/bin/py-spy record -t --pid 1 \
  --duration "$DUR" --rate 200 --format raw -o /out/raw_t.txt 2> "$OUTDIR/pyspy.err"
read -r U2 S2 _ < <(sed 's/.*) //' "/proc/$HPID/stat" | awk '{print $12, $13}')
curl -sf "$URL/healthz" > "$OUTDIR/h2.json"
echo "$U2 $S2 $CLKTCK" >> "$OUTDIR/window.txt"
wait "$LGPID" || true
tail -1 "$OUTDIR/loadgen.log" > "$OUTDIR/loadgen_summary.json"
/home/user/flowlens-venv/bin/python tools/exp/p2b_pyspy_cat.py "$OUTDIR/raw_t.txt" > "$OUTDIR/cat.txt" 2>&1
# 主进程 CPU 窗口（窗口.txt 两行差 ÷ CLK_TCK ÷ 窗口时长）
/home/user/flowlens-venv/bin/python - "$OUTDIR" <<'PY'
import sys, json, pathlib
o = pathlib.Path(sys.argv[1])
u1, s1, ct = (o / "window.txt").read_text().splitlines()[0].split()
u2, s2, _ = (o / "window.txt").read_text().splitlines()[1].split()
cpu = ((int(u2) - int(u1)) + (int(s2) - int(s1))) / float(ct)
h1 = json.loads((o / "h1.json").read_text()); h2 = json.loads((o / "h2.json").read_text())
acc = int(h2.get("events_ingested", 0)) - int(h1.get("events_ingested", 0))
wall = 30.0  # 采样窗口
print(f"主进程窗口：CPU {cpu:.2f}s / {wall}s = {cpu/wall*100:.0f}%  接受 {acc} 条 = {acc/wall/1000:.0f}k/s")
if acc: print(f"主进程每条 = {cpu/acc*1e6:.2f} μs（采样期含 py-spy 扰动，偏高）")
PY
echo "[$LABEL/$RATE] 分类汇总："
cat "$OUTDIR/cat.txt"
docker rm -f "$NAME" >/dev/null 2>&1
echo "[$LABEL/$RATE] DONE -> $OUTDIR"
