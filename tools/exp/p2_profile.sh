#!/usr/bin/env bash
# P2a 主进程剖析驱动（WSL 宿主机运行）。
#
# 流程：起容器（--cap-add SYS_PTRACE）→ 等服务就绪 → 起 loadgen（长尾，rate 持续
# WARMUP+DUR+TAIL 秒）→ 预热 WARMUP 秒后 py-spy 记录主进程 DUR 秒（raw 格式）→
# 同时采集主进程 /proc CPU 时间与线程名 → 打汇总行。只剖析主进程，不加 --subprocesses。
#
# 用法：P2_PASSWORD=<wsl sudo 密码> bash p2_profile.sh <label> <image> <mem> <rate> <env...>
#   env... 是传给 docker run -e 的键值（如 FLOWLENS_FRONTENDS=3），可多个。
#   py-spy 需要 root（SYS_PTRACE），sudo 密码从 P2_PASSWORD 管道传入（-S）。
set -u
LABEL="$1"; IMG="$2"; MEM="$3"; RATE="$4"; shift 4
WARMUP=${WARMUP:-30}; DUR=${DUR:-30}; TAIL=${TAIL:-10}
VENV="$HOME/.venv-p2"
OUTDIR="${OUTDIR:-$HOME/p2prof/$LABEL}"
SVC_CP=${SVC_CP:-0,2,4,6}; LOAD_CP=${LOAD_CP:-8-15}; LOAD_WORKERS=${LOAD_WORKERS:-8}
BATCH=${BATCH:-5000}
mkdir -p "$OUTDIR"
NAME="p2prof-$LABEL-$$"
PORT=$((20000 + RANDOM % 10000))
URL="http://127.0.0.1:$PORT"

docker rm -f "$NAME" >/dev/null 2>&1
docker run --rm -d --name "$NAME" --cap-add SYS_PTRACE \
  --memory="$MEM" --memory-swap="$MEM" --cpuset-cpus="$SVC_CP" \
  "$@" -p "$PORT:8080" "$IMG" >/dev/null

ready=0
for i in $(seq 1 40); do
  if curl -sf "$URL/healthz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { docker logs "$NAME" | tail -30; docker rm -f "$NAME"; echo "[$LABEL] SETUP_FAIL"; exit 1; }
MPID=$(docker inspect -f '{{.State.Pid}}' "$NAME")
NPARSE=$(pgrep -P "$MPID" | grep -vc resource_tracker || true)
echo "[$LABEL] setup main_pid=$MPID parse_procs=$NPARSE port=$PORT"

# 负载：长尾，rate 条/秒，持续全程；taskset 固定到压测核（与 P1b s1_measure 同口径，
# 不固定时压测 worker 会挤占服务核 0,2,4,6 把服务饿死）
cd /mnt/d/dhy/code/flowlens-c
nohup taskset -c "$LOAD_CP" "$VENV/bin/python" tools/loadgen.py --url "$URL" \
  --rate "$RATE" --mode 长尾 \
  --workers "$LOAD_WORKERS" --conc 8 --batch "$BATCH" \
  --duration $((WARMUP + DUR + TAIL)) --seed 42 \
  > "$OUTDIR/loadgen.log" 2>&1 &
LGPID=$!

sleep "$WARMUP"

# 采样窗口：H0/U0/S0 与 H1/U1/S1 必须都夹住 py-spy 的 30 秒采样段，
# 让"接受条数、主进程 CPU 秒、py-spy 样本"三者同窗口（否则 us/ev 被稀释）。
CLKTCK=$(getconf CLK_TCK)
H0=$(curl -sf "$URL/healthz"); echo "$H0" > "$OUTDIR/h0.json"
# /proc/<pid>/stat：整行 1 基 $14=utime $15=stime；sed 去掉 "pid (comm)" 后
# 各字段前移两位 → utime=$12、stime=$13（与 s1_measure.py 的 f[11]/f[12] 同口径）
read -r U0 S0 _ < <(sed 's/.*) //' "/proc/$MPID/stat" | awk '{print $12, $13}')
U0=${U0:-0}; S0=${S0:-0}
mkdir -p "$OUTDIR/threads"
for t in /proc/$MPID/task/*; do
  tid=$(basename "$t")
  printf '%s %s\n' "$tid" "$(cat "$t/comm" 2>/dev/null)" >> "$OUTDIR/threads/threads.txt"
done
# 不加 --nonblocking：冒烟实测 nonblocking 模式采样丢失 ~85%（4 秒只采到 125/800），
# ptrace 短暂暂停（每 5 ms 约 1~4 ms）对 30 秒窗口的类别占比影响可忽略
echo "$P2_PASSWORD" | sudo -S -p "" "$VENV/bin/py-spy" record -t --pid "$MPID" \
  --duration "$DUR" --rate 200 --format raw \
  -o "$OUTDIR/raw_t.txt" 2> "$OUTDIR/pyspy.err"
read -r U1 S1 _ < <(sed 's/.*) //' "/proc/$MPID/stat" | awk '{print $12, $13}')
H1=$(curl -sf "$URL/healthz"); echo "$H1" > "$OUTDIR/h1.json"
echo "$LABEL" "$RATE" "$U0" "$S0" "$U1" "$S1" "$CLKTCK" > "$OUTDIR/window.txt"

wait "$LGPID" || true
tail -1 "$OUTDIR/loadgen.log" > "$OUTDIR/loadgen_summary.json"

# 汇总行：接受/去重/迟到/非法（healthz 差值）+ 主进程 CPU 秒 + py-spy 采样数
python3 - "$OUTDIR" <<'PYEOF'
import json, sys
d = sys.argv[1]
h0 = json.load(open(d + "/h0.json")); h1 = json.load(open(d + "/h1.json"))
ev = h1.get("events_ingested", 0) - h0.get("events_ingested", 0)
cpu_s = None
try:
    label, rate, u0, s0, u1, s1, clkt = open(d + "/window.txt").read().split()
    cpu_s = ((int(u1) - int(u0)) + (int(s1) - int(s0))) / int(clkt)
except Exception:
    pass
n_samples = 0
for _line in open(d + "/raw_t.txt", encoding="utf-8"):
    if _line.startswith("thread"):
        try:
            n_samples += int(_line.rsplit(" ", 1)[1])
        except (IndexError, ValueError):
            pass
us_ev = cpu_s / ev * 1e6 if (cpu_s and ev) else None
lg = {}
try:
    lg = json.load(open(d + "/loadgen_summary.json"))
except Exception:
    pass
print(f"[{label}] acc={ev} dedup={h1.get('deduped',0)-h0.get('deduped',0)} "
      f"late={h1.get('late',0)-h0.get('late',0)} invalid={h1.get('invalid',0)-h0.get('invalid',0)} "
      f"main_cpu_s={cpu_s and round(cpu_s,2)} us/ev={us_ev and round(us_ev,2)} "
      f"samples={n_samples} lg_acc={lg.get('accepted')} lg_cpu={lg.get('client_cpu_pct')}% "
      f"ws_peak={round(h1.get('memory_bytes',0)/1048576,1)}MiB")
PYEOF
docker rm -f "$NAME" >/dev/null 2>&1
echo "[$LABEL] DONE -> $OUTDIR"
