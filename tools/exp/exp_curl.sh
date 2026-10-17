#!/usr/bin/env bash
# curl 客户端实验：POST 5 MB 请求体到 /v1/ingest，测 413 稳定性 / 耗时 / 连接是否被重置。
# 用法: exp_curl.sh <big_body_file> <mode>     mode: expect | noexpect
#   expect   -> curl 对 >1KB 的 body 默认发 "Expect: 100-continue"
#   noexpect -> 加 -H "Expect:" 把该头去掉，直接发送请求体
# 每次试验用全新 curl 进程（新 TCP 连接），模拟真实客户端。
# 输出：每行 TRIAL <i> <http_code> <time_total_s> <curl_exit> <follow_code>
#   http_code=000 表示没拿到 HTTP 响应（多半是连接被重置）
#   curl_exit 非 0 时 56=接收数据失败(RST)、55=100-continue 相关、28=超时
#   follow_code 是被拒后换新连接发正常写入请求得到的状态码（期望 202）
# 末尾一行 SUMMARY <n413> <n_bad> <follow_ok> <median_ms> <max_ms>
set -u
BIG="$1"; MODE="$2"
URL="http://127.0.0.1:8080/v1/ingest"
N=20
NOW_MS=$(( $(date +%s) * 1000 ))
NORMAL=$(mktemp)
cat > "$NORMAL" <<EOF
[{"event_id":"n0","user_id":"u","url":"/n","ts":$((NOW_MS-1000)),"bytes":100},
 {"event_id":"n1","user_id":"u","url":"/n","ts":$((NOW_MS-900)),"bytes":100},
 {"event_id":"n2","user_id":"u","url":"/n","ts":$((NOW_MS-800)),"bytes":100},
 {"event_id":"n3","user_id":"u","url":"/n","ts":$((NOW_MS-700)),"bytes":100},
 {"event_id":"n4","user_id":"u","url":"/n","ts":$((NOW_MS-600)),"bytes":100}]
EOF
# --noproxy '*'：WSL 有系统级 HTTP 代理(127.0.0.1:7890)，必须绕过，
# 否则 curl 走代理、测到的不是服务端真实 TCP 行为（代理会在大 body 转发时偶发 502）
export NO_PROXY=localhost,127.0.0.1
EXTRA=(--noproxy "*")
[ "$MODE" = "noexpect" ] && EXTRA+=( -H "Expect:")

n413=0; nbad=0; f_ok=0
TIMES_FILE=$(mktemp)   # 只记录拿到 413 的耗时（连接被重置的没有正常响应）
for i in $(seq 0 $((N-1))); do
  out=$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 10 \
          "${EXTRA[@]}" --data-binary "@$BIG" "$URL" 2>/dev/null)
  exitc=$?
  code=${out%% *}; t=${out##* }
  # 被拒后换新连接发正常请求
  fcode=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
            --data-binary "@$NORMAL" "$URL" 2>/dev/null)
  echo "TRIAL $i $code $t $exitc $fcode"
  if [ "$code" = "413" ]; then n413=$((n413+1)); echo "$t" >> "$TIMES_FILE"; else nbad=$((nbad+1)); fi
  if [ "$fcode" = "202" ]; then f_ok=$((f_ok+1)); fi
done
rm -f "$NORMAL"
# 统计 413 耗时（毫秒）：中位数、最大值
stats=$(awk '{a[NR]=$1*1000} END{
  if (NR==0) {print "0 0"; exit}
  for (i=1;i<=NR;i++) for (j=i+1;j<=NR;j++) if (a[j]<a[i]) {x=a[i];a[i]=a[j];a[j]=x}
  if (NR%2) m=a[(NR+1)/2]; else m=(a[NR/2]+a[NR/2+1])/2
  printf "%.0f %.0f", m, a[NR] }' "$TIMES_FILE")
rm -f "$TIMES_FILE"
echo "SUMMARY $n413 $nbad $f_ok $stats"
