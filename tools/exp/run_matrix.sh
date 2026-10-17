#!/usr/bin/env bash
# 在 WSL(Linux) 里跑对比矩阵：对每个 lingering_time 值(0/1/10)，启动服务端，
# 用三种客户端各测 20 次，汇总 413 稳定性 / 连接被重置数 / 耗时 / 拒绝后新连接可用性。
# 用法: run_matrix.sh
# 依赖: ~/flowlens-venv 已装 numpy/orjson/aiohttp；big5mb.bin 已生成。
set -u
cd /mnt/d/dhy/code/flowlens
# WSL 有系统级 HTTP 代理(127.0.0.1:7890)：清掉，并显式让 localhost/127.0.0.1 不走代理，
# 保证客户端直连服务端、测到的是真实 TCP 行为（代理会在大 body 转发时偶发 502，污染结果）
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY no_proxy NO_PROXY
export NO_PROXY=localhost,127.0.0.1
source ~/flowlens-venv/bin/activate
BIG=tools/exp/big5mb.bin
OUT=tools/exp/results
mkdir -p "$OUT"
PORT=8080
# 大请求体（5 MB）在运行时临时生成，不再入库（git rm --cached 后已加 .gitignore）
if [ ! -s "$BIG" ]; then
  python - "$BIG" <<'PY'
import sys
path = sys.argv[1]
with open(path, "wb") as f:
    # 5 MB，内容无所谓（服务端在发体前就 413/503，从不解析）
    f.write(b"x" * (5 * 1024 * 1024))
PY
fi

SRV_PID=""
kill_server() { [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null; SRV_PID=""; sleep 1; }
wait_port() {
  for _ in $(seq 1 60); do
    if curl -s -o /dev/null "http://127.0.0.1:$PORT/healthz"; then return 0; fi
    sleep 0.2
  done
  echo "SERVER_NOT_UP"; return 1
}
trap kill_server EXIT

for LT in 0 1 10; do
  echo "================ LINGERING_TIME=$LT ================"
  kill_server
  # 启动服务端（PYTHONHASHSEED=0，覆盖 lingering）
  FLOWLENS_LINGERING_TIME=$LT PYTHONHASHSEED=0 \
    nohup python -m flowlens > "$OUT/server_lt$LT.log" 2>&1 &
  SRV_PID=$!
  if ! wait_port; then echo "lt=$LT server 没起来"; tail "$OUT/server_lt$LT.log"; continue; fi

  # case A: curl 默认（发 Expect: 100-continue）
  bash tools/exp/exp_curl.sh "$BIG" expect  > "$OUT/lt${LT}_curl_expect.txt"
  # case B: curl 禁用 Expect
  bash tools/exp/exp_curl.sh "$BIG" noexpect > "$OUT/lt${LT}_curl_noexpect.txt"
  # case C: aiohttp 客户端（expect100=True）
  python tools/exp/exp_client.py "$BIG" 1     > "$OUT/lt${LT}_aiohttp_expect100.txt"
  # case C2: aiohttp 客户端（expect100=False，直接发体）
  python tools/exp/exp_client.py "$BIG" 0     > "$OUT/lt${LT}_aiohttp_plain.txt"

  kill_server
done
echo "ALL DONE"
ls -la "$OUT"
