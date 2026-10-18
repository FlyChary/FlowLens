#!/usr/bin/env bash
# 在没有 Docker 的 Linux 上启动 FlowLens：创建虚拟环境、安装依赖、固定
# PYTHONHASHSEED=0 并启动服务（监听 0.0.0.0:8080）。
#
# 用法：
#   ./run.sh                    # 首次会自动建 venv 并安装依赖
#   FLOWLENS_PORT=9000 ./run.sh # 换端口
#   FLOWLENS_DEBUG=1 ./run.sh   # 启用调试接口 GET /debug/structure
set -euo pipefail

cd "$(dirname "$0")"

PORT="${FLOWLENS_PORT:-8080}"
HOST="${FLOWLENS_HOST:-0.0.0.0}"
VENV_DIR="${FLOWLENS_VENV:-.venv}"

# 选择 python3
PYTHON="${PYTHON:-python3}"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "找不到 $PYTHON；请安装 Python 3.11+ 或用 PYTHON=python3.x 指定" >&2
    exit 1
fi

# 固定哈希种子：字符串哈希依赖它，重启前后一致（CLAUDE.md 规则 5）
export PYTHONHASHSEED=0

# 创建虚拟环境并安装（已存在且依赖齐了就跳过安装）
if [ ! -x "$VENV_DIR/bin/python" ]; then
    echo "创建虚拟环境 $VENV_DIR ..."
    "$PYTHON" -m venv "$VENV_DIR"
fi

# 依赖变化或从未装过时安装。这里简单判断 marker 文件，缺失则安装。
MARKER="$VENV_DIR/.flowlens-deps"
if [ ! -f "$MARKER" ] || [ "pyproject.toml" -nt "$MARKER" ]; then
    echo "安装依赖（numpy、orjson、aiohttp）..."
    "$VENV_DIR/bin/pip" install --upgrade pip >/dev/null
    "$VENV_DIR/bin/pip" install .
    : > "$MARKER"
fi

echo "启动 FlowLens：http://$HOST:$PORT/healthz（PYTHONHASHSEED=$PYTHONHASHSEED）"
exec "$VENV_DIR/bin/python" -m flowlens --host "$HOST" --port "$PORT"
