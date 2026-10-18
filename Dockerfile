# syntax=docker/dockerfile:1
# 基础镜像保持 python:3.12-slim（numpy / orjson / aiohttp 都有现成的 manylinux wheel，
# 无需编译工具链，也不引入 builder 阶段）。tests、docs、tools 由 .dockerignore 排除，
# 不进镜像。
#
# 用 `pip install -c constraints.txt .` 安装：pyproject.toml 的包列表已改为自动发现
# （include = ["flowlens*"]），会装出 flowlens 及其全部子包（api、sketch）。装完删除
# /app 下的源码，运行时从 site-packages import，与 CWD 无关（真正"不依赖源码目录"，
# 见 B1b 第六节的验证）。constraints.txt 把运行时依赖固定到评测测试过的确切版本。
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONHASHSEED=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
# 只拷构建需要的文件（.dockerignore 已排除 tests/docs/tools）。
COPY pyproject.toml constraints.txt ./
COPY flowlens ./flowlens
# PIP_INDEX_URL 默认为空；构建方可用 --build-arg PIP_INDEX_URL=... 指定包源（不写死
# 任何国内源或代理地址，保持镜像可移植）。有值时追加 --index-url 传给 pip。装完删源码。
ARG PIP_INDEX_URL
RUN case "$PIP_INDEX_URL" in \
        "") pip install --no-cache-dir -c constraints.txt . ;; \
        *)  pip install --no-cache-dir --index-url "$PIP_INDEX_URL" -c constraints.txt . ;; \
    esac \
    && rm -rf /app/flowlens /app/pyproject.toml
# 以非 root 用户运行
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser
EXPOSE 8080
# 镜像里没有 curl：用 Python 标准库请求 /healthz 做健康检查
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys,urllib.request; r=urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=2); sys.exit(0 if r.status==200 else 1)"
# PID 1 直接是服务进程。__main__.py 已为 SIGTERM/SIGINT 注册处理器（优雅清理后退出码 0），
# docker stop 的 SIGTERM 能干净退出，不再需要信号转发 init。
CMD ["python", "-m", "flowlens", "--port", "8080"]
