# syntax=docker/dockerfile:1
# 基础镜像保持 python:3.12-slim（numpy / orjson / aiohttp 都有现成的 manylinux wheel，
# 无需编译工具链，也不引入 builder 阶段）。镜像的 Python 版本（3.12）是运行环境的
# 唯一标准：constraints.txt 在 3.12 环境中生成、测试在 test 阶段（同为 3.12）执行。
#
# 结构（阶段顺序即默认目标，app 放最后 → `docker build .` 默认建正式服务镜像）：
#   base  —— 装运行时依赖（constraints 固定版本），把 flowlens 装进 site-packages
#   test  —— 测试镜像：与 base 同依赖，额外装 pytest、复制 tests 与 tools、跑 pytest -q
#            （`docker build --target test .` 执行；构建成功即全量快测试在 3.12 通过）
#   app   —— 正式镜像（**默认目标**，放最后）：非 root 运行、健康检查、启动服务
# 正式镜像（默认 app 目标）的内容与以前完全一致；test 阶段只用于收尾验证。
#
# 用 `pip install -c constraints.txt .` 安装：pyproject.toml 的包列表已改为自动发现
# （include = ["flowlens*"]），会装出 flowlens 及其全部子包（api、sketch）。装完删除
# /app 下的源码，运行时从 site-packages import，与 CWD 无关（真正"不依赖源码目录"，
# 见 B1b 第六节的验证）。constraints.txt 把运行时依赖固定到评测测试过的确切版本。

FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONHASHSEED=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MALLOC_MMAP_THRESHOLD_=131072 \
    MALLOC_TRIM_THRESHOLD_=131072
# 固定 glibc 的"大块"门槛与 trim 触发门槛为 128 KB（不随每次大块归还自动上调）。
# 依据（C3 g5 对照实验，docs/lanes/C3-report.md §二-B）：默认门槛下，几轮归还后
# ~650 KB 的请求体改从普通堆分配、用完变成 glibc 保留的碎片，满载工作集峰值
# 236.8 MiB、中后段贴第 3/4 级、4.6% 请求被拒；固定 128 KB 后这类大块走 mmap、
# free 即还 OS，峰值 179.1 MiB（−57.7，−24%）、32 连接满载 5 分钟全程第 0 级、
# 接受率 100%。
# PIP_INDEX_URL 默认为空；构建方可用 --build-arg PIP_INDEX_URL=... 指定包源（不写死
# 任何国内源或代理地址，保持镜像可移植）。有值时追加 --index-url 传给 pip。
ARG PIP_INDEX_URL
# 只拷构建需要的文件（.dockerignore 已排除 docs；tests/tools 只给 test 阶段用，
# base 阶段不拷）。装完删源码。
COPY pyproject.toml constraints.txt ./
COPY flowlens ./flowlens
RUN case "$PIP_INDEX_URL" in \
        "") pip install --no-cache-dir -c constraints.txt . ;; \
        *)  pip install --no-cache-dir --index-url "$PIP_INDEX_URL" -c constraints.txt . ;; \
    esac \
    && rm -rf /app/flowlens /app/pyproject.toml

# ---- 测试镜像：与 base 同基础与依赖，额外装 pytest、复制测试与工具、跑全量快测试 ----
# `docker build --target test .` 执行；构建成功即全量快测试在 Python 3.12 通过。
FROM base AS test
ARG PIP_INDEX_URL
# pytest 不在 constraints.txt（constraints 只固定运行时依赖），单独装
RUN case "$PIP_INDEX_URL" in \
        "") pip install --no-cache-dir "pytest>=8" ;; \
        *)  pip install --no-cache-dir --index-url "$PIP_INDEX_URL" "pytest>=8" ;; \
    esac
# tests 与 tools 进测试镜像（.dockerignore 已放行）；pyproject.toml 提供 pytest 配置
# （testpaths/addopts/markers）。tools/ 不在包路径里，测试自己把 tools/ 加进 sys.path。
COPY pyproject.toml ./
COPY tests ./tests
COPY tools ./tools
# 运行全部快测试（pyproject 的 addopts 已跳过 slow）
RUN pytest -q

# ---- 正式镜像（默认目标，放最后：docker build . 默认建它）----
FROM base AS app
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
