# syntax=docker/dockerfile:1
# 基础镜像保持 python:3.12-slim（numpy / orjson / aiohttp 都有现成的 manylinux wheel，
# 无需编译工具链，也不引入 builder 阶段）。tests、docs、tools 由 .dockerignore 排除，
# 不进镜像。
#
# 说明：这里不用 `pip install .`，而是单独装三个依赖、再把 flowlens/ 源码直接拷进镜像。
# 原因是 pyproject.toml 的 `packages` 只列了 flowlens 与 flowlens.sketch，漏了
# flowlens.api（见 docs/lanes/B1-notes.md），`pip install .` 会缺掉 api 子包导致容器
# 起不来。直接从源码目录跑可以绕开这个打包缺陷（待主线修 pyproject.toml 后可换回
# `pip install .`）。
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONHASHSEED=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
# 三个运行时依赖（与 pyproject.toml 的 dependencies 一致）
RUN pip install --no-cache-dir numpy orjson aiohttp
# 拷源码：python -m flowlens 在 CWD=/app 下把 /app 放进 sys.path，直接 import flowlens
COPY flowlens ./flowlens
# 生成启动/信号转发脚本 entrypoint.py（base64 嵌入，兼容经典构建器；用 heredoc 在
# 非 BuildKit 下会得到 0 字节文件）。脚本内容见 docs/lanes/B1-notes.md 的"AI 使用记录"。
RUN echo "aW1wb3J0IG9zCmltcG9ydCBzaWduYWwKaW1wb3J0IHN1YnByb2Nlc3MKaW1wb3J0IHN5cwoKCmRlZiBtYWluKCkgLT4gaW50OgogICAgaG9zdCA9IG9zLmVudmlyb24uZ2V0KCJGTE9XTEVOU19IT1NUIiwgIjAuMC4wLjAiKQogICAgcG9ydCA9IG9zLmVudmlyb24uZ2V0KCJGTE9XTEVOU19QT1JUIiwgIjgwODAiKQogICAgIyDnlKggLW0gZmxvd2xlbnMg6LW355yf5a6e5pyN5Yqh5L2c5Li65a2Q6L+b56iL77yb5pys6L+b56iL77yIUElEIDHvvInotJ/otKPmiorlrrnlmajkv6Hlj7fovaznu5nlroPjgIIKICAgIHNydiA9IHN1YnByb2Nlc3MuUG9wZW4oW3N5cy5leGVjdXRhYmxlLCAiLW0iLCAiZmxvd2xlbnMiLCAiLS1ob3N0IiwgaG9zdCwgIi0tcG9ydCIsIHBvcnRdKQoKICAgIGRlZiBmb3J3YXJkKHNpZ251bSwgZnJhbWUpOgogICAgICAgICMgZG9ja2VyIHN0b3Ag5Y+RIFNJR1RFUk3vvIzogIwgX19tYWluX18ucHkg55qE5LyY6ZuF5YWz6Zet77yIcnVubmVyLmNsZWFudXDvvIwKICAgICAgICAjIOeri+WNs+e7k+adn+aJgOaciea1ge+8iei1sCBLZXlib2FyZEludGVycnVwdO+8jOWNsyBTSUdJTlTjgIJfX21haW5fXy5weSDkuI3lnKjmnKzmlK/nur8KICAgICAgICAjIOaUueWKqOiMg+WbtO+8jOaVheWcqOi/memHjOaKiuWuueWZqOS/oeWPt+i9rOaIkCBTSUdJTlQg5Lqk57uZ5a2Q6L+b56iL77yM6K6p5a6D6LWw5Y6f5pyJ5LyY6ZuF5YWz6ZetCiAgICAgICAgIyDot6/lvoTvvJvlkKbliJkgUElEIDEg6buY6K6k5b+955WlIFNJR1RFUk3vvIxkb2NrZXIgc3RvcCDkvJrnrYnmu6Hlrr3pmZDmnJ/lho0gU0lHS0lMTOOAggogICAgICAgIGlmIHNydi5wb2xsKCkgaXMgTm9uZToKICAgICAgICAgICAgc3J2LnNlbmRfc2lnbmFsKHNpZ25hbC5TSUdJTlQpCgogICAgc2lnbmFsLnNpZ25hbChzaWduYWwuU0lHVEVSTSwgZm9yd2FyZCkKICAgIHNpZ25hbC5zaWduYWwoc2lnbmFsLlNJR0lOVCwgZm9yd2FyZCkKICAgIHJldHVybiBzcnYud2FpdCgpCgoKaWYgX19uYW1lX18gPT0gIl9fbWFpbl9fIjoKICAgIHN5cy5leGl0KG1haW4oKSkK" \
    | base64 -d > /app/entrypoint.py \
    && python -c "import py_compile; py_compile.compile('/app/entrypoint.py', doraise=True)"
# 以非 root 用户运行
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser
EXPOSE 8080
# 镜像里没有 curl：用 Python 标准库请求 /healthz 做健康检查
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys,urllib.request; r=urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=2); sys.exit(0 if r.status==200 else 1)"
# PID 1 = 信号转发 init：把容器的 SIGTERM/SIGINT 转成子进程（真实服务）的 SIGINT，
# 让 __main__.py 的优雅关闭路径（立即结束所有流）在 docker stop 时真正生效。
CMD ["python", "/app/entrypoint.py"]
