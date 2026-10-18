# B1（支线）交接记录

本支线在独立工作目录 `D:\dhy\code\flowlens-b`（分支 `lane-b`）完成，只动了
`Dockerfile`、`.dockerignore`、`run.sh`、`flowlens/api/debug.py`（新建）、
`flowlens/api/app.py`（仅加调试路由注册）、`tests/e2e/test_debug.py`（新建）和本文档。
合并主线后再统一并入 PROGRESS / AI_LOG / DESIGN_TODO / CLAUDE。

**进度：B1 全部完成。** 两部分（Docker、调试接口）均实现并在 WSL 实测验证；
Windows 与 WSL 两边全量 `pytest -q` 均通过。

## 已定的决策

1. **调试接口只读、现读现算**：`GET /debug/structure` 的返回完全由只读数据现场组装，
   不修改任何统计状态，也不缓存。单进程单写者模型下，一个 handler 协程里连续读
   （不让出事件循环）得到的就是一致快照。
2. **时间划分必须复用 `engine._plan`**：带 `at` 与 `window` 时，返回的拆分直接调
   `engine._plan(d, split_range(from, to))`（真实查询唯一的划分入口），环内整秒、
   线段树节点（`engine._cover`）、边缘秒都从 `_Plan` 结构读出，不另写划分逻辑，
   保证页面展示与真实查询一致。
3. **事件数统一取 url 维叶子的 DDSketch 总计数（`dd.n`）**：两维处理同一批事件，
   某秒事件数在两维相同；而只有 url 维概要里存了计数（user 维是 HLL，不存 `n`）。
   所以无论 `by` 取哪维，"活跃环某秒事件数""节点事件数""叶子事件数"都从 url 维读。
   节点事件数：带统计概要的节点（level 0 或 level ≥ stats_min_level）直接 `dd.n`，
   其他节点按覆盖范围内叶子 `dd.n` 求和——与指令一致。
4. **`FLOWLENS_DEBUG=1` 门控**：`create_app` 里 `if debug.is_enabled()` 才 `register`，
   未设置时 `/debug/structure` 走正常 404（与其他不存在路径一致）。判断读环境变量
   在 `create_app` 时一次即可（进程生命周期内不变）。
5. **秒桶"前 3 个 key"**：用 `TopK.top(n)` 的公开接口取（次数降序、同次数按 key
   字典序），不读底层堆的私有字段；名单容量取公开配置 `cfg.heap_cap`。
6. **满载耗时测量用慢测试**：`tests/e2e/test_debug.py::test_debug_perf_full_tree`
   标 `@pytest.mark.slow`（默认 `pytest -q` 跳过），逐秒写入并定稿出完整 10 分钟
   历史（660 秒、两维名单写满 512），测 `build_structure`+序列化的中位耗时与
   响应体大小。不放进日常快测试，避免拖慢 CI。
7. **Docker 用单阶段 + 直接拷源码，不用 `pip install .`**：见"未解决的问题"#1
   （pyproject 的 packages 漏了 flowlens.api，`pip install .` 会缺 api 子包）。
   单独装 numpy/orjson/aiohttp 三个 wheel，再 `COPY flowlens ./flowlens`，
   `python -m flowlens` 在 `CWD=/app` 下直接 import。基础镜像保持 `python:3.12-slim`。
   （**B1b 已更新**：`packages` 漏包改自动发现修复（#15），Dockerfile 回到
   `pip install -c constraints.txt .`、装完删源码，见 #16。）
8. **容器优雅停机靠镜像内的信号转发 init**：`CMD ["python", "/app/entrypoint.py"]`。
   `entrypoint.py` 起 `python -m flowlens` 为子进程，把自己（PID 1）收到的
   SIGTERM/SIGINT 转成子进程的 SIGINT——因为 `__main__.py` 的优雅关闭
   （`runner.cleanup()` → 立即结束所有流）挂在 `KeyboardInterrupt`（SIGINT），
   而 `docker stop` 发的是 SIGTERM。`__main__.py` 不在本支线改动范围，故用 init
   包装在镜像层解决（见下）。（**B1b 已更新**：`__main__.py` 现处理 SIGTERM，
   去掉本 init，CMD 改回 `python -m flowlens`，见 #14。）
9. **`entrypoint.py` 用 base64 内嵌进 Dockerfile**：本环境的 docker 是经典构建器
   （非 BuildKit），Dockerfile heredoc 会静默生成 0 字节文件；base64 blob +
   `base64 -d` + `py_compile` 校验，兼容经典构建器，且坏 blob 会直接让构建失败。
   脚本明文见"AI 使用记录"。（**B1b 已随 init 一并删除。**）
10. **非 root 运行**：`useradd --uid 10001 appuser`，`USER appuser`。`chown /app`
    给 appuser。
11. **HEALTHCHECK 用 Python 标准库**：镜像无 curl，`urllib.request` 请求
    `/healthz`，200 则健康。
12. **run.sh 幂等**：venv 与依赖装过一次就用 marker 文件跳过重装（`pyproject.toml`
    或 `constraints.txt` 变新才重装）；`FLOWLENS_PORT`/`FLOWLENS_HOST`/`FLOWLENS_VENV`
    可覆盖；固定 `PYTHONHASHSEED=0`；用 `pip install -c constraints.txt .` 安装。

## B1b 追加的决策（B1b 任务）

13. **依赖版本由 `constraints.txt` 固定**：`aiohttp` 不同版本的行为差异已两次影响本
    项目（中间件写法、传输层自动解压），评测方构建必须装到测试过的版本。`constraints.txt`
    写入 WSL 测试环境 `numpy`/`orjson`/`aiohttp` 及其全部间接依赖的确切版本（`pip freeze`
    去掉 flowlens 本身与开发包）；Dockerfile 与 run.sh 都用 `pip install -c constraints.txt .`
    安装；`pyproject.toml` 的依赖保留版本范围、不写死（constraints 负责固定）。
14. **`docker stop` 的退出方式（B1b）**：`__main__.py` 不再 `asyncio.Event().wait()`
    永远等下去，改为等待一个停止事件；在支持的平台用 `loop.add_signal_handler` 为
    SIGTERM 与 SIGINT 注册处理器、收到即置位停止事件，随后照常 `runner.cleanup()` 并
    记录收到的信号（退出原因）。Windows 不支持 `add_signal_handler`，保留 Ctrl+C
    （KeyboardInterrupt）路径、不报错。因此 Docker CMD 直接用 `python -m flowlens`，
    PID 1 就是服务进程，不再需要 B1 的信号转发 init。
15. **`pyproject.toml` 的包列表改自动发现**：`[tool.setuptools.packages.find]` +
    `include = ["flowlens*"]`，不再手工列举，避免再漏子包；`tests/test_packaging.py`
    （slow）用真实 `pip wheel .` 构建后与源码 `.py` 清单比对守护。
16. **Dockerfile 装完删源码**：`pip install .` 后 `rm -rf /app/flowlens /app/pyproject.toml`，
    运行时从 site-packages import、与 CWD 无关，真正"不依赖源码目录"（B1b 第六节从
    /tmp 验证）。非 root 用户、HEALTHCHECK、`.dockerignore` 沿用 B1。
17. **Dockerfile 的 `ARG PIP_INDEX_URL`**：默认为空；有值时传 `--index-url` 给 pip，
    不在镜像里写死任何国内源或代理地址（构建方可用 `--build-arg` 指定，B1b 第六节用
    清华源加速构建）。

## 未解决的问题

1. **~~`pyproject.toml` 的 `packages` 漏了 `flowlens.api`~~**（B1b 已修复）。
   改为自动发现 `[tool.setuptools.packages.find] include = ["flowlens*"]`，`pip install .`
   会装出全部子包（`tests/test_packaging.py` 用真实 `pip wheel .` 构建后比对源码 .py 清单
   守护）。Dockerfile 因此回到 `pip install -c constraints.txt .`。
2. **~~应用不处理 SIGTERM，`docker stop` 会等满宽限期再 SIGKILL~~**（B1b 已修复）。
   `__main__.py` 现在用 `loop.add_signal_handler` 为 SIGTERM/SIGINT 注册处理器、置位
   停止事件后照常 `runner.cleanup()`；Windows 走 Ctrl+C（KeyboardInterrupt）路径。
   Docker CMD 改回 `python -m flowlens`，去掉 B1 的信号转发 init（`tests/e2e/test_signals.py`
   用真实子进程 + SIGTERM 守护）。
3. **容器空闲内存读数偏低/偏高取决于宿主**：WSL 里容器 `memory_bytes` 读到的是
   cgroup `memory.current`（含页缓存），本容器空闲约 88–115 MB。评测以
   `--memory` 是否超限判定，口径正确，无需处理。仅记录。

## 设计文档待改项

1. **`DESIGN.md`（或对应运维章节）补"容器部署与信号处理"**：说明容器 PID 1 是服务
   进程、`__main__.py` 已处理 SIGTERM/SIGINT（B1b 已修，见"已定的决策"#14），
   `docker stop` 干净退出；Dockerfile 的 `CMD` 为 `python -m flowlens`。
2. **`DESIGN.md` 补调试接口契约**：`GET /debug/structure`（`FLOWLENS_DEBUG=1` 门控）
   的字段与语义（活跃环秒桶、线段树各层节点/构建中、`_plan` 拆分），供控制台
   "内部结构"视图对接。
3. **`DESIGN.md` / 打包说明**：`pyproject.toml` 的包列表用 `packages.find`
   （`include = ["flowlens*"]`）自动发现全部子包（B1b 已修，见"已定的决策"#15）；
   运行时依赖由 `constraints.txt` 固定版本（见"已定的决策"#13）。

## AI 使用记录

- **调试接口实现**：`flowlens/api/debug.py` 由 AI 起草。`_plan_summary` 一开始被写成
  "自己判断环/树边界"，被否——指令明确要求复用 `engine._plan`，已改为直接调
  `_plan` 并把 `_Plan` 的 `ring_buckets`/`history`/`edges` 转出；树节点列表用
  `engine._cover`（与 `collect_parts` 同一入口）取。
- **"前 3 个 key"取法**：初版直接读底层 `IndexedMinHeap` 的 `q.prios`/`q.vals` 私有
  字段做 `heapq.nlargest`；被改为走 `TopK.top(n)` 公开接口（`heavy.py`/`topk.py` 已
  冻结，能走公开接口就不碰私有堆字段），容量也改取 `cfg.heap_cap`。
- **Docker 多阶段 → 单阶段**：初版写了 builder/runtime 多阶段（`pip install
  --target` + `PYTHONPATH`）；实测 `pip install .` 因 pyproject 漏 api 而装出坏包，
  遂改为"单阶段装三个依赖 + 拷源码"。指令说"多阶段**或其他方式**减小体积"，
  单阶段直接装 wheel 已把 tests/docs/tools 排除，体积 292 MB（含 numpy），可接受。
- **优雅停机**：AI 定位到"PID 1 忽略 SIGTERM"（用"纯 sleep 当 PID 1"做对照实验证明
  与业务无关），方案是镜像内加信号转发 init。`__main__.py` 不能改，故 init 放
  Dockerfile。init 脚本先试 heredoc（经典构建器下 0 字节失败），改用 base64 内嵌 +
  `py_compile` 校验。init 明文：
  ```python
  import os, signal, subprocess, sys

  def main() -> int:
      host = os.environ.get("FLOWLENS_HOST", "0.0.0.0")
      port = os.environ.get("FLOWLENS_PORT", "8080")
      srv = subprocess.Popen([sys.executable, "-m", "flowlens", "--host", host, "--port", port])
      def forward(signum, frame):
          if srv.poll() is None:
              srv.send_signal(signal.SIGINT)
      signal.signal(signal.SIGTERM, forward)
      signal.signal(signal.SIGINT, forward)
      return srv.wait()

  if __name__ == "__main__":
      sys.exit(main())
  ```
- **测试数据装载**：`tests/e2e/test_debug.py::_load_tree` 逐秒写入并推进
  `FakeClock`（`advance()` 只在 watermark 越过该秒时定稿），时钟只前进、符合活跃环
  语义。初版曾想"倒序写旧秒"，被否（旧秒会被判迟到、不写进树）。

### 用到的冻结模块内部属性（清单）

只读、不改。公开属性（`Bucket.sec`/`.dims`、`DimBucket.hh`/`.stats`、`DimBucket.stats.dd`、
`DDSketch.n`、`TopK.top(n)`/`__len__`、`HeavyHitters.u()`、`Summary.start`/`.level`/`.end`/
`.keys`/`.u`/`.stats`、`Tree.newest`/`.cfg`/`.frozen_range()`、`Config.*`、`Ring.next`、
`Engine.clock`）不在此列。以下属于"非标准公开接口/私有结构"的读取：

| 读取项 | 所在模块 | 用途 |
|---|---|---|
| `engine._plan(d, range)` | engine（非冻结上层） | 指令明确要求：拆分与真实查询一致 |
| `engine._cover(d, s0, e0, stats_only)` | engine（非冻结上层） | 取历史区间被选中的节点（与 `collect_parts` 同入口） |
| `Tree.levels[k]`（各层 `dict[start, Summary]`） | segtree.py（冻结） | 枚举每个已冻结节点做摘要 |
| `Tree.builders[k]`（`_Builder` 对象） | segtree.py（冻结） | 各层"构建中"节点：读 `b.level`/`b.start`/`b.span`/`len(b.cand)` |
| `Ring.slots`（`list[Bucket]`） | ring.py（冻结） | 遍历活跃环找有数据的秒桶 |
| `_Plan`/`_PlanEdge` 字段（`ring_buckets`/`history`/`edges`、`sec`/`frac`/`in_ring`） | engine.py（非冻结上层） | 转出查询拆分 |
