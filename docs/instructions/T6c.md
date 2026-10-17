# T6c：超限请求的连接处理

背景：T6b 把 aiohttp 的 lingering_time 设为 0，让超限请求被拒绝后立即断开连接。出发点正确（默认逗留 10 秒期间客户端可能等不到响应），但有风险：服务端关闭连接时如果接收缓冲区里还有未读完的请求体，操作系统通常会发送 RST，客户端可能还没读到 413 就报"连接被重置"。不同操作系统表现不同，而评测运行在 Linux 容器里。之前的实验用的是裸 socket，需要用真实 HTTP 客户端在 Linux 上重新验证。

## 一、支持 Expect: 100-continue（优先做）

curl 等客户端上传较大的请求体时，会先发送 Expect: 100-continue，等服务端同意后才发送请求体。在这一步就拒绝，客户端根本不会发送请求体，连接也不会出问题。

- 为 POST /v1/ingest 注册自定义的 expect_handler：Content-Length 超过 4 MB 时直接返回 413 的 JSON 响应；RawBudget 预留不下时返回 503 + Retry-After: 1；两种情况都不发送 100 Continue。检查通过时才发送 100 Continue（参考 aiohttp 默认 expect_handler 的写法）
- 注意不要重复预留：expect_handler 里只做判断，真正的预留仍在 _read_raw_body 中完成，或者把预留结果可靠地传递过去，二选一并说明理由

## 二、在 Linux（WSL）上用真实客户端对比验证

用户的机器没有安装 Docker，改用 WSL（Ubuntu）作为 Linux 环境，它的 TCP 行为与评测用的 Linux 容器一致。项目在 WSL 中的路径是 /mnt/d/dhy/code/src。你可以通过 wsl -e bash -lc "..." 执行 Linux 命令。

准备：

- 在 WSL 里为本项目建立独立的虚拟环境（例如 ~/flowlens-venv），安装 numpy、orjson、aiohttp；不要动 Windows 这边的 Python 环境
- 服务和客户端都在 WSL 里运行，服务用 PYTHONHASHSEED=0 python -m flowlens 启动
- 如果 WSL 里没有 curl，用 sudo apt-get install -y curl 安装；需要输入密码时停下来告诉用户

测试以下三种客户端：

- curl --data-binary @5mb.json（默认会发 Expect: 100-continue）
- curl -H "Expect:" --data-binary @5mb.json（禁用 Expect，直接发送请求体）
- aiohttp 客户端直接 POST 5 MB 请求体

对每种客户端，分别在 lingering_time 为 0、1、默认值三种设置下各测 20 次，记录：

- 客户端收到 413 的次数、收到"连接被重置"或其他错误的次数
- 收到响应的耗时（中位数、最大值）
- 被拒绝后，用新连接发一个正常的写入请求是否成功

把结果整理成一张表写进汇报。如果条件允许，在 Windows 本机上也用同样的三种客户端各测几次，作为对照。

## 三、根据结果选定设置

- 目标：三种客户端在 20 次里都稳定收到 413，且耗时不超过 2 秒
- 如果 lingering_time=0 能满足，保留；如果出现连接被重置，改用能满足目标的最小值
- 选定后，把设置和理由追加到 docs/PROGRESS.md 的"已定的决策"
- 为 Expect: 100-continue 的拒绝路径补充自动化测试（在 tests/e2e/test_api.py 中，用 aiohttp 客户端加 expect100=True）；容器内的对比实验不需要写成自动化测试

## 收尾

1. pytest -q 必须通过。
2. 更新 docs/PROGRESS.md：当前进度、进行中、未解决的问题、下一步；新定下的决策追加到"已定的决策"。
3. 实现与 DESIGN.md 描述不一致的地方，追加到 docs/DESIGN_TODO.md。
4. 在 AI_LOG.md 末尾追加本任务的记录。
5. 按下面的格式汇报，然后停下，等待审查：
   1. Expect: 100-continue 的实现方式，以及预留放在哪一步、为什么
   2. 对比实验的结果表
   3. 最终选定的 lingering_time 及理由
   4. pytest -q 的结果
   5. 需要确认的问题
   6. 本任务新建或修改的文件清单

如果上下文快满、任务做不完：先把已完成和未完成的部分写进 docs/PROGRESS.md 的"进行中"一节，然后停下汇报。
