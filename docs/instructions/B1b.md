# B1b（支线）：修复打包与退出信号，Dockerfile 回到清晰写法

继续在支线工作目录 D:\dhy\code\flowlens-b（分支 lane-b）中进行。B1 的调试接口审查通过；Docker 部分发现的两个问题需要修根源，而不是在 Dockerfile 里绕开。

## 本次允许修改的文件

在 B1 允许的文件之外，本次额外允许修改：pyproject.toml、flowlens/__main__.py，以及新建 constraints.txt、tests/test_packaging.py、tests/e2e/test_signals.py。主线的 T10a 不会修改这两个文件。其余规则与 B1 相同（不改 PROGRESS.md、AI_LOG.md、DESIGN_TODO.md、CLAUDE.md，记录写进 docs/lanes/B1-notes.md）。

## 一、打包：所有子包都要被安装

1. pyproject.toml 的包列表改为自动发现（[tool.setuptools.packages.find]，include = ["flowlens*"]），不要再手工列举，避免以后新增子包时再次遗漏
2. 新建 tests/test_packaging.py：在临时目录中用 pip wheel . --no-deps 构建安装包，检查其中包含 flowlens/ 下的每一个 .py 文件（用源码目录的文件清单比对，而不是写死文件名）。如果耗时超过几秒，标记为 slow

## 二、退出信号：docker stop 要能正常退出

1. __main__.py 中把"永远等待"改为等待一个停止事件；在支持的平台上用 loop.add_signal_handler 为 SIGTERM 和 SIGINT 注册处理函数，收到时设置停止事件，然后照常执行 runner.cleanup()，日志记录收到的信号与退出原因
2. Windows 不支持 add_signal_handler，保持现有的 Ctrl+C（KeyboardInterrupt）路径，不能因此报错
3. 新建 tests/e2e/test_signals.py（非 Linux 平台跳过）：用子进程以一个空闲端口启动 python -m flowlens，等 /healthz 可用后发送 SIGTERM，断言 5 秒内以退出码 0 结束；有一个 SSE 订阅连接未断开时，同样断言能在 5 秒内退出

## 三、Dockerfile 回到清晰的写法

1. 删除嵌入的 base64 启动脚本与信号转发进程；CMD 改回 ["python", "-m", "flowlens", "--port", "8080"]
2. 恢复用 pip install . 安装（第一节修好后它会装出完整的包）
3. 增加 ARG PIP_INDEX_URL（默认为空；有值时传给 pip 的 --index-url），这是 B1 中遗漏的要求。不要在镜像里写死任何国内源或代理地址
4. 保留 B1 中的非 root 用户、HEALTHCHECK、.dockerignore

## 四、固定依赖版本

aiohttp 不同版本的行为差异已经两次影响过本项目（中间件写法、传输层自动解压），评测方构建时必须装到我们测试过的版本。
1. 新建 constraints.txt：写入 WSL 测试环境中 numpy、orjson、aiohttp 及其全部间接依赖的确切版本（pip freeze 的结果，去掉 flowlens 本身和开发用的包）
2. Dockerfile 与 run.sh 都用 pip install -c constraints.txt . 安装
3. pyproject.toml 中的依赖保留版本范围，不要写死（constraints.txt 负责固定）

## 五、run.sh 的可执行权限

用 git update-index --chmod=+x run.sh 设置。这个命令只改 git 记录的权限位，与 Windows 的 core.fileMode 设置无关。

## 六、在 Docker 中重新验证

在 WSL 中重新构建（可以用 --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple 加速），然后验证并写进汇报：
- 镜像大小
- docker run --memory=256m --memory-swap=256m 启动后，/healthz 正常，记录空闲 memory_bytes
- docker stop 的耗时与退出码（应在几秒内、退出码 0），以及日志中记录的信号
- 在有一个 SSE 订阅连接未断开的情况下 docker stop，同样记录耗时与退出码
- 用 pip install -c constraints.txt . 在一个全新的虚拟环境中安装后，从非项目目录（例如 /tmp）运行 python -m flowlens 能正常启动（证明不再依赖源码目录）

## 收尾

1. 在 lane-b 工作目录中运行：Windows 上 pytest -q 和 pytest -q -m slow；WSL 中 pytest -q 和 pytest -q -m slow。都必须全部通过
2. 更新 docs/lanes/B1-notes.md：删除"pyproject 漏包、SIGTERM 未处理"的待办，记录修复方式；把"依赖版本由 constraints.txt 固定"、"docker stop 的退出方式"写进"已定的决策"
3. 提交到分支 lane-b（不要推送），然后按下面的格式汇报，停下等待审查：
   1. 第一～六节的完成情况
   2. Docker 验证结果
   3. Windows 与 WSL 两边的测试结果
   4. 和指令不一致的地方，以及原因
   5. 需要确认的问题
   6. 本任务新建或修改的文件清单
