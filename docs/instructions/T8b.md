# T8b：测试与运行环境解耦、Linux 上验证

背景（审查时在 Linux 上实测）：全量 pytest 在 Linux 上有 28 个测试失败，在 Windows 上全部通过。原因：
- Windows 上 memstat.memory_bytes() 永远返回 0，降级控制器从未因内存触发第 3、4 级，相关逻辑在本地一次都没有真正执行过
- Linux 上读到的是 pytest 进程的真实内存。测试在同一进程里逐个运行，每个测试结束后进程常驻内存增长约 45 MB（强制 gc.collect() 后也不下降，但 Engine 对象确实已被回收）。到第 4 个测试时约 239 MB，超过 256 MB × 0.9，服务进入第 4 级，之后所有写入都返回 503

不得修改冻结的基础模块。

## 一、测试不能依赖运行环境的真实内存

1. create_app 增加参数 memory_reader: Callable[[], tuple[int, int | None]]，返回 (memory_bytes, memory_limit_bytes)，默认使用 memstat 的两个函数。Ticker 喂给降级控制器的值、/healthz 和 /v1/stats 中的 memory_bytes 与 memory_limit_bytes，都统一从它读取
2. tests/e2e 中所有创建应用的地方（_serve 等辅助函数）传入返回 (0, None) 的假读数；需要验证内存触发降级的测试，传入能够控制数值的假读数，例如先返回低值、再返回超过阈值的值，验证第 3、4 级的进入与退出
3. 检查其他测试中是否还有依赖真实内存或真实时钟的地方，一并改为注入

## 二、查清每个测试后内存增长约 45 MB 的原因

这和 T9 的长时间内存测试直接相关，必须弄清楚：
- 用 tracemalloc 对比连续几个测试前后的分配快照，找出增长来自哪里
- 判断属于哪种情况：对象没有被释放（例如未取消的任务、闭包、全局缓存、循环引用持有 numpy 数组），还是对象已释放但内存没有退还给操作系统（分配器的行为）
- 如果是前者，在非冻结代码中修复；如果是后者，说明依据（例如 tracemalloc 显示 Python 层已无残留），并评估在单实例长时间运行时是否会持续增长
- 把结论写进汇报，并追加到 docs/PROGRESS.md 的"未解决的问题"或"已定的决策"

## 三、Windows 上的内存读数

memstat 在所有方式都读不到时，Windows 上用 ctypes 调用 psapi 的 GetProcessMemoryInfo 读取进程的 WorkingSetSize，让本地开发时 /healthz 显示真实值、内存相关逻辑也能被真正执行到。任何情况下都不能抛异常，读不到时仍返回 0。补充对应的测试（非 Windows 平台跳过）。

## 四、小修

1. engine.ingest 采样时不要重新计算 event_id 的哈希：去重时已经算过一次，直接按下标复用。这段代码只在降级时运行，而降级本身就是因为 CPU 紧张
2. 第 3 级把在途上限从 4 MB 降为 2 MB，这期间解压后 2～4 MB 的请求会收到 503 而不是 413。这是有意的（暂时性的，退出第 3 级后即可成功），在 503 的错误信息里说明"降级期间单个请求的上限暂时为 2 MB"，并追加到 docs/DESIGN_TODO.md

## 五、以后每个任务都必须在 Linux 上验证

在 CLAUDE.md 的"每个任务结束时"一节加一条：除了在 Windows 上运行，还必须在 WSL 中运行一次全量 pytest -q（WSL 中的项目路径为 /mnt/d/dhy/code/flowlens，使用 T6c 建立的虚拟环境）。评测在 Linux 上进行，两个平台的结果都必须全部通过，汇报时分别给出。

## 收尾

1. 重跑全部测试与性能基准，性能基准每项运行 3 次取中位数：
   - Windows：pytest -q、pytest -q -m slow、python tools/bench_core.py 长尾、python tools/bench_model.py、python tools/bench_engine.py
   - WSL：pytest -q、pytest -q -m slow
2. 在 docs/PROGRESS.md 的"基准数据"表中追加本次的一列（注明提交号与机器）。对比上一次时，如果机器负载不同导致数字整体偏移，用同机对比旧提交的方式说明，与 T8 的做法一致。
3. 更新 docs/PROGRESS.md 的其他各节；新定下的决策追加到"已定的决策"（至少包括：测试通过 memory_reader 注入内存读数；每个任务都要在 WSL 上跑全量测试）。实现与 DESIGN.md 不一致的地方追加到 docs/DESIGN_TODO.md。在 AI_LOG.md 末尾追加本任务的记录，其中写明"Windows 上内存读数恒为 0，导致内存相关逻辑在本地从未执行，Linux 上才暴露"这一教训。
4. 提交到 git（不要推送，推送由用户完成），然后按下面的格式汇报，停下等待审查：
   1. 第一～五节的完成情况
   2. 内存增长的调查结论
   3. Windows 与 WSL 两边的测试结果，以及基准结果与上一次的对比
   4. 和指令不一致的地方，以及原因
   5. 需要确认的问题
   6. 本任务新建或修改的文件清单

如果上下文快满、任务做不完：先把已完成和未完成的部分写进 docs/PROGRESS.md 的"进行中"一节，然后停下汇报。
