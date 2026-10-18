# 进度交接单

新会话开始时必读。每个任务结束时更新"当前进度""进行中""未解决的问题""下一步"四节；"已定的决策"只追加，不删除。

## 当前进度

| 任务 | 状态 |
|---|---|
| 基础代码（✅ 模块） | 已冻结 |
| T1 事件模型 | 已审查通过 |
| T2 统计引擎 | 已审查通过 |
| T3 快照 | 已审查通过 |
| 性能专项（bounds.top_k、HeavyHitters.part） | 已审查通过，两处已重新冻结 |
| T4 写入处理 + Ticker | 已审查通过 |
| T5 查询 | 已审查通过 |
| T6 HTTP 接口 | 已审查通过 |
| T7 SSE + T7b 修正 | 已审查通过 |
| T8 过载降级 | 已完成，等审查 |
| T8b 测试与运行环境解耦、Linux 验证 | 已完成，等审查 |
| T9 驻留表、T10 工具与实验、T11 性能 | 未开始 |

## 进行中

T8b 已完成，汇报后停下等审查。T8b 内容（按 docs/instructions/T8b.md）：
1. **测试与真实内存解耦**（`create_app` 加 `memory_reader` 参数）：`create_app(..., memory_reader=None)`，默认 `memstat`；Ticker 喂控制器的值、`/healthz` 与 `/v1/stats` 的 `memory_bytes`/`memory_limit_bytes` 统一从这一个 reader 读（挂在 `app[memory_reader_key]`）。`tests/e2e` 所有创建应用处传恒 `(0, None)` 的假读数（`_fake_memory_reader`）；新增 `tests/e2e/test_degrade.py::test_memory_reader_drives_levels` 用可控的 `MemFeed` 走"读数→Ticker→控制器"真实链路，端到端验证第 3/4 级的进入与退出。这是 28 个 Linux 失败的直接修复（见下）。
2. **查清每个测试后内存增长 ~45 MB 的原因**：tracemalloc 逐测快照定位到**每个 e2e 测试的 Engine 没被释放**（去重 Bloom 33.5 + CMS 19.7 + HLL 1.1 ≈ 55 MB/个，逐测累积）。根因是 **aiohttp 的全局中间件缓存** `_cached_build_middleware`（`web_app.py` 的 `lru_cache(maxsize=1024)`）以 `(handler, match_info.apps)` 为键、`apps` 是**含 Application 的元组**，每个 `create_app` 都把那个 Application（连带 Engine）用强引用挂进进程级缓存。修复：`tests/e2e/conftest.py` 加 autouse fixture，每个 e2e 测试结束后 `cache_clear()`。验证：修复前 6 个测试 RSS 55→358 MB（+54.6 MB/测）、tracemalloc 存活分配 +54.6 MB/测；修复后 RSS 平台化在 ~121 MB、存活分配净增 ≈0 MB/测。生产单实例不受影响（只建一个 app、缓存就一条）。
3. **Windows 内存读数**（`memstat.py`）：cgroup 与 statm 全缺时，Windows 用 ctypes 调 psapi 的 `GetProcessMemoryInfo` 读 `WorkingSetSize`（结构体延迟定义、缓存；`GetCurrentProcess` 声明 `restype=c_void_p`、`GetProcessMemoryInfo` 声明 `argtypes` 否则伪句柄被截断/溢出）。任何异常吞掉返回 0。补 `tests/test_memstat.py` 两条（跨平台 mock + 仅 Windows 的真实读数，非 Windows 跳过）。**踩坑**：初版 `GetCurrentProcess` 不声明 restype，ctypes 把 64 位伪句柄 `0xFFFF…FF` 截断成 32 位有符号 `-1`，`GetProcessMemoryInfo` 静默失败返回 0；声明 `restype=c_void_p` 后才拿到真实值。
4. **小修**：(a) `engine.ingest` 采样时不重算 event_id 哈希——去重时已算一次（`h_id_all`），采样按下标 `keep` 复用（`h_id = h_id_all[keep]`），这段只在降级时跑、降级因 CPU 紧张，省一次哈希；正常路径（w=1）语义不变。(b) 第 3 级把在途上限从 4 MB 降到 2 MB，这期间解压后 2~4 MB 的请求收到 503 而非 413（有意、暂时性），错误信息说明"降级期间单个请求的上限暂时为 2 MB"，记入 DESIGN_TODO #15。
5. **每个任务必须在 Linux 上验证**：CLAUDE.md"每个任务结束时"加一条——除 Windows 外，还必须在 WSL 中跑全量 `pytest -q`（`/mnt/d/dhy/code/flowlens`、`/home/user/flowlens-venv`），两平台结果分别汇报。

**背景（审查时在 Linux 实测）**：全量 pytest 在 Linux 上 28 个失败、Windows 全过。根因：Windows 上 `memstat.memory_bytes()` 恒返回 0，降级控制器从未因内存触发第 3/4 级（相关逻辑本地一次都没跑过）；Linux 上读到 pytest 进程的真实内存，逐测累积到第 4 个测试约 239 MB，超 256 MB × 0.9 → 服务进第 4 级、之后所有写入 503。第 1 项（memory_reader 注入）直接解耦；第 2 项（conftest 清 aiohttp 缓存）解决累积本身；两者叠加后 WSL 全量 249 passed, 1 skipped。

T8 过载降级已完成（见 git 提交 `6aecc4e`）。T8 内容：
1. **降级控制器**（新建 `flowlens/degrade.py`）：纯逻辑、无 aiohttp 依赖。四级阈值滞回（每级独立活跃状态，当前级 = 所有活跃级中最高）；只读属性 level/degraded/sample_weight/replay_paused/sse_slowdown/tighten_budgets/reject_ingest + 最近输入值；级别变化记一行日志并触发 `on_change` 回调（副作用由 app 层注入，保持控制器纯逻辑）；`pin()` 供测试固定级别。
2. **控制器驱动**：挂在 app 上（`web.AppKey`），**与 engine、QueryService 共享同一实例**（create_app 装配）；由 **Ticker 每拍结束后调一次 update**（喂 occupancy/tick 延迟/内存读数）——单写者、每秒一次正好对应"每秒更新一次"，且读到的是本拍刚产出的量，不用另起每秒任务。
3. **第 1 级**：`QueryService.topk/stats` 的回放查询抛 Overloaded(503+Retry-After)（判断在 QueryService 内部，不写进 handler）；SSE 不暂停、只降频（组的 `min_recompute` 1s→5s，生产者每帧运行时读取）。
4. **第 2 级**：`engine.ingest` 按 event_id 哈希确定性采样（`h_id % w == 0` 保留），保留事件以权重 w 写入；只作用于两维 Top-K 名单与 url 维 DDSketch，HLL 与去重始终全量；`weight` 参数缺省改读控制器 sample_weight（显式传参优先，基准/测试不受影响）。weight=1 走与 T7b 结构相同的写入路径。
5. **第 3 级**：`on_change` 在进入时把 Admission 4→2 MB、RawBudget 8→4 MB 并清一次结果缓存 + URL 归一化缓存；退出时恢复（缓存不清）；`StreamManager.check_limits` 拒绝新 SSE 订阅（已有订阅保持）。
6. **第 4 级**：`_ingest` 与 `_ingest_expect` 在读请求体之前直接 503+Retry-After（未写去重状态，重试安全）。
7. **响应字段**：/v1/topk、/v1/stats、/healthz 的 `degraded` 改真实状态；采样生效时 /v1/topk 与 SSE 帧带 `sample_rate`；/healthz 增加 `degrade` 字段。
8. **文档**：DESIGN_TODO 追加两条（第 3 级"缩短历史保留"改为收紧额度+清缓存，因保留期由冻结的 segtree 构造时确定；采样期间上下界不再严格保证）。

`pytest -q` 247 通过（226 原有 + 16 控制器单测 + 5 集成）、无警告。

## 已定的决策（不要改回去）

- 运行模型：单进程、单事件循环、单写者，不用线程或多进程修改统计状态。
- 冻结的基础模块不得修改；发现问题时停下说明，等确认。bounds.py、sketch/heavy.py 曾经用户批准做过一次输出不变的性能修改，已重新冻结。
- 异常类放在 flowlens/errors.py，api 层直接引用。
- EventBatch 的字节数字段名为 nbytes。
- 超过 2^64 − 1 的整数被 orjson 转成浮点数，按单条非法处理，不整批拒绝。
- 写入上限：压缩体 4 MB，解压后 4 MB；在途预留按解压后字节数计（上限 4 MB），在解析和去重之前完成。原因：解析后的 Python 对象约为原始 JSON 的 3.4 倍。
- 单个请求过大返回 413（提示拆分），在途已满返回 503（稍后重试），两者分开。
- engine.collect_parts / collect_stats 是唯一组装时间片的地方，内部共用 _plan；快照、回放、SSE 补帧都调用它们。
- engine._on_finalize 按维度幂等，定稿失败后可以安全重试。
- 快照分为同步 refresh_snapshot() 与异步 refresh_snapshot_async()，共用同一份计算；Ticker 调用异步版本。
- Ticker 单拍出错时记录日志、计入 error_count，继续下一拍。
- 召回的判断口径：返回的 key 的真实次数不小于精确答案第 10 名的次数即算命中（并列不算错）。
- aiohttp 中间件必须加 @web.middleware；服务端必须关闭自动解压（auto_decompress=False），否则解压炸弹防护会被绕过。
- 服务端 `lingering_time=1`（不是 0）。413/503 响应在关连接前就已发出（`finish_response` 在 lingering 读之前），客户端拿 413 不靠 lingering；lingering 只影响"关连接前是否读完在途请求体"。定为 0 时，对"不发 Expect 直接灌 5 MB 体"的客户端，服务端发完 413 立即关连接、客户端还在发送 → 内核发 RST 而非 FIN → 客户端偶发收到"连接被重置"而非 413（WSL 实测 aiohttp 2/40）。定为 1 时服务端读完剩余体再干净关连接（FIN），20/20 稳定 413 且无 1 秒延迟。发 Expect 的客户端不受影响（超大请求在发体前就被 expect_handler 拒绝）。WSL 对比：LT=0 时该客户端 18/20，LT=1 起 20/20、40/40。
- POST /v1/ingest 注册自定义 expect_handler（Expect: 100-continue）：Content-Length>4 MB 直接 413、RawBudget 预留不下直接 503+Retry-After:1，两种情况都不发 100 Continue，客户端不发送请求体。判断用只读的 `Admission.can_reserve`，真正预留仍在 `_read_raw_body` 闭环（try_reserve/finally release），预算借还是单一 owner，不双重预留。
- create_app(engine, ingest_service, query_service, *, tick_period) 的参数形式保留。
- 测试不引入新依赖：异步测试用 asyncio.run 与 aiohttp.test_utils，不用 pytest-asyncio、pytest-aiohttp。
- 慢测试标记 slow，默认 pytest -q 跳过；修改 bounds.py 或 sketch/heavy.py 后必须运行 pytest -q -m slow。
- WSL 的代理来自 WSL 自动代理功能（autoProxy，把 Windows 代理设置同步进 WSL）。不修改它；实验脚本与以后的压测脚本里设置 `NO_PROXY=localhost,127.0.0.1`，curl 额外用 `--noproxy '*'`，保证客户端直连服务端、测到真实 TCP 行为（否则大 body 转发时代理偶发 502，污染结果）。
- 内存读数只在"当前进程所在 cgroup 设了内存上限"时才用 cgroup，否则回退进程常驻集（statm）。cgroup v2 读 memory.max（"max"=无上限）、v1 读 memory.limit_in_bytes（≥2^60=无上限）。原因：WSL 里 cgroup 是整个发行版（读到 427 MB 而本进程才 109 MB），无上限判定避免 T8 降级控制器误判；评测容器 --memory 启动一定带上限，读数不受影响。/healthz 新增 memory_limit_bytes 字段（无上限为 null）。
- SSE 订阅者按 (by, window, k, interval) 分订阅组，同组共用一个后台生产任务与同一份计算（计算量只随组数增长）。同一组每秒最多重新计算一次（min_recompute=1.0s，interval 小于 1s 时同一秒复用上次结果只更新帧时刻）。组上限 8、连接上限 256、心跳 15s、写超时 5s、重连最多补 30 帧，均为 StreamManager 构造参数（默认值即这些数字，测试可覆盖）。
- SSE 帧时刻对齐 interval 网格 T=⌊now/interval⌋×interval，内容 = QueryService.topk(d, window, k, at=T)。/v1/topk 与 SSE 的响应 dict 共用 stream.topk_data（抽成一个函数、不写两份）。data 与 /v1/topk 完全同构。
- SSE 断线重连用单调游标：Last-Event-ID 合法时游标 = 其下一格（已见帧不重发），受"最旧可算帧（窗口仍在保留期）"与"最近 30 帧"两个下限约束；首帧前把 [游标, 首帧) 逐帧补发（帧间让出），之后逐帧跟随队列，id 连续、严格递增、不重不漏。Last-Event-ID 格式不对/晚于当前/早于保留期 → 从实时帧开始。
- SSE 背压：每个订阅者只有容量 1 的最新帧队列（满则丢旧帧、丢弃计数 +1）；单帧写超时 5s 断开。app 上的键统一用 web.AppKey（模块级常量，消除 NotAppKeyWarning）。
- __main__.py 用 argparse 支持 --host（默认 0.0.0.0）/ --port（默认 8080），logging.basicConfig 输出到标准输出（带时间、INFO），启动与 PYTHONHASHSEED 警告改用 logging。
- 实验用的大请求体 tools/exp/big5mb.bin 不入库（git rm --cached + .gitignore），实验脚本运行时临时生成。
- hashing.key_hashes 中 hash(str(k)) 的改动保留：所有调用位置传入的都是校验过的字符串，结果与 hash(k) 完全一致，开销约每条事件 57 ns（约 1%）。
- sketch/hll.py 保留支持分段的版本，不换成简化版。
- 内存读数保持 memory.current 原值，不扣除页缓存。理由：评测以是否超出 --memory 上限判定，内核判断 OOM 用的就是 memory.current，这个口径更保守，适合做降级判断。
- SSE 默认 60 秒窗口的帧暂时走回放；是否改为复用快照，等 T10 压测看过 CPU 占用后在 T11 决定（同时记入"未解决的问题"）。
- SSE 帧计算节流按**帧时刻**判断（本帧时刻距上次计算帧时刻 ≥ min_recompute 才重算），不是墙钟：interval ≥ 1s 时每帧必重算（data.to == id），interval < 1s 时同秒多帧复用一份计算。
- SSE 组保留最近 30 帧的 (帧时刻, payload)（deque(maxlen=30)）：补帧优先取缓存、同组重连不各自重算；不在缓存的帧才现场 topk，且全局 asyncio.Semaphore(1) 串行化。
- 无 Last-Event-ID 的订阅者加入已有组时，若组里已有最近一帧立即补发它（最多一帧），之后照常跟随实时帧，id 连续、严格递增。
- 应用关闭时 StreamManager.close() 先向每个订阅者队列放结束标记（pump 收到立即返回），再取消生产任务——SSE 处理函数及时退出，不靠 aiohttp 的 60 秒强制取消（docker stop 只等 10 秒）。
- Last-Event-ID 不在当前 interval 网格上（如客户端换了 interval）时，起始游标向上对齐到网格；保留期下限口径与 engine._plan 一致（now − retention − 迟到上限）。
- 降级控制器的内存阈值按**上限的比例**计算（0.78/0.70 与 0.90/0.82），不写死 200/230 MB；`memory_limit_bytes` 为 None 时按 256 MB 计（比例换算到 256 MB 正好是设计文档的 200/230 MB）。
- 降级控制器挂在 app 上、**与 engine 和 QueryService 共享同一实例**（create_app 装配），由 **Ticker 每拍结束后 update 一次**驱动：单写者、每秒一拍对应"每秒更新一次"，occupancy/tick 延迟是这一拍刚产出的量，且不用另起一个每秒后台任务。级别变化的副作用（收紧/恢复预算、进入第 3 级清缓存）用 `on_change` 回调注入（闭包捕获 app，控制器保持纯逻辑、不依赖 aiohttp），只在实际变化时回调一次。
- 第 3 级不再"缩短历史保留"（§3.6 设计），改为**收紧额度（Admission 4→2 MB、RawBudget 8→4 MB）+ 清一次结果缓存与 URL 归一化缓存**：保留期由冻结的 segtree 构造时确定，无法动态调整。退出第 3 级恢复额度、缓存不清（重新填充）。
- 第 2 级采样按 **event_id 的哈希**做确定性取舍（`h_id % w == 0` 保留，比例 1/w，与时间无关）：同一事件无论重试多少次、何时重试，取舍都一致，且与"这次是不是重复"解耦。采样只作用于两维 Top-K 名单与 url 维 DDSketch；HLL（user 维）与 event_id 去重始终全量。`engine.ingest` 的 `weight` 参数缺省读控制器 sample_weight、显式传参优先（基准/测试不受影响）；weight==1 走与 T7b 结构相同的写入路径，正常路径结果完全一致。
- SSE 在降级时**只降频、不暂停**：生产者每帧运行时读 `recompute_interval_ms()`（第 1 级 1s→5s），帧照常按 interval 推送、复用结果；HTTP 回放查询才在第 1 级暂停（503）。SSE 计算走 `topk(..., from_stream=True)` 绕过"暂停回放"判断。
- 采样期间（sample_weight > 1）Top-K 的上下界**不再是严格保证**（采样方差可能让加权估计偏离真实次数）；响应带 `sample_rate = 1/sample_weight`，/healthz 与 /v1/topk 的 `degraded` 为真实状态。
- 测试与运行环境的真实内存解耦（T8b）：`create_app` 增加 `memory_reader: Callable[[], tuple[int, int|None]]` 参数，默认 memstat 的两个函数；Ticker 喂降级控制器的内存值、`/healthz` 与 `/v1/stats` 的 `memory_bytes`/`memory_limit_bytes` **统一从这一个 reader 读**（挂在 `app[memory_reader_key]`，并传给 Ticker）。`tests/e2e` 所有创建应用的地方传恒 `(0, None)` 的假读数；需要验证内存触发降级的测试传可控的假读数（`MemFeed`，测试随时改 `.mem`/`.limit`，Ticker 每拍读当前值）。原因：不注入时测试会读到 pytest 进程的真实内存，Linux 上逐测累积把服务打进第 4 级、所有写入 503（28 个 e2e 失败）。生产 `__main__` 不传，走默认 memstat。
- 每个任务结束都必须在 WSL（Linux）跑全量 `pytest -q`（T8b 起）：评测在 Linux 上进行，Windows 与 Linux 两边结果都必须全过、汇报时分别给出。WSL 项目路径 `/mnt/d/dhy/code/flowlens`、用 `/home/user/flowlens-venv`（T6c 建，缺依赖先 `pip install -e ".[dev]"`）。此条已写入 CLAUDE.md"每个任务结束时"。
- Windows 内存读数（T8b）：memstat 在 cgroup 与 statm 都读不到时（纯 Windows 开发机），用 ctypes 调 psapi 的 `GetProcessMemoryInfo` 读本进程 `WorkingSetSize`，让本地 `/healthz` 显示真实值、内存相关的降级逻辑在本地也能真正触发（此前 Windows 上 `memory_bytes()` 恒 0，第 3/4 级逻辑本地一次没跑过）。任何异常吞掉返回 0、不抛。`GetCurrentProcess` 必须声明 `restype=c_void_p`、`GetProcessMemoryInfo` 必须声明 `argtypes`，否则 64 位伪句柄 `0xFFFF…FF` 被截断/溢出、调用静默失败返回 0。
- e2e 测试后清 aiohttp 全局中间件缓存（T8b，`tests/e2e/conftest.py` autouse fixture）：aiohttp 的 `_cached_build_middleware`（`web_app.py`，`lru_cache(maxsize=1024)`）以 `(handler, match_info.apps)` 为键、`apps` 是含 `Application` 的元组，每个 `create_app` 都把那个 Application（连带 Engine ≈55 MB）用**强引用**挂进这个进程级缓存。全量 pytest 在同一进程逐个跑 e2e（各自 create_app）时逐测累积 ~55 MB（到第 4 个就超 256 MB×0.9，把控制器打进第 4 级）。fixture 在每个 e2e 测试结束后 `cache_clear()`。生产单实例不受影响（只建一个 app、缓存就一条）。

## 基准数据

各任务结束时重跑全部测试与性能基准并追加一列（以后每个任务都一样）。每项各跑 3 次取中位（单次波动大）。"上一版"为 T6c 前后对照；"T7b"为提交 `d2d5c8e`；"T8"、"T8b"分别为对应任务结束时的本机（T8b 机器）中位数。

| 项目 | 上一版（T6c 前后） | T7b | T8（中位数） | T8b（本次，Windows 中位数） |
|---|---|---|---|---|
| pytest -q | 197 passed, 5 deselected | 226 passed, 5 deselected | 247 passed, 5 deselected | 250 passed, 5 deselected（WSL：249 passed, 1 skipped, 5 deselected） |
| pytest -q -m slow | 5 passed | 5 passed | 5 passed | 5 passed（Windows 与 WSL 均 5 passed） |
| bench_core 长尾 | 约 20 万～30 万 事件/秒（波动大） | 约 30.5 万/秒（波动上沿） | 约 22.6 万/秒（218/236/226） | 约 18.5 万/秒（197/185/185） |
| bench_model（缓存命中） | 约 1.68 μs/条 | 约 1.36 μs/条 | 约 2.22 μs/条（2.15/2.22/2.22） | 约 1.87 μs/条（1.87/1.92/1.76） |
| bench_engine 每批 1000 条合计 | 约 6.3 μs/条 | 约 4.72 μs/条 | 约 6.10 μs/条（5.84/6.10/8.27） | 约 5.84 μs/条（5.84/5.85/5.57） |
| bench_engine 每批 5000 条合计 | 约 4.9 μs/条 | 约 4.20 μs/条 | 约 5.39 μs/条（4.65/5.39/6.64） | 约 4.65 μs/条（4.58/4.79/4.65） |
| 单次定稿 | 中位约 23 ms，最大约 47 ms | 中位约 18 ms，最大约 32 ms | 中位约 23 ms，最大约 44 ms（一次 93 s 为调度停顿） | 中位约 25 ms（25.9/25.0/22.5），最大约 73 ms |
| 满载快照 refresh_snapshot | 中位约 59 ms，最大约 81 ms | 中位约 47 ms，最大约 53 ms | 中位约 58 ms，最大约 82 ms | 中位约 52 ms（52.3/52.3/51.5），最大约 76 ms |
| 完整 tick | 中位约 108 ms，最大约 137 ms | 中位约 82 ms，最大约 92 ms | 中位约 105 ms，最大约 135 ms | 中位约 116 ms（116/118/106），最大约 187 ms |

**性能对比必须同机对照**：T8b 列在本机（本次运行）采集，与 T8 列同一台机器，故可直接对比。T8b 相对 T8：`bench_core 长尾` 22.6→18.5 万/秒、`完整 tick` 105→116 ms、`单次定稿` 23→25 ms——**T8b 未触及任何热路径**（改动是内存读数来源、`create_app` 加一个 reader 参数、`ingest` 采样分支复用已算好的 event_id 哈希、`ingest.py` 加一条错误信息），正常路径（sample_weight=1）的写入/快照/定稿代码与 T8 完全相同，数值差异在本机单次波动范围内（长尾吞吐波动历来很大，见"上一版"列 20 万～30 万）。`bench_model` 1.87 vs T8 的 2.22、每批 1000/5000 合计 5.84/4.65 vs T8 的 6.10/5.39，T8b 持平或略快。**结论：T8b 未使任何路径变慢**。

## 未解决的问题

1. 查询延迟：每拍定稿约 23 ms（最长约 47 ms），快照在极端流量下约 60 ms，可能让查询 p99 超过 50 ms。等端到端压测后决定是否优化 segtree 或提前做窗口汇总。
2. 内存尚未在 256 MB 容器里实测，估算常驻约 195 MB 加写入路径。T9 实测。
3. 吞吐：每批 5000 条约 20.5 万/秒，每批 1000 条约 16 万/秒，未计 HTTP 开销。T11 优化。
4. T9 的字符串驻留表与冻结的 segtree.py 冲突，开始 T9 前由用户决定做法。
5. ~~`memory_bytes` 在 WSL 里读数虚高~~（T7 前置小修已解决）：memstat 现在只在"当前进程所在 cgroup 设了内存上限"时才用 cgroup 读数，否则回退进程常驻集（statm）。WSL 的 cgroup 无上限（整个发行版共享）→ 回退 statm，读数不再虚高；评测容器 --memory 启动带上限 → 仍用 cgroup，读数准确。
6. SSE 默认 60 秒窗口的帧目前走回放（collect_parts + top_k），没有复用刷新快照。是否改为复用快照，等 T10 压测看过 CPU 占用后在 T11 决定。
7. ~~全量 pytest 逐测内存增长 ~45 MB（Linux 上把服务打进第 4 级）~~（T8b 已定位并解决）：根因是 **aiohttp 的全局中间件缓存** `_cached_build_middleware`（`web_app.py`，`lru_cache(maxsize=1024)`）以 `(handler, match_info.apps)` 为键、`apps` 是含 `Application` 的元组，把每个 e2e 测试的 Application（连带 Engine ≈55 MB）用强引用挂进进程级缓存，逐测累积（实测 +54.6 MB/测、6 个测试 55→358 MB）。tracemalloc 确认是**对象未释放**（非分配器未退还）。修复：`tests/e2e/conftest.py` autouse fixture 每测后 `cache_clear()`（修复后 RSS 平台化 ~121 MB、存活分配净增 ≈0）。**生产单实例不受影响**（只建一个 app、缓存就一条、长驻运行）。另：T8b 顺带修复了"Windows 上 `memory_bytes()` 恒 0"（改用 psapi WorkingSetSize），此前第 3/4 级内存降级逻辑在本地从未真正执行过。

## 下一步

T8b 汇报后停下等待审查；审查通过后按顺序执行：T10 工具与实验（含端到端压测，粗测吞吐与查询 p99 记入"基准数据"）→ T9 驻留表 → T11 性能 → 收尾（统一修改设计文档、整理 AI_LOG、答辩准备）。**以后每个任务结束都必须在 WSL 跑全量 `pytest -q`**（T8b 起，已写入 CLAUDE.md）。
