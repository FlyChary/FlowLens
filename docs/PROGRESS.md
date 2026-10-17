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
| T7 SSE + T7b 修正 | 进行中：SSE、前置小修、T7b 修正均已完成，等审查 |
| T8 降级、T9 驻留表、T10 工具与实验、T11 性能 | 未开始 |

## 进行中

T7b 已完成（SSE 修正与 T7 收尾补做），汇报后停下等审查。T7b 内容：
1. 长窗口帧 TooOld 修复：`engine._plan` 保留期检查改为 `now − retention − 迟到上限`（用 `LATENESS_MS` 常量），窗口 600s 的帧与查询不再误报 TooOld。
2. 补帧不重算：`SubscriptionGroup` 保留最近 30 帧 (帧时刻, payload)，补帧优先取缓存，现场计算全局 `Semaphore(1)` 串行化。
3. 新订阅者立即收帧：无 Last-Event-ID 加入已有组时立即补发组里最近一帧。
4. 关闭时立即结束所有流：`StreamManager.close()` 向每个订阅者队列放结束标记，pump 收到即返回。
5. 两处小修：Last-Event-ID 起始游标向上对齐网格；帧计算节流改按帧时刻判断（interval ≥ 1s 每帧必重算、data.to == id）。
6. T7 收尾补做：本文"已定的决策"追加三项（hash(str(k))、hll 分段版、内存读数不扣页缓存）+ SSE 60s 帧走回放的未解决问题；任务顺序更新为 T7 → T8 → T10 → T9 → T11 → 收尾。

`pytest -q` 226 通过、无警告（详见"基准数据"与收尾汇报）。

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

## 基准数据

各任务结束时重跑全部测试与性能基准并追加一行（以后每个任务都一样）。"上一版"为 T6c 前后的对照数字；"本次"为 T7b 收尾重跑，提交 `d2d5c8e`。

| 项目 | 上一版（T6c 前后） | 本次（T7b） |
|---|---|---|
| pytest -q | 197 passed, 5 deselected | 226 passed, 5 deselected |
| pytest -q -m slow | 5 passed | 5 passed |
| bench_core 长尾 | 约 20 万～30 万 事件/秒（不同次运行波动较大） | 约 30.5 万 事件/秒（单次，落在此前波动上沿） |
| bench_model（缓存命中） | 约 1.68 μs/条 | 约 1.36 μs/条 |
| bench_engine 每批 1000 条合计 | 约 6.3 μs/条（约 15.9 万/秒） | 约 4.72 μs/条（约 21.2 万/秒） |
| bench_engine 每批 5000 条合计 | 约 4.9 μs/条（约 20.5 万/秒） | 约 4.20 μs/条（约 23.8 万/秒） |
| 单次定稿 | 中位约 23 ms，最大约 47 ms | 中位约 18 ms，最大约 32 ms |
| 满载快照 refresh_snapshot | 中位约 59 ms，最大约 81 ms | 中位约 47 ms，最大约 53 ms |
| 完整 tick | 中位约 108 ms，最大约 137 ms | 中位约 82 ms，最大约 92 ms |

本次各项均持平或好于上一版，无一项劣化 10% 以上。T7b 未动写入热路径（engine._plan 的保留期检查只是多一次整数比较，SSE 相关改动都在查询/推送路径外），性能差异主要是本机单次运行的波动，非 T7b 引入。

## 未解决的问题

1. 查询延迟：每拍定稿约 23 ms（最长约 47 ms），快照在极端流量下约 60 ms，可能让查询 p99 超过 50 ms。等端到端压测后决定是否优化 segtree 或提前做窗口汇总。
2. 内存尚未在 256 MB 容器里实测，估算常驻约 195 MB 加写入路径。T9 实测。
3. 吞吐：每批 5000 条约 20.5 万/秒，每批 1000 条约 16 万/秒，未计 HTTP 开销。T11 优化。
4. T9 的字符串驻留表与冻结的 segtree.py 冲突，开始 T9 前由用户决定做法。
5. ~~`memory_bytes` 在 WSL 里读数虚高~~（T7 前置小修已解决）：memstat 现在只在"当前进程所在 cgroup 设了内存上限"时才用 cgroup 读数，否则回退进程常驻集（statm）。WSL 的 cgroup 无上限（整个发行版共享）→ 回退 statm，读数不再虚高；评测容器 --memory 启动带上限 → 仍用 cgroup，读数准确。
6. SSE 默认 60 秒窗口的帧目前走回放（collect_parts + top_k），没有复用刷新快照。是否改为复用快照，等 T10 压测看过 CPU 占用后在 T11 决定。

## 下一步

T7b 汇报后停下等待审查；审查通过后按新顺序（TASKS.md 已确认）执行：T8 降级控制器 → T10 工具与实验（含端到端压测，粗测吞吐与查询 p99 记入"基准数据"）→ T9 驻留表 → T11 性能 → 收尾（统一修改设计文档、整理 AI_LOG、答辩准备）。
