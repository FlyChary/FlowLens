# 实现任务清单

按顺序做。每个任务都有"验收"，全部满足才进入下一个。T1~T7 完成后主功能就齐了；T8 之后是加固、实验和优化。

## T1　事件模型（flowlens/model.py）

- `parse_events(body: bytes) -> EventBatch`：用 orjson 解析 JSON 数组；顶层不是数组时抛 `BadRequest`。
- `EventBatch` 是按列存放的 dataclass：`ids, users, urls`（list[str]）、`ts, nbytes`（numpy int64）、`invalid`（int）。逐条校验：event_id、user_id、url 为非空字符串；ts、nbytes 为整数且 ts > 0、nbytes ≥ 0。不合法的条目只计数并剔除。
- `normalize_url(raw) -> str`：去掉 `?` 与 `#` 之后的部分；合并连续的 `/`；去掉末尾 `/`（根路径除外）；纯数字、UUID、长度 ≥ 16 的十六进制路径段替换成 `{id}`；按 UTF-8 边界截断到 128 字节；不改大小写。user_id 只做截断。
- 归一化结果用有上限的字典缓存（例如 10 万条，满了整体清空），因为同一个 URL 会反复出现。

验收：表驱动测试至少 15 条，覆盖 query、fragment、`//`、UUID、中文路径、截断落在多字节字符中间、各种非法字段。

## T2　统计引擎（flowlens/engine.py）

`Engine` 拥有 `Ring`、`trees: list[Tree]`（按维度下标）、`GenerationalBloom`、计数器，由 `Config` 构造。

- `ingest(batch, now_ms) -> IngestResult`：
  1. `classify` 得到 late / future；
  2. `bloom.check_and_set` 去重；
  3. 剩下的按秒分组，对每个秒桶、每个维度调用 `bucket.dims[d].observe(...)`（url 维度传 url 哈希，user 维度传 user 哈希；bytes 两边都传，各自的 Stats 只取自己需要的）。`ring.slot` 返回 None 的计入 `late_at_engine`。
  返回 `{accepted, deduped, late, invalid}`。
- 定稿回调：对每个维度 `trees[d].add_leaf(bucket.dims[d].leaf(sec))`，bucket 为 None 时传 `LeafInput(sec)`。
- `tick(now_ms)`：`ring.advance`、`bloom.advance`、生成快照（见 T3）。
- **只写一个组装时间片的方法**，快照、回放、SSE 补帧全部调用它：
  `collect_parts(d: Dim, r: timewin.Range) -> list[Part]`
  `[ring.next, r.stop)` 内的秒用 `ring.get(s).dims[d].part()`（没有数据的秒跳过），更早的用 `trees[d].cover(...).parts()`；边缘秒取对应的秒桶或叶子，并设置 frac。区间早于保留期时抛 `TooOld`。
  对应的 `collect_stats(d, r) -> Stats`：合并统计概要，边缘秒覆盖 ≥ 0.5 才计入。

验收：用 FakeClock 驱动，覆盖：写入后能在 `collect_parts` 结果中查到；定稿后数据进入线段树；同批与跨批重复；迟到与未来事件；快照与同一区间回放结果完全相同。

## T3　快照（flowlens/engine.py 内）

- 每次 tick 生成一个不可变的 `Snapshot` dataclass：`at_ms`、每个维度前 `snapshot_top_n` 个 `Result`、`distinct_users`、`p95_bytes`。区间为 `split_range((sec(now)−60)·1000, sec(now)·1000)`，用 `collect_parts` + `top_k`、`collect_stats`。
- 引擎持有 `self.snapshot`，整体替换（单线程下赋值即原子）。默认窗口的 topk 和 stats 直接读它。

验收：快照内容与对同一区间调用回放的结果一致。

## T4　写入处理（flowlens/ingest.py）

顺序固定：

1. `Content-Encoding` 只能为空或 `gzip`（不区分大小写），否则 415。
2. `decode_body`：请求体（压缩后）不超过 `MAX_BODY_BYTES = 4 MB`，gzip 解压后不超过
   `MAX_DECOMPRESSED_BYTES = 4 MB`（用 `zlib.decompressobj` 分块解压并累计长度，超限立即 413
   提示拆分，不先把整块解压进内存），超限 413；解压失败/截断 400。
3. **按解压后字节数预留容量**（不是按条数）：`Admission` 记录在途请求解压后字节数之和，
   上限 `MAX_PENDING_BYTES = 4 MB`。本请求解压后字节数加上去会超限时，直接返回
   503 + `Retry-After: 1`。这一步必须在**解析和任何去重之前**（CLAUDE.md 规则 2）——
   orjson 解析后的对象约为原始 JSON 的 3.4 倍，条数上限挡不住解析本身的内存，
   而解压后字节数在解析前已确定，能覆盖解析峰值。
4. `load_array` 解析出顶层数组（`parse_events` 拆成的解析步骤）。
5. 交给 `engine.ingest`。大请求按每 5000 条一块 `validate`，块之间 `await asyncio.sleep(0)`；
   每处理完一块把 `arr` 中该块元素置 `None`（等长切片赋值），让已处理的 dict 尽早释放。
   无论成功还是异常，都在 `finally` 里 `release`。
6. 返回 202 和计数。

后台每秒任务 `flowlens/ticker.py` 的 `Ticker.run`：`engine.advance()` → `await sleep(0)`
→ `await engine.refresh_snapshot_async()`，按绝对时刻调度（单拍耗时不累积漂移）；
单拍抛普通异常时记日志、`error_count` 加一、继续下一拍（不吞 `CancelledError`）。

验收：覆盖空数组、损坏的 gzip、解压炸弹、解压后恰好/超过 4 MB、批内重复、跨批重复、
迟到与未来事件、在途字节满时返回 503 且之后重试不会被判重复、分块让出事件循环、
异常时释放预留、已处理元素置 None。

## T5　查询（flowlens/query.py）

- `QueryService.topk(d, from_ms, to_ms, k)`：`split_range` → `engine.collect_parts` → `top_k`。不带 at 且 window=60 s 时直接读快照。
- `to_ms` 早于 watermark 的结果是终态：标 `final: True`，放进 LRU 缓存（容量 256）。
- `QueryService.stats(window)`：`collect_stats` 后取去重人数与 p95。
- 查询是同步计算，单次耗时超过 20 ms 的大区间要分块并让出事件循环。

验收：对照测试——随机生成 10 分钟数据，随机 200 个区间（含毫秒边界），每个 key 的真实次数都在返回的 [lower, upper] 内；长尾数据下 Top-10 召回 ≥ 9/10。

## T6　HTTP 接口（flowlens/api/，flowlens/__main__.py）

- 路由：`POST /v1/ingest`；`GET /v1/topk?window=60s&k=10&by=url|user&at=<ms>`；`GET /v1/stats?window=60s`；`GET /v1/topk/stream?window=60s&k=10&interval=1s`；`GET /healthz`。
- 参数解析成一个 `QueryParams` dataclass，由它的类方法 `from_request` 完成校验：window 接受 `60s`、`2m` 或纯数字秒，范围 1~600；k 范围 1~100；by 用 `Dim.parse`；at 在未来返回 400。
- 错误处理只写一处：异常类已定义在 `flowlens/errors.py`（`BadRequest`、`TooOld`、`Overloaded`、`PayloadTooLarge`、`UnsupportedEncoding`，每个类带 `status` 属性），model、engine 也已引用它，api 层直接引用、不要另建一份。一个 aiohttp 中间件统一把异常转成 `{"error": "..."}` 和对应状态码。各接口只抛异常，不自己写状态码。方法不对由 aiohttp 返回 405。
- topk 响应：`{window_ms, from, to, by, k, final, degraded, items:[{key, count, err}]}`。

验收：e2e 测试启动真实服务器跑通每个接口和每个错误码。

## T7　SSE（flowlens/api/stream.py）

- 参数相同的订阅者共用一个定时任务和计算结果；interval 限制在 200 ms ~ 60 s。
- 帧格式：`id: <to 的毫秒>`、`event: topk`、`data: <JSON>`；连接时先发 `retry: 3000`；每 15 秒发 `: ping`。
- 重连带 `Last-Event-ID` 时，用回放补发错过的帧，最多 30 帧，再接实时。
- 每个客户端只保留最新一帧（容量 1 的 `asyncio.Queue`，满了先取出旧的再放新的）；写超时 5 秒断开；最多 256 个连接，超出返回 503。

验收：慢客户端不会让服务端内存增长；断线重连后 id 连续。

## T8　降级、内存读数与 healthz（flowlens/degrade.py，flowlens/memstat.py）

- memstat：优先读 `/sys/fs/cgroup/memory.current`（cgroup v2），其次 v1 的 `memory.usage_in_bytes`，都读不到时读 `/proc/self/statm` 的 RSS。
- degrade：每秒根据待处理事件占用率、处理延迟、内存计算级别（见 DESIGN.md §3.6），进入和退出用不同阈值。第 2 级的采样率取 1/2、1/4，权重为整数。暴露 `level` 和 `degraded`。
- healthz 返回 `{ok, events_ingested, memory_bytes, degraded}`，另附 deduped、late、invalid 计数。

验收：用假的内存读数和占用率驱动，覆盖各级进入与退出。

## T9　字符串驻留表与内存加固（flowlens/intern.py）

- 驻留表：哈希 → 字符串，硬上限约 12 万条；线段树冻结时，没有名额的 key 不进入名单，次数并入 u。
- 用 `docker run --memory=256m` 跑 10 分钟全随机 URL 与 user 的流量，记录峰值内存。

## T10　离线精确解、压测与实验（tools/exact.py，tools/loadgen.py，tests/e2e）

- loadgen：长尾、接近均匀、全随机三种分布；可选 gzip、乱序、迟到、重复；可指定速率与并发。
- exact：读同一份 NDJSON，输出任意区间的精确 Top-K、去重人数、p95。
- e2e 边界用例 14 条（DESIGN.md §7 列出的全部）。
- 跑实验，把结果填进 DESIGN.md §7 的"待补实验"。

## T11　性能

- 端到端压测，目标 20 万事件/秒；用 `py-spy` 或 cProfile 找瓶颈。统计核心单核基准约 25 万/秒（`tools/bench_core.py`），JSON 解析与 gzip 每条约 0.7 微秒。
- 补上 DESIGN.md 提到但 T3 简化掉的部分：窗口汇总 Count-Min 与默认窗口的候选堆。

## T12　规划中（DESIGN.md §9）

节点废弃状态与可选的历史落盘。主功能和实验都完成后再做。
