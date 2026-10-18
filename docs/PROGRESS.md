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
| T9 驻留表 | 未开始 |
| T10a 压测工具、离线精确解、边界用例 | 已完成，等审查 |
| T10b 实验场景与端到端粗测 | 已完成，等审查 |
| T10c 调试接口与控制台页面 | 未开始 |
| T11 性能 | 未开始 |

## 进行中

T11a 进行中（按 docs/instructions/T11a.md）。**上下文快满，先停下**，修 bug 与重跑留待下一会话。

### 已完成

1. **剖析脚本 `tools/profile_server.py`**（serve + run 两种模式）与测试 `tests/test_profile_server.py`（9 条，全过；全量 `pytest -q` 298 passed）。脚本按任务要求改为**单次固定速率测量**：20 万/秒、每批 5000、不 gzip、预热 30 s + 测量 30 s。机制：容器内起真实服务（`memory_reader` 传假读数 50 MB/256 MB，永不因内存降级）；写入路径与 tick 的函数做包装计时（替换模块属性/类方法，wall 与 CPU 双口径）；`gc.callbacks` 记回收代数/耗时/对象数；每 100 ms 小任务测事件循环延迟；每秒读真实工作集、超 220 MB 提前结束。开发中发现并修掉的关键 bug：
   - **GC 回调恒空**：`gc.START`/`gc.FINISH` 在 CPython（3.11/3.12）里根本不存在，回调阶段是字符串 `'start'`/`'stop'`，原比较抛 AttributeError 被 gc 静默吞掉。已改字符串判断并有测试锁定。
   - **异步阶段 CPU 双重计数**：`read_body`、`refresh_snapshot_async` 是 async，`process_time` 差值会把"协程挂起期间事件循环干的其他活"算进来（实测顶层阶段之和 = 进程 CPU 的 184%）。修正：异步 IO 阶段（`read_body`）CPU 归因按 0、只报 wall，排除出"顶层阶段 CPU 之和"；快照改包**同步**的 `_dim_tops` + `_snapshot_stats`。修正后校验通过（见下）。
   - **窗口事件数**：每条事件的 CPU 时间改用测量窗口内接受数（预热期也写入，用累计值会稀释）。
2. **正式跑过一次**（WSL docker，服务 0 号核、256 MB、loadgen 压 1-6 核）：结果在 `results/T11a/`（本次从 WSL `/tmp/flowlens_t11a/` 全量复制，含早期四组数据 `g1`~`g4`、短验证 `shortval`、本次 `run_200k.json`）。

### 单次 20 万/秒 测量结果（`results/T11a/run_200k.json`）

**参数**：20 万/秒、每批 5000、不 gzip、预热 30 s + 测量 30 s；假内存读数（不降级）。

**两个总前提**：
- **20 万/秒超出单核能力，没喂满**：服务核 CPU ~85%，实际接受速率远低于 20 万/秒（见窗口 bug 说明）。按每条 ~10.7 μs CPU 估算，20 万/秒需要 ~2.1 个核。
- **⚠ 测量窗口 bug（未修）**：请求测量 30 s，实际 `measure_sec=46.96 s`。原因：测量循环用 `sleep(0.5)×60` 计时，满载下每次 sleep 都被延迟触发（循环延迟 p50 68 ms），名义 30 s 拖成 47 s；而 loadgen 只发 70 s（预热 30+测量 30+余量 10）→ 测量窗口最后 ~10 s 没有流量，把**基于窗口时长**的 `accepted_rate`（79 426/s）与 `cpu_pct_of_core`（84.7%）**稀释**。每条事件的 CPU 数字不受影响（分子分母都只含流量期）。**修复方案**：测量循环改墙钟 deadline（`deadline = monotonic() + measure`，循环条件改 `monotonic() < deadline`），待下一会话改后重跑。

**分阶段 CPU 占比（可靠，与窗口时长无关；进程 CPU 39.78 s）**：

| 阶段 | CPU 占比 | 每条事件 | 备注 |
|---|---|---|---|
| **validate** | **49.5%** | **5.28 μs** | 最大头；逐条校验 + URL 归一化 |
| **ingest**（合计） | 29.1% | 3.10 μs | 引擎写入 |
| └ dim_write（写秒桶） | 12.7% | 1.35 μs | 两维 DimBucket.observe |
| └ dedup（Bloom 去重） | 7.4% | 0.79 μs | check_and_set |
| └ hash（key_hashes） | 5.3% | 0.57 μs | 去重 + 两维 key 哈希 |
| └ classify + groupby | 0.4% | 0.04 μs | |
| **tick_snapshot**（快照） | 5.2% | 0.56 μs | 每秒一次 |
| load_array（orjson） | 7.7% | 0.82 μs | JSON 解析 |
| tick_finalize（定稿） | 1.6% | 0.17 μs | 每秒一次 |
| decode（解压） | 0.01% | ~0 | 不 gzip，符合预期 |
| read_body | 0（IO） | — | 收包等待 wall 275.9 s，不计 CPU |

- **顶层阶段 CPU 之和 = 进程 CPU 的 93.1%** → "各阶段之和 ≈ 服务核 CPU 时间"校验通过；余下 ~7% 为 aiohttp 协议处理、JSON 响应序列化等未包装部分。
- **每条被接受事件平均 CPU ≈ 10.7 μs**（可靠值；T10b 端到端 20 μs 的差距由此拆开了）。

**与基准对比**：
- bench_engine 纯统计核心 ~4.1 μs/条 vs 端到端 ingest 3.10 μs/条 → **统计核心没变慢**，与基准一致。
- **validate 5.28 μs/事件是最大异常**：本地热缓存基准 0.93 μs（Python 3.14）、容器空闲 1.84 μs（Python 3.12）、**满载 5.28 μs**（比容器空闲还慢 ~2.9×）。N_URL=2 万 < 缓存上限 10 万，热缓存理应命中，满载下变慢的根因未查明（方向：归一化缓存命中率 / 满载下其他干扰）。**首要优化点。**

**其他观测**：
- 事件循环延迟极高：p50=68 ms、p99=347 ms、max=405 ms —— 直接决定查询 p99 超标；来源是 validate+ingest 的大同步块、每秒快照、GC 停顿。
- GC：每秒 16.7 次回收；第 2 代仅 4 次但**单次最长 31.7 ms**（gen2_total 80.8 ms）——一次 gen2 ≈ 30 ms 停顿。
- 内存：峰值 219.5 MB（贴 220 MB 阈值未触发提前结束），结束 200 MB。
- loadgen 侧：客户端 CPU 252%（压测工具自身吃紧）、写入 p99 1.5 s、503 重试 795 次、接受率 99.1%（窗口被稀释，真实流量期接受率更高）。

### 待办（下一会话）

1. **修测量窗口 bug**：`tools/profile_server.py` 的测量循环 `sleep(0.5)×N` 改墙钟 deadline，然后重跑拿到干净的窗口内速率与核占用，确认单核可持续速率量级（预期 ~8 万/秒：1/(10.7 μs)×效率）。
2. **查 validate 满载慢 2.9× 的根因**：满载下归一化缓存命中率（可加包装在 `normalize_url` 上计数命中/未命中），或内存压力/其他干扰。
3. 写 `docs/perf/T11a-report.md`（报告六节：分阶段表、与基准对比、GC、事件循环延迟、无降级干扰的最高可持续速率、优化方向清单）。
4. 收尾：`docker build --target test`、更新 PROGRESS.md"未解决的问题"与"基准数据"、AI_LOG.md、提交（不推送）。

---

T10b 已完成（按 docs/instructions/T10b.md），汇报后停下等审查。T10b 内容：

1. **第零节前置修正（4 项，全部完成）**：(a) 容器内存构成实测 + 降级读数改"工作集"（`memory.current − inactive_file`，`/healthz` 加 `memory_current_bytes` 对照），推翻 T7b"保持原值"决定（见"已定的决策"）；(b) Dockerfile 加 `test` 阶段（与正式同基础同依赖 + pytest，跑全量快测试）、constraints.txt 在 python:3.12-slim 重新生成（3.12 比 3.14 多 `typing_extensions`）；(c) 修 flaky 测试——SSE 的 `test_shared_computation_same_params`/`test_recompute_at_most_once_per_second` 改 FakeClock 手工驱动网格点（连跑 20 次全过），`test_all_missing_no_fake_data` 改 monkeypatch 固定替身；(d) `.gitignore` 补 `build/ dist/ results/`。
2. **第一节（压测流量更真实）**：(a) `flowgen.py` 约一半 key 模板含 `{id}` 段（`/api/order/{id}/x{i}` 等），`raw_url` 把 `{id}` 换成随机数字/UUID/长十六进制并按比例加 scheme+主机名，保持 `normalize_url(raw_url(key))==key`、全池注入性验证通过；(b) `model.py` 归一化缓存先剥 query/fragment 再查（`str.find` 找第一个 `?`/`#`），真实流量组 `bench_model` 每条约 **3.87 μs → 0.50 μs（约 7.7×）**，缓存命中组不受影响（1.80 μs）。
3. **第二节（实验脚本）**：`tools/experiments.py`（S1/S2/S3/S5，`--target docker|local`、docker 目标 `--memory=256m --memory-swap=256m --cpuset-cpus` 固定单核、端口 9000 避 WSL2 保留段）、`tools/summarize.py`（读结果目录打印 Markdown 汇总）、`docs/EXPERIMENTS_FORMAT.md`（结果文件字段，供 T10c 控制台回放）、`tests/test_experiments.py`（纯函数单测）、`tests/e2e/test_experiments_smoke.py`（slow 冒烟）。按补充要求把"工作集起始/峰值/结束 + 降级级别是否变化、正常负载进 3 级单独标出"做进 timeline 与 summarize。
4. **第三节（第一次端到端粗测，WSL docker）**：S1（不压缩 + gzip）、S2、S3（长尾 3 万/秒×5 分钟）、S5 全部跑完，结果在 `results/20261018_190021/`。**最大可持续吞吐 = 5 万/秒**（接受率≥99%、无 503、写入 p99<500ms 的最高级；10 万/秒起 p99 升到 ~1.2s、30 万/秒 503 4%）。混合负载（3.5 万/秒）下各查询 p99：topk_default 195ms、topk_replay 156ms、stats 171ms，**均远超"p99<50ms"目标**。过载（S5，1.5×=7.5 万/秒×60s）进入第 1 级、停发后 ~1s 回 0 级，无崩溃。

**⚠ 主要发现：内存随持续写入增长，3 万/秒×5 分钟就进第 4 级，精度数据因此无效。** 各场景工作集（峰值 / 进级时刻）：S1_plain 204.6 MiB（进 3 级）、S1_gzip 190.6（1 级）、S2 210.5（3 级）、**S3 229.4（3 级 @90s、4 级 @200s）**、S5 190.1（1 级）。内存增长由**持续写入填满 600s 保留期**驱动（与瞬时速率无关）：S3 累积 588 万条后工作集 ~218 MiB、长期贴在第 3~4 级，结束时抽样全部落在降级期（服务返回空 topk / 回放暂停），召回 1.0、误差 0 是**空响应的假象**。**该精度数据无效，留 T9b 解决内存问题后重做。** 且与支线 C1 的内存账本矛盾：C1 进程内模拟同样 3 万/秒×11 分钟常驻增量仅 ~97 MB，容器实测工作集 ~229 MiB，差 ~90 MB（疑为请求解析碎片 / 并发瞬时峰值 / 某处泄漏），T9b 专门查。本任务**不做任何内存优化**。

5. **收尾修复**：`exact.compare` 修 bug——服务返回空、精确答案非空时召回记 **0**（原来分母为 0 记 1.0，会把降级期空响应误判成完美精度）；S3 每条抽样记录 `status` 与 `degraded`，分 `valid`/`degraded`/`rejected` 三桶，精度只由 valid 统计（`summarize.py` 同步、兼容旧格式并标注不可信）；`summarize.py` 修 `client_cpu_pct` 显示（原把已是百分数的值当分数显示成 12990%）。

**验证**：Windows `pytest -q` **290 passed, 2 skipped, 9 deselected**、`-m slow` **9 passed**；Docker（WSL，Python 3.12）`docker build --target test` **291 passed, 1 skipped, 9 deselected**。基准（Windows，同机对照 T10a 列，见"基准数据"）：`bench_model` 真实流量组 3.87→0.50 μs/条（归一化缓存修改生效）、缓存命中 1.45→1.80 μs/条（本机波动、未触及热路径）、其余持平。**结论：除归一化缓存显著变快外，未使任何路径变慢。**

---

T10a 已完成（按 docs/instructions/T10a.md），汇报后停下等审查。T10a 内容：

1. **事件生成 `tools/flowgen.py`**（可导入模块 + CLI）：三种确定性分布（长尾 Zipf s=1.1/0.9、均匀、全随机不重复）；URL/用户用整数编号表示，`key_of(i)` 是归一化 URL（`/api/item/x{i}`，末尾 `x{i}` 以字母开头、不命中 {id} 归一化规则），`raw_url(key, rng)` 只加 query 与 fragment（归一化整体剥掉）→ `normalize_url(raw)==key`（压测数据因此既测 URL 归一化、又让离线精确解按编号分组与服务按 key 分组对上同一个划分）；bytes 对数正态（p95≈10 KB）；迟到 61.5~119.5s、未来 5.5~30s、正常 0~3s，都避开 60s/5s 判定边界 ±0.5s（"服务按收到时刻、精确解按发送时刻"两口径结论一致）；可配迟到/未来/跨批重复/批内重复/非法比例；`ArchiveWriter` 只存被接受（202）批次的整数列（npz）、多 worker 按 shard 写、`finalize_archive` 合并 meta.json。
2. **发送器 `tools/loadgen.py`**：多进程（spawn）× 每进程 C 并发连接（aiohttp `trust_env=False` 避开 WSL 自动代理）；总速率平均分给各进程、按"理想发送时刻"发批（有界队列背压）；503 按 Retry-After 重发同一批、重试次数可配；每秒一行 JSON（发送/接受/拒绝/错误 + 延迟 p50/p95/p99，按"发起时刻"归秒、每批只计一次）、结束一行汇总（含客户端 CPU 占比，判断压测工具是否成瓶颈）。
3. **离线精确解 `tools/exact.py`**：读存档按服务规则求"被接受集合"（去非法/未来、按 **sent_ms** 判迟到、event_id 去重保留首次）；按 ts 排序建索引，任意毫秒区间 `[from,to)` 查 topk（并列按 key 字典序）、distinct_users、quantile_bytes；`compare()` 算召回（并列不算错）、计数相对误差、真实值是否落 [lower,upper]。**分位数口径**：按秩取值、目标秩 R=⌊q(n−1)⌋+1（1 起算）= numpy `quantile(x,q,method="lower")`，与 `sketch.DDSketch.quantile` 的秩定义完全一致（DDSketch 在同一秩上返回桶代表值，精确解返回该秩真实值——位置相同、取值精度不同）。
4. **边界用例 `tools/boundary_cases.py`**（18 条，题目自测脚本）：`tests/e2e/test_boundaries.py`（进程内 TestServer）与 `tools/selftest.py`（对运行中服务 --url、打印表格、任一项失败退出码非 0）共用同一套 `CASES` 与 `Target` 抽象。
5. **工具自测 `tests/test_tools.py`**：生成确定性、分布形状（长尾前 10 占比 >30% vs 均匀 <6%）、全随机不重复、`raw_url` 归一化、时间戳避边界、存档往返、精确解对照最朴素逐条实现（含毫秒边界）、compare 召回并列口径、发送器 503 重试+存档+逐秒/汇总 JSON 格式。
6. **`/v1/stats` 支持 `at`**（`query.py`/`app.py`）：原 `/v1/stats` 只收 window、默认恒读快照；改为与 `/v1/topk` 同口径——不带 `at` 且 60s 读快照，带 `at`（或非 60s）走回放合并概要、at 早于保留期同样 422、第 1 级同样暂停。原因：边界用例"区间无数据 p95 为 null"需对"已运行"的服务指向一个无数据的过去区间（见 DESIGN_TODO #16）。

**WSL 三步实跑**（端口用 9000——本机 WSL2 的 80xx 段 bind 报 98 被保留，9000/12345 可用）：服务启动（healthz ok、内存读数 ~105 MB）→ loadgen 4 进程 ×8 并发、2 万/秒长尾 60s、gzip、存档 → **120 万条 100% 接受、0 拒绝 0 错误、接受速率 19670/秒、客户端 CPU 48%、延迟 p50 73ms/p95 132ms/p99 151ms** → selftest **18/18 通过** → exact.py 最近 60s 精确 Top-10 与 `/v1/topk`（同 at）对比：**召回 1.000、平均相对误差 0.18%、最大 0.27%、真实值全部落在 [lower,upper] 内**（如 x0 精确 171371 vs 服务 171133）。

**验证（两平台均全过）**：Windows `pytest -q` **263 passed, 5 deselected**、`-m slow` **5 passed**；WSL（Python 3.14）`pytest -q` **262 passed, 1 skipped**（1 skip=仅 Windows 的 memstat 真实读数）, 5 deselected、`-m slow` **5 passed**。

T8b 已完成（按 docs/instructions/T8b.md）：
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
- ~~内存读数保持 memory.current 原值，不扣除页缓存。理由：评测以是否超出 --memory 上限判定，内核判断 OOM 用的就是 memory.current，这个口径更保守，适合做降级判断。~~ **（T10b 已被取代，见下条）**
- 降级判断与 /healthz 的 `memory_bytes` 改用**工作集** = memory.current − memory.stat 的 inactive_file（cgroup v1 为 memory.usage_in_bytes − total_inactive_file）；/healthz 另加 `memory_current_bytes` 字段保留原始读数便于对照；读不到 memory.stat 时退回 memory.current，无 cgroup 上限时仍用进程读数（T10b）。理由：实测容器（--memory=256m）内存构成——空闲时 memory.current ≈ 168.6 MiB，其中 anon ≈ 85.5、file ≈ 77.8（active ≈ 17.5 / **inactive ≈ 60.4**），即约 36% 是页缓存；以 5 万/秒写入 30 秒后 memory.current ≈ 248.2 MiB（峰值 256 MiB 已摸到上限），anon ≈ 164.0（增长 78.5 MiB 是真实常驻）、file ≈ 78.2（几乎不变、inactive ≈ 60.4）。file 部分基本全是不活跃页缓存，内存紧张时会被内核优先回收、不导致 OOM，把它算进读数会让降级控制器虚高触发（空闲即 66%，离第 3 级阈值 78% 只差 12 个百分点，稍有负载就降级）。改后的口径（工作集）与 docker stats、Kubernetes 驱逐判断一致：空闲 ≈ 108 MiB（42%）、写入 30 秒后 ≈ 184 MiB（72%），贴近真实常驻（anon + active_file），降级判断不再被页缓存干扰。T7b 的决定当时是为了"保守"，实测表明它会让实验全程处于降级状态、干扰结果，故推翻。
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
- 压测工具与精确解的口径（T10a）：事件用**整数编号**表示（URL `key_of(i)=/api/item/x{i}`、用户 `user_of(i)=u{i}`），存档只存整数列、字符串可由编号按 `KEY_RULE_VERSION` 重建——存档体积与事件数线性、与字符串长度无关。发送的原始 URL 只在 key 上加 **query 与 fragment**（归一化整体剥掉），保证 `normalize_url(raw)==key`：压测数据既覆盖 URL 归一化路径，又让"离线精确解按编号分组"与"服务按归一化 key 分组"落到**同一个划分**（否则精确解与服务对不上、无法验证召回）。时间戳避开判定边界 ±0.5s（迟到 61.5~119.5s、未来 5.5~30s），使"服务按收到时刻、精确解按发送时刻"两口径结论一致。
- 离线精确解的**迟到判定按 `sent_ms`**（该批发送时刻，`ts < sent_ms − 60s`），不是存档读取时刻（T10a）：精确解离线重放、无法知道服务真实收到时刻，用发送时刻是它所能拿到的最接近值；因时间戳已避开边界 ±0.5s、收-发延迟远小于余量，与服务按收到时刻判定的结论一致。记入 DESIGN_TODO #18。
- 精确解 `quantile_bytes` 的分位数口径（T10a）：按秩取值，目标秩 R=⌊q·(n−1)⌋+1（1 起算）= numpy `quantile(x, q, method="lower")`，与 `sketch.DDSketch.quantile`（目标排名 ⌊1+q(n−1)⌋）**同一套秩定义**。DDSketch 在同一秩上返回的是**桶代表值**（相对误差 ≤ α），精确解返回该秩的**真实值**——两者取"同一个位置"、比较的是取值精度而非位置。所以"服务 p95 ≈ 精确 p95"是同一秩上的近似关系。
- `compare()` 的召回口径（T10a，沿用既有决策）：返回的 key 的**真实次数 ≥ 精确第 k 名的次数即算命中**（并列不算错），分母是服务返回条数；另算计数相对误差、真实值是否落在 `[lower,upper]`。
- `/v1/stats` 支持 `at`（T10a）：与 `/v1/topk` 同口径——不带 `at` 且 window=60s 读快照；带 `at`（或非 60s）走回放合并概要，`at` 早于保留期同样 422、第 1 级降级同样暂停。原因：边界用例"区间无数据 approx_p95_bytes 为 null"需对**已运行**（默认窗口有数据）的服务指向一个无数据的过去区间，必须能用 `at` 回放（见 DESIGN_TODO #16）。`stats(window_ms, at_ms=None)`，`at_ms=None` 时行为与原来完全一致。
- 镜像的 Python 版本（3.12）是运行环境的唯一标准，依赖约束（constraints.txt）在 python:3.12-slim 容器环境中生成（安装依赖后 pip freeze，含 3.12 需要而 3.14 不需要的全部间接依赖，如 typing_extensions）；收尾检查以 `docker build --target test`（与正式镜像同基础同依赖、加 pytest 后在 3.12 上跑全量快测试）为准，其余检查按需进行（T10b，规则见 CLAUDE.md"每个任务结束时"）。原因：WSL 测试环境是 Python 3.14、constraints 原先在 3.14 上生成（缺 3.12 的 typing_extensions），测试从未在镜像的真实运行环境（3.12）里执行过。
- 边界用例 18 条、两种形式共用一套定义（T10a）：`tools/boundary_cases.py` 的 `CASES` + `Target` 抽象（进程内用 TestClient、远端用 ClientSession），`tests/e2e/test_boundaries.py` 与 `tools/selftest.py` 都调 `run_all(t)`。题目原列 14 条，实际补到 18 条（gzip、解压炸弹、SSE 重连、区间无数据 p95 为 null 等，见 DESIGN_TODO #17）。
- 多 worker 的 event_id 互不冲突（T10a）：每个 worker 用不相交的序号区间（`seq_offset = wid × SEQ_STRIDE`，SEQ_STRIDE=10⁸），event_id=`e{seq}` 全局唯一；worker 随机种子 = `seed + wid`。存档按 shard 写、`finalize_archive` 把各 shard 文件按 (shard,seq) 排序合并——同一 worker 内按发送序、跨 worker 的 id 不冲突，"保留第一次出现"去重直接按此序。
- 本机 WSL2 的 80xx 端口段 bind 报 errno 98（被保留，`ss` 里也看不到占用者），9000/12345 等可用（T10a 实测）：实验脚本端口改用 9000。这不是服务问题，是开发机 WSL2 环境特性；评测容器 `--memory` 启动走 8080 不受影响。

## 基准数据

各任务结束时重跑全部测试与性能基准并追加一列（以后每个任务都一样）。每项各跑 3 次取中位（单次波动大）。"上一版"为 T6c 前后对照；"T7b"为提交 `d2d5c8e`；"T8"、"T8b"分别为对应任务结束时的本机（T8b 机器）中位数。

| 项目 | 上一版（T6c 前后） | T7b | T8（中位数） | T8b（本次，Windows 中位数） | T10a（本次，Windows 中位数） | T10b（本次，Windows 单次） |
|---|---|---|---|---|---|---|
| pytest -q | 197 passed, 5 deselected | 226 passed, 5 deselected | 247 passed, 5 deselected | 250 passed, 5 deselected（WSL：249 passed, 1 skipped, 5 deselected） | 263 passed, 5 deselected（WSL：262 passed, 1 skipped, 5 deselected） | 290 passed, 2 skipped, 9 deselected（Docker 3.12：291 passed, 1 skipped, 9 deselected） |
| pytest -q -m slow | 5 passed | 5 passed | 5 passed | 5 passed（Windows 与 WSL 均 5 passed） | 5 passed（Windows 与 WSL 均 5 passed） | 9 passed（新增 4 条 e2e slow 冒烟） |
| bench_core 长尾 | 约 20 万～30 万 事件/秒（波动大） | 约 30.5 万/秒（波动上沿） | 约 22.6 万/秒（218/236/226） | 约 18.5 万/秒（197/185/185） | 约 26.4 万/秒（282/264/243） | 约 30.0 万/秒 |
| bench_model（缓存命中） | 约 1.68 μs/条 | 约 1.36 μs/条 | 约 2.22 μs/条（2.15/2.22/2.22） | 约 1.87 μs/条（1.87/1.92/1.76） | 约 1.45 μs/条（1.49/1.45/1.41） | 约 1.80 μs/条（本机波动，未触及该路径） |
| bench_model（真实流量，T10b 新增） | — | — | — | — | 改前 ~3.82 / 改后 ~0.50 μs/条 | 改前 3.87 / 改后 0.50 μs/条（归一化缓存剥离 query/fragment 生效，约 7.7×） |
| bench_engine 每批 1000 条合计 | 约 6.3 μs/条 | 约 4.72 μs/条 | 约 6.10 μs/条（5.84/6.10/8.27） | 约 5.84 μs/条（5.84/5.85/5.57） | 约 5.01 μs/条（5.01/4.83/5.84） | 约 5.24 μs/条 |
| bench_engine 每批 5000 条合计 | 约 4.9 μs/条 | 约 4.20 μs/条 | 约 5.39 μs/条（4.65/5.39/6.64） | 约 4.65 μs/条（4.58/4.79/4.65） | 约 4.15 μs/条（4.15/4.55/3.75） | 约 4.13 μs/条 |
| 单次定稿 | 中位约 23 ms，最大约 47 ms | 中位约 18 ms，最大约 32 ms | 中位约 23 ms，最大约 44 ms（一次 93 s 为调度停顿） | 中位约 25 ms（25.9/25.0/22.5），最大约 73 ms | 中位约 18.9 ms（18.1/18.9/19.8），最大约 37.7 ms | 中位约 18.2 ms，最大约 50.9 ms |
| 满载快照 refresh_snapshot | 中位约 59 ms，最大约 81 ms | 中位约 47 ms，最大约 53 ms | 中位约 58 ms，最大约 82 ms | 中位约 52 ms（52.3/52.3/51.5），最大约 76 ms | 中位约 48.0 ms（47.4/48.2/48.0），最大约 66.2 ms | 中位约 46.5 ms，最大约 57.3 ms |
| 完整 tick | 中位约 108 ms，最大约 137 ms | 中位约 82 ms，最大约 92 ms | 中位约 105 ms，最大约 135 ms | 中位约 116 ms（116/118/106），最大约 187 ms | 中位约 85.2 ms（85.8/84.6/85.2），最大约 113.1 ms | 中位约 83.0 ms，最大约 105.6 ms |

T10b 列与 T10a 列**同一台机器**（本机）采集，但 T10b 为**单次**（未取 3 次中位）。T10b 唯一触及热路径的改动是 `model.py` 的归一化缓存键（剥离 query/fragment），`bench_model` 真实流量组据此显著变快（3.87→0.50 μs/条）；`bench_model` 缓存命中组 1.45→1.80 μs 属本机波动（该路径代码未动）。`bench_core`、`bench_engine`、定稿/快照/tick 与 T10a 持平或更快（T10a 本机本就偏快时段），**未使任何路径变慢**。

### 端到端（Docker 256 MB）

WSL 的 `docker run --memory=256m`（服务固定单核、压测固定其他核，`--target docker`），结果目录 `results/20261018_190021/`。注意：查询 p99 在**接近/进入降级**的负载下测得，受每秒定稿 + 快照 + 内存降级影响，**不是**低负载下的稳态查询延迟。

| 项目 | 数值 | 备注 |
|---|---|---|
| S1 最大可持续吞吐（不压缩） | **5 万 事件/秒** | 接受率≥99%、无 503、写入 p99<500ms 的最高级；离 20 万/秒目标差 4 倍 |
| S1 最大可持续吞吐（gzip） | **5 万 事件/秒** | gzip 各级接受率 100%（0 拒绝），但高率 p99 更高（解压开销） |
| S1 各率写入 p99 | 5 万：375ms / 10 万：1185ms / 30 万：1505ms（不压缩） | 5 万级已偏高（客户端 4 进程×8 并发 + WSL 单核争抢）；服务 CPU 100%（单核打满） |
| S2 混合负载查询 p99 | topk_default 195ms / topk_replay 156ms / stats 171ms | 写入 3.5 万/秒 + 查询 + 20 SSE；**均远超 p99<50ms 目标**（S2 峰值工作集 210.5 MiB、进第 3 级，查询被定稿/快照与降级拖累） |
| S3 精度（3 万/秒×5 分钟） | **无效** | 内存增长进第 4 级，抽样全落在降级期（服务返回空 topk / 回放暂停），召回 1.0、误差 0 是空响应假象；留 T9b 解决内存后重做 |
| S5 过载（1.5×=7.5 万/秒×60s） | 进第 1 级、停发后 ~1s 回 0 级 | 无崩溃、无 503 反复（接受率 99.0%） |

**性能对比必须同机对照**：T8b 列在本机（本次运行）采集，与 T8 列同一台机器，故可直接对比。T8b 相对 T8：`bench_core 长尾` 22.6→18.5 万/秒、`完整 tick` 105→116 ms、`单次定稿` 23→25 ms——**T8b 未触及任何热路径**（改动是内存读数来源、`create_app` 加一个 reader 参数、`ingest` 采样分支复用已算好的 event_id 哈希、`ingest.py` 加一条错误信息），正常路径（sample_weight=1）的写入/快照/定稿代码与 T8 完全相同，数值差异在本机单次波动范围内（长尾吞吐波动历来很大，见"上一版"列 20 万～30 万）。`bench_model` 1.87 vs T8 的 2.22、每批 1000/5000 合计 5.84/4.65 vs T8 的 6.10/5.39，T8b 持平或略快。**结论：T8b 未使任何路径变慢**。

T10a 列与 T8b 列**同一台机器**（本机）采集。T10a 同样**未触及任何热路径**（改动是 `query.stats` 加一个 `at_ms` 参数、`app.py` 一处调用、`conftest` 加 `sys.path`、`test_api` 一处测试桩签名，其余全是 `tools/` 与 `tests/` 新增文件，不进入服务端运行路径），写入/解析/定稿/快照代码与 T8b 完全相同。T10a 列整体比 T8b 快（`bench_core` 18.5→26.4 万/秒、`完整 tick` 116→85 ms、`单次定稿` 25→18.9 ms），**这是本机单次/时段波动**（长尾吞吐历来 20 万～30 万摆动、`bench_model` 1.87→1.45），非 T10a 引入的优化。判读基准对比时应以"同一次采集内各指标是否自洽"为准，跨任务的机器/时段差不可当结论。**结论：T10a 未使任何路径变慢**。

## 未解决的问题

8. **【主要发现，T10b】内存随持续写入增长，3 万/秒×5 分钟就进第 4 级，S3 精度数据因此无效；且与 C1 账本矛盾，差 ~90 MB，T9b 专门查。**
   - 现象（`results/20261018_190021/`，工作集口径）：S1_plain 峰值 204.6 MiB（进 3 级）、S1_gzip 190.6（1 级）、S2 210.5（3 级）、**S3 229.4（3 级 @90s、4 级 @200s，结束后仍贴 4 级）**、S5 190.1（1 级）。
   - 关键：内存增长由**持续写入填满 600s 保留期**驱动，与瞬时速率无关——S3 累积 588 万条后工作集 ~218 MiB 并长期不回落；S5 虽然 7.5 万/秒（1.5×）但只跑 60s、累积少，只到 1 级。
   - 影响：S3 结束时服务处于 3~4 级，回放查询暂停（503）或返回空 topk，200 个抽样区间全部落在降级期，**召回 1.0 / 误差 0 / stats 全 null 是空响应的假象，精度数据无效**（`exact.compare` 已修：空服务答案对非空精确答案召回记 0，不再记 1.0；S3 抽样分 valid/degraded/rejected 三桶）。**精度实验留 T9b 解决内存问题后重做。**
   - 与 C1 账本矛盾：支线 C1 在**进程内**模拟同样 3 万/秒、持续 11 分钟，服务常驻增量仅 **~97 MB**；容器实测工作集 **~229 MiB**，差 **~90 MB**。怀疑方向（未定）：① 频繁解析大请求的内存碎片 / 分配器未退还；② 并发请求的瞬时峰值叠加；③ 某处对象泄漏。**本任务不做任何内存优化，T9b 专门调查。**

1. 查询延迟：每拍定稿约 23 ms（最长约 47 ms），快照在极端流量下约 60 ms，可能让查询 p99 超过 50 ms。等端到端压测后决定是否优化 segtree 或提前做窗口汇总。（T10b 端到端已证实：混合负载下各查询 p99 156~195 ms，远超 50 ms 目标，主要受定稿 + 快照 + 内存降级拖累。）
2. 内存尚未在 256 MB 容器里实测，估算常驻约 195 MB 加写入路径。T9 实测。
3. 吞吐：每批 5000 条约 20.5 万/秒，每批 1000 条约 16 万/秒，未计 HTTP 开销。T11 优化。
4. T9 的字符串驻留表与冻结的 segtree.py 冲突，开始 T9 前由用户决定做法。
5. ~~`memory_bytes` 在 WSL 里读数虚高~~（T7 前置小修已解决）：memstat 现在只在"当前进程所在 cgroup 设了内存上限"时才用 cgroup 读数，否则回退进程常驻集（statm）。WSL 的 cgroup 无上限（整个发行版共享）→ 回退 statm，读数不再虚高；评测容器 --memory 启动带上限 → 仍用 cgroup，读数准确。
6. SSE 默认 60 秒窗口的帧目前走回放（collect_parts + top_k），没有复用刷新快照。是否改为复用快照，等 T10 压测看过 CPU 占用后在 T11 决定。
7. ~~全量 pytest 逐测内存增长 ~45 MB（Linux 上把服务打进第 4 级）~~（T8b 已定位并解决）：根因是 **aiohttp 的全局中间件缓存** `_cached_build_middleware`（`web_app.py`，`lru_cache(maxsize=1024)`）以 `(handler, match_info.apps)` 为键、`apps` 是含 `Application` 的元组，把每个 e2e 测试的 Application（连带 Engine ≈55 MB）用强引用挂进进程级缓存，逐测累积（实测 +54.6 MB/测、6 个测试 55→358 MB）。tracemalloc 确认是**对象未释放**（非分配器未退还）。修复：`tests/e2e/conftest.py` autouse fixture 每测后 `cache_clear()`（修复后 RSS 平台化 ~121 MB、存活分配净增 ≈0）。**生产单实例不受影响**（只建一个 app、缓存就一条、长驻运行）。另：T8b 顺带修复了"Windows 上 `memory_bytes()` 恒 0"（改用 psapi WorkingSetSize），此前第 3/4 级内存降级逻辑在本地从未真正执行过。

## 下一步

T10b 汇报后停下等待审查。审查通过后：优先 **T9b 内存专项**——查"未解决的问题"第 8 条（容器持续写入内存增长 ~229 MiB、与 C1 账本差 ~90 MB 的来源），解决后**重做 S3 精度实验**（S1/S2/S5 结果可保留复用）；再按原顺序 T10c 调试接口与控制台页面（界面参考 docs/prototype/console.html，直接读 `results/` 下各场景文件，格式见 docs/EXPERIMENTS_FORMAT.md）→ T9 驻留表 → T11 性能 → 收尾（统一修改设计文档、整理 AI_LOG、答辩准备）。**每个任务结束都必须在 WSL 跑全量 `pytest -q`**（T8b 起，已写入 CLAUDE.md）。
