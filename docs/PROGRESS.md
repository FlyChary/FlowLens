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
| T9 驻留表 | 已完成（支线 C3/T9b-A：intern.py 驻留表，engine.ingest 写桶前驻留 user、model.normalize_url 驻留 url 结果；C1 论证与冻结 segtree 无冲突） |
| T9a 内存账本（支线 C1） | 已完成（tools/mem_report.py 满载账本 + 方案测算，见 C1-report.md） |
| T9b 内存专项（支线 C2 调查 + C3 加固） | 已完成（C2 定位"非泄漏、碎片+并发"；C3 加固 malloc_trim + 驻留 + glibc mmap/trim 阈值固化进 Dockerfile，见 C2/C3-report.md） |
| T10a 压测工具、离线精确解、边界用例 | 已完成，等审查 |
| T10b 实验场景与端到端粗测 | 已完成，等审查 |
| T10c 调试接口与控制台页面 | 调试接口已完成（支线 B1：GET /debug/structure，FLOWLENS_DEBUG=1 门控）；控制台页面未开始 |
| Docker/部署（支线 B1+B1b） | 已完成（constraints.txt 固定版本、__main__.py 处理 SIGTERM、非 root、单阶段 pip install 装完删源码） |
| T11 性能 | 进行中（T11a-2 完成等审查；T11b 缩小范围：第零/一/二节完成等审查，第三～六节留作提交后优化） |
| P1 解析进程池（支线 lane-c） | 检查点一完成（第一～三节：parse_pool + 引擎拆分 + 写入并发限制 + 等价性/正确性测试），等审查；第四节容器实测留检查点二 |

## 进行中

**P1 检查点一已完成（支线 lane-c，分支 lane-c，已合并 origin/main）**：按 docs/instructions/P1.md 完成第一～三节。改动：新建 `flowlens/parse_pool.py`（ParsedBatch + parse_body + spawn 进程池 ParsePool，哈希自检、按实际核数算 worker 数、崩溃重建、关闭）；`flowlens/engine.py` 写入核心拆成 `ingest`（外层算三列哈希）→ `_ingest_core`（classify/去重/采样/写桶，哈希改由调用方传入、内部不再 key_hashes）+ 新入口 `ingest_parsed`；`flowlens/ingest.py` 拆 `_ingest_inproc`（现有路径，块 5000→2000）与 `_ingest_pooled`（解压后字节交子进程→切块写引擎），写引擎用 `asyncio.Semaphore(1)` 保护；`api/app.py` 关闭时关进程池（`_stop_parse_pool`）；`__main__.py` 在创建引擎**之前**启动并预热进程池。

1. **等价性（不依赖子进程，稳定跑）**：同一 body 经单进程路径（decode→validate→ingest）与进程池路径（decode→parse_body→ingest_parsed→2000 切块）写两个全新引擎，topk(url/user)/stats/计数器逐项相等，覆盖长尾/全随机/含非法+重复/全非法/gzip/NDJSON。`ingest_parsed(ParsedBatch)` 与 `ingest(parse_events)` 两入口亦逐项相等。
2. **正确性**：503 后重试不判重（预留在池提交前）、BrokenProcessPool→重建→恢复、worker 数计算（核数 1/2/4 + env 覆盖/非法/夹 [0,3]）、自检不一致→构造期 BrokenProcessPool、close 无残留子进程（e2e SIGTERM 测试 Linux-only）。
3. **验证**：Windows `pytest -q` **366 passed, 4 skipped, 9 deselected**（PYTHONHASHSEED=0，真子进程用例生效）；`-m slow` **9 passed**。基准（Windows 单次）：`bench_core 长尾` 约 29.9 万/秒（T10b 30.0 持平）、`bench_engine 每批5000 engine.ingest` 2.31 μs/条（拆分后未变慢）。**第四节容器实测（S1/S2/S3/S4/单核）留检查点二**。

（T11b 已缩小范围完成：第零、一、二节已做，汇报后等审查。第三～六节留作提交后的优化，见"未解决的问题"第 9 条。支线 B1/C1/C2/C3 的代码已合并进 main。）

---

T11b 已完成（**缩小范围**：按 docs/instructions/T11b.md，用户中途指示只完成第零、一、二节，第三～六节全部跳过，提交截止日所限），汇报后停下等审查。改动集中在 `flowlens/model.py`（归一化 + validate）与 `tools/`、`tests/` 的适配，未碰 Dockerfile、config.py、ingest.py、engine.py、__main__.py。

1. **第零节（子进程与文本文件编码）**：4 处 `subprocess` 文本模式调用加 `encoding="utf-8", errors="replace"`（`tests/test_packaging.py`、`tools/experiments.py` 的 `git_commit`、`tools/profile_server.py` 的 `_run` 与 loadgen Popen——后者本已带，核过未漏）；`tools/` 与 `tests/` 里读 `/proc` 的 3 处 `open()`（experiments/mem_probe/mem_report）补 `encoding="utf-8"`。grep `text=True`/`universal_newlines` 确认无遗漏。
2. **第一节（归一化未命中快路径）**：`_normalize_url` 改名 `_normalize_url_impl`、返回 `(结果, 是否 ID 替换)`；无 `"//"` 时跳过 scheme/多斜杠两个正则；ID 段判断改预编译正则 `_ID_SEG` 的 `fullmatch`（`[0-9]`/`[0-9a-fA-F]`，**不用 `\d`**，与既有 `isascii()` 口径一致）；发生 ID 段替换的路径不写缓存（稳定路径照常）。输出逐字节不变（参考实现对照测试）。
3. **第二节（validate 列式快路径）**：先判"全 dict"（否则走逐条慢路径）；全 dict 时一次性取五列、整列 `type is str/int` + 非空 + event_id 长度 + ts/bytes int64 范围检查；全合法走 `_validate_fast`（user_id 仅"非 ASCII 或 len>128"才按字节截断），任一不合法退回 `_validate_one_by_one`（保留原实现）。
4. **等价性测试**：改前 `normalize_url`/`_normalize_url`/`validate` 原样复制为 `ref_*`，覆盖全部表驱动用例 + 专门边界（Unicode/阿拉伯-印度/天城文数字、全角、恰好 15/16 位十六进制、大小写混合 UUID、截断 128 字节/多字节中间）+ flowgen 5000 条随机 URL + validate 各种非法混入（非 dict/缺字段/类型错/bool 冒充整数/越界/超长与多字节 user_id/int64 上界/≥2^64 转 float/空列表），逐列比较 EventBatch，全部一致。**未走退路**。
5. **适配**：`tools/profile_server.py` 的归一化命中率包装改包 `_normalize_url_impl`（透传 `(结果, replaced)`，计数口径不变）；`tools/bench_model.py` 重写（改前基准用原样复刻的 `_base_normalize`，报告未命中改前/改后耗时 + 命中率）。
6. **bench_model（Windows 单次）**：未命中路径改前 **3.57 μs** → 改后 **3.04 μs**（**1.17×**；无 `//` 子集 3.20→2.31 = 1.38×，flowgen 50% URL 带 `https://` 前缀仍跑正则故整体被稀释）；命中率改前/改后策略均 37.6%（带 ID 路径缓存与否都命中不了，改后策略的价值是"不占缓存槽位"，小池下未体现差异）。新/旧 `validate` 同 body 对照 3.95 vs 4.10 μs/条（1.04×）——列式快路径未拖慢全合法路径。
7. **验证**：Windows `pytest -q` 340 passed, 3 skipped, 9 deselected（+1 个 `test_memstat` Windows 专属 WorkingSetSize 偶发失败，与本任务无关、重跑 2 过 1 挂）；Docker（WSL，Python 3.12）`docker build --target test` **342 passed, 2 skipped, 9 deselected** 全过。

---

T11a 已完成（**T11a-2**，按 docs/instructions/T11a-2.md），汇报后停下等审查。仍只改 `tools/profile_server.py` 与报告，未动 `flowlens/`。

1. **测量窗口修正（修上一版的 bug）**：`tools/profile_server.py` 的测量循环从 `sleep(0.5)×N`（满载下被循环延迟拖成 47 s、窗口尾部无流量、稀释速率与核占比）改为**墙钟 deadline**（`deadline = monotonic() + measure`，循环 `monotonic() < deadline`）；loadgen 时长改覆盖"启动延迟 3 s + 预热 + 测量 + 10 s 余量"（73 s）。重跑后 `measure_sec` 实测 30.1 s / 30.1 s，干净。
2. **垃圾回收单独计账**：`gc.callbacks` 记每次回收的墙钟起止（`GC_SPANS`），各阶段包装时用 `_gc_overlap_ns` 算"调用时段与回收区间的交叠"、从阶段 wall/cpu 里各扣一次（新增 `gc_us` 字段），GC 单独汇总（按代分列：次数/总耗时/最长一次/占核）。修正后 load_array 单阶段扣了 61～131 ms 的 gen2 回收（原被算进解析开销）、tick_finalize 扣 4～23 ms——扣后才是各阶段自身开销。校验口径改"（扣后阶段之和 + GC 总耗时）/ 进程 CPU ≈ 1.0"（10 万 0.948、6 万 0.932）。
3. **归一化缓存命中率**：运行时包装 `flowlens.model.normalize_url`（总调用）与 `_normalize_url`（未命中才调），记命中/未命中、未命中平均耗时、未命中中"路径带 ID 段"的占比（`_id_seg_like` 口径与 `model._is_id_segment` 一致、有对照测试）。
4. **两组测量（WSL 容器，256 MB、服务 0 号核、假内存读数）**：`results/T11a/run_100k.json`、`run_60k.json`。
   - **10 万/秒**：核 **97.2%**（饱和）、接受 **72,911/秒**、每条 CPU **13.33 μs** → 单核上限 ~7.5 万/秒。
   - **6 万/秒**：核 **71.6%**（余量 40%）、全部接受 **59,973/秒**、每条 CPU **11.94 μs** → 单核上限 ~8.4 万/秒。
   - **单核可持续（无降级干扰）≈ 7.5 万～8 万/秒**。T10b 端到端只测到 5 万/秒，差距主要由当时的**内存第 3 级降级**（额度减半→503）解释。
5. **关键发现**（详见 `docs/perf/T11a-report.md`）：
   - **validate 占核 60%+（7.2～8.2 μs/条）是第一大头**，根因是**归一化缓存命中率只有 38.5%**——flowgen 约 50% 的 URL 路径带随机 ID（UUID/十六进制），路径每条都不同、缓存必未命中（未命中里 90.6% 是带 ID 路径）；未命中 4.4～5.0 μs/次。**首要优化点**（方向 1：未命中提速，预计每条省 ~2.5 μs / 核省 ~13%）。
   - **GC 总耗时 <0.6% 核（非吞吐瓶颈），但 gen2 单次 13～19 ms**（遍历长期存活对象），是事件循环 p99/max 的主要次级来源。优化方向 3：`gc.freeze()` + 调高阈值。
   - **事件循环延迟**：6 万/秒 p50 仅 66 μs（探针自同步、偏乐观）、p99 41 ms / max 61 ms ≈ 一个 **5000 条写入同步块**（wall 54～61 ms，`ingest.py` 块内不 yield）的长度；10 万/秒核饱和后 p50 就被拖到 204 ms。**最长停顿主因是写入块、次因是 gen2 GC**。
   - **查询 p99**：加查询探针实测 60 s 窗口 k=10（走快照路径）**p99 0.04 ms**（亚毫秒，快照预计算）；默认查询远达标。回放查询（带 `at`）计算本身 ~10～50 ms，p99<50 ms 的瓶颈是写入块的 54 ms 独占段，靠**缩短写入块**（方向 2：5000→~1000，一块 ~10 ms）解锁。
   - 优化方向清单（按收益排序，只列不实施）：① 归一化未命中提速（validate，model.py，非冻结）② 写入块 5000→~1000（事件循环延迟，ingest.py，非冻结）③ `gc.freeze()`+阈值（gen2 停顿，`__main__`）④ 快照改维护窗口汇总（tick 5%，engine.py，可能碰冻结 bounds）⑤ gen2 停顿与 tick 拆段（与③叠加）。
6. **测试**：`tests/test_profile_server.py` 12 条（新增 GC 交叠、`_id_seg_like` 对照、norm 包装行为不变、报告新字段），全过。
7. **踩坑**：① WSL2 宿主有间歇性负载抖动，6 万/秒组首两次（带查询探针）被污染（内存 8～14 s 就摸到 220 MB、loadgen 36～51% error），确认宿主空闲后第三次跑干净；② `read_body` 的 `cpu_us_measured` 巨大（满载 212 s）是"协程挂起期间事件循环干的其他活"，已按 0 归因、不计入 CPU（T11a 已修，本次沿用）。

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

### 支线 B1/B1b（Docker 与调试接口）

- 依赖版本由 `constraints.txt` 固定（B1b）：`aiohttp` 不同版本的行为差异已两次影响本项目（中间件写法、传输层自动解压），评测方构建必须装到测试过的版本。`constraints.txt` 写入 WSL 测试环境 `numpy`/`orjson`/`aiohttp` 及其全部间接依赖的确切版本；Dockerfile 与 run.sh 都用 `pip install -c constraints.txt .` 安装；`pyproject.toml` 的依赖保留版本范围、不写死。`pyproject.toml` 的包列表改自动发现（`[tool.setuptools.packages.find] include = ["flowlens*"]`），不再手工列举（曾漏 `flowlens.api` 导致 `pip install .` 装出坏包，`tests/test_packaging.py` slow 守护）。
- 容器优雅停机（B1b）：`__main__.py` 用 `loop.add_signal_handler` 为 SIGTERM 与 SIGINT 注册处理器、收到即置位停止事件、随后照常 `runner.cleanup()`（退出码 0）；Windows 不支持 `add_signal_handler`，保留 Ctrl+C（KeyboardInterrupt）路径、不报错。Docker CMD 直接用 `python -m flowlens`，PID 1 就是服务进程，不需要 B1 的信号转发 init（`tests/e2e/test_signals.py` 用真实子进程 + SIGTERM 守护）。
- Dockerfile 装完删源码（B1b）：`pip install -c constraints.txt .` 后 `rm -rf /app/flowlens /app/pyproject.toml`，运行时从 site-packages import、与 CWD 无关（从 /tmp 验证过）。非 root（`useradd --uid 10001 appuser`）、`HEALTHCHECK` 用 Python 标准库（镜像无 curl）、`ARG PIP_INDEX_URL` 默认为空（有值才 `--index-url`，不写死国内源）沿用 B1。
- 调试接口只读、现读现算（B1）：`GET /debug/structure`（`FLOWLENS_DEBUG=1` 门控，未设置走正常 404）的返回完全由只读数据现场组装，不修改任何统计状态、不缓存。时间划分复用 `engine._plan`（真实查询唯一的划分入口），环内整秒、线段树节点（`engine._cover`）、边缘秒都从 `_Plan` 读出，保证页面展示与真实查询一致；事件数统一取 url 维叶子的 DDSketch 总计数（`dd.n`）；秒桶"前 3 个 key"用 `TopK.top(n)` 公开接口。满载耗时用 slow 测试（`tests/e2e/test_debug.py::test_debug_perf_full_tree`）。

### 支线 C1/C2/C3（内存账本、增长调查、加固）

- 内存加固推荐组合 = **g5**（C3）：① Ticker 定期 `malloc_trim(0)`（默认每 10s，`FLOWLENS_MALLOC_TRIM_SEC` 可配、0/负数关闭、非 Linux no-op；`/healthz` 暴露 `malloc_trim` 只读字段）；② 字符串驻留（`flowlens/intern.py` 的 `Intern`，满则整体清空、上限 user 8192 / url 32768 为模块常量、**不进 config.py**；user 在 `engine.ingest` 写秒桶前 `np.fromiter` 驻留、url 在 `model.normalize_url` 返回前驻留结果——不改归一化逻辑）；③ Dockerfile base 阶段固化 `ENV MALLOC_MMAP_THRESHOLD_=131072 MALLOC_TRIM_THRESHOLD_=131072`（用户批准、例外放行；`docker run` 不显式 `-e` 也继承镜像 ENV）。实测（256 MB 容器、长尾 3 万/秒、32 连接）：g5 峰值工作集 **179.1 MiB**、246/246s **全程第 0 级**、接受率 100%（对比 g1 基线峰值 233.4、进 L4 拒写、接受率 87.8%）。
- 在途额度做成环境变量 `FLOWLENS_MAX_PENDING_BYTES`（C3，默认 4 MB 不变）：本负载下降到 2 MB 无增益也无额外 503（在途窗口不是内存瓶颈），故保留为实验变量、供"大请求突发"场景按需收紧。
- 内存增长**不是泄漏**，是"分配器保留已释放页（碎片）"+"并发瞬时峰值叠加"（C2 判定）：`gc.get_objects()` 全程平稳、`getallocatedblocks()` 震荡、MemWalker 各结构 `walked` 平台化——无对象结构持续增长。碎片证据：负载中 RssAnon 爬升、停发后基线 0 回落、`malloc_trim` 一停发就回收 ~60 MiB；`MALLOC_ARENA_MAX=2` 与 `PYTHONMALLOC=malloc` 均无改善（排除 arena 计数与 CPython 私有块分配器）。并发：32 连接比单连接多 ~45 MiB（in-flight 解析临时对象 + 每连接 aiohttp/TCP 栈 ~1.4 MiB）。**T9b-B（缩 Bloom/Count-Min，需改冻结 config.py）不需要**——g5 已全程 0 级、无内存压力可解决，且 C1 已证缩 Bloom/HLL 都是拿精度换内存。
- 内存读数口径：降级判断与 `/healthz` 的 `memory_bytes` 用**工作集**（T10b 已定）；C1/C2 的账本用"纯服务端稳态 RSS 增量"交叉核对，三层口径（对象图=存活、RSS 增量=容器看到的常驻、EventGen 驱动 RSS=被模拟工具污染）分开用，不混。
- 记账类工具的身份自检（C1 事故教训）：worktree 开发的独立 tools/*.py 必须显式把项目根插到 `sys.path` 最前（脚本模式 CWD 不在 path、conda editable install 不随 worktree 变化）；出数前打印 `flowlens.__file__` 自检，出现与写入路径模型矛盾的数字（如"每值仅 1 个对象"）立即停手排查。C1 首轮因 import 解析到主仓库、全部数据作废重跑。
- 探针自保（C2 事故教训）：被测系统已近内存上限时，测量必须让位于保命——`memory.current > 上限×0.80` 时跳过 `gc.get_objects()`+MemWalker 深度遍历（瞬时对象会把 current 推过上限、内核 OOM 杀掉探针本身）。

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

### 端到端 CPU 剖析（T11a-2，容器 256 MB、服务单核、假内存读数）

以下为**每条被接受事件的 CPU 时间**与**事件循环延迟**（容器内实测，非本机基准；墙钟 deadline 测量窗口，已扣 GC）：

| 项目 | 10 万/秒（核 97.2%） | 6 万/秒（核 71.6%） |
|---|---:|---:|
| 每条被接受事件 CPU 时间 | **13.33 μs** | 11.94 μs |
| 单核 CPU 上限（1/每条 CPU） | ~7.5 万/秒 | ~8.4 万/秒 |
| 实际接受速率 | 72,911/秒（核饱和） | 59,973/秒（全部接受） |
| 事件循环延迟 p99 | 397 ms（核饱和，队列积压） | **41.0 ms**（max 61 ms ≈ 一个 5000 条写入块） |
| 默认查询 p99（60s 窗口 k=10，走快照） | —（未开探针） | **0.040 ms**（亚毫秒，快照预计算） |

**单核可持续速率（无内存降级干扰）≈ 7.5 万～8 万/秒**（6 万/秒核才 71.6% 有余量、10 万/秒核 97.2% 饱和夹逼）。比 T10b 端到端测到的 5 万/秒高，差值主要是 T10b 当时服务处于内存第 3 级降级（额度减半→503）。`validate` 占核 60%+（归一化缓存命中率仅 38.5%，flowgen ~50% URL 路径带随机 ID 必未命中）是最大头与首要优化点；GC 总耗时 <0.6% 核但 gen2 单次 13～19 ms；事件循环 p99 的瓶颈是 5000 条写入同步块（54～61 ms）。详见 `docs/perf/T11a-report.md`。

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

9. **T11b 第三～六节（性能/内存收尾）留作提交后的优化**（T11b 因提交截止缩小范围，只完成第零/一/二节）：
   - **第三节 写入分块大小**：`ingest.py` 的 `CHUNK_EVENTS` 仍为 5000。需按 T11a 结论用新每条耗时重测 1000/2000/5000，选单块同步耗时 ≤15 ms 的最大块（预计能解锁回放查询 p99<50 ms）。C2 实测单批 5000→1000 还能把解析高水位从 ~58 降到 ~12 MiB，与查询 p99 同向。
   - **第四节 垃圾回收调优**：`gc.freeze()` + 调高 gen0 阈值 + Ticker 定期再 freeze，削 gen2 单次 13～19 ms 停顿（事件循环 p99/max 的次级来源）。
   - **第五节 内存收尾（T9b-B）**：~~归一化缓存上限 10 万→2 万、Bloom `per_gen` 容量（需用户批准改 config.py）~~ **已不需要**——支线 C3（T9b-A）用 malloc_trim + 驻留 + glibc mmap/trim 阈值固化（ENV）后，3 万/秒满载已全程第 0 级、无内存压力，T9b-B（拿精度换内存）不做了（见"已定的决策"支线 C1/C2/C3 与"未解决的问题"第 8 条的解决）。归一化缓存上限（`NORM_CACHE_CAP` 10 万）仍可按需下调，非必需。
   - **第六节 容器验证**：profile_server（6/10 万每秒）、S1（最大可持续吞吐）、S2（混合负载查询 p99）、S3（长尾精度）、S4（全随机工作集）留待提交后在容器（真实内存读数）上统一重跑（C3 用户指令：等合并后在最终代码上统一跑）。
   - T11b 已做的第零/一/二节（编码修正 + 归一化未命中快路径 + validate 列式快路径）只改 `model.py`/`ingest.py` 上游的校验归一化路径与编码，未触及写入分块/GC/内存上限，故上面各项仍按原方案进行。

8. ~~**【主要发现，T10b】内存随持续写入增长，3 万/秒×5 分钟就进第 4 级，S3 精度数据因此无效；且与 C1 账本矛盾，差 ~90 MB，T9b 专门查。**~~ **（支线 C2 调查 + C3 加固已解决）**
   - **原因判定（C2，容器探针三层读数）**：**不是泄漏**（`gc.get_objects()` 平稳、`getallocatedblocks()` 震荡、MemWalker 各结构 `walked` 平台化，无对象结构持续增长）。是**"分配器保留已释放页（碎片）"+"并发瞬时峰值叠加"**的组合：碎片证据 = 负载中 RssAnon 爬升、停发后基线 0 回落、`malloc_trim` 一停发就回收 ~60 MiB，且 `MALLOC_ARENA_MAX=2`/`PYTHONMALLOC=malloc` 均无改善；并发 = 32 连接比单连接多 ~45 MiB（in-flight 解析临时对象 + 每连接 aiohttp/TCP 栈 ~1.4 MiB）。**"90 MB 差距"对齐口径后实为 ~49 MiB 负载增量差**（①并发瞬时峰值 ~45 + ②glibc 保留比 MSVC 高 ~21 − ③存活结构差 −30 + ④工作集含 file 缓存 ~13），其余 ~43 是空载基线固有差（口径混用带入）。
   - **解决方法（C3，推荐组合 g5）**：① Ticker 定期 `malloc_trim(0)`（默认 10s）；② 字符串驻留（user 写桶前 / url 归一化结果）；③ Dockerfile 固化 `MALLOC_MMAP_THRESHOLD_=131072`/`MALLOC_TRIM_THRESHOLD_=131072`。实测（256 MB、长尾 3 万/秒、32 连接）：**峰值工作集 233.4 → 179.1 MiB、进 L4 拒写变全程第 0 级、接受率 87.8% → 100%**。
   - **S3 精度重做（g3 配置，T9b 首份有效精度数据）**：200/200 有效样本（degraded 0 / rejected 0）、Top-K url/user 召回均 **1.000**、计数误差 0.05%/0.09%、真实值 100% 落上下界、HLL 去重 0.92%、DDSketch p95 0.14%。C2 那次"召回 1.0/误差 0"的空响应假象已不复存在。
   - **待办**：g5（含 mmap ENV）配置下的最终 S3 精度数据，等合并后在最终代码上统一跑（C3 用户指令）；S4/S1/S2 容器验证见第 9 条第六节。

1. **查询延迟（T11a-2 已定位瓶颈，待优化实施）**：T10b 混合负载下各查询 p99 156~195 ms（当时受定稿+快照+内存降级拖累）。T11a-2 无降级、单核、分速率实测拆开了来源——
   - **默认查询（60 s 窗口、不带 `at`）走快照，p99 0.04 ms（亚毫秒）**，早已达标，不是问题。
   - **回放查询（带 `at`/非 60 s）**：计算本身 ~10～50 ms（`collect_parts + bounds.top_k`），6 万/秒核有余量（71.6%），但**事件循环延迟 p99 41 ms**——主因是 **5000 条写入同步块独占事件循环 54～61 ms**（`ingest.py` 块内不 yield），次因是 **gen2 GC 单次 13～19 ms**。所以回放查询 p99<50 ms 的瓶颈**不是 CPU 而是写入块的独占段**。
   - **待办（T11 优化，方向已排序，见 `docs/perf/T11a-report.md` 第六节）**：① 归一化未命中提速（validate 占核 60%，首要）② 写入块 5000→~1000（解锁查询 p99）③ `gc.freeze()`+阈值（削 gen2 停顿）④ 快照改维护窗口汇总 ⑤ gen2/tick 拆段。其中 ①② 是非冻结模块、风险低、收益最大。
2. ~~内存尚未在 256 MB 容器里实测，估算常驻约 195 MB 加写入路径。T9 实测。~~（支线 C1 进程内账本 + C2/C3 容器实测已解决，见第 8 条）
3. 吞吐：纯统计核心每批 5000 条约 20.5 万/秒（bench_core，未计 HTTP/validate）。**T11a-2 端到端（容器、含 HTTP+validate、单核）实测可持续 ~7.5 万～8 万/秒**——差距全部在 validate（占核 60%，归一化缓存命中率仅 38.5%）与 HTTP 层，统计核心本身没变慢。T11 优化方向见 `docs/perf/T11a-report.md`。
4. ~~T9 的字符串驻留表与冻结的 segtree.py 冲突，开始 T9 前由用户决定做法。~~（C1 论证冲突不存在：驻留发生在数据进引擎**之前**（`engine.ingest` 写秒桶前 / `model.normalize_url` 返回前），进引擎后各结构（秒桶堆、候选堆、叶子/节点 keys、快照）引用的都是同一字符串对象，冻结的 segtree 只是引用、无需修改。T9 已由 C3 落地，见"已定的决策"支线 C1/C2/C3。）
5. ~~`memory_bytes` 在 WSL 里读数虚高~~（T7 前置小修已解决）：memstat 现在只在"当前进程所在 cgroup 设了内存上限"时才用 cgroup 读数，否则回退进程常驻集（statm）。WSL 的 cgroup 无上限（整个发行版共享）→ 回退 statm，读数不再虚高；评测容器 --memory 启动带上限 → 仍用 cgroup，读数准确。
6. SSE 默认 60 秒窗口的帧目前走回放（collect_parts + top_k），没有复用刷新快照。**T11a-2 数据支持改复用快照**：60 s 窗口 k≤100 的查询走快照路径 p99 仅 0.04 ms（亚毫秒、预计算），而走回放的 60 s 帧要吃 `collect_parts + top_k`（~10～50 ms）并在事件循环里和写入块争抢。是否改在 T11 决定（改动落在 SSE 生产者，非冻结）。
7. ~~全量 pytest 逐测内存增长 ~45 MB（Linux 上把服务打进第 4 级）~~（T8b 已定位并解决）：根因是 **aiohttp 的全局中间件缓存** `_cached_build_middleware`（`web_app.py`，`lru_cache(maxsize=1024)`）以 `(handler, match_info.apps)` 为键、`apps` 是含 `Application` 的元组，把每个 e2e 测试的 Application（连带 Engine ≈55 MB）用强引用挂进进程级缓存，逐测累积（实测 +54.6 MB/测、6 个测试 55→358 MB）。tracemalloc 确认是**对象未释放**（非分配器未退还）。修复：`tests/e2e/conftest.py` autouse fixture 每测后 `cache_clear()`（修复后 RSS 平台化 ~121 MB、存活分配净增 ≈0）。**生产单实例不受影响**（只建一个 app、缓存就一条、长驻运行）。另：T8b 顺带修复了"Windows 上 `memory_bytes()` 恒 0"（改用 psapi WorkingSetSize），此前第 3/4 级内存降级逻辑在本地从未真正执行过。

## 下一步

**今天是提交截止日，当前在做 F2（准备提交材料，见 docs/instructions/F2.md）**：请求体支持 NDJSON（代码改动）、合并支线笔记、重写 README、AI_LOG 加摘要、从全新克隆验证。**不改服务代码行为**（NDJSON 除外）。设计文档与配图由用户另行更新。

提交后（按原计划）：① **T11 性能优化**——按 `docs/perf/T11a-report.md` 第六节方向清单实施（① 归一化未命中提速、② 写入块 5000→~1000、③ `gc.freeze()`+阈值，①② 是非冻结模块、风险低收益最大，优先）；② **T10c 控制台页面**（调试接口已由支线 B1 完成）；③ 容器里统一重跑 S3/S4/S1/S2（C3 用户指令，最终代码 + g5 配置）；④ 收尾（统一修改设计文档、整理 AI_LOG、答辩准备）。**每个任务结束都必须在 WSL 跑全量 `pytest -q`**（T8b 起，已写入 CLAUDE.md）。

**支线（lane）状态**：B1/B1b（Docker + 调试接口）、C1（内存账本）、C2（内存增长调查）、C3（内存加固 + glibc mmap/trim 阈值）的代码均已合并进 main，本 F2 收尾把它们各自的决策/待解决/设计待改/AI 记录并入 PROGRESS / DESIGN_TODO / AI_LOG（见"已定的决策"支线小节、"未解决的问题"第 8 条、AI_LOG 末尾支线小节）。支线原始笔记保留在 `docs/lanes/`。
