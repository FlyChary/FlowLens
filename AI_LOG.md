# AI 使用记录

> 草稿，由 AI 根据对话整理，提交前请本人核对和修改。

## 使用的工具与分工

- Claude（对话）：方案讨论、设计文档与配图、核心模块代码（config、dims、hashing、timewin、sketch、dedup、bounds、segtree、ring）及其测试。
- Claude Code：按 docs/TASKS.md 实现其余模块，每完成一个任务在本文末尾追加记录。
- 本人：提出并决定关键取舍（见下），审阅所有 AI 生成的代码与文档。

## 关键交互

| # | 我的提问或想法 | AI 的方案 | 结果与理由 |
|---|---|---|---|
| 1 | 用大顶堆维护热点 | 指出堆只能排序不能计数、哈希表会无界增长，引导到固定容量的 Space-Saving | 采纳思路 |
| 2 | 哈希表分"热点表 + 历史表" | 建议历史表改用 Count-Min：内存固定、不存 key 字符串 | 采纳，形成 Count-Min + 小顶堆 |
| 3 | 历史层 10 秒桶为什么还要存 Count-Min，能否只存结果 | AI 原方案在历史层保留 Count-Min | 推翻 AI 方案：历史层只存堆和缺席上界，省约 15 MB，误差区间变成确定性的 |
| 4 | — | AI 自查发现自己方案的问题：两层 Count-Min 宽度不同无法相加；"回放慢一些可以接受"与 p99 < 50 ms 冲突；分片后内存未计入；内存按典型值而非最坏值 | 全部修正 |
| 5 | 历史层用线段树实现任意精度 | AI 认为完整线段树内存太大，建议十叉两层 | 我提出只保留 66 槽活跃环、其余叶子只存必要信息、上层增量更新；推导出"每个节点只截断一次"，采纳为最终方案 |
| 6 | at 是毫秒参数，1 秒精度回放是必要的 | AI 之前认为按 10 秒对齐也可以 | AI 承认判断错误，改为稀疏线段树 |
| 7 | 节点加废弃标识，退出的节点落盘保存 | AI 补充：正在读的查询需要"已废弃"中间态；写文件的页缓存会计入 cgroup 内存 | 写入设计文档第 9 节，主功能完成后实现 |

## AI 生成代码中发现并修正的问题

- 写入被拒绝（503）时若已写入 Bloom，调用方重试会被判为重复而丢数据。规定：先预留队列容量，成功后才去重（CLAUDE.md 规则 3）。
- 线段树节点的缺席上界公式：最初的测试对公式不敏感（故意改坏公式测试仍通过）。补了两个针对性测试，确认"候选堆顶"和"各秒堆顶之和"两项缺一不可。
- Bloom 误判率测试本身写错：测量时边查边插入，越测越满，测出 1.9%。改为只查不写后为 0.1% 左右。
- HLL 估计：早期原型用 HLL++ 的经验偏差表；正式实现改用 Ertl 的改进估计，全范围无需偏差表。

## 代码结构审查

我要求检查设计模式是否合适、是否有冗余又不过度设计。AI 自查出三处重复并按面向对象方式重构：

- TopK 与线段树候选堆是两份几乎相同的"带索引小顶堆"，抽成通用的 `heapx.MinHeap`。热路径基准从约 52 ns 变为约 57 ns，可以接受；为此专门加了只查一次索引的 `Bump`。
- url、user 两套字段改成按维度下标的数组，差异由 `Stats` 对象自己承担（url 带 DDSketch，user 带 HLL）。
- 线段树里 HLL 与 DDSketch 的附带逻辑写了三遍，收进 `sketch.Stats`。
- 预防性地把将来容易重复的逻辑先做成公共能力：区间拆分 `timewin.SplitRange`、参数换算 `config.ForShard`，并在 CLAUDE.md 列出"已有能力不要重写"的清单。
- 明确不做：统一的 Sketch 接口、工厂与依赖注入框架、只有一个实现的接口。

## 实现语言从 Go 改为 Python

AI 最初用 Go 写了核心代码。我不熟悉 Go，答辩时无法讲清代码，要求全部改用 Python。AI 指出并处理了随之而来的问题：

- Python 有全局解释器锁，"每个分片一个线程"的并发模型不再成立；多进程每个都有几十 MB 基础内存，256 MB 里开不了几个。改为单进程、单写者（所有统计状态只在一个事件循环里修改，不需要锁），性能靠按批处理和 numpy 向量化。
- 实测统计核心单核约 25 万事件/秒，gzip 加 JSON 解析每条约 0.7 微秒，端到端预计接近但不保证达到 20 万/秒，需要压测确认。
- 移植时，原有的性质测试抓到一个 Python 特有的 bug：`levels[k][b.start] = b.freeze()` 中右边先执行，`freeze()` 清空了 `start`，节点被存到了错误的键上。改为先取出 start 再冻结。
- AI 自己写的吞吐基准脚本最初在循环里重复切片列表，测出的吞吐只有真实值的一半左右，用性能分析定位后修正。

## Claude Code 任务记录

（每完成一个任务追加一段）

### 2026-10-16 环境确认：修复包布局与 import 风格不一致（已按方案 1 执行）

- 更新 CLAUDE.md"目录与归属"一节末尾说明：✅ 基础代码不得修改（含测试文件与 tools/bench_core.py），不得在上层复制改过的版本绕开。
- `pip install -e ".[dev]"` 成功，但 `pytest -q` 在 conftest 处 `ModuleNotFoundError: No module named 'flowlens'`，`python tools/bench_core.py 长尾` 同样无法执行。
- 诊断：全部源码平铺在 `src/` 下（无 `src/flowlens/` 子目录，全包也找不到任何 `flowlens` 包）；而 tests/、tools/ 的 import 全是 `from flowlens.xxx`，pyproject 的 `pythonpath = ["."]` 指向根目录（根目录没有包），Dockerfile 写的是 `COPY flowlens ./flowlens`（仓库里根本没有 `flowlens/` 目录）。editable 安装实际把 `src/` 加进了 sys.path，`import bounds` 可用、`import flowlens` 不可用。
- 两种修法（二选一，待确认后再动）：
  1. 把 `src` 改名为 `flowlens`（根级包，与 Dockerfile/README 一致）；pyproject 加 `[tool.setuptools] package-dir = {"" = "flowlens"}` 并删除 `pythonpath = ["."]`；顺手删掉遗留的 `src/flowlens.egg-info`。
  2. 保留 src 布局：删掉 `src/__init__.py`、目录改名为 `src/flowlens/`；pyproject 加 `[tool.setuptools] package-dir = {"" = "src"}`、`packages = ["flowlens", "flowlens.sketch"]`，同样删除 `pythonpath = ["."]`。
- 用户确认采用方案 1。执行：`src/` 改名为 `flowlens/`（根级包），pyproject 加 `[tool.setuptools] packages = ["flowlens", "flowlens.sketch"]`、删 `pythonpath = ["."]`；删除遗留的 `src/flowlens.egg-info` 和各 `__pycache__`；CLAUDE.md 里 `src/` 路径同步改为 `flowlens/`（"环境与命令"一句 + 目录与归属表）。
- 验证：`pytest -q` 46 passed；`python tools/bench_core.py 长尾` 输出"长尾：400000 条用时 2.11 s，约 190 k 事件/秒（单核，不含 HTTP 与 JSON）"，与 TASKS.md T11 记录的约 25 万/秒同一量级，无回归。

### 2026-10-16 T1 事件模型（flowlens/model.py、errors.py）

- 新增 `flowlens/errors.py`：`FlowLensError` 基类（类属性 `status`/`message`）+ BadRequest(400)/TooOld(422)/PayloadTooLarge(413)/UnsupportedEncoding(415)/Overloaded(503, 带 `retry_after`)。放在 flowlens 根层而非 api/ 下，因为 model、engine 也要抛；TASKS.md T6 原写 api/errors.py，T6 时 api 直接引用即可（见"与指令不一致"汇报）。
- 新增 `flowlens/model.py`：`EventBatch`（按列 dataclass，`ids/users/urls` + `ts/nbytes` int64 + `invalid`）、`parse_events`（orjson 解析，顶层非数组/非法 JSON 抛 BadRequest，逐条校验剔除）、`normalize_url`（去 scheme+host、去 query/fragment、合并/、补/、去尾/、纯数字/UUID/≥16 十六进制段→`{id}`、截 128 字节不截多字节、不改大小写/不百分号解码）。
- 校验热路径纯 Python 快速判断（不碰正则）；正则只在归一化缓存未命中时用。event_id 长度检查对短 ASCII 走 `len`、非 ASCII 才 `encode`。
- 归一化结果缓存 10 万条（模块级 dict，满了整体清空），由 `parse_events` 内部使用。
- `tests/test_model.py`：normalize_url 表驱动 29 条（覆盖 query/fragment/两者、连续斜杠、末尾斜杠、根路径、scheme+host、纯数字、UUID 大小写、长/短十六进制、中文、截多字节中间、空串、补斜杠）+ parse_events 全部边界（顶层非数组、非法 JSON、元素非对象、缺字段、类型错误、bool 冒充整数、ts=0/负、bytes 负、超长 event_id、多余字段忽略、user 截断）。
- "截断不截多字节字符"的性质起初只在表驱动精确值里隐式覆盖，用户指出后加了显式断言（长度≤128 + 无 U+FFFD + 是原串合法前缀 + 精确值），并做负向自检确认能抓出 `decode('replace')` 的错误实现。
- 新增 `tools/bench_model.py`：1000 条、200 种重复 URL（缓存命中），测 parse_events。
- 验证：`pytest -q` 107 passed；`tools/bench_model.py` 输出"每条约 1.66 μs，约 60 万条/秒"，满足"缓存命中 1000 条 < 3 ms"的要求。

### 2026-10-16 T1 增补（int64 上限、超长 URL 不缓存、两条 URL 小规则）

- TASKS.md 同步：T1 字段名 `bytes`→`nbytes`；T6 `api/errors.py`→`flowlens/errors.py`（注明 api 层直接引用）。
- int64 上限：`ts`/`bytes` 增加 `> 2^63−1` 判单条 invalid，避免转 int64 numpy 数组时 `OverflowError`。另发现 orjson 会把 `>= 2^64` 的整数退化成 float，新增 `_is_corrupt_bignum` 识别这种"整数值且 >= 2^64 的 float"并整批抛 BadRequest（普通小数不会命中）；对应测试 `orjson.dumps` 造不出这种输入，改为手工拼原始字节。
- 归一化缓存内存上限：新增常量 `NORM_CACHE_MAX_KEY_LEN = 1024`，`normalize_url` 对超过 1024 字符的原始 URL 照常归一化但不写缓存（键是无界原始串，防极端长 URL 撑爆内存）。
- 两条 URL 小规则：① 协议相对 `//host/...`（无 scheme）现在也剥掉主机只留路径（`_SCHEME_HOST` 正则的 scheme 段改为可选）；② 截断到 128 字节后若末尾恰好是 `/`（非根），再去掉这个 `/`。
- 验证：`pytest -q` 113 passed；`tools/bench_model.py` 每条约 1.68 μs（改前 1.66），无回归。

### 2026-10-16 T1 修正：> 2^64−1 的整数从"整批 BadRequest"改为"单条 invalid"

- 原指令假设 orjson 遇到超大整数会报错，实现时按"超出 64 位 = 损坏输入，整批抛 BadRequest"处理（`_is_corrupt_bignum`）。
- 实际发现 orjson 遇到 `>= 2^64` 的整数不会报错，而是转成**浮点数**，解析后与 JSON 里本就写成浮点数的值（如 `1e20`）无法区分。`_is_corrupt_bignum` 因此会误伤正常的浮点数。
- 改为删除 `_is_corrupt_bignum` 及其调用，`> 2^64−1` 的整数（orjson 转成的 float）落回现有的"非整数 = 单条非法"规则，与"单条不合法只剔除这一条、不影响同批其他事件"的原则一致。
- 对应测试从 `test_int_beyond_uint64_raises_badrequest` 改为 `test_int_beyond_uint64_single_invalid`：`ts`/`bytes` 为 `2^64` 时单条 invalid、同批保留、不抛异常（手工拼原始字节，因 `orjson.dumps` 造不出这种输入）。
- 验证：`pytest -q` 113 passed；另确认 `1e20`、`1.5` 等正常浮点数同样按单条 invalid 处理、不抛异常。

### 2026-10-16 T2 统计引擎（flowlens/engine.py）

- 新增 `flowlens/engine.py`：`IngestResult`（frozen）、`Counters`（累计值）、`Engine`（拥有 `trees` 按维度、`ring`、`bloom`、`counters`）。
- `ingest` 全向量化：`classify` 分 late/future/accept（FUTURE 计入 invalid，加 batch.invalid）；accept 的事件 `bloom.check_and_set` 去重；剩余按**一次 argsort + 边界切片**分秒（不对每秒做整批掩码）；每秒每维 `observe`。维度载荷用 `{Dim.URL: (h_url, urls), Dim.USER: (h_user, users)}` 字典，不写 `if by == "url"` 两份。
- `collect_parts` / `collect_stats`：整秒 [max(start,ring.next), stop) 取环桶、[start, min(stop,ring.next)) 取树（与 frozen_range 取交集，空树跳过）；边缘秒按 sec 是否在环内决定取环桶还是树叶子；保留期早于 now−600s 抛 TooOld；collect_stats 边缘秒 frac≥0.5 才计入。
- `tests/test_engine.py`：16 条（bounds 逐 key 落在上下界、跨树/环边界、随机 100 个毫秒区间、同批/跨批重复、迟到/未来/invalid、late_at_engine 用时钟回拨构造、TooOld、空树不报错、collect_stats 的 HLL/p95 误差与 0.5 边缘规则、维度 Stats 形状）。
- 新增 `tools/bench_engine.py`：1000 条一批、长尾 URL + 大量不同用户，测 parse_events + engine.ingest。

- 关键决策（需确认）：
  - **accepted 只算真正写进桶的条数**，`ring.slot` 返回 None（已定稿）的那部分只计入 late_at_engine。初版把 accepted 写成 `nkeep`（去重后全部），导致 late_at_engine 测试失败才修正。
  - **late_at_engine 的测试**：正常单时钟下 ACCEPT 的秒永远 ≥ ring.next（next 跟 watermark 走），`ring.slot` 不会返回 None。用"时钟先推进再回拨"构造（tick 后回拨 5s，让某秒重新变 ACCEPT 但已定稿离开环）。
  - **benchmark 口径**：与 `bench_core` 一致——now 固定、ts 在 now 前 2 秒内聚簇、不推进时钟。初版每批 advance 时钟 + 60 秒散布，触发频繁定稿，把吞吐打到 18k/s；修正后 135k/s。
  - **benchmark 计时只含 parse+ingest**，数据生成（make_batch_body）预先生成、不计时。初版把生成算进去，数字虚低。

- 验证：`pytest -q` 129 passed；`tools/bench_engine.py` 每条约 7.39 μs、约 135 k 条/秒。
  - 拆解：ingest 单独 ~4.7 μs/条（213k/s，**超过** ✅ bench_core 的 190k/s，引擎核心无回归）；parse_events ~2.6 μs/条（T1 逐条校验固有成本）。两者串联理论上限约 135k/s，低于 160k/s 目标 16%。瓶颈是 parse 叠加，非引擎代码。

### 2026-10-16 T2 代码审查修改

- **修 bug**：`ingest` 里 `np.asarray(batch.users/urls)` 生成定长字符串数组（`<U..`），短串补 `'\x00'` 会改变 `key_hash`（`hash("userA\x00") != hash("userA")`），且比 object 数组慢约 7 倍。改 `dtype=object`。新增 `test_user_with_trailing_nul_char` 用 orjson 造 `"userA\x00"` 验证。
- **消除重复**：`collect_parts`/`collect_stats` 的保留期检查与时间划分重复。抽 `_plan(d, r) -> _Plan`（frozen dataclass：ring_buckets、history 或 None、edges 每个带 sec/frac/in_ring），两个方法只消费 plan 结果。
- **cover 返回 None 抛 RuntimeError**：历史部分 `cover(...)` 为 None 时不再静默跳过，改抛 `RuntimeError`（内部状态不一致，叶子应当连续），抽 `_cover` 辅助。
- **小修**：删掉无作用的 `if r.stop > 0`；`r` 参数加 `Range` 类型注解。
- **新增测试**：① 推进 70s + tick 后对树区间 `collect_stats`（HLL/p95 误差内）+ 边缘秒落在树里的 0.5 规则（frac 0.4 不计、0.6 计）；② `ingest(batch, weight=2)` 真实次数×2 落在上下界；③ user_id 末尾 `'\x00'` 用原始串哈希可查到、次数正确。
- **benchmark 扩展**：`tools/bench_engine.py` 分项耗时（parse / ingest / 合计）、1000 与 5000 条批大小对比、单次定稿进树耗时的中位数与最大值（逐秒推进时钟 + tick，记录每次 finalize）。

- 验证：`pytest -q` 133 passed；`tools/bench_engine.py`：
  - 批 1000：parse 2.78 μs（360k/s）、ingest 4.01 μs（250k/s）、合计 6.78 μs（147k/s）
  - 批 5000：parse 2.55 μs（392k/s）、ingest 2.90 μs（345k/s）、合计 5.45 μs（184k/s）
  - **单次定稿进树：中位数 23514 μs、最大 42721 μs**（每 200 秒桶各一次）。

- **性能发现（已记录，T3 之后再统一决定，本轮不优化）**：单次 finalize 中位数 ~23ms、最大 ~43ms 偏高。cProfile 确认全花在 ✅ 模块（`segtree._Builder.accept` → `IndexedMinHeap.reprioritize` 对候选堆全量重排 + 逐候选 `offer`），不是引擎胶水。segtree 是 ✅ 定稿代码，T2 不能改。按 CLAUDE.md"耗时操作要分块、块间 `await asyncio.sleep(0)`"，23ms 已逼近 50ms 查询延迟预算，后台 tick 若与查询并发可能被拖过。候选方向：T11 阶段分批 finalize + 让出事件循环，或把 tick 改 async。

### 2026-10-16 T3 快照（flowlens/engine.py）

- 新增 `Snapshot`（frozen dataclass）：`at_ms`、`from_ms/to_ms`（区间端点）、`tops`（按维度下标的 `Result` 元组元组，≤ `snapshot_top_n`）、`unseen_upper`（按维度下标，对应 `Answer.unseen_upper`）、`distinct_users`（user 维 HLL 估计）、`p95_bytes`（url 维 DDSketch 的 0.95 分位，空区间为 `math.nan`）。
- 拆分 `tick`：`advance()`（原 tick 内容：ring/bloom 推进）+ `refresh_snapshot()`（now=clock；区间 `split_range((sec(now)−60)·1000, sec(now)·1000)`，即最近 60 整秒、**不含当前秒**；每维 `collect_parts` + `top_k`，`collect_stats(USER).hll.estimate()` 取去重人数，`collect_stats(URL).dd.quantile(0.95)` 取 p95；整体赋 `self.snapshot`）+ `tick()`（依次调 `advance()`、`refresh_snapshot()`）。`__init__` 末尾调一次 `refresh_snapshot()`，启动即有快照。数据只走 `collect_parts`/`collect_stats`，与回放同一条路径（指令第 3 点）。
- `tests/test_engine.py` 新增 6 条：与直接 `collect_parts`+`top_k` 逐 `Result` 相等；不含当前秒（当前秒写 `NOW+500`、上一秒写 `NOW−100`，断言前者缺席、p95 不被 5000 字节拉高）；空引擎 tops 为空/`distinct_users==0`/`p95` 为 nan；时钟推进 30s + tick 后与直接查询一致；旧快照对象不受新快照影响（frozen + 整体替换）。
- `tools/bench_engine.py` 扩展 `measure_snapshot`：先把 60s 窗口每秒写满（两维名单 512/512），再各测 `refresh_snapshot` 与完整 `tick` 20 次的中位数/最大值。

- 关键决策（需确认）：
  - **"满载"窗口用独立的 `_make_full_body`，不复用 `make_body`**。`make_body` 的 URL 是 `/api/item/{u}`（纯数字段），`normalize_url` 会把它全部归一成单个 `/api/item/{id}`，url 维度名单永远只有 1 个 key，根本填不满，快照基准失真。改用 `/api/item/x{u}`（非数字段，归一化后仍是 2 万个不同 URL）。实测每秒 1500 条（长尾）才能让 61 个秒的两维名单都满 512，`_full_window_engine` 里有断言兜底。
  - **tick 测量让每轮恰好"一次定稿"**：每轮时钟恰好推进 1s，`advance()` 里正好定稿 1 秒（用 `ring.finalize` 计数验证过 = 1 次/轮），对应指令"完整 tick（含一次定稿）"。两组测量各用新引擎，避免相互干扰。
  - **`p95_bytes` 断言用区间 `8<=v<=12` 而非精确 10.0**：DDSketch 返回的是桶代表值（单条 10 字节事件的分位数约为 10.07），是近似值，不保证等于样本本身。

- **性能发现（重要，需决策）**：满载窗口下 `refresh_snapshot` 中位数 **~206ms**、完整 `tick` ~267ms，**远超 50ms 查询延迟预算**。cProfile 确认全花在"每秒重算整段 60s×2 维回放"：~122 次 `part()` 建 Part + 2 次 `top_k`（跨 ~3.1 万个唯一 key 的 `np.unique`/`bincount`/剪枝 + 每片 `refine` 的 `_isin`）。这是 T3 简化设计的固有代价——TASKS.md 把"窗口汇总 Count-Min 与默认窗口的候选堆"明确推迟到 T11。若后台每秒同步跑 200ms 的 `tick`，会长时间阻塞事件循环、拖垮并发查询的 p99。候选方向（T11 落地前是否要先缓解）：① 按 T11 增量维护窗口汇总；② 把 `refresh_snapshot` 分块 + `await asyncio.sleep(0)`（需 tick 改 async）；③ 降低快照刷新频率（如每 N 秒）。等确认。

- 验证：`pytest -q` 139 passed；`tools/bench_engine.py` 新增两行输出见汇报。

### 2026-10-16 T3 性能专项：top_k / part 优化（用户批准开放两处基础代码）

- 快照满载耗时约 206 ms，主因是 top_k 对几乎全部候选做 refine、part 逐条创建对象；经用户批准，开放这两处基础代码做输出不变的性能改动（bounds.top_k 函数内部、HeavyHitters.part 方法内部，签名/返回值/现有测试不动）。
- `bounds.top_k` 两处改动：① refine 只对"点估计前 k（含并列）"的候选做（`np.partition` 找第 k 大点估计选候选），不再先按门槛筛出全部 alive 再逐个 refine；正确性：refine 只降上界不改点估计，被精化剪掉的候选点估计 < 门槛 < 至少 k 个候选的点估计，本就不在前 k，结果不变（理由写入函数注释）。② 排序只对选中的 ≤k 个候选做，不再排序全部存活候选。
- `HeavyHitters.part`：直接读候选堆 `q.keys/prios/vals` 生成数组，不再调 `entries()` 逐条建 `Entry`。
- 新增 `tests/test_perf_equivalence.py`（3 条）：改动前 `top_k`/`part` 原样复制为 `_top_k_reference`/`_part_reference`，在长尾/接近均匀/全随机三种满载窗口（各 61 秒、两维名单 512/512）+ 边缘秒（frac 0.6/0.3）+ 小窗口（k>候选数）上逐项断言输出相同，k 取 1/10/100/5000。
- 快照耗时前后对比（满载窗口，`bench_engine.py`，同会话内基线）：
  - refresh_snapshot：中位数 229.6 ms → 72.1 ms（约 3.2 倍），最大 271.5 ms → 87.8 ms
  - 完整 tick（含一次定稿）：中位数 278.9 ms → 110.7 ms（约 2.5 倍），最大 329.7 ms → 128.2 ms
  - 吞吐段与单次定稿耗时不变（波动在机器噪声范围内；`bench_core` 长尾 179~190 k 事件/秒，与基线一致——ingest 路径不经过 part/top_k）。
- 仍未达 50 ms 预算：剩余成本是"每秒重放 60 秒×2 维"本身（~122 次 part + 跨 ~3 万唯一 key 的 unique/bincount），根治要靠 T11 的窗口汇总 Count-Min 与默认窗口候选堆（增量维护，不再每秒全量重放）。等 T11。
- 验证：`pytest -q` 142 passed（139 + 3 等价性）；`tools/bench_core.py 长尾` 约 179~183 k 事件/秒，无明显下降。

### 2026-10-16 T3 性能专项（续）：top_k 精确选 k + 并列数诊断 + 等价性提速

- **修正一处与指令不符**：上一轮对"点估计前 k **含全部并列**"的整批做 refine + Python 排序。接近均匀 / 全随机流量下第 k 名点估计有成千上万个并列（全随机实测 30720 个并列，`bench_engine` 新增的并列数打印确认），整批 refine + 排序抵消了收益（全随机 k=100 实测 334ms，比参考实现 382ms 还慢不了多少）。改为精确选 k：① 点估计严格大于第 k 大的全入选（必 < k 个）；② 剩余名额从等于第 k 大的并列组里按 key 字典序取最小的若干个，用 `heapq.nsmallest`（不排序整批）；③ 只对最终选出的这 k 个 refine 再排序。与原实现"点估计降序、并列按 key 升序"完全一致，等价性测试原样通过（全随机 k=100：334ms→43ms）。
- `tools/bench_engine.py` 快照测量新增打印每维"第 k 名点估计的并列数量"（长尾满载窗口 url=4、user=599）。
- `tests/test_perf_equivalence.py`：满载窗口改模块级惰性构造、只建一次（`_window_part` / `_window_mode` 缓存）。**但未达 10s 目标**：全量 pytest 从 ~28s 降到 ~20s（142 passed）。cProfile + 分项计时确认瓶颈**不是构造**（4 个满载窗口共 ~4.3s，现已只建一次），而是**参考实现 `_top_k_reference` 本身**（旧版慢代码，并列重模式下每次 refine ~3 万 key，3 模式 × 2 维 × 4 k × 2 区间 ≈ 48 组对比共 ~9s）。要再往下压只能减少调用参考实现的对比组数（牺牲覆盖），与"覆盖范围不变"冲突，故未擅自缩减——待确认。
- 快照耗时进一步下降（并列少时收益更明显，长尾 url 仅 4 并列）：refresh_snapshot 中位数 59.3 ms、完整 tick 中位数 108 ms。仍高于 50ms 预算，根治仍靠 T11 窗口汇总。

### 2026-10-16 性能专项收尾（bounds.py / heavy.py 重新冻结）

- 用户审查通过后，`flowlens/bounds.py`、`flowlens/sketch/heavy.py` **重新冻结**，不再改动。
- 慢测试标记：`pyproject.toml` 注册 `slow` 标记 + `addopts = "-m 'not slow'"`（日常 `pytest -q` 默认跳过）；`tests/test_perf_equivalence.py` 三条测试加 `@pytest.mark.slow`；CLAUDE.md 命令说明补"改 bounds/heavy 后须跑 `pytest -q -m slow`"。
- `bounds.py` 模块文档字符串按用户指定措辞更新（只改 docstring，代码未动）：改为"按点估计选出前 K 个，只对它们用 Count-Min 收紧上界；其余候选不需要精算，因为上界够不到第 K 大下界的候选不可能进前 K"。
- `engine.py` 删掉没用的 `import math` 与 `Result`。
- 验证：`pytest -q` → 139 passed, 3 deselected，6.85 s；`pytest -q -m slow` → 3 passed, 139 deselected，13.72 s。

### 2026-10-16 T4 写入处理（flowlens/ingest.py、flowlens/ticker.py、model.py、engine.py）

- `model.py`：`parse_events` 拆成 `load_array(body) -> list`（orjson 解析顶层数组，不合法抛 BadRequest）+ `validate(objs) -> EventBatch`（原逐条校验与归一化循环）；`parse_events = validate(load_array(body))`，现有 67 条 model 测试原样通过。大请求据此"解析一次、按块逐块 validate"。
- `engine.py`：`refresh_snapshot` 的计算抽成共享辅助（`_snapshot_range`/`_dim_tops`/`_snapshot_stats`/`_build_snapshot`），同步版 `refresh_snapshot()` 不变，新增 `async refresh_snapshot_async()`——计算完全相同，只在两个维度之间、统计概要之前各 `await asyncio.sleep(0)` 一次。删掉无用的 `import math`/`Result`。
- 新建 `flowlens/ingest.py`：`decode_body`（identity/空/gzip，gzip 用 `zlib.decompressobj(16+MAX_WBITS)` **分块**解压、`max_length` 限输出 + 累计超 64MB 立即抛 PayloadTooLarge，不先把整块解压进内存；损坏/截断抛 BadRequest）、`Admission`（try_reserve/release/pending/occupancy）、`IngestService.ingest`（decode→load_array→条数超 MAX_PENDING_EVENTS 抛 PayloadTooLarge→try_reserve（**先去重前**）→按 5000 分块 validate+engine.ingest 块间 sleep(0)→finally release）。
- 新建 `flowlens/ticker.py`：`Ticker`（`run` 每秒 advance + sleep(0) + refresh_snapshot_async，记录 `last_tick_duration_ms`/`last_tick_delay_ms`/`tick_count`；`stop()` 干净退出，取消也干净）。
- `tests/test_ingest.py`（21 条）：decode_body 全分支（含 200MB 零的解压炸弹→PayloadTooLarge 且过程不真分配 200MB、损坏/截断）、IngestService（空批/正常计数/gzip 批/批内跨批重复/迟到未来/超限/Overloaded 后释放重试不判重/分块让出/异常释放）、Ticker（tick 定稿+快照+stop+cancel）、async 与 sync 快照一致。
- 关键决策（需确认）：
  - **gzip 分块解压用"输入也分块 + max_length + unconsumed_tail"**：实测"一次喂全部压缩输入、只靠 max_length 限输出"会**丢失**未吐出的解压数据（10MB 只回 1MB），必须边喂输入边用 `unconsumed_tail` 续接，才既不丢数据又不把整块解压进内存。
  - **Ticker 放独立 `ticker.py` 而非 ingest.py**：Ticker 是长生命周期后台驱动（每秒一拍、永不结束），ingest.py 是一次性请求处理，合在一个模块会让"请求处理"混进一个常驻任务。
  - **分块让出测试用"记录执行时刻"**（指令建议）：断言并发任务有某次执行时刻早于请求结束，直接证明处理期间让出了事件循环，比"数让出次数"稳（次数受事件循环调度顺序影响，初版 `>=3` 抓到 2 才改）。
  - **Ticker 用绝对时刻调度而非"工作完再睡 period"**：后者会把每拍耗时累积成越来越晚的漂移（快照 ~60ms 一拍会持续累积）。改为每拍睡到"预定时刻 next_at"，只要单拍 < period 就不漂移。实测延迟稳定在 ~10ms（不单调增长）。

### 2026-10-16 T4 修正：按解压后字节预留（内存安全）

- **背景（用户审查发现）**：实测 orjson 解析后 Python 对象约为原始 JSON 的 **3.4 倍**（10 万条 12.9MB JSON → ~43MB）。原 T4 用 64MB 解压上限 + 按条数预留（且预留发生在**解析之后**），一个合法大请求解析后约 215MB，直接超出 256MB 容器，且条数上限挡不住解析本身的内存。
- 改为**按解压后字节数预留、且预留提前到解析之前**：
  - 新参数 `MAX_BODY_BYTES = MAX_DECOMPRESSED_BYTES = MAX_PENDING_BYTES = 4 MB`（删除 `MAX_PENDING_EVENTS`）。
  - `Admission` 记录在途请求的**解压后字节数之和**（`try_reserve(n_bytes)`/`release`/`pending`/`occupancy=pending/limit`）。
  - `IngestService.ingest` 顺序改为：`decode_body` → `try_reserve(解压后字节数)`（失败 503，**在解析/去重之前**）→ `load_array` → 分块 `validate`+`ingest` → `finally` `release`。
  - 每处理完一块把 `arr` 该块元素置 `None`（等长切片赋值），让已处理的 dict 尽早释放（降低单请求峰值）。
  - `_gunzip` 直接返回 `bytearray`（去掉末尾 `bytes(out)` 复制，orjson.loads 接受 bytearray）。
- `Ticker`：单拍抛普通异常时 `logging` 记完整异常、`error_count` 加一、继续下一拍；只捕获 `Exception`，`CancelledError` 照常传播。
- `docs/TASKS.md` T4 同步更新（新参数、按解压后字节预留、置 None、Ticker 异常处理）。
- 验证：`pytest -q` 164 passed, 3 deselected，9.54 s；`pytest -q -m slow` 3 passed；`tools/bench_core.py 长尾` ~204 k 事件/秒（无回归）。
- 边界测试：恰好 4MB 接受、4MB+1 拒绝、60MB gzip→PayloadTooLarge（过程不真分配 60MB）、200MB 零炸弹→PayloadTooLarge。

### 2026-10-16 T5 查询（flowlens/query.py）

- 新增 `flowlens/query.py`：`TopKAnswer`/`StatsAnswer`（frozen dataclass）+ `QueryService`（由 Engine 构造，内部 256 容量 LRU，`collections.OrderedDict`）。
  - `topk`：参数二次校验（window [1000,600000]、k [1,100]，越界 BadRequest）→ 默认窗口（at None + 60s + k≤snapshot_top_n）直接读 `engine.snapshot`（items 取 tops[d] 前 k，final=False）→ 否则回放：at None 取 `sec(now)·1000`、at>now 抛 BadRequest、`final = to_ms ≤ watermark(now)`；final 时先查缓存命中直接返回；未命中 `collect_parts` + `top_k`（**两步之间不让出事件循环**，refine 引用的活跃秒桶 CMS 会被后续写入污染）；final 结果入 LRU，TooOld 原样上抛。
  - `stats`：60s 读快照；否则 `collect_stats` 取 HLL 人数与 p95。
- `tests/test_query.py`：快测试（参数校验/TooOld/默认窗口读快照/带 at 走回放/final 缓存同一对象/非 final 不缓存/LRU 淘汰 LRU/快照 stats/30s 窗口 stats 误差）用小引擎；慢测试（10 分钟数据 + 200 随机区间逐 key 上下界 + ≥30s 窗口 Top-10 召回≥9/10 的比例≥90%）标 `@pytest.mark.slow`（构造 10 分钟数据 ~40s，进不了 15s 预算，按指令"做不到就标 slow"处理）。
- **测试 fixture bug 自查**：初版 fixture 每批 event_id 都用 `e0..e299`，同一 10 秒 Bloom 代内相邻批次同号 id 被去重丢弃，引擎只保留约 1/10 事件，而"精确答案"按原始事件算 → real≈10×upper（20549 vs 2005）看起来像引擎 bug。改为 `e{t}-{i}` 全局唯一后 200 个查询全部通过上下界断言。排查过程：standalone 复现（唯一 id）一直过、pytest 一直挂，一度误判为 PYTHONHASHSEED 相关（逐个种子复现均挂、standalone 均过才排除）。
- 验证：`pytest -q` 173 passed, 5 deselected，9.34 s；`pytest -q -m slow` 5 passed（3 等价性 + 2 查询），HASHSEED 0/7 均过；`bench_core 长尾` 无回归（未动热路径）。
- 验证：`pytest -q` 160 passed, 3 deselected，11.25 s；`pytest -q -m slow` 3 passed；`tools/bench_core.py 长尾` ~214 k 事件/秒（无回归）。ingest 热路径（engine.ingest）未动。

### 2026-10-17 T6 前的两处小修（engine 定稿幂等 + 召回口径）

- **engine._on_finalize 改为幂等**（`flowlens/engine.py`）：定稿按维度逐个写 `trees[d].add_leaf`，若第二个维度中途抛异常，第一个维度的这一秒已写入；下一拍重试时第一个维度会撞 `add_leaf` 的"秒号必须递增"再次抛异常，从此每拍都失败，一分钟后 `ring.slot` 也随之失败、写入报错。改为按维度幂等：`tree.newest is not None and sec <= tree.newest` 时跳过该维度（这一秒在之前失败的那次里已写入）。
- **补测试** `test_engine.test_on_finalize_idempotent_after_partial_failure`：让 USER 维 `add_leaf` 第一次写 S0−1 时抛异常，断言 URL 维已含 S0−1、USER 维只到 S0−2、环未推进；恢复后下一拍 `advance()` 正常完成，两棵树都含 S0−1 且只写一次（叶子 upper 之和 == 100，重复写会变 200）。
- **召回口径改为并列不算错**（`tests/test_query.py` 慢测试）：原"返回的 key 必须都在精确 Top-10 里"会把点估计并列的 key 误判为漏。改为"返回 key 的真实次数 ≥ 精确第 10 名的次数即命中"；并恢复严格断言——每个 ≥30s 的窗口命中不少于 9/10（此前放宽成"比例 ≥90%"，现改回逐窗口硬断言）。
- 验证：`pytest -q` 174 passed（含新增幂等测试）、`pytest -q -m slow` 5 passed。

### 2026-10-17 T6 HTTP 接口（flowlens/api/、flowlens/__main__.py）

- 路由 `POST /v1/ingest`、`GET /v1/topk`、`GET /v1/stats`、`GET /healthz`；SSE（`/v1/topk/stream`）留到 T7。`params.py` 的 `QueryParams.from_request` 统一校验（window `60s`/`2m`/纯数字秒 1~600、k 1~100、by 走 `Dim.parse`、at 在未来 400）。错误处理只在 `_error_middleware` 一处：各接口只抛 `flowlens.errors` 的异常，中间件统一转 `{"error": ...}` + 状态码；405 由 aiohttp 路由返回。topk 响应 `{window_ms, from, to, by, k, final, degraded, items:[{key,count,err}]}`（`degraded` 恒 false、`memory_bytes` 恒 0，T8 替换）。后台 `Ticker` 挂在 app 生命周期（`on_startup`/`on_shutdown`）。`__main__.py` 启动前检查 `PYTHONHASHSEED`（规则 5）。
- **修两个 aiohttp 3.14.5 的行为变化（原实现直接 500 / 400）**：
  1. **中间件必须标 `@web.middleware`（新式）**。3.14+ 对没有 `__middleware_version__` 标记的"旧式"中间件，调用时把 `app` 绑到第一个形参（`handler = await m(app, handler)`），我的 `_error_middleware(request, handler)` 于是把 `app` 当成了 `request`，`request.app` 直接 `AttributeError`。加 `@web.middleware` 后按 request 调用 `m(request, handler=handler)`，`request.app` 由 aiohttp 自带的 `_fix_request_current_app`（新式）设好。
  2. **必须关传输层自动解压 `auto_decompress=False`**。3.14+ 的 `RequestHandler` 默认在传输层先把 gzip 请求体解压，`request.read()` 拿到的是**解压后**字节，但 `Content-Encoding: gzip` 头还在 → `decode_body` 二次 gunzip 报"incorrect header check"，且解压炸弹防护（分块 + 4MB 上限，在 `decode_body` 里）被整个绕过。关掉它让请求体保持压缩、交给 `decode_body` 分块解压并做 4MB 限制，与 T4 设计一致。生产 `__main__.py` 用 `web.AppRunner(app, auto_decompress=False)`；e2e 用 `server.start_server(auto_decompress=False)`。
- **重写 e2e 测试**（`tests/e2e/test_api.py`）。上一轮崩溃时留下的版本依赖 `pytest-asyncio`（dev 依赖只有 pytest，装了会让全仓库风格分裂）、用墙钟 `time.time()`+真实 `Clock()`（快照定时不可靠）、且 `test_topk_with_at_replays` 断言 `final is True` 是错的（区间只到 now−30s，没到 now−60s 的 watermark）。重写为：每个用例自己 `asyncio.run` 起真实 TCP 服务器、`FakeClock` 控制时间、需要快照时手动 `engine.tick()`、`tick_period=1000` 让后台 Ticker 在用例期间不干扰——与 T4/T5 的 `asyncio.run` 风格一致、不引新依赖。覆盖每个接口与每个错误码：healthz、ingest→topk/stats、gzip 写入、415/400/413、503（占满在途预留触发，并验证被拒请求没写去重状态、释放后原样重试被接受不判重，对应规则 2）、topk 参数 400 组、at 未来 400、TooOld 422、405、带 at 回放终态（`final is True`，`to` 早于 watermark）。
- 验证：`pytest -q` 182 passed, 5 deselected（含 8 条 e2e）；`pytest -q -m slow` 5 passed。

### 2026-10-17 T6 修订：stats 字段名、无 NaN JSON、memstat、RawBudget、错误中间件、参数

按审查意见逐项补齐（create_app 参数形式不变）：

1. **/v1/stats 字段名**：`approx_distinct_users`（四舍五入取整）、`approx_p95_bytes`、`memory_bytes`；保留 `window_ms/from/to/degraded`。
2. **所有 JSON 无 NaN/Infinity**：统一 `_json_response`（orjson 序列化，默认把 NaN/Infinity 输出为 null）；p95 空区间为 NaN → 输出 `null`。e2e 用 `json.loads(text, parse_constant=拒绝)` 严格解析兜底。
3. **`flowlens/memstat.py`**：`memory_bytes(root="/")` 依次读 cgroup v2 `memory.current`、v1 `memory.usage_in_bytes`、`/proc/self/statm` 第二列（resident 页数）× 页大小；全缺（如 Windows）返回 0、任何情况不抛异常。Windows 上 `os.sysconf` 不存在（`AttributeError`）也要兜住——测试抓到后改为 `_page_size()` 小函数统一兜（ValueError/OSError/AttributeError）。/stats 与 /healthz 接入。
4. **RawBudget（原始请求体内存保护）**：app 层新增 `Admission(limit=8MB)` 复用。`_read_raw_body` 在 handler 内读体：有 Content-Length 且 >4MB → 413 且不读请求体；否则预留 CL、失败 503+Retry-After:1；分块传输预留 4MB、边读边累计超限 413；预留/释放在函数内闭环（try/finally）。为此 app 的 `client_max_size` 设为 0（不设 aiohttp 会先于 RawBudget 以自己的 413 拒绝）。
5. **错误中间件补全**：`FlowLensError` → 各自状态码；`web.HTTPException`（404/405/413 等）→ `{"error": e.reason}`，保持原状态码与原有响应头（405 的 Allow）、Content-Type 改 JSON；其他未预期异常 `logging.exception` 记完整 traceback + 500 `{"error":"internal error"}`。
6. **/healthz** 补 `tick_errors`（Ticker.error_count）、`last_tick_ms`（Ticker.last_tick_duration_ms），`memory_bytes` 用 memstat。
7. **params.py**：at 只接受非负整数（负数/小数/非数字 400）；新增 `interval` 参数（`500ms`/`1s`/纯数字秒 → 毫秒，200ms~60s，默认 1s，T7 用）。
8. **topk count/err**：整数输出整数，否则两位小数（`_fmt_num`）。

**新发现的 aiohttp 3.14.5 行为（第 4 点引出的）**：
- **必须 `lingering_time=0`**。默认 lingering_time=10：请求体没读完就返回响应时，协议层会先"边读边丢"最多 10 秒再关连接，期间**不向客户端发响应**——用裸 socket 只发头（Content-Length 5MB）验证：服务端日志 0.000s 就算出 413，但客户端 10 秒收不到任何字节。设为 0 后 413 立即发出（实测 0.01~0.04s）、连接关闭，客户端不必把几 MB 传完。生产 `__main__.py`（`web.AppRunner(..., lingering_time=0)`）与 e2e（`server.start_server(..., lingering_time=0)`）都加了。
- **副作用**：413/503 立即断连会让 aiohttp 测试客户端的连接池复用到死连接（`ConnectionResetError`）——e2e 测试里给 POST/GET 包了一层"连接被重置就重试"的辅助，与生产行为无关。

**e2e 新增用例**（tests/e2e/test_api.py 共 14 条）：Content-Length>4MB 只发头不发体、断言立即 413 且 IngestService 从未被调用（spy）；分块 >4MB 413、分块小请求正常；RawBudget 满 503+Retry-After、释放后同批重试被接受不判重；404/405/500 响应体均为 `{"error"}` JSON、405 保留 Allow、500 通过让 `QueryService.stats` 抛意外异常制造并断言日志含完整 traceback；stats 空区间 `approx_p95_bytes` 为 null 且严格 JSON 可解析、字段名集合精确断言；topk count 整数化（50 条同 URL → `count: 50` 且 `isinstance int`）；at 负数/小数、interval 格式错误/越界 400，合法写法（`500ms/1s/2/60s/200ms`）200；应用关闭后 Ticker 任务 `done()` 且 `cancelled()`。另新增 `tests/test_memstat.py` 6 条（v2/v1/statm 顺序与换算、全缺返回 0、乱码文件不抛、空文件跳过）。

- 验证：`pytest -q` 194 passed, 5 deselected；`pytest -q -m slow` 5 passed。本地 `python -m flowlens` 实测四接口：ingest 100 条 → topk `/api/item/{id}` count=50（整数）、stats `approx_distinct_users=10`、`approx_p95_bytes=383.81`（两位小数）、healthz 全字段。README 补了 PowerShell 启动与四接口 curl 示例。

### 2026-10-17 T6c 超限请求的连接处理（Expect: 100-continue + lingering_time 实测）

- **Expect: 100-continue 拒绝路径**（`flowlens/api/app.py` 的 `_ingest_expect`，`ingest.py` 加只读 `Admission.can_reserve`）：为 `POST /v1/ingest` 注册自定义 `expect_handler`（`app.router.add_post(..., expect_handler=...)`，aiohttp 支持按路由指定）。curl 等客户端上传大请求体时先发 `Expect: 100-continue` 等同意再发体，在这一步拒绝客户端根本不发体、连接上也没有没读完的字节，正好化解 lingering_time=0 的 RST 风险。判定：Content-Length>4 MB 直接回 413 JSON、RawBudget `can_reserve` 放不下回 503+Retry-After:1，都不发 100 Continue；通过时显式发 100 Continue（照 aiohttp 默认实现，含 `output_size=0` 重置）。**预留放在哪一步**：T6c 给了两个选项（expect_handler 里预留 / 传结果给 `_read_raw_body`），我选"expect_handler 只做只读判断、真正预留仍在 `_read_raw_body` 的 try_reserve/finally release 闭环"。理由：aiohttp 在 expect_handler 返回非 None 响应时直接 return、handler（含 `_read_raw_body`）不再执行，所以"判断通过但没预留"根本不会发生；若让 expect_handler 预留，被拒请求会留下一个没有释放点的预留（预算泄漏）。判断用 `can_reserve`、预留用 `try_reserve`，二者判定条件一致（`can_reserve` 抽出后 `try_reserve` 复用它），不会分叉。expect_handler 在错误中间件之前执行，所以自己构造 JSON 响应、不靠抛异常。
- **WSL(Linux) 对比实验**（`tools/exp/`，结果 `tools/exp/results/`）：WSL Ubuntu 26.04、aiohttp 3.14.5（与 Windows 一致）、独立 venv `~/flowlens-venv`。对 lingering_time=0/1/10 各测 20 次，三客户端：curl 默认（发 Expect）、curl `-H "Expect:"`（禁 Expect 直接灌体）、aiohttp（expect100=True / False）。**关键排查**：第一轮 curl 的"禁 Expect"组出现 4/20 的 502——抓原始报文发现带 `Proxy-Connection: keep-alive`，是 WSL 的系统级 HTTP 代理（`http://127.0.0.1:7890`）返回的，不是服务端（aiohttp 不发 Proxy-Connection 头）。代理透传 413、但在"转发 5MB 大 body 时服务端已快速断连"的竞争下自己报 502。`exp_curl.sh` 加 `--noproxy '*'`、`run_matrix.sh` 加 `unset` 代理变量后重跑，curl 全组 20/20 413、~2ms、零 502。**真实结果**：发 Expect 的客户端（curl 默认、aiohttp expect100）在所有 lingering 值下都 20/20 413（body 没发送、无在途体）；不发 Expect 直接灌 5MB 的 aiohttp 客户端在 lingering=0 时 18/20（2/40 加强验证也是 2/40 RST），lingering=1 起 20/20、40/40 干净；curl 直接灌体在所有值都 20/20（本地回环灌 5MB 极快，RST 窗口极小）。aiohttp 客户端 `trust_env` 默认 False 不走代理，其数据自始可信。
- **选定 lingering_time=1**（`flowlens/__main__.py`，可用 `FLOWLENS_LINGERING_TIME` 覆盖；e2e 的 `_serve` 同步改为 1）。理由：目标"三客户端 20 次稳定收到 413 且 ≤2s"。lingering=0 不满足——不发 Expect 的客户端偶发收到 RST 而非 413。lingering=1 是最小满足值：40/40 干净、413 仍毫秒级返回（无 1 秒延迟）。**为什么无 1 秒延迟**（读 aiohttp `web_protocol` 源码确认）：413 响应在 `_handle_request` 的 `finish_response`（响应发出）里完成，早于 `start()` 里的 lingering 读（`if not payload.is_eof()`）。lingering 读发生在**响应发出之后**，只影响"关连接前读完没读完在途体"：读完→干净 FIN，超时/读完→关连接。所以客户端拿到 413 不靠 lingering，读完后关连接是后台的、不阻塞客户端。这与 T6b 裸 socket 观察（LT=10 时客户端 10s 才收到响应）不矛盾：那是"响应在 lingering 读之前才发出"的旧假设下的测量，实际 aiohttp 先发响应再 linger。Windows 对照（本机 curl + aiohttp，lingering=1）：全 413、~2-30ms，lingering=0 时 aiohttp 无 Expect 组此前在 Windows proactor 上也 RST，改 1 后 6/6 干净。
- **发现的 memstat 读数虚高（WSL 环境假象）**：用户观察"内存用得多"。排查：`/healthz` 的 `memory_bytes` 报约 427 MB，但 flowlens 进程真实 RSS 仅约 109 MB。原因是 memstat 优先读 cgroup v2 `memory.current`，而 WSL 里该 cgroup 是整个 Ubuntu 发行版（36 个进程共享），不是 flowlens 本身；目标 Docker 容器里 cgroup 就等于服务，读数才准。未改 memstat（cgroup-first 是 T6b 定的决策，在目标容器里正确），记入 PROGRESS 未解决问题待用户决定是否要在非容器环境回退进程 RSS。
- 测试：`tests/e2e/test_api.py` 新增 3 条 expect100 用例（大 body expect100 拒绝 413 且服务端未读体、被拒后新连接正常写入；小 body expect100 发 100 再 202；RawBudget 满 expect100 拒绝 503 且判断只读不改预算）；`_serve` 的 lingering_time 由 0 改 1（`LINGERING_TIME` 常量，与生产一致）。
- 验证：`pytest -q` 197 passed, 5 deselected；`tools/bench_core.py 长尾` ~307 k 事件/秒（无回归，未动热路径）。WSL 与 Windows 各客户端对比数据见上。
