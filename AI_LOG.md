# AI 使用记录

> 草稿，由 AI 根据对话整理，提交前请本人核对和修改。

## 摘要（提交前核对）

### 1. 使用的工具与分工

- 设计讨论、方案审查、指令编写：Claude（对话）；代码实现：Claude Code。绝大部分代码由 AI 生成。
- 本人的工作：提出与筛选需求、做关键决策、审查 AI 的方案与代码、组织任务与验证流程。
- 本人在使用 AI 之前做过独立的算法调研（HyperLogLog、HyperLogLog++ 等），见 [docs/research/初期调研.md](docs/research/初期调研.md)。

### 2. 本人做出的关键决定

- **实现语言由 Go 改为 Python**：AI 最初用 Go 写核心代码，因答辩需讲清代码、本人不熟悉 Go，要求全部改用 Python。
- **坚持回放支持 1 秒精度**：推翻 AI"按 10 秒对齐也可以"的建议，改为稀疏线段树（`at` 是毫秒参数）。
- **历史层用"堆加缺席上界"代替 Count-Min**：推翻 AI 原方案在历史层保留 Count-Min，省约 15 MB、误差区间变确定性。
- **冻结已审查的基础代码**：config/dims/hashing/timewin/sketch/dedup/bounds/segtree/ring 定稿后不得修改，只在批准范围内解冻（`bounds.top_k`、`heavy.py` 各做过一次"输出不变"的性能修改，已重新冻结）。
- **要求在 Linux 与 Docker 中验证**：T8b 起每个任务结束都在 WSL（Linux）跑全量 `pytest -q`；评测以 Docker 镜像（Linux、Python 3.12）为唯一标准环境。
- **调整任务顺序**：压测工具（T10a/T10b）先于内存与性能优化（T9/T11），控制台页面放到提交之后。
- **提交当天缩小 T11b 的范围、以验证为先**：只完成第零/一/二节，第三~六节留作提交后优化。

### 3. AI 方案被推翻或修正的例子

1. **Python 版改为单进程单写者**：AI 原"每个分片一个线程/多进程"的并发模型因 Python GIL 与"每进程几十 MB 基础内存、256 MB 开不了几个"而不成立，改为单事件循环、单写者（不加锁），性能靠按批处理 + numpy 向量化。
2. **写入预留必须先于去重**：AI 初版解析后才预留、去重与预留顺序有误；改为先为整个请求预留待处理队列容量、预留成功后才去重，否则被 503 拒绝的 event_id 已写进 Bloom、调用方重试会被当成重复丢掉。
3. **64 MB 请求上限实测后改为 4 MB**：实测 orjson 解析后 Python 对象约为原始 JSON 的 3.4 倍，64 MB 的请求解析后约 215 MB 直接超 256 MB 容器，且条数上限挡不住"解析本身"的内存；改为解压后 4 MB、按解压后字节在解析前预留。
4. **内存读数从 `memory.current` 改为工作集**：原口径（T7b"保持 `memory.current` 原值"）空闲即 66%、离第 3 级阈值 78% 只差 12 个百分点、稍有负载就降级；T10b 实测约 36% 是不活跃页缓存，改为工作集（`current − inactive_file`）推翻该决定。
5. **C1 的内存估算被容器实测推翻（用实测推翻估算）**：C1 进程内模拟 3 万/秒×11 分钟常驻增量仅 ~97 MB，容器实测 ~229 MiB，差 ~90 MB——C1 账本没算进真实 HTTP 层与分配器行为。C2 用容器探针判定**不是泄漏**、而是"分配器保留已释放页（碎片）+ 并发瞬时峰值叠加"（口径对齐后真实负载增量差 ~49 MiB）；C3 加 malloc_trim + 字符串驻留 + 固定 glibc 大块分配阈值（`MALLOC_MMAP_THRESHOLD_=128KB`），峰值下降约 58 MiB、从"贴 3/4 级"变全程第 0 级、接受率 87.8%→100%。

### 4. AI 犯的错被发现的例子

6. **numpy 定长字符串数组吞掉末尾的空字符**：`np.asarray` 默认生成 `<U..` 定长串、短串补 `\x00`，改变 `key_hash`（`hash("userA\x00") != hash("userA")`）且比 object 数组慢约 7 倍；改为 `dtype=object`，补 `test_user_with_trailing_nul_char` 锁住。
7. **Windows 上内存读数恒为 0**：`memstat` 读不到 cgroup/statm 时返回 0，导致第 3/4 级内存降级逻辑在本地一次都没真正执行过、问题只在 Linux 上暴露；T8b 改用 psapi 的 `WorkingSetSize`（并修了 `GetCurrentProcess` 未声明 `restype` 把 64 位伪句柄截断成 `-1` 的坑）。
8. **打包配置漏掉 `flowlens.api`**：`pyproject.toml` 手工列举 packages 漏了 `api` 子包，`pip install .` 装出缺子包的坏包、Docker 里 import 失败；改为 `packages.find`（`include = ["flowlens*"]`）自动发现，`tests/test_packaging.py` 用真实 `pip wheel .` 比对守护。
9. **对比函数把空结果算作召回 100%**：`exact.compare` 原实现 `hits/n if n else 1.0` 在服务返回空（n=0）时记 1.0，把降级期空响应误判成完美精度（T10b 的 S3"召回 1.0/误差 0"即此假象）；改为"服务空、精确非空"记 0，两皆空才 1.0。

---

> 以上由 AI 根据正文整理，**请本人核对**（每条都对应正文中的记录，未写正文之外的内容）。核对无误后即可作为提交材料。

## 使用的工具与分工

- Claude网页版（对话）：方案讨论、设计文档与配图、核心模块代码（config、dims、hashing、timewin、sketch、dedup、bounds、segtree、ring）及其测试。
- Claude Code：使用qwen3.8-27B-F8精度作为基座模型，按 docs/TASKS.md 实现其余模块，每完成一个任务在本文末尾追加记录。与Claude通过Github进行协助开发。共使用3个CC窗口并行开发，管理两个分支代码（主分支、辅助试验分支）与git提交与合并操作。
- 本人：提出并决定关键取舍（见下），审阅所有 AI 生成的代码与文档。精确设计与审阅核心模块代码后，由于开发工作量大工程细节多，协调Claude与qwen完成最终程序工程实现开发与调试验证。

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

### 2026-10-17 T7 SSE 实时推送 + 前置小修（flowlens/api/stream.py、flowlens/api/app.py、flowlens/memstat.py、flowlens/__main__.py、tests/e2e/test_stream.py、tests/test_memstat.py、tools/exp/*）

**前置小修（1~7）**：
1. **memstat 读数选择**：只在"当前进程所在 cgroup 设了内存上限"时才用 cgroup，否则回退进程常驻集（statm）。新增 `memory_limit_bytes(root="/")`（v2 读 memory.max，"max"=无上限；v1 读 memory.limit_in_bytes，≥2^60=无上限；读不出返回 None）。`memory_bytes` 据此选择来源。这解决了 PROGRESS 里"WSL 读数虚高"的未解决问题——WSL 的 cgroup 是整个发行版（无上限）→ 回退 statm；评测容器 --memory 启动带上限 → 仍用 cgroup。/healthz 加 `memory_limit_bytes`（无上限为 null）。重写 test_memstat（v1/v2 有上限用 cgroup、无上限退回 statm 都覆盖）。
2. **__main__.py** 用 argparse 支持 `--host`（默认 0.0.0.0）/ `--port`（默认 8080，Dockerfile 的 --port 8080 才生效）；`logging.basicConfig` 输出到 stdout（带时间、INFO），启动与 PYTHONHASHSEED 警告改用 logging。
3. **AppKey**：app 上的键全改 `web.AppKey`（模块级常量，按身份比较，不能每次新建），消除全部 NotAppKeyWarning。同步改了 test_api.py 的键访问。
4. **app.py 文档修正**："预留在整个请求处理完毕后释放"改为"读完请求体就释放"，并说明为什么安全（读体后紧接着在 IngestService 按解压后字节预留，中间不让出事件循环，进入处理阶段的请求数已由在途预留限住）。代码不变。
5. **big5mb.bin 出库**：`git rm --cached` + 加 .gitignore；run_matrix.sh 运行时临时生成（5 MB 内容无所谓，服务端发体前就 413/503）。
6. **NO_PROXY**：实验脚本设 `NO_PROXY=localhost,127.0.0.1`（curl 额外 `--noproxy '*'`），WSL autoProxy 不动，记入 PROGRESS 决策。
7. **路径不一致**：T6c.md 的 WSL 路径 `/mnt/d/dhy/code/src` 改为 `/mnt/d/dhy/code/flowlens`（与 CLAUDE.md 一致）。

**SSE（GET /v1/topk/stream）**（flowlens/api/stream.py：SubscriptionGroup + Subscriber + StreamManager，挂在 app 上 web.AppKey）：
- **订阅组共享计算**：按 (by, window, k, interval) 分组，同组共用一个后台生产任务、同一份 payload。生产任务是"网格步进"协程：每个 interval 网格点产一帧（帧时刻 T=⌊now/interval⌋×interval），把 (T, payload) 塞进组里每个订阅者的最新帧队列。**同一组每秒最多重算一次**：生产者在"距上次计算 ≥ min_recompute(1.0s)"时才真调 topk，否则复用上次 payload（interval<1s 时同一秒的多个网格点共享一份计算、只更新帧时刻）。这解决了"几个 200ms/600s 的订阅占满回放 CPU"的问题。
- **背压**：每个订阅者只有容量 1 的 asyncio.Queue（满则丢旧帧、dropped+1）+ 单帧写超时 5s 断开（写满内核缓冲后 5s 没写出去）。组上限 8、连接上限 256（在发出任何字节之前检查，超限 503+Retry-After 的普通 JSON 响应）。统计（组数/订阅者数/累计丢弃帧）经 /healthz 的 sse 字段暴露。
- **断线重连（Last-Event-ID）**：用**单调游标**保证 id 连续、严格递增、不重不漏。`compute_next_tid` 算起始游标：合法 Last-Event-ID → 其**下一格**（已见帧不重发），受"最旧可算帧（窗口仍在保留期，否则 topk 抛 TooOld）"与"最近 30 帧"两个下限约束；格式不对/晚于当前 → 当前网格点（从实时帧开始）。pump 在首帧前把 [游标, 首帧) 逐帧用 topk 补发（帧间让出），之后逐帧跟随队列；慢客户端被丢的帧直接跳过（丢弃，不重算）。**踩过的坑**：初版把"已追平"的游标 clamp 到当前网格，导致重连重复交付已见帧；改为"已见帧从下一格开始 + 游标不后退"后修复。生产者对单帧计算异常做 try/except 跳过（与 Ticker 单拍一致）。
- **data 与 /v1/topk 共用**：把 /v1/topk 构造响应 dict 的代码抽成 `stream.topk_data(ans)`，两处共用。
- **连接断开检测**：实验确认 Windows proactor 下 `transport.is_closing()` 不可靠（断开后仍 False），可靠信号是 aiohttp 在客户端断开时**取消 handler（CancelledError）**；`_stream` 的 finally 调 `manager.leave`（最后一个订阅者离开时取消组的生产任务）。应用关闭时 `manager.close()`（on_shutdown）取消所有组任务（先快照再取消/等待，避免与 handler 的 leave 并发改 dict）。

**测试（tests/e2e/test_stream.py，12 条）**：用真实时钟（FakeClock 静态，会让生产者"睡到的网格点"与实际时间脱节，所以引擎必须真实时钟）+ 真实 TCP 服务器 + aiohttp 测试客户端 readuntil 按空行切帧、自己解析。覆盖：帧格式（retry:3000 + id/event/data 三行、data 严格 JSON 且与 /v1/topk 同构）、id 严格递增且相邻差=interval、共享计算（同组同任务 + 稳态计算频率 1 订阅者 vs 2 订阅者相同）、计算频率（interval=200ms 时 2.5s 内 2~5 次，而非无限制的 ~12 次）、组/连接上限 503+Retry-After（JSON）、慢客户端队列 ≤1 帧 + 其他订阅者正常、Subscriber 丢帧单测（确定性）、Last-Event-ID 补发 id 连续、格式错/未来 id 忽略、心跳（调小周期收 : ping）、断开移除订阅者/最后一个离开释放组、应用关闭流结束。

**踩过的坑（测试侧）**：aiohttp `ClientResponse.close()` 是**同步**的（`resp.close()` 返回 None，不能 `await`），`client.close()` 才是异步的——初版误 `await resp.close()` 全挂。慢客户端的"丢弃计数"在 loopback 上测不准（内核 socket 缓冲会吸收全部帧、write() 不阻塞、队列不堆积），所以 e2e 只断言队列 ≤1（结构性保证），丢弃计数用 Subscriber 的确定性单测验证。

- 验证：`pytest -q` 217 passed, 5 deselected（新增 12 条 SSE e2e + 重写 8 条 memstat）、无警告；`tools/bench_core.py 长尾` ~277k 事件/秒（无回归，未动热路径）。本地 `python -m flowlens --port 8099` 实测：/healthz 有 sse + memory_limit_bytes 字段；curl -N 订阅 5s 见 retry + 连续帧（id 差=interval、重算限流可见）；带 Last-Event-ID 重连首帧 id = 上一帧 id + interval、id 连续。

### 2026-10-17 T7b SSE 修正与 T7 收尾补做（flowlens/engine.py、flowlens/api/stream.py、flowlens/api/app.py、tests/e2e/test_stream.py、tests/test_engine.py、tests/test_query.py、tests/e2e/test_api.py、docs/*）

**一、长窗口帧 TooOld 修复**（`engine._plan`）：保留期检查由 `m < now_sec − retention_sec` 改为 `m < now_sec − retention_sec − LATENESS_MS//1000`（用 `timewin.LATENESS_MS` 常量，不写死），并在文档字符串里说明原因。原因：线段树保留"最新定稿秒往前 retention_sec"，最新定稿秒约 now−60s（迟到上限），所以实际能查到 now−660s 的数据；旧口径让 window=600s 的 SSE 帧（起点 T−600s，T 对齐网格后往往早于 now）与普通查询（at 对齐网格后比 now 早）大量误报 TooOld。

**二、补帧不重算**（`stream.py`）：`SubscriptionGroup` 加 `frames = deque(maxlen=30)` 存 (帧时刻, payload)，生产者每产一帧先 `append` 再下发。补帧抽成 `_catchup`：优先取 `frames` 里落在缺口内的帧（同组共享、不重算），不在缓存里的帧（通常只有被 30 帧缓存挤出的最老一帧）才现场 `topk`，并用 `StreamManager` 上的全局 `asyncio.Semaphore(1)`（`compute_lock`）串行化，多个订阅者（甚至不同组）同时重连时不会并发打满回放。

**三、新订阅者立即收帧**：`pump` 加 `immediate` 参数；`immediate=True`（无 Last-Event-ID）且组里已有最近一帧时，先把该帧补发出去、游标推到它之后，再照常吃实时帧——id 仍连续、严格递增。

**四、关闭时立即结束所有流**：新增模块级结束标记 `STOP`。`Subscriber.stop()` 向队列放 `STOP`（满则先丢一帧再放）；`StreamManager.close()` 先给每个订阅者 `stop()`，再取消生产任务。`pump` 取到 `STOP` 立即 return，SSE 处理函数正常退出（不等 aiohttp 的 60 秒强制取消，也不靠关连接时的 CancelledError）。

**五、两处小修**：(1) `compute_next_tid` 的起始游标从 `last_id + interval` 改为向上对齐网格（`-((last_id+interval)//-interval)*interval`），客户端换了 interval 时补帧与实时帧都落在网格上；保留期下限口径同步改成 now−retention−迟到上限。（2）帧计算节流由墙钟（`t0 − last_compute ≥ min_recompute`）改为**按帧时刻**（`g − last_compute_g ≥ min_recompute`）：interval ≥ 1s 时每帧间隔都 ≥ min_recompute，保证每帧必重算、`data.to == id`；interval < 1s 时同秒多帧仍复用一份计算。这是修复一个真 bug——原墙钟口径下生产者跑慢时（本帧与上帧的墙钟间隔 < 1s）1500ms 网格会错误地复用上一份结果，导致 data.to 早于 id。

**六、T7 收尾补做**：PROGRESS"已定的决策"追加 hash(str(k)) 保留、hll 分段版保留、内存读数保持 memory.current 不扣页缓存、SSE 60s 帧暂走回放（同时记入未解决问题）；任务顺序更新为 T7 → T8 → T10 → T9 → T11 → 收尾。

**测试**（新增 7 条）：`test_engine.py` 加保留期含迟到余量的边界；`test_query.py` 加 window=600s 各 interval 各时刻帧不抛 TooOld、at=60s 前正常/at=61s 前 422；`test_api.py` 加 /v1/topk 长窗口 422 边界；`test_stream.py` 加新订阅者 1s 内收帧、同组 10 订阅者同时重连 topk 调用次数不随订阅者数增长、Last-Event-ID 不在网格上时对齐、interval=1500ms 每帧 data.to==id、关闭时 3 个活跃订阅者 2s 内流正常结束。

**与指令不一致 / 说明**：无实质偏离。"帧计算节流改按帧时刻"是执行第 5 节"验证重算节流"时发现原墙钟口径无法满足"interval>1s 每帧重算"而做的必要修正（行为更正确、与指令意图一致），已记入 PROGRESS 决策。基准表以 T7b 提交号标注；各项持平或好于上一版，无劣化 10% 以上。

**验证**：`pytest -q` 226 passed, 5 deselected、无警告；`pytest -q -m slow` 5 passed（未动 bounds/heavy，输出不变）；bench_core 长尾 ~30.5 万/s、bench_model ~1.36 μs/条、bench_engine 1000 条 ~4.72 μs/条、5000 条 ~4.20 μs/条，均持平或好于上一版。本地 `python -m flowlens --port 8091` 实测：create_app 在事件循环外创建 StreamManager（其 `asyncio.Semaphore` 3.11 构造不绑 loop）正常启动，curl -N 见 retry + 连续帧（data.to==id、间隔=interval）。

### 2026-10-18 T8 过载降级（flowlens/degrade.py、engine.py、query.py、ticker.py、api/app.py、api/stream.py、model.py、tests/test_degrade.py、tests/e2e/test_degrade.py、docs/*）

**一、降级控制器（flowlens/degrade.py）**：纯逻辑、不依赖 aiohttp，便于单测。四级阈值各带一个"活跃"滞回状态（进入阈值高、退出阈值低），当前级别 = 所有活跃级别中最高的一级。只读属性 level/degraded/sample_weight/replay_paused/sse_slowdown/tighten_budgets/reject_ingest 与最近一次输入值（供 /healthz）；`sample_weight`：level≥2 时 2、occupancy>0.95 时 4、否则 1（整数）。级别变化记一行日志（旧/新级别 + 触发输入值）并触发 `on_change(旧,新)` 回调。内存阈值按**上限比例**（0.78/0.70、0.90/0.82），`memory_limit_bytes` 为 None 时按 256 MB 计——比例换算到 256 MB 正好是设计文档的 200/230 MB。`pin(level)` 供集成测试固定级别（None 恢复阈值判定）。

**二、控制器挂在 app 上、由谁驱动**：T8 说"挂在 app 上（web.AppKey），由 Ticker 每拍结束后调用一次 update（或单独每秒任务，选一个并说明理由）"。**选 Ticker 每拍结束后 update**：单写者、每秒一拍正好"每秒更新一次"，且 update 的输入（occupancy、tick 延迟）就是这一拍刚产出的量，放这里读到的就是本拍值；另起每秒任务要多维护一个后台任务、还要处理它与 Ticker 的先后。副作用（第 3 级收紧/恢复预算、清缓存）不能塞进 update（控制器要保持纯逻辑、不依赖 aiohttp），改用 `on_change` 回调，由 `create_app` 里 `_make_degrade_change_hook(app)` 闭包捕获 app 注入，只在实际级别变化时回调一次——"进入第 3 级清一次缓存"因此天然成立。

**三、共享一个控制器实例（T8 未明说，但必须）**：T8 要求"engine.ingest 的 weight 改由控制器提供"、"判断放在 QueryService 内部"，但第 1 级可由 **occupancy>0.6** 触发——occupancy 只有 app 层（ingest 的 Admission）知道、只有 Ticker 喂得进来。若 engine 与 QueryService 各建一个控制器，两个都收不到 occupancy、第 1 级的占用率分支永远不触发，SSE 降频（读 query 的控制器）与采样（读 engine 的控制器）也会各看各的级别。故 `create_app` 建**一个**控制器，赋给 `engine.controller` 与 `query_service.controller` 并挂到 `app[degrade_controller_key]`，三处读同一级别。`make_engine`（__main__ 用）保持原样，create_app 会重新装配成共享实例。

**四、第 2 级采样的实现**：`engine.ingest(batch, weight=None)`，weight 缺省读控制器 sample_weight、**显式传参优先**（bench_engine 与旧测试显式传 weight，不受控制器影响）。采样按 **event_id 的哈希**做确定性取舍（`h_id % w == 0` 保留，比例 1/w，与时间无关）：同一事件无论重试多少次、何时重试，取舍都一致，且与"这次是不是重复"解耦。只对两维 Top-K 名单（`hh`）与 url 维 DDSketch 采样（保留事件、权重 w）；**HLL（user 维 stats）与 event_id 去重始终全量**（HLL 用全部事件、权重 1）。因为 ring.py 冻结（`DimBucket.observe` 把名单和统计一起写），引擎改为**分别**调 `bucket.dims[d].hh.observe(...)` 与 `bucket.dims[d].stats.observe(...)`。**不采样时（weight=1）走与 T7b 结构完全相同的写入路径**（`payload` dict + `DimBucket.observe`），保证正常路径结果与 T7b 一致。

**五、如何保证不采样时结果与之前完全一致**：weight==1 的分支与 T7b 的 ingest 循环逐行同构（同一 `payload = {URL:(h_url,urls), USER:(h_user,users)}`、同一 `b.dims[d].observe(h[sl], ks[sl].tolist(), a_bytes[sl], weights[sl])`，weights 全 1），唯一的净增开销是 `self.controller.sample_weight` 一次属性读取。226 条原有测试原样通过（含上下界、去重、快照断言）即为证明。

**六、各级动作接入**：
- **第 1 级**：`QueryService.topk`（带 at、或 window≠60s、或 k>snapshot_top_n 的回放查询）与 `stats`（非 60s 窗口）在降级时抛 `Overloaded(retry_after=1)`（判断在 QueryService 内，不写进 handler）；读快照的默认查询照常。SSE **不暂停、只降频**：`SubscriptionGroup` 每帧运行时读 `recompute_interval_ms()`（第 1 级 1s→5s），帧照常按 interval 推送、复用结果；生产者与补帧的 topk 调用走 `from_stream=True` 绕过"暂停回放"判断（`QueryService.topk` 加该 kwarg）。**踩坑**：`topk` 加 `from_stream` kwarg 后，T7 的 3 个 SSE spy 测试（`async def spy(d, window_ms, k, at_ms=None)`）报 `got an unexpected keyword argument`，改成 `**kw` 透传后修复（属"新增参数破坏现有 spy 签名"的测试侧小修，非 T8 行为）。
- **第 3 级**：`on_change` 进入时 `Admission.limit 4→2 MB`、`RawBudget.limit 8→4 MB`、`QueryService.clear_cache()` + `model.clear_norm_cache()`（清一次）；退出时恢复额度、缓存不清。`StreamManager.check_limits` 在 `tighten_budgets` 时抛 Overloaded（新订阅 503、已有订阅保持）。
- **第 4 级**：`_ingest` 在**读请求体之前**判 `reject_ingest` 抛 503；`_ingest_expect`（Expect 路径，handler 之前执行）同样判、发 503 不发 100。此时未写任何去重状态，重试安全。
- **响应字段**：`stream.topk_data(ans, degraded=, sample_weight=)` 加两参（/v1/topk 与 SSE 共用），采样生效时带 `sample_rate=1/sample_weight`；`_stats`/`_healthz` 的 `degraded` 改真实状态，`/healthz` 增 `degrade` 字段。

**七、与指令不一致 / 说明**：
1. 第 3 级不"缩短历史保留"，改收紧额度 + 清缓存（T8 已指明：保留期由冻结 segtree 构造时定、无法动态调）→ 记入 DESIGN_TODO #13。
2. `test_engine.py::test_ingest_weight_doubles_counts` 原断言 `l <= C*2 <= h`（weight=2 = 全体 ×2），T8 后 weight 语义变为"保留 1/2 + 每条 ×2"（无偏估计，期望 = 真实值 C，而非 2C），该断言与新语义矛盾。因 T8 明确"level 0 时现有测试原样通过"是行为等价性的证明、而本测试测的正是采样路径，**重写为 `test_ingest_weight_sampling_unbiased`**：断言加权估计围绕真实次数（非 2C），且**不**断言上下界夹住真实值（T8 明说采样期间上下界非严格保证）。这是唯一改动的现有测试，且是语义变更而非放松。
3. SSE 帧的 `degraded`/`sample_rate` 用**产生该帧时**的控制器状态（而非订阅时）。level 0 时两者都是 false/1，与"现有测试原样通过"一致；level≥2 时按指令"SSE 帧带 sample_rate"带上。
4. `bench_engine`/`bench_model` 未动（T8 未要求），基准对比需**同机**进行：本机整体比 T7b 记录时慢约 60%（原始代码本机 bench_model 2.19 μs/条 vs T7b 记录 1.36），T8 列相对 T7b 列的数值变大全部是本机更慢、非 T8 引入；**同机对照（原始 vs T8，各 5 次中位）** `engine.ingest` 每批 1000 条 3.44→3.29 μs、5000 条 2.74→2.57 μs（持平或略快），证明正常路径未变慢。

**八、测试**：新增 `tests/test_degrade.py` 16 条（每级进入/退出、滞回区间内不变、多条件取最高级、sample_weight 随 occupancy/级别、内存上限 None 按 256 MB、阈值随上限比例缩放、日志、pin、on_change 只在实际变化时触发、status 字段）；新增 `tests/e2e/test_degrade.py` 5 条（第 1 级回放 503 + 默认查询照常 + SSE 降频 5s 仍推帧、第 2 级采样无偏 + HLL 全量 + 两独立引擎取舍一致 + 响应/SSE 帧带 degraded 与 sample_rate、第 3 级预算收紧与恢复 + 新订阅 503 + 已有订阅保持 + 两缓存清空、第 4 级拒写 + 重试不判重）。

**验证**：`pytest -q` 247 passed, 5 deselected、无警告；`pytest -q -m slow` 5 passed（未动 bounds/heavy，输出不变）；bench_engine 正常路径同机对照持平或略快（见上）。本地 `python -m flowlens` 启动正常。

### 2026-10-18 T8b 测试与运行环境解耦、Linux 上验证（api/app.py、ticker.py、engine.py、ingest.py、memstat.py、tests/e2e/*、tests/test_memstat.py、tests/e2e/conftest.py、CLAUDE.md、docs/*）

**背景与教训（务必记住）**：审查时在 Linux 上实测，全量 pytest 有 **28 个测试失败、Windows 全过**。根因是**两个叠加的本地盲区**：
1. **Windows 上 `memstat.memory_bytes()` 恒返回 0**（没有 cgroup、没有 `/proc`），降级控制器从未因内存触发第 3、4 级——**内存相关的降级逻辑在本地一次都没有真正执行过**，直到 Linux 才暴露。
2. Linux 上读到的是 pytest 进程的真实内存，全量测试在同一进程逐个跑、每个 e2e 测试后常驻内存增长 ~45 MB（到第 4 个约 239 MB），超过 256 MB × 0.9 → 服务进第 4 级、之后**所有写入 503** → 28 个 e2e 失败。
**教训**：测试若依赖运行环境的真实状态（内存、时钟），在不同平台会得到不同行为。凡是要在"评测平台（Linux）"上跑的逻辑，必须在本地（Windows）也能被真正触发到——要么注入假状态、要么让本地读数真实，二者至少占一个。

**一、测试与真实内存解耦（`create_app` 加 `memory_reader`）**：`create_app(..., memory_reader=None)`，默认 `_default_memory_reader`（`memstat.memory_bytes()`/`memory_limit_bytes()`）；挂在 `app[memory_reader_key]` 并传给 Ticker。**Ticker 喂控制器的内存值、`/healthz` 与 `/v1/stats` 的 `memory_bytes`/`memory_limit_bytes` 统一从这一个 reader 读**（不再各处直接调 memstat）。`tests/e2e` 所有 `create_app` 传恒 `(0, None)` 的 `_fake_memory_reader`；新增 `tests/e2e/test_degrade.py::test_memory_reader_drives_levels` 用可控的 `MemFeed`（`.mem`/`.limit` 测试随时改、Ticker 每拍读当前值）走"读数→Ticker→控制器"真实链路，端到端验证第 3/4 级进入与退出（tick_period=0.1，不等墙钟 1 秒）。这是 28 个失败的直接修复。

**二、查清每个测试后内存增长 ~45 MB 的原因（关键调查）**：WSL 上用 tracemalloc 逐测拍快照 + `resource.getrusage` 看 RSS。发现每测净增**恰是一个 Engine**（去重 Bloom 33.5 MB + 各维度 CMS 19.7 MB + HLL 1.1 MB ≈ **54.6 MB**），且**逐测累积**不回落。因 tracemalloc 只统计"当前存活"分配（释放会减），存活分配还在涨 → **是对象没被释放，不是分配器/OS 未退还**。用 weakref + `gc.get_referrers` 逐层追：Engine 被 `Application` 引用，而 `Application` 被 **aiohttp 的全局中间件缓存** `_cached_build_middleware`（`web_app.py:84`，`lru_cache(maxsize=1024)(_build_middlewares)`）引用——其缓存键是 `(handler, match_info.apps)`，`apps` 是**含 Application 的元组**。所以每个 `create_app` 都把那个 Application（连带 Engine）用**强引用**挂进进程级缓存。`cache_clear()` 后 weakref 立即失效（`alive=True→False`、条目 1→0），坐实。
- **属于"对象未释放"**，按指令在非冻结代码修复：`tests/e2e/conftest.py` 加 autouse fixture，每个 e2e 测试结束后 `aiohttp.web_app._cached_build_middleware.cache_clear()` + `gc.collect()`（用 `getattr` 守卫，aiohttp 改名不至于全挂）。
- **验证**：修复前 6 个 e2e 测试 RSS 55→358 MB（+54.6 MB/测）；修复后 RSS 平台化在 ~121 MB、tracemalloc 存活分配净增 ≈0 MB/测。**生产单实例不受影响**（只建一个 app、缓存就一条、长驻运行，清缓存只针对"一个进程反复建/拆 app"的测试场景）。
- **与 T9 长时间内存测试相关**：单实例长时间运行**不会**因此持续增长（缓存只按 app 数增长，生产就一个 app）；若 T9 在单实例下测出持续增长，那是别的来源（例如未释放的在途请求、SSE 组），与本条无关。

**三、Windows 内存读数（`memstat.py`）**：cgroup 与 statm 都读不到时（纯 Windows），ctypes 调 psapi 的 `GetProcessMemoryInfo` 读本进程 `WorkingSetSize`，让本地 `/healthz` 显示真实值、内存降级逻辑本地能真正触发。`PROCESS_MEMORY_COUNTERS` 结构体**延迟定义并缓存**（`ctypes.windll` 只存在于 Windows，顶层定义会让非 Windows 的 import 失败）；`GetCurrentProcess` 声明 `restype=c_void_p`、`GetProcessMemoryInfo` 声明 `argtypes`。**踩坑**：初版不声明 `restype`，ctypes 把 64 位伪句柄 `0xFFFF…FF` 截断成 32 位有符号 `-1`，`GetProcessMemoryInfo` 静默失败返回 0；再声明 `argtypes` 才解决（否则传参 `OverflowError`）。任何异常吞掉返回 0、绝不抛。补 `tests/test_memstat.py`：跨平台 mock `_windows_working_set` 验证"全缺→退回该读数"的调用链 + 仅 Windows 的真实读数（`skipif sys.platform != "win32"`）。另把原来断言"全缺返回 0"的 2 条测试改成平台感知（Windows 上现在返回真实值 > 0，原断言锁死了"Windows 恒 0"的旧 bug）。

**四、小修**：(1) `engine.ingest` 采样（w>1）不再重算 event_id 哈希——去重时已算一次（`h_id_all`），采样直接按下标 `keep` 复用（`h_id = h_id_all[keep]`）。这段只在降级时跑、降级本就因 CPU 紧张，省一次 `key_hashes`；同一批里同一 event_id 两次哈希结果相同（hash 是字符串纯函数），复用不改变确定性采样语义；w=1 正常路径不变。(2) 第 3 级把在途上限 4→2 MB，这期间解压后 2~4 MB 的请求在预留阶段放不下、收 503（非 413），`ingest.py` 在 `admission.limit < MAX_PENDING_BYTES` 时把 503 信息改成"降级期间单个请求的上限暂时为 2 MB，请稍后重试"（有意、暂时性），记入 DESIGN_TODO #15。

**五、每个任务必须在 Linux 上验证**：CLAUDE.md"每个任务结束时"加一条——除 Windows 外必须在 WSL 跑全量 `pytest -q`（`/mnt/d/dhy/code/flowlens`、`/home/user/flowlens-venv`，缺依赖先 `pip install -e ".[dev]"`），两平台结果分别汇报。

**与指令不一致 / 说明**：无实质偏离。指令第二节只要求"查清原因 + 属于前者则修、属于后者则说明"；本项**属于前者（对象未释放）且根因在 aiohttp（第三方库）而非 flowlens 代码**——无法直接改 aiohttp 的 lru_cache（会改依赖），故修复落在 flowlens 拥有的测试侧（`tests/e2e/conftest.py`），这与"在非冻结代码中修复"一致。指令未指定假读数注入点，按"create_app 加参数、默认 memstat"实现。

**测试**：新增 3 条（e2e `test_memory_reader_drives_levels` 走真实 Ticker 链路、memstat 跨平台 mock 全缺退回、memstat 仅 Windows 真实读数）；改 2 条 memstat 测试为平台感知；e2e 全部 `create_app` 注入假内存读数。

**验证（两平台均全过）**：
- Windows：`pytest -q` **250 passed**, 5 deselected；`pytest -q -m slow` **5 passed**。
- WSL（Python 3.14）：`pytest -q` **249 passed, 1 skipped**（1 skip=仅 Windows 的 memstat 真实读数测试）, 5 deselected；`pytest -q -m slow` **5 passed**。
- 基准（Windows，每项 3 次中位，与 T8 同机）：bench_core 长尾 ~18.5 万/s（197/185/185）、bench_model ~1.87 μs/条、bench_engine 每批 1000/5000 合计 ~5.84/~4.65 μs/条、单次定稿 ~25 ms、满载快照 ~52 ms、完整 tick ~116 ms。**T8b 未触及任何热路径**，正常路径与 T8 代码相同，差异在本机波动范围内；bench_model 与每批合计相对 T8 持平或略快。
- 本地 `python -m flowlens` 的 `/healthz` 现在 `memory_bytes` 是真实 WorkingSetSize（约 15 MB），不再是 0。

### 2026-10-18 T10a 压测工具、离线精确解、边界用例（tools/flowgen.py、tools/exact.py、tools/loadgen.py、tools/boundary_cases.py、tools/selftest.py、tests/test_tools.py、tests/e2e/test_boundaries.py、flowlens/query.py、flowlens/api/app.py、tests/e2e/conftest.py、tests/e2e/test_api.py、docs/*）

**一、事件生成 `tools/flowgen.py`（可导入模块 + CLI）**：三种确定性分布（长尾 Zipf s=1.1/0.9、均匀、全随机不重复）；URL/用户用整数编号表示——`key_of(i)=/api/item/x{i}`（末尾 `x{i}` 以字母开头、不命中 {id} 归一化规则）、`raw_url(key, rng)` 只在 key 上加 query 与 fragment（归一化整体剥掉）→ `normalize_url(raw)==key`。**关键决策：为什么原始 URL 只加 query/fragment 而不是路径段**——压测数据要同时满足"覆盖 URL 归一化路径"与"离线精确解能和服务对上"。若原始 URL 的路径段归一化后变掉（例如 `/item/123` → `/item/{id}`），精确解按编号 `i` 分组、服务按归一化 key 分组，两者的"划分"不同，Top-K 对不上、无法验证召回。让归一化只剥 query/fragment，两边落到**同一个划分**（同一个 URL 编号 = 同一个归一化 key），精确解与服务才可比。bytes 对数正态（p95≈10 KB）；迟到 61.5~119.5s、未来 5.5~30s、正常 0~3s，都避开 60s/5s 判定边界 ±0.5s（"服务按收到时刻、精确解按发送时刻"两口径结论一致）。可配迟到/未来/跨批重复/批内重复/非法比例。`ArchiveWriter` 只存被接受（202）批次的整数列（npz）、多 worker 按 shard 写、`finalize_archive` 合并 meta.json。

**二、发送器 `tools/loadgen.py`**：多进程（spawn，Windows 上本就是 spawn、Linux 也统一用 spawn 避免 fork 继承状态）× 每进程 C 并发连接（aiohttp `trust_env=False` 避开 WSL 自动代理）；总速率平均分给各进程、按"理想发送时刻"发批（`next_send_at += batch/rate_pw`，有界队列 BATCH_QUEUE_CAP=8 背压防内存无界）；503 按 Retry-After 重发同一批（同 event_id）、连接错误短退避重试、重试次数可配；每秒一行 JSON、结束一行汇总（含 `client_cpu_pct`，判断压测工具是否成瓶颈）。**踩坑（WSL 实测 2 万/秒时曾少计 ~86% 的 accepted）**：初版按"发起时刻"归秒累加 accepted，但响应是跨秒异步到达的、写进了已发送的旧桶 → 少计。改为**每批完成时**才把该批一次性计入"完成时刻"所在秒（collector 按 done_q 汇总），每批只计一次，accepted 不再丢。

**三、离线精确解 `tools/exact.py`**：读存档按服务规则求"被接受集合"——去非法/未来、按 **sent_ms** 判迟到（`ts < sent_ms−60s`）、event_id 去重保留首次（顺序：迟到/未来判断在去重**之前**，与 `engine.ingest` 先 classify 再 bloom 一致，迟到的重复事件不进"已见"集合、不影响后到同 id 事件）。按 ts 排序建索引，任意毫秒区间 `[from,to)`（半开，`searchsorted(side="left")`，等于 to 的行排除）查 topk（次数降序、并列按 key 字典序，与服务一致）、distinct_users、quantile_bytes、count_of。`compare()` 算召回（真实次数 ≥ 精确第 k 名即命中、并列不算错）、计数相对误差、真实值是否落 [lower,upper]。**分位数口径**：目标秩 R=⌊q(n−1)⌋+1（1 起算）= numpy `quantile(x,q,method="lower")`，与 `sketch.DDSketch.quantile`（目标排名 ⌊1+q(n−1)⌋）同一套秩定义；DDSketch 在同一秩上返回桶代表值（相对误差 ≤ α）、精确解返回该秩真实值——**位置相同、取值精度不同**，"服务 p95 ≈ 精确 p95"是同一秩上的近似。

**四、边界用例 `tools/boundary_cases.py`（18 条）**：`tests/e2e/test_boundaries.py`（进程内 TestServer）与 `tools/selftest.py`（对运行中服务 --url、打印表格、任一项失败退出码非 0）共用同一套 `CASES` + `Target` 抽象（进程内 TestClient、远端 ClientSession）。题目原列 14 条，补到 18（gzip、解压炸弹、SSE 重连、区间无数据 p95 为 null 等）。**SSE 重连用例的放宽**（c18）：带 Last-Event-ID 重连后要求"id 连续、不重发已见帧"是硬要求；但首帧**不要求恰好 = last+interval**——若第一个连接断开后订阅组已被释放，重连开新组、新生产者从"当前网格点"起产帧，首帧是 ≥ last+interval 的下一个网格点（负载高时重连可能跨一格）。所以断言 `got[0] >= last+interval`、`got[0] % interval == 0`、`got[0] <= last+4*interval` 且之后逐帧 +interval，不锁死首帧精确值。

**五、工具自测 `tests/test_tools.py`**：生成确定性、分布形状（长尾前 10 占比 >30% vs 均匀 <6%）、全随机不重复、`raw_url` 归一化（对 >3000 条随机样本逐一验证 `normalize_url(raw)==key`）、时间戳避边界、存档往返（多 shard）、精确解对照**最朴素逐条实现**（40 个随机毫秒区间逐项比 topk/distinct/quantile/count_of，含毫秒边界）、compare 召回并列口径、发送器 503 重试+存档+逐秒/汇总 JSON 格式（用 `TestServer` 起桩服务、前 2 次 503 后 202）。

**六、`/v1/stats` 支持 `at`（`query.py`/`app.py`）**：原 `/v1/stats` 只收 window、默认恒读快照；改为与 `/v1/topk` 同口径——不带 `at` 且 60s 读快照，带 `at`（或非 60s）走回放合并概要、at 早于保留期同样 422、第 1 级同样暂停。**AI 给的方案被修改**：最初想"只在 /v1/stats 上加 at、复用 /v1/topk 的回放路径"，但 /v1/stats 走的是 `collect_stats`（HLL+DDSketch 概要合并）而非 `collect_parts`（CMS 时间片），两者是不同的组装路径，不能直接复用 topk 的回放逻辑；实际做法是 `stats(window_ms, at_ms=None)` 在 `at_ms is None and window==60s` 时读快照、否则回放 `collect_stats`，与 topk 的 at 语义对齐但走各自的数据路径。原因：边界用例"区间无数据 approx_p95_bytes 为 null"需对**已运行**（默认窗口有数据）的服务指向一个无数据的过去区间，必须能用 at 回放（DESIGN_TODO #16）。`test_api.py` 里 mock 的 `qs.stats` 桩补一个 `at_ms=None` 形参以匹配新签名。

**七、与指令不一致 / 说明**：
1. 端口用 9000 而非 8080：本机 WSL2 的 80xx 段 bind 报 errno 98（被保留，`ss` 看不到占用者），9000/12345 可用。这是开发机环境特性、非服务问题；评测容器 `--memory` 走 8080 不受影响（记入 PROGRESS 决策）。
2. `loadgen` 的 `main()` 原有一处 `if crashed:` 引用了未定义变量（`crashed` 只在 `summary_dict` 里是局部量），会 NameError——改为 `if any("error" in d for d in done)`（按各 worker 的 done 消息判断崩溃），这也是 T10a 收尾发现的 bug 修复。
3. 基准的 T10a 列整体比 T8b 列快（bench_core 18.5→26.4 万/s、完整 tick 116→85 ms），**这是本机单次/时段波动、非 T10a 引入的优化**——T10a 未触及任何热路径（服务端只改了 query.stats 加一个 at_ms 参数与一处调用，其余全是 tools/ 与 tests/ 新增），正常路径代码与 T8b 完全相同。判读跨任务基准应以"同一次采集内各指标是否自洽"为准（见 PROGRESS 基准说明）。

**八、WSL 三步实跑（端口 9000）**：服务启动（healthz ok、内存 ~105 MB、未降级）→ loadgen 4 进程 ×8 并发、2 万/秒长尾 60s、gzip、存档 → **120 万条 100% 接受、0 拒绝 0 错误、接受速率 19670/秒、客户端 CPU 48%、延迟 p50 73ms/p95 132ms/p99 151ms** → selftest **18/18 通过** → exact.py 最近 60s 精确 Top-10 与 `/v1/topk`（同 at）对比：**召回 1.000、平均相对误差 0.18%、最大 0.27%、真实值全部落在 [lower,upper] 内**（x0 精确 171371 vs 服务 171133；去重用户精确 158084；p95 精确 10031）。

**验证（两平台均全过）**：Windows `pytest -q` **263 passed, 5 deselected**、`-m slow` **5 passed**；WSL（Python 3.14）`pytest -q` **262 passed, 1 skipped**（1 skip=仅 Windows 的 memstat 真实读数）, 5 deselected、`-m slow` **5 passed**。基准（Windows，每项 3 次中位，与 T8b 同机）：bench_core 长尾 ~26.4 万/s（282/264/243）、bench_model ~1.45 μs/条、bench_engine 每批 1000/5000 合计 ~5.01/~4.15 μs/条、单次定稿 ~18.9 ms、满载快照 ~48.0 ms、完整 tick ~85.2 ms（未触及热路径，数值高于 T8b 列是本机波动，见上）。

### 2026-10-18 T10b 实验场景与第一次端到端粗测（flowlens/memstat.py、flowlens/model.py、flowlens/api/app.py、tools/flowgen.py、tools/exact.py、tools/experiments.py、tools/summarize.py、tools/bench_model.py、tests/test_model.py、tests/test_memstat.py、tests/test_tools.py、tests/test_experiments.py、tests/e2e/test_stream.py、tests/e2e/test_api.py、tests/e2e/test_experiments_smoke.py、Dockerfile、constraints.txt、.gitignore、.dockerignore、docs/EXPERIMENTS_FORMAT.md、docs/DESIGN_TODO.md、docs/PROGRESS.md）

**一、第零节前置修正（4 项）**：
1. 容器内存构成 + 工作集读数：`docker run --memory=256m` 里实测 `memory.stat`——空闲 `memory.current` ≈169 MiB 中约 60 MiB 是不活跃页缓存（`inactive_file`）；5 万/秒×30s 后 ≈248 MiB（峰值摸到 256 上限），file 几乎不变（60.4→60.4）、真实增长全在 anon（85.5→164）。`memstat.py` 拆成两个读数：`memory_current_bytes`（原始）+ `memory_bytes`（工作集 = current − inactive_file，v1 为 total_inactive_file）；`/healthz` 加 `memory_current_bytes` 对照、降级判断与 `memory_bytes` 用工作集；读不到 memory.stat 退回原始、无 cgroup 上限用进程读数。**推翻 T7b"保持 memory.current 原值"决定**（原口径空闲即 66%、离第 3 级 78% 只差 12 点、稍有负载就降级，会干扰所有实验）。补 `tests/test_memstat.py` 用临时目录伪造 memory.stat。
2. 镜像内测试 + 依赖约束：Dockerfile 加 `test` 阶段（与 base 同基础同依赖 + pytest + 复制 tests/tools、跑 `pytest -q`）；constraints.txt 在 python:3.12-slim 重新 `pip freeze`（**发现 3.12 比 3.14 多 `typing_extensions==4.16.0`**，原 3.14 生成的 constraints 缺它、3.12 上会装不全）。
3. 修 flaky：SSE `test_shared_computation_same_params`/`test_recompute_at_most_once_per_second` 从"真实 3s/2.5s 窗口数后台计算次数"改为 **FakeClock + 手工步进网格点、直接驱动 producer 节流逻辑**（与机器负载解耦，连跑 20 次全过）；`test_all_missing_no_fake_data`（T8b 遗留）改 monkeypatch 固定替身（原连读两次真实 WorkingSetSize 再断言相等，Windows 上随内存漂移偶发失败）。
4. `.gitignore` 补 `build/ dist/ results/`。

**二、第一节（压测流量更真实）**：`flowgen.py` 约一半 key 模板含 `{id}` 段（`/api/order/{id}/x{i}`、`/api/user/{id}/profile/x{i}`，比例可配），`raw_url` 把 `{id}` 换成随机纯数字/UUID/长十六进制、按比例加 scheme+主机名 + query + fragment，保持 `normalize_url(raw_url(key))==key`（全池注入性验证）。`model.py` 归一化缓存**先剥 query/fragment 再查**（`str.find` 找第一个 `?`/`#`、截掉之后部分查缓存），scheme/主机名去除仍在缓存未命中时做。`bench_model.py` 加"真实流量"组（flowgen 生成、带随机 query 与路径 {id}）：改前 ~3.87 μs/条 → 改后 ~0.50 μs/条（**约 7.7×**）；缓存命中组不受影响（真实流量下原以整条原始串为键、几乎不命中、每条走正则，改后按稳定前缀命中）。`tests/test_model.py` 把改前 `normalize_url` 原样复制为参考实现、用大量随机原始 URL 逐一比对（结果完全一致）。

**三、第二节（实验脚本）**：`tools/experiments.py`（S1 吞吐爬坡 / S2 混合负载 / S3 精度 / S5 过载恢复；`--target docker|local`、docker 目标 `docker run --rm --memory=256m --memory-swap=256m --cpuset-cpus=0`、压测 `taskset` 到其他核、端口 9000 避 WSL2 保留段；每场景全新服务、逐秒采样 /healthz + 服务 CPU）、`tools/summarize.py`（读结果目录打印 Markdown 汇总）、`docs/EXPERIMENTS_FORMAT.md`（结果文件字段，供 T10c 控制台回放）、`tests/test_experiments.py`（纯函数单测：pctl/level_ok/max_sustainable/mem_timeline/parse_loadgen/merge_timeline/pick_finalized_intervals + summarize 分桶）、`tests/e2e/test_experiments_smoke.py`（slow 冒烟）。按补充要求把"工作集起始/峰值/结束 + 降级级别是否变化、正常负载进 3 级单独标出"做进 timeline 与 summarize。**踩坑**：Dockerfile 最后一个阶段是 `test`（跑测试、无 CMD、直接退出），`experiments.py` 的 `_build_image` 必须显式 `--target app`，否则建出测试镜像、容器起来就退。

**四、第三节（第一次端到端粗测，WSL docker，结果 `results/20261018_190021/`）**：S1（不压缩 + gzip）、S2、S3（长尾 3 万/秒×5 分钟）、S5 全跑完。**最大可持续吞吐 = 5 万/秒**（接受率≥99%、无 503、写入 p99<500ms 的最高级；10 万/秒 p99 ~1.2s、30 万/秒 503 4%、服务 CPU 单核 100%）。混合负载（3.5 万/秒）查询 p99：topk_default 195 / topk_replay 156 / stats 171 ms，**均远超 p99<50ms 目标**（S2 峰值工作集 210.5 MiB、进第 3 级）。过载 S5（7.5 万/秒×60s）进第 1 级、停发 ~1s 回 0 级、无崩溃。**主要发现（内存增长）**：内存随**持续写入填满 600s 保留期**增长、不回落（与瞬时速率无关）——S3 累积 588 万条后工作集 ~218 MiB、3 级@90s、4 级@200s；S2（3.5 万/秒×120s）也到 210.5、进 3 级。S3 结束时服务仍 4 级，回放查询暂停/返回空 topk，200 个抽样全落在降级期，**召回 1.0 / 误差 0 / stats 全 null 是空响应的假象 → S3 精度数据无效，留 T9b 解决内存后重做**。且与 C1 账本矛盾（进程内同样 3 万/秒×11 分钟常驻仅 ~97 MB vs 容器 ~229 MiB，差 ~90 MB）。**本任务不做任何内存优化，T9b 专门查。**

**五、收尾修复**：`exact.compare` 修 bug——服务返回空、精确答案非空时召回记 **0**（原 `hits/n if n else 1.0` 在 n=0 时记 1.0，把降级期空响应误判成完美精度；两皆空才 1.0）。S3 每条抽样记录 `status` 与 `degraded`、分 `valid`/`degraded`/`rejected` 三桶，精度只由 valid 统计（`summarize.py` 同步、兼容旧格式并标"不可信"）。`summarize.py` 修 `client_cpu_pct` 显示（原 `_fmt_pct` 把已是百分数的值当分数、显示成 12990%）。

**六、与指令不一致 / 被修改否决的地方**：
1. **S3 未按"3 万/秒×5 分钟出精度数字"交付**——用户明确指示：不降低速率重跑（那会绕开最重要的发现），如实写明精度无效、留 T9b。内存增长是本任务**最重要的发现**，比精度数字更重要。
2. 基准的 T10b 列为**单次**（非 3 次中位），且首轮 bench_core/bench_model/bench_engine 与 Docker build 并发跑、机器受载，已单独干净重跑一次取数。
3. `experiments.py` 的 `_build_image` 显式 `--target app`（Dockerfile 默认目标曾是 test 阶段）；收尾后已把 app 挪回默认目标。

**验证（全过）**：Windows `pytest -q` **290 passed, 2 skipped, 9 deselected**、`-m slow` **9 passed**；Docker（WSL，Python 3.12）`docker build --target test` **291 passed, 1 skipped, 9 deselected**。基准（Windows，与 T10a 同机）：bench_core 长尾 ~30.0 万/s、bench_model 缓存命中 ~1.80 μs/条、**真实流量组 3.87→0.50 μs/条（归一化缓存剥离 query/fragment 生效）**、bench_engine 每批 1000/5000 合计 ~5.24/~4.13 μs/条、单次定稿中位 ~18.2 ms、满载快照 ~46.5 ms、完整 tick ~83.0 ms。除归一化缓存显著变快外，未触及其他热路径、未使任何路径变慢。

### 2026-10-18 T11a-2 修正测量并补充 GC/归一化缓存统计，完成 CPU 剖析（tools/profile_server.py、tests/test_profile_server.py、docs/perf/T11a-report.md、docs/PROGRESS.md、results/T11a/run_100k.json、run_60k.json）

**一、测量方法的两处修正（任务书第一节）**：
1. **测量窗口改墙钟 deadline**：上一版测量循环用 `sleep(0.5)×N` 累计名义时长——满载下每次 sleep 都被事件循环延迟触发（实测循环延迟 p50 68 ms），名义 30 s 拖成 47 s，而 loadgen 只发 70 s（预热 30+测量 30+余量 10、**没算起 loadgen 后的 3 s 建连延迟**），窗口尾部 ~10 s 没流量，把 `accepted_rate`（稀释到 79 426/s）与 `cpu_pct_of_core`（84.7%）稀释了。改为 `deadline = monotonic() + measure`、循环条件 `monotonic() < deadline`，loadgen 时长改 `3s 启动 + 预热 + 测量 + 10s 余量`（73 s）。重跑后 `measure_sec` 实测 30.1 s，干净。
2. **垃圾回收单独计账**：`gc.callbacks` 原来只记代数/耗时/对象数，GC 烧的 CPU 混在各阶段的 `process_time` 差值里（阶段之和虚高，把一次 18.9 ms 的 gen2 回收算进 3.8 ms 的 orjson 解析）。现在每次回收记**墙钟起止**（`GC_SPANS`/`GC_ENDS`），各阶段包装时用 `_gc_overlap_ns`（bisect 定位、O(log n)）算"调用时段与回收区间的交叠"、从该阶段 wall 与 cpu 里各扣一次（单线程下交叠部分两者相等），新增 `gc_us` 字段。校验口径改"（扣后阶段之和 + GC 总耗时）/ 进程 CPU ≈ 1.0"（10 万 0.948、6 万 0.932）。效果：load_array 单阶段扣掉 61～131 ms 的 gen2 回收、tick_finalize 扣 4～23 ms。
   - **实现踩坑**：`_gc_overlap_ns` 初版交叠公式写错（把 `e-t0` / `t1-s` 直接加，没做 `max(t0,s)/min(t1,e)` 区间求交），单测锁住；`GC_SPANS` 线性扫会让每条事件多花 ~μs，改 bisect + 平行 end 列表压到 O(log n)（回收稀疏时绝大多数调用直接返回 0）。

**二、归一化缓存命中率（任务书第一节第 3 条）**：运行时包装 `flowlens.model.normalize_url`（总调用）与 `_normalize_url`（未命中才调）——**只包装、不重写 model 内部逻辑**（初版曾把 model 的查表+回填逻辑复制进包装函数，违反"只加计时不改行为"，已改为用 `_norm_pending` 记录每次调用的原始 raw、未命中时配对判断"带 ID 段"，不重复 model 的缓存语义）。结果：**命中率仅 38.5%**（两组一致），未命中 4.4～5.0 μs/次，**未命中里 90.6% 是路径带随机 ID 的**（flowgen ~50% URL 路径含 `{id}` 段、替换成随机数字/UUID/十六进制、路径每条都不同、缓存必未命中）。这解释了 validate 占核 60%+（7.2～8.2 μs/条）——是 T11a 那版"validate 满载慢 2.9× 根因未查明"的**答案**：不是内存压力，是缓存命中率天然低。

**三、两组测量（WSL 容器、256 MB、服务 0 号核、假内存读数）**：
- **10 万/秒**：核 **97.2%**（饱和）、接受 **72,911/秒**、每条 CPU **13.33 μs** → 单核上限 ~7.5 万/秒。
- **6 万/秒**：核 **71.6%**（余量 40%）、全部接受 **59,973/秒**、每条 CPU **11.94 μs** → 单核上限 ~8.4 万/秒。
- **单核可持续（无降级干扰）≈ 7.5 万～8 万/秒**。
- 加了一个**查询探针**（仅 6 万/秒组开，每 250 ms 一次 `topk(URL, 60s, k=10)`）：60 s 窗口 k≤100 命中快照路径，**p99 0.04 ms（亚毫秒）**——直接回答"6 万/秒时查询 p99 能否低于 50 ms"：默认查询远达标。
- **事件循环延迟**：6 万/秒 p50 66 μs（探针自同步、偏乐观）、p99 41 ms / max 61 ms ≈ 一个 **5000 条写入同步块**（wall 54～61 ms）；10 万/秒核饱和后 p50 被拖到 204 ms。最长停顿主因是写入块、次因是 gen2 GC（13～19 ms 单次）。

**四、报告**：`docs/perf/T11a-report.md` 六节（两组结果 + 扣 GC 后阶段表、GC 分代统计、归一化缓存、事件循环延迟分布与最长停顿来源、6 万/秒查询 p99<50ms 判断、按收益排序的 5 个优化方向：① 归一化未命中提速 ② 写入块 5000→~1000 ③ `gc.freeze()`+阈值 ④ 快照改维护窗口汇总 ⑤ gen2/tick 拆段，各自写明阶段/预计节省/需改文件/是否冻结/风险）。

**五、与指令不一致 / 踩坑**：
1. **6 万/秒组首两次（带查询探针）被 WSL2 宿主负载抖动污染**：内存 8～14 s 就摸到 220 MB 提前结束、loadgen 36～51% error、`measure_sec` 只有 8～14 s。确认宿主空闲（load 0.08、无孤儿进程/容器）后第三次跑干净（30.1 s、0 error 窗口内、核 71.6%）。**教训：容器测量对宿主瞬时负载敏感，跑前要看 `uptime`/`docker ps`**。
2. 查询探针走**快照路径**（60 s 窗口 k=10 ≤ snapshot_top_n=100），计算亚毫秒、探针 CPU 开销可忽略，未影响 6 万/秒组的吞吐数字（核 71.6% 与无探针一致）。
3. **loadgen 汇总的 `errors` 不是服务端问题**：是测量结束后 10 s 余量里客户端往正在关闭的容器发送的连接重置（`sent=accepted+rejected+errors` 严格相等、error≈10s×目标速率）。测量窗口内服务端 0 拒绝 0 重试。

**六、验证**：Windows `pytest -q` **302 passed, 2 skipped, 9 deselected**（新增 4 条 profile 测试：GC 交叠、`_id_seg_like` 对照 model、norm 包装行为不变、报告新字段）；`tests/test_profile_server.py` 单独 12 条全过。**未触及热路径**（只改 `tools/profile_server.py` 剖析工具与其测试，未动 `flowlens/`），**不跑性能基准**（任务书明确：本任务不修改服务代码）。收尾的 `docker build --target test` 在 WSL Python 3.12 上执行（见收尾汇报）。

### 2026-10-19 T11b（缩小范围）：子进程编码修正 + 归一化未命中快路径 + validate 列式快路径（flowlens/model.py、tools/、tests/）

**范围**：用户中途明确"今天是提交截止日，T11b 缩小范围——只完成第零、一、二节，第三～六节全部跳过"。第五节第 1 项（Dockerfile 固定 glibc 大块分配阈值）由支线 lane-c 做对照实验中，本任务不碰 Dockerfile。

**一、第零节（子进程与文本文件编码）**：4 处 `subprocess` 文本模式调用加 `encoding="utf-8", errors="replace"`（tests/test_packaging.py、tools/experiments.py 的 `git_commit`、tools/profile_server.py 的 `_run` 与 loadgen Popen——后者其实已有，核过没漏）；`tools/` 与 `tests/` 里读 `/proc` 的 3 处 `open()`（experiments/mem_probe/mem_report）补 `encoding="utf-8"`。grep `text=True`/`universal_newlines` 确认无遗漏。

**二、第一节（归一化未命中快路径，model.py）**：
1. `_normalize_url`（缓存未命中才走）改名 `_normalize_url_impl`，返回 `(结果, 是否发生 ID 段替换)`。无 `"//"` 时跳过 scheme/多斜杠两个正则（一次判断同时跳过两个——后续处理只删字符、不造 `"//"`）；ID 段判断用预编译正则 `_ID_SEG`（`[0-9]+` / UUID 8-4-4-4-12 / `[0-9a-fA-F]{16,}`）的 `fullmatch` 替代逐字符 `_is_hex` 循环。字符类显式 `[0-9]`/`[0-9a-fA-F]`、**不用 `\d`**（`\d` 会匹配其他语言数字，与既有 `isascii()` 前置判断不一致）。
2. **缓存策略**：发生 ID 段替换的路径不写缓存（这类键几乎每条都不同、几乎不会再次出现，写进去只会把能命中的稳定路径挤出）；稳定路径照常缓存。`_HEX_DIGITS`/`_is_id_segment`/`_is_hex` 保留（profile_server 的 `_id_seg_like` 对照口径与参考实现仍用）。

**三、第二节（validate 列式快路径，model.py）**：`validate` 先检查"全部元素是 dict"（否则直接走逐条慢路径——列式取列依赖 `.get`，对非 dict 不安全）；全 dict 时一次性列表推导取五列（`dict.get`）、整列 `type(x) is str/int`（`type is int` 天然排除 bool）+ 非空 + event_id 长度 + ts/bytes int64 范围检查；全合法走 `_validate_fast`（user_id 仅"非 ASCII 或 len>128"才走按字节截断的 `_truncate_utf8`，ASCII 且 ≤128 直接复用；url 列表推导调 `normalize_url`），任一条不合法退回 `_validate_one_by_one`（保留原逐条实现，不删）。

**四、等价性测试（tests/test_model.py）**：把改前的 `normalize_url`/`_normalize_url`/`validate` 及依赖（`_is_id_segment`/`_is_hex`/`_truncate_utf8`/`_is_int`/常量）原样复制为 `ref_*`，与被测代码独立。覆盖：① 全部既有表驱动用例 + 专门构造的边界（Unicode/阿拉伯-印度/天城文数字、全角字符、恰好 15/16 位十六进制、大小写混合 UUID、UUID 末位 g、截断 128 字节边界、多字节字符中间截断、空/单段/`//`/scheme/query/fragment 各位置）逐一断言 `normalize_url` 与 `ref_normalize_url` 一致；② flowgen 生成 5000 条随机 URL 逐一比较；③ validate 全合法（3000 条 flowgen + 超长 ASCII user_id）、各种非法类型混入（非 dict/缺字段/类型错/bool 冒充整数/ts=0/负数/int64 上界/超长 event_id/非 ASCII event_id/超长多字节 user_id/空列表）逐列比较 EventBatch（ids/users/urls/ts/nbytes/invalid）；④ orjson 把 ≥2^64 整数转 float 的口径；⑤ 快路径 vs 慢路径在"混入一条非法"时的切换一致性。另加行为测试：无 `//` 时不调 scheme/多斜杠正则（monkeypatch 抛错验证）、带 ID 路径不写缓存、稳定路径写缓存。

**五、适配被改名函数的引用**：`tools/profile_server.py` 的归一化命中率包装从 `_normalize_url` 改包 `_normalize_url_impl`（T11b 起它返回 `(结果, replaced)` 元组，包装透传、计数口径不变；`NORM_STATS` 仍是总调用/未命中/未命中耗时/带 ID 段占比，T11a 报告的可比性不受影响）。`tools/bench_model.py` 重写：改前基准用原样复刻的 `_base_normalize`（逐字符判 ID），改后用 `_normalize_url_impl`，报告未命中路径改前/改后耗时 + 命中率（改前策略 monkeypatch 让 `replaced` 恒 False 模拟"一切路径都写缓存"）。

**六、bench_model（Windows，单次）**：
- 未命中路径：改前 **3.57 μs** → 改后 **3.04 μs**（**1.17×**）。分解：无 `//` 子集 3.20→2.31（**1.38×**），有 `//` 子集（flowgen 50% URL 带 `https://` 前缀）3.72→3.62（1.03×，仍跑正则）。整体被 50% 带 `//` 的流量稀释。容器 T11a 口径（未命中 4.4~5.0 μs）是"每条 URL 都不同 → 缓存必未命中"的极端，本基准 flowgen 有 500 个 key 池、部分命中，故绝对值偏低、且 1.17× 未达"4.7→1.5"目标——**该目标是针对未命中绝对值的，需容器 S2/S1 复测**；本基准证明快路径正确（输出逐字节一致）且在无 `//` 路径上确有 1.38×。
- 命中率：改前策略与改后策略在本 flowgen 流量下**都是 37.6%**（带 ID 路径无论缓存与否都命中不了；改后策略的价值是"不占缓存槽位、缓存满时不把稳定路径挤出"，在本 500-key 池 + 200 种 URL 的小池下没体现差异）。
- 缓存命中组（200 种重复 URL）：4.3~5.0 μs/条，与 T10b 的 1.80 有差距，但**直接对照新/旧 `validate`（同 body）为 3.95 vs 4.10 μs/条（1.04×）**——列式快路径没有拖慢全合法路径，差距是本机时段波动（与 T10b 不同机器/时段，见 PROGRESS"基准数据"判读口径）。

**七、与指令不一致 / 被否决的 AI 方案**：
1. **validate 列式取列前未先判"全 dict"**：初版直接 `o.get("event_id") for o in objs`，遇到非 dict 元素（如 `"not-obj"`）抛 `AttributeError: 'str' object has no attribute 'get'`，`test_element_not_object`/`test_invalid_count_and_valid_kept` 挂掉。修复：取列前先 `any(type(o) is not dict for o in objs)` → 有非 dict 直接走逐条慢路径（慢路径本就逐条 `isinstance(obj, dict)` 判非法、计入 invalid，语义不变）。
2. **user_id 截断的快路径判断写错**：初版 `len(u) > 128` 才截断——但截断判的是**字节**长度，`"中"*100`（100 字符、300 字节）字符数短字节数长会被漏截。改为"ASCII 且 len≤128 才跳过截断"，非 ASCII 一律走按字节的 `_truncate_utf8`。等价性测试用 `_ev(uid="中"*100)` 锁住。
3. 用户补充指令（第五节第 1 项由 lane-c 做、先 `git pull` 确认 Dockerfile、跑容器前问支线）——本任务缩小范围后不涉及第五/六节，未跑容器实验，Dockerfile 未动。

**八、验证**：Windows `pytest -q` **340 passed, 3 skipped, 9 deselected** + 1 个 `tests/test_memstat.py::test_windows_working_set_reads_real_value` 偶发失败（Windows 专属、读两次 `WorkingSetSize` 差 4 KB，重跑 2 过 1 挂，与本任务改动无关，本任务未碰 memstat.py）；Docker（WSL，Python 3.12）`docker build --target test` **342 passed, 2 skipped, 9 deselected** 全过。`bench_model` 见上。**未走退路**（等价性测试全部通过，未 `git checkout` model.py）。

### 2026-10-18 支线 B1/B1b：Docker 与调试接口（Dockerfile、.dockerignore、run.sh、flowlens/api/debug.py、flowlens/api/app.py、tests/e2e/test_debug.py、tests/e2e/test_signals.py）

- 调试接口 `GET /debug/structure`（`FLOWLENS_DEBUG=1` 门控）由 AI 起草。**被否决/修正**：① `_plan_summary` 初版"自己判断环/树边界"被否——指令要求复用 `engine._plan`（真实查询唯一的划分入口），改为直接调 `_plan` 转出 `ring_buckets`/`history`/`edges`；② "前 3 个 key"初版直接读 `IndexedMinHeap` 私有字段 `q.prios`/`q.vals` 做 `heapq.nlargest`，被改为走 `TopK.top(n)` 公开接口（冻结模块能走公开接口就不碰私有堆字段）。
- **Docker 多阶段 → 单阶段**：初版写 builder/runtime 多阶段，实测 `pip install .` 因 pyproject 漏 `flowlens.api` 装出坏包，遂改"单阶段装三个依赖 + 拷源码"；B1b 再改回 `pip install -c constraints.txt .` 装完删源码（`packages.find` 修复后）。
- **优雅停机**：AI 定位"PID 1 忽略 SIGTERM"（用"纯 sleep 当 PID 1"对照实验证明与业务无关），B1 方案是镜像内加信号转发 init（heredoc 在经典构建器下 0 字节失败，改 base64 内嵌 + `py_compile` 校验）；B1b 把 SIGTERM 处理移进 `__main__.py`（`loop.add_signal_handler`）、去掉 init、CMD 改回 `python -m flowlens`。
- 测试数据装载：`test_debug.py::_load_tree` 逐秒写入并推进 `FakeClock`（`advance()` 只在 watermark 越过该秒时定稿）。初版想"倒序写旧秒"被否（旧秒判迟到、不进树）。
- 只读使用冻结模块内部属性（`Tree.levels`/`Tree.builders`/`Ring.slots`、`_Plan`/`_PlanEdge` 字段等，只读不改、清单见 docs/lanes/B1-notes.md）。

### 2026-10-18 支线 C1（T9a）：内存账本（tools/mem_report.py、tests/test_mem_report.py）

- 方案：满载走完整真实写入路径（flowgen → orjson → `IngestService.ingest` → `engine.tick()`）；记账用"按对象 id 去重的深度遍历 + 组件有向遍历"（共享 seen、先到达者记账）。**被修正/发现**：① 初版把"组件"当遍历 owner，发现 `ring.finalize` 绑定方法经 `__self__` 向上走到 engine 导致大量对象提前归错（"活跃环·其他结构"虚高 8 MB），改为"每组件从自己子结构出发 + 跳过方法对象"；② 初版用"绝对 RSS"交叉核对，发现 Python 堆不归还已释放页（RSS 高水位），改"RSS 增量（相对空载基线）"。
- **事故：首轮全部数据作废**——conda editable install 指向主仓库，`python tools/mem_report.py` 脚本模式下 CWD 不在 sys.path，`import flowlens` 解析到主线（model.py 已被 T10b 改成"先剥 query 再查缓存"、url 字符串天然共享），首轮对象图/驻留测算全是主线行为。修复：启动时把项目根显式插到 sys.path 最前 + 打印 `flowlens.__file__` 自检。教训：worktree 的独立 tools/*.py 必须显式把项目根放 sys.path 最前，不能依赖 CWD。
- 被否决：曾考虑用 `objgraph` 第三方库做遍历，被"不引入新依赖"规则否决，自己写了 40 行 `MemWalker`；驻留方案用静态测算（值→对象集合直接算节省）而非双引擎重跑。

### 2026-10-18 支线 C2（T9b）：容器内查清内存增长原因（tools/mem_probe.py、tests/test_mem_probe.py）

- 方案：容器内探针逐 10s 采三层读数（cgroup 工作集 / 进程 RssAnon / Python 层 gc+allocated_blocks）以**区分**碎片 vs 泄漏 vs 并发；用 C1 的 `MemWalker` 统计"存活结构"，与工作集对照，两者之差 = "非存活 anon"（分配器保留 + 解释器 + HTTP 层）。
- **事故：探针自身 OOM**——满负载下探针的 30s `gc.get_objects()`+MemWalker 把 `memory.current` 从 243 MiB 推过 256 MiB 上限，内核 OOM 杀掉探针（dmesg 确认）。修复：`memory.current > 上限×0.80` 时跳过记账。教训：**探针自己也是容器里的一个进程，其测量开销会挤占被测进程本就紧张的上限**；被测系统近上限时，测量必须让位于保命。
- **被修正的结论**：初版报告把"90 MB 差距"直接归因"glibc 保留 + HTTP 层"（不拆口径）。复盘发现 C1 的 97 是**增量**口径、容器 229 是**绝对值**口径，直接相减混了口径——改为先对齐口径（增量 vs 增量 = 49 MiB 真实差距），结论从"碎片为主因"修正为"并发瞬时峰值叠加 ~45 是 C1 差距的最大单一成分、碎片 ~21 被存活结构差 −30 部分抵消"。教训：**跨实验对比内存数字前先核对口径（增量 vs 绝对值、事件数、平台）**。
- `MALLOC_ARENA_MAX=2` 假设（"多线程 arena 计数导致保留"）被实测否决（单连接也复现 ~53 MiB churn 保留、arena2 无改善）。踩坑：`taskset` 的 env 前缀（`NO_PROXY=...` 被当命令名执行、loadgen 没跑，第一次"基线"是无流量空跑）。

### 2026-10-19 支线 C3（T9b-A）：内存加固（flowlens/intern.py、ticker.py、engine.py、model.py、ingest.py、api/app.py、tools/experiments.py、tools/summarize.py、tests/）

- 方案：malloc_trim 放 Ticker（`_maybe_trim` 到点触发、记 `TrimStat`）+ `/healthz` 暴露；`intern.py` 提供 `Intern`（满则整体清空）+ user/url 单例 + 环境变量开关；`engine.ingest` 写桶前 `np.fromiter` 驻留 user；`model.normalize_url` 返回前驻留结果；`ingest.py` 的 `MAX_PENDING_BYTES` 改读环境变量。实验用 `SUST`（持续负载+停发尾巴）+ `S4`（全随机攻击）场景、`--extra-env` 切特性。
- **用户修改/否决点**：① 用户确认"主线未跑实验，可以跑容器"；② 用户确认"允许改 app.py 的 `_healthz` 加 `malloc_trim` 只读字段"（原清单不允许改 app.py，例外放行）；③ 用户追加 **glibc mmap/trim 阈值对照实验（g5）** 并**批准改 Dockerfile**（例外放行）固化 `MALLOC_MMAP_THRESHOLD_=131072`/`MALLOC_TRIM_THRESHOLD_=131072`——实测峰值 −57.7 MiB、246/246s 全程第 0 级，是本组实验最大单项收益；S3/S4 不重跑、等合并后在最终代码上统一跑。
- 消融结论：trim 是主杠杆（进 L4 拒写 → 不拒写、接受率 87.8%→100%）、驻留是次杠杆（中后段水位降 ~11 MiB、把"贴 3 级"压成"偶发尖峰"）、在途降 2 MB 无增益（保留为实验变量）。**T9b-B（缩 Bloom/Count-Min，需改冻结 config.py）不需要**——g5 已全程 0 级、无内存压力，且 C1 已证缩 Bloom/HLL 都是拿精度换内存。

### 2026-10-19 P1：解析进程池与写入并发限制（flowlens/parse_pool.py 新建、engine.py、ingest.py、api/app.py、__main__.py、tests/test_parse_pool.py、tests/test_ingest.py、tests/e2e/test_parse_pool_e2e.py）

- **目标**：把"逐条处理"里可并行的纯计算（load_array、validate 含归一化、三列 64 位哈希，约 9.5 μs/条）搬到多个解析子进程，主进程只留必须单写者的 classify/Bloom/采样/写桶/tick（约 3.5 μs）。统计状态仍只在主进程，单写者、无锁、去重语义不变。
- **用户五个关键决定（直接照做）**：① gzip 解压留在主进程（在途额度按解压后字节预留，解压后大小只有解压完才知道，解压进子进程会破坏"先预留再解析再去重"顺序）；② 解析进程数按"实际能用的核"算（`len(os.sched_getaffinity(0))` 与 cgroup cpu.max/cfs_quota 取小，`min(3, 核数−1)`，<1 不启用，FLOWLENS_PARSE_WORKERS 可覆盖）；③ 进程池在引擎创建**之前**启动并预热（spawn 上下文，initializer 做哈希自检）；④ 引擎写入核心只写一份（`ingest` 外层算哈希 → `_ingest_core`；新 `ingest_parsed` 组装 EventBatch 走同一核心；驻留在核心里、写桶前做）；⑤ 写引擎用 `asyncio.Semaphore(1)` 保护、按 2000 条分块、块间让出。
- **AI 方案被修正/踩坑**：
  1. **`BrokenProcessPool` 从 `concurrent.futures` 顶层 import 失败**——它在 `concurrent.futures.process`，改 import 路径。
  2. **整批 invalid 的计数口径**：初版把 ParsedBatch 各子块 invalid 都设 0、只在响应上 `+extra_invalid`，导致**响应与引擎计数器不一致**（`counters.invalid` 没计到，`test_ingest_parsed_equals_ingest` 断言 `(…,1) == (…,0)` 挂）。改为**整批 invalid 放第一个块**（`engine.ingest_parsed` 会像单进程 `engine.ingest` 一样把它计入 counters），全非法时切不出块则用单空块承载，响应与计数器口径自然一致。
  3. **子进程崩溃检测**：池已坏时 `submit()` 是**同步**抛 BrokenProcessPool（不是等到 await 才抛），初版 try 只包了 `await`、没包 `submit`，导致崩溃测试里异常直接漏出。改为 try 包住 `submit` 与 `await` 两步。
  4. **`_build` 半成品池泄漏**：自检失败时 ProcessPoolExecutor 已建但预热抛异常，会留下管理线程与残留子进程。加 try/except 在失败时 `shutdown(wait=False, cancel_futures=True)` 并置 `_executor=None` 再抛。
  5. **单进程路径不能"先 validate 全部再统一写"**：初版为了对称把 in-proc 也改成"先切块 validate 完、再统一写"，丢了原"逐块 validate→ingest→释放 dict→让出"的交错结构（大请求体会一次性 validate 全部才写、事件循环让出被破坏）。按决定五"不启用进程池时走现有路径"改回：in-proc 保持原交错结构、只是块大小 5000→2000 且写引擎段包进信号量；pooled 才走"子进程一次解析→主进程切块写"。
  6. **`_stop_parse_pool` 忘了 async**：aiohttp 的 `on_shutdown` handler 会被 `await`，写成同步函数导致 `app.shutdown()` 时 `TypeError: NoneType can't be used in await`。改为 `async def`。
- **等价性（核心，不依赖子进程，稳定跑）**：同一 body 经 `IngestService` 单进程路径（decode→validate→ingest）与进程池路径（decode→parse_body→ingest_parsed→2000 切块→invalid 放首块）写两个全新引擎，topk(url/user)/stats(distinct_users,p95)/计数器逐项相等；覆盖长尾（跨 3 块）、全随机不重复、含非法+批内重复、全非法、gzip、NDJSON。另 `test_ingest_parsed_equals_ingest` 直接对比 `ingest(parse_events)` 与 `ingest_parsed(ParsedBatch)` 两入口。
- **正确性**：503 后重试不判重（预留在池提交之前）、BrokenProcessPool→重建→恢复、worker 数计算（核数 1/2/4 与 env 覆盖/非法/夹到 [0,3]）、自检不一致→构造期 BrokenProcessPool、close 后无残留子进程（join + is_alive）。
- **验证**：Windows `pytest -q` **366 passed, 4 skipped, 9 deselected**（PYTHONHASHSEED=0，真子进程用例生效）；`-m slow` **9 passed**（engine 写入核心改了、保险起见跑）。基准（Windows 单次）：`bench_core 长尾` 约 29.9 万/秒（与 T10b 30.0 持平）、`bench_engine 每批 5000 engine.ingest` 2.31 μs/条（T10b 4.13、T8b 4.65——engine.ingest 拆分后更快，属本机时段波动，未变慢）。**容器实测（第四节）留待检查点二，本提交只到第三节等价性与正确性**。
- **未做**：第四节容器实测（S1/S2/S3/S4/单核）、PROGRESS 容器数字，按 P1 时间盒留到检查点二。镜像标签 `flowlens:p1` 未动（容器阶段才用）。

### 2026-10-19 支线 P1c：空闲内存基线（tools/idle_mem.py 新建）

- 按 P1c 一的要求先测基线（池关 / 池开 3，256 MB 容器、空闲 5 秒）：池关工作集 71.5 MiB、池开 3 为 141.7 MiB，每个解析进程 RssAnon 约 18.8 MB（工作集增量约 23.5 MB/个，含碎片），与背景"约 25 MB"一致。
- 测量工具 `tools/idle_mem.py`（WSL 宿主运行，与 s1_measure 同口径读宿主 /proc，不进容器）：主进程与各解析进程 RssAnon + /healthz 工作集/原始读数，断言解析进程数与期望一致。

### 2026-10-19 支线 P1c：四项内存调整（逐项提交，逐项测空闲内存）

- **一.1 线程数**：Dockerfile base 阶段 ENV 加 `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1`。容器内验证各解析 worker `Threads=1`、主进程 3（Python 主+GC+信号线程）。空闲：池关 71.5→70.2、池开 3 141.7→139.8 MiB（单项收益小，保留）。
- **一.2 worker 默认 3→2**：`resolve_worker_count` 默认 `min(2, 核−1)`，env 覆盖仍夹 [0,3]（实验可显式 3）。测试期望更新（8/4/3 核→2）。空闲池开 2 = 121.2 MiB（比 3 省 18.6）。
- **一.3 池开时在途额度默认 8 MB**：`ingest.py` 加 `POOLED_PENDING_BYTES=8MB` 与 `pooled_pending_limit()`（env 可覆盖），`make_engine` 池开时用它初始化 `Admission`；单进程路径不变（4 MB）。新增 `test_pooled_pending_limit_env_override`。
- **一.4 进 3/4 级立即 malloc_trim**：`_make_degrade_change_hook` 的 on_change 里 `new>=3 and old<new`（级别升到 ≥3）调一次 `ticker.malloc_trim()`。测试 `test_entering_level3_calls_malloc_trim_once`（可控内存读数进 3 级→恰好一次；级别不变不重复；退出再进再调）。
- **AI 方案被修正/说明**：① 一.4 的触发条件初版写成 `old<3 and new>=3`，被指令措辞"进入第 3 级或第 4 级的那一刻"纠正为 `old<new and new>=3`（3→4 也算进入第 4 级，也要 trim；4→3 回落不算）。② 空闲内存四项合并测（同一次构建），按进程数差值归因（调整 3/4 空闲无影响，注释写明）。③ Dockerfile 注释里"正式镜像内容与以前完全一致"一句与改动矛盾，改为"test 阶段只用于收尾验证"。
- **验证**：Windows 本地 `pytest -q` 369 passed（+2 新测试）；`docker build`（含 test 阶段 pytest，Python 3.12）通过。镜像 `flowlens:p1c-2f`（--target app）。

### 2026-10-19 支线 P1c：256 MB 下的验证（S1/S4/S3/单核，WSL docker，同 P1b 核分配）

- **S1（池 2w + 8MB 默认）**：验收标准 1 不满足。4 万档 95%、进 3 级；6 万起 4 级。主进程 CPU 在 4 万档仅 19.6%——**瓶颈是内存不是 CPU**：工作集 20 秒爬到 222 MiB > L3 线（256×0.78≈199.7 MiB）→ L3 收紧额度 → 拒绝。P1b §5（512 MiB）同样 ~220 MiB 不降级，差异纯在 L3 线位置（199.7 vs 399 MiB）。
- **S4（全随机 30k/s ×10 分钟）**：验收标准 2 不满足。第 1 分钟进 3 级、45 秒进 4 级，9/10 分钟卡 4 级，接受率 10.7%。调整 4 的 trim 生效（累计回收 30 MiB、57 次）但收不掉存活数据——满载平台 ~213 MiB > L4 退出线 209.9 MiB。
- **备选配置（P1c §二指示，不放宽标准）**：6 MB 在途无效（与 C3 结论一致：在途窗口不是瓶颈）；**1 worker 是池配置里最好的**（4/6 万 100% L0、8 万 98.8% L3），但仍 <9 万。
- **池关（单进程）对照**：8 万档工作集 200.6 MiB 同样碰 L3 + 主进程 CPU 87%——**统计状态常驻内存 ~200 MiB 是 256 MB 下的真约束**，池把解析 CPU 搬走（主进程 CPU% 65→30 @6万）但省不掉统计状态内存，2 worker 还顶 ~50 MiB 把 2w 配置推过 L4 线、接受率反而最低。**"2×单进程 @256MB" 买不到**：池的 2× 收益只在内存有余量（512 MB，P1b §5）时兑现。
- **S3（30k/s 长尾 ×5 分钟，与精确解比）**：池 1w 进 4 级、200 抽样全 503（无有效样本）；池关 200/200 有效——召回 1.000、落界 100%、去重 0.98%、p95 0.10% 与 T9b g3 参照一致，计数误差 0.35%/0.48%（参照 0.05%/0.09%，因本机 256 MB 下 30k/s 也进 L3、采样权重 2）。
- **单核对照（标准 4）**：`--cpuset-cpus=0` + s1_measure 新增 `--pool auto`（不注入 env，让服务自动决定）→ 解析进程数 0、4/6 万 100% L0，与已提交版本单核一致。**标准 4 满足**。
- **AI 方案被修正/踩坑**：① s1_measure 的 `--pool on` 会注入 `FLOWLENS_PARSE_WORKERS`，单核对照若用它会把"自动退回单进程"覆盖成"强制 3 worker"——初版命令用错了，改为加 auto 档。② S4/S3 用 experiments.py（自带监控逐秒采样 + 精确解），S1 用 s1_measure（严格串行、CPU 秒口径），两个 driver 各取所长，未合并。③ 一次后台 S1 被系统低内存保护杀掉，留下孤儿容器；重跑前先清容器。

### 2026-10-19 支线 P1c：收尾（S2 记录、验收表、DESIGN_TODO、最终验证）

- **S2 混合负载（仅报告）**：写入 42k/s（池 1w 最大可持续 6 万 × 70%）+ 各类查询 + 20 个 SSE，120 秒：查询 p99 50~95 ms（已提交版 160~173 ms，**明显降低达成**——块 10000 + 池分担解析后事件循环独占时间变短）；工作集峰值 208.6 MiB 瞬时碰 L3、写入接受 99.3%。结果入 P1-notes §6.12。
- **验收结论（P1-notes §6 总结）**：标准 1（S1 ≥9 万、2×、0 级）**不满足**、标准 2（S4）不满足、标准 3（S3）部分（池关达参照、池开精度作废）、标准 4（单核退回）**满足**、标准 5（pytest + docker build）**满足**。核心发现：**256 MB 下主进程统计状态常驻 ~200 MiB 本身贴 L3 线**，池的 2× 收益与 256 MB 的内存余量互斥；256 MB 下池配置里 1 worker 最好（6 万 100% L0、8 万 98.8% L3），但达不到验收组合。
- **设计文档待改项**：按惯例记入 `docs/DESIGN_TODO.md` 第 29~34 条（§3.2 进程池与单写者、§3.1 在途额度 8 MB/块 10000、§3.6 进 3/4 级立即 trim、§5 内存表按实测重写、§7 P1c 实验表、§8 吞吐取舍），DESIGN.md 本身未动（用户另行更新）。
- **最终验证**：`pytest -q` **369 passed, 4 skipped, 9 deselected**；`pytest -q -m slow` **9 passed**；`docker build --target test`（WSL，Python 3.12 全量快测试）通过。基准：`bench_core 长尾` 约 **27.8 万/秒**（本机单次，历史区间 20 万~30 万内，与 P1 提交时的 29.9 万持平，未变慢）。
- **被否决/修正**：无（收尾无新方案）。**待用户确认**：① 256 MB 下验收标准 1/2 不满足是否接受（是否按"1 worker 为 256 MB 下池配置最优、2× 仅在 512 MB 兑现"合并进 main）；② 补发邮件的内容口径。

### 2026-10-20 支线 P2a：冲刺 20 万/秒第 1 步——诊断与准备（只诊断与工具，未改 flowlens/）

- **主进程剖析（py-spy，A/B 两组，详见 docs/lanes/P2-notes.md §一）**：A（lane-c 池 3、16 MB、512 MB、12 万/秒）主进程 7.5 μs 口径的构成——池通信 23.5%（~1.76 μs，QueueFeederThread 13.5% + 池管理线程 9.4%，两线程抢走主进程 23% 的 GIL）、写入引擎 46.4%（Bloom 22.2% 最大头、各维度 14.5%、tick 15.7%）；B（已提交版本单进程、256 MB、5 万/秒，10.5 μs 口径）——解析侧 46.3%（解压+orjson+校验+归一化 41.8%）、写入引擎 35.9%。**"主进程那约 6 μs 是什么"有了答案：池通信 ~1.8 + tick ~1.2 + 写入细项（P1 的 2.5 没细拆的 intern/秒分组/入口/Bloom 哈希派生）~1.5 + HTTP/事件循环 ~1.1 + 饱和档排队 ~1.0**。aiohttp 访问日志未开启（AppRunner 未传 access_log）。
- **压测工具快速模式（tools/loadgen.py --fast）**：每 worker 预生成 32 个请求体模板（dict 里直接写占位再 orjson.dumps，O(n)），发送前 bytes.replace 两个等宽占位（13 位 ts + 12 位 event_id 序号）。每条压测 CPU 从 55.0 μs 降到 **0.75~0.77 μs（~73 倍）**；本机 8 核上 30/40/50 万每秒全部 100% 达成、CPU 仅 7~14%。正确性：打真实服务 10 万/秒 ×25 s，deduped=late=invalid=0。
- **msgspec 与正则基准（tools/bench_parse.py 新建）**：msgspec Struct(gc=False) 比 orjson+validate 快 **1.23×**（长尾 3.57→2.89 μs/条）、解析一批 5000 条内存峰值 11.1→1.96 MiB；正则一次 sub 做 ID 段替换比逐段 fullmatch 快 **2.33×**（1.84→0.79 μs/条），与 tests 参考实现 20054 条模糊检查 0 不一致。msgspec 是**诊断用依赖**（WSL venv `~/.venv-p2`），不进项目依赖。
- **路线估计（P2-notes §4.5）**：路线一（512 MB+池）纯 Python 上限 ~16-18 万（砍池通信+tick），20 万需再砍写入引擎；路线二（256 MB 单进程）msgspec+正则后 ~9.2 μs/条 ≈ 11 万 CPU 上限，但内存墙（统计状态 ~200 MiB 贴 L3 线）先顶死，现实 ~7-9 万。**20 万的下一步杠杆是写入核心与统计状态的原生化**（与 P1 总结方向一致）。
- **AI 方案被修正/踩坑**：① 模板生成初版对 800 KB 串逐条 `bytes.replace(...,1)` 找占位是 O(n²)（32×10000 条 ~13 分钟、8 worker 全卡死），改为 dict 内写占位一次 dumps（11.4 s）。② `deadline` 原先在模板生成前起算，~11 s 生成吃掉短 --duration 的发送窗口（sent=0），改为生成后起算。③ 占位串必须互不为子串（`999…`/`888…`），否则先后 replace 互相破坏。④ 收尾时 `pytest` 挂 `test_loadgen_503_retry_and_archive`：`_worker_async` 新增读 `cfg["fast"]` 而测试的 `_cfg()` 没带该键——在 `tests/test_tools.py` 的 `_cfg()` 里补 `fast=False, fast_templates=32`（对齐 argparse 接口），修复后 369 passed。
- **验证**：Windows `pytest -q` **369 passed, 4 skipped, 9 deselected**（PYTHONHASHSEED=0）；`docker build --target test`（WSL，Python 3.12）通过。未触及 bounds/sketch/Dockerfile（无需 slow）；未触及服务热路径（flowlens/ 未改，未跑基准）。

### 2026-10-20 支线 P2b：Bloom 去重提速与 tick 剖析（dedup.py 经用户批准解冻，本任务结束重冻）

- **一（输出不变的提速，dedup.py）**：`check_and_set` 判"代是否就绪"改 numpy 数组比较（`_gen_ids[gen_ids%代数]==gen_ids`，替代逐条列表推导），0.077→0.004 μs/条。写位曾按指令改"按字下标排序 + reduceat 合并 + 一次 |="，实测（标准镜像 numpy 2.5.4、稳态一代 ~200 万 id、批 1 万）`bitwise_or.at` 2.35 ms/批 vs reduceat 13.98 ms/批——**at 快 5.96×**（本布隆填充率 ~6%、同一字碰撞少、reduceat 被 argsort 主导），故**保留 at**。输出不变部分由 `tests/test_dedup.py` 的改前参考实现 `RefBloom` 对照守护（批内/跨批/跨代/未就绪代/轮换清空，k=6 与 k=12 各跑，每步比对 dup/active 与每代 words/n/代号）。
- **二（减少 k，改变误判率，dedup.py + config 经 dedup 的 BloomConfig）**：`BloomConfig` 加 `k: int|None = 6`（None 按公式=12 行为不变、整数直接取用）。实测（`tools/bench_bloom.py`，per_gen 200 万、写满一代后查 100 万未写入 id）：k=12 0.030%/1.33 μs、k=8 0.044%/0.88、k=6 0.075%/0.66、k=4 0.197%/0.45。"误判率 ≤0.1%"下 **k=6 耗时最低**（比 k=12 省 0.676 μs/条、~51%）。设计文档 §3.1/§4 待更新（DESIGN_TODO 第 35 条）。
- **三（tick 剖析，只诊断，未改 flowlens/）**：`tools/bench_tick.py`（FakeClock 驱动 Engine、运行时包装计时细分子步骤）。**关键发现：tick ~65-85 ms/s、不随吞吐涨**（长尾/随机 × 5/12/20 万，50k→200k 略降，噪声内）——修正 P2a"tick 随吞吐涨 40→141 ms"（那是饱和事件循环里 tick 与写路径重叠/排队的假象）。最大子项：定稿 CMS 入层（_Builder.accept，~26-36 ms，冻结 segtree.py）、快照 TopK（bounds.top_k，~18-27 ms，冻结 bounds.py）、组装时间片（~20 ms，engine.py）。建议：CMS 入层改"仅新增候选才重估"或按秒分摊、快照增量合并（缓存冻结历史段 top_k）、组装 Part 跨秒缓存；标注了哪些涉及冻结模块。
- **四（端到端，s1_measure --fast，池 3、块 10000、两额度 16 MB、512 MB）**：before/after 各跑 12/16/20/24 万。主进程每条 μs 全档降、降幅随档位增大：12 万 5.26→4.87、16 万 5.31→4.96、20 万 5.71→4.92（20 万档降级 1→0、拒写消失）、24 万 6.55→5.18（发送达成 79.5%→96.7%）。20 万档 Δ=0.79 μs 与 bench_bloom 的 k（0.676）+判代（0.073）合计 ~0.75 量级自洽。py-spy 前后对照（12 万、容器内 root 采样）：**Bloom 占比 26.1%→16.1%**（降 10 个百分点），主进程 62%→53% CPU。
- **AI 方案被修正/踩坑**：① 指令的 reduceat 写位方案实测更慢（5.96×），否决并保留 at（已在 dedup.py 注释与 P2-notes 写明原因）。② py-spy 原方案（P2a）在 WSL 宿主跑、需 sudo 密码；本任务改为**py-spy 在容器内以 root 运行**（`docker exec --user root`、容器 `--cap-add SYS_PTRACE`、主进程 pid=1，二进制从挂载的 venv 读入），免宿主 sudo 密码，口径同 P2a（tools/exp/p2b_pyspy_incontainer.sh）。③ 端到端 before/after 用 P2a 快速模式，主进程远未饱和（12 万档 42.5% CPU），故 before 基线 5.26 μs 比 P2a §1.1 的 7.52 μs（原模式压测、主进程 79.9% CPU）低——P2-notes 已注明口径差异，before/after 同用快速模式，delta 才是干净收益。
- **验证**：Windows `pytest -q` **370 passed, 4 skipped, 9 deselected**（PYTHONHASHSEED=0，含新等价性测试）；`pytest -q -m slow` **9 passed**；`docker build --target test`（WSL，Python 3.12 全量快测试）通过；`bench_core 长尾` ~27.7 万/条（稳定读 277-281k，与 P1c 的 27.8 万持平、未变慢——首次 230k 为冷启动，3× 取中位 277k）。dedup.py 本任务结束重新冻结（PROGRESS"已定的决策"lane-c P2b）。

### 2026-10-20 支线 P2c：验证每秒 20 万条，并用快速压测重测 256 MB（只验证、未改 flowlens/）

- **一（512 MB、20 万/秒 ×10 分钟 + 停发 60s，A512 池 3/两额度 16 MB）**：用 `experiments.py SUST --fast --svc-mem 512m --sust-rate 200000 --sust-sec 600`（新增 `--svc-mem`/`--sust-rate`/`--sust-sec`，SUST 的 loadgen 走快速模式）。**不满足"20 万稳定达成"**：负载段第 0 级 416s/第 1 级 128s/第 2 级 56s（标准"第 1 级 ≤10s"不达标）、接受率 99.77%（≥99.5% 达标）、0.22% 拒绝（loadgen 503 重试耗尽 23 万条、healthz raw 5362+ingest 915）。工作集随 600s 保留期填满 141→**345 MiB**（P2b 的 20s 档只 257 MiB/0 级，差别在保留期填满）；瓶颈是 **CPU 饱和（主进程打满 1 核）+ 保留期内存上涨**，不是 512 MB 内存墙（345<L3 线 400）。真实可持续 ≈**187 万/秒**（loadgen 背压压到 190 万发送、再扣拒绝）。停发 ~20s 回落 345→272 MiB。
- **二（查询延迟，A512，`S2 --fast --s2-rate`）**：`run_s2` 加 `--s2-rate` 直接指定写入速率（14 万/20 万，S2_WRITE_FRAC 的 70% 推算被覆盖）。**p99<50ms 不满足**：14 万 topk_default 78/topk_replay 73/stats 59 ms；20 万 102/109/85 ms。p50 仅 2-13 ms（快照快），p99 尾是单事件循环查询排队（写块+tick 独占段），与 T11a-2 一致。SSE 两组 0 错误、1430 帧。
- **三（精度，A512 原模式 3 万/秒×5 分钟 S3 + 精确解，200 区间）**：200/200 全 valid（degraded/rejected 0）、召回 1.000（两维）、计数误差 0.028%/0.045%、落界 100%、HLL 去重 0.979%（F1 参照 0.92%，HLL p=14 标准误差 0.81% 的 ~1.2σ 内）、p95 0.097%（参照 0.14%）。**k=6 的 Bloom 未损害精度**（只影响去重误判率）。
- **四（256 MB 快速压测重测，S256/L256-0/L256-1/L256-2）**：`s1_measure --fast --stop-below 0.9`（新增 `--stop-below`）。**池在 256 MB 反而更差**：L256-1（池 1）10 万进 L3（202 MiB，worker 常驻挤余量）、L256-2（池 2）4 万 L3/8 万 L4（两 worker 顶 ~50-80 MiB 过 L4 线）。**lane-c 最佳 = L256-0（单进程 k=6）：10 万 100%/L0/175 MiB**（比 S256 的 194 低 19 MiB）。10 分钟长跑（`SUST --fast --sust-sec 600`，按指令进 L3 降一档重跑 10 万→8 万）：**256 MB 存在"保留期内存墙"**——保留期填满工作集 ~220-236 MiB 必进 L3；L256-0 8 万 L3 采样/100% 接受/0 拒写是"不拒写"上限，S256 8 万 L4 拒写 0.1%。
- **五（单核对照，`--cpuset-cpus=0` + `--pool auto`）**：解析进程数 0（`resolve_worker_count` 1 核返回 0）、4/6 万 100%/L0，与 L256-0 一致。**单核退回单进程验证通过**。
- **AI 方案被修正/踩坑**：① 初版 `run_sustained` 的 per_minute 接受率恒 0（`_monitor` 采样没有 accepted/sent 字段，`s.get("accepted",0)` 恒 0）——改用 `merge_timeline(samples, lg_seconds)` 的逐秒行取数；② per_minute 原用"±1s 分钟标记窗"（`sec-1<=t<=sec+1`），改**真分钟窗** `[t0+m*60, t0+(m+1)*60)` 并加主进程 CPU 秒/每条 μs（按逐秒 `procs` cpu_pct × 间隔积分）；③ argparse help 里 `%` 必须写 `%%`（否则 `_expand_help` 抛 ValueError）；④ WSL nohup 后台进程随会话回收被杀、留孤儿容器——改用 harness 的 run_in_background（跨 turn 存活、结束通知），首次 10 分钟长跑被系统低内存保护 reap（容器孤儿已清），重跑成功；⑤ WSL2 宿主 9000-9003 段 bind 报 98（保留段、无可见监听），实验改端口 12345；⑥ `flowlens:submitted` 的 `/healthz` 无 `raw_rejected`/`ingest_rejected`（P1 才加的诊断字段），S256 拒绝数从 loadgen 侧取。
- **验证**：Windows `tests/test_experiments.py`+`tests/test_tools.py` **25 passed**（PYTHONHASHSEED=0）；未触及热路径与 bounds/sketch/heavy（未跑 slow、未跑性能基准，见汇报）；`docker build --target test`（Python 3.12 全量快测）见收尾。
