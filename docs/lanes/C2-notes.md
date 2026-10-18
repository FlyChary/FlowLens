# C2（T9b 支线）交接记录：在容器中查清内存增长的原因

本支线在独立工作目录 `D:\dhy\code\flowlens-c`（分支 `lane-c`）完成。只新建
`tools/mem_probe.py`、`tests/test_mem_probe.py`、`results/c2/run_experiment.sh`、
`results/c2/analyze.py` 与本文档及 `C2-report.md`；**未改任何 `flowlens/` 文件、
`docs/PROGRESS.md`、`AI_LOG.md`、`docs/DESIGN_TODO.md`、`CLAUDE.md`、`Dockerfile`，
未引入新依赖**。

**进度：实验已全部跑完，结论与报告见 `C2-report.md`。**

## 目标回顾

主线 T10b 在容器（`--memory=256m`）以 3 万/秒跑 S3：工作集 ~90s 进第 3 级、~200s
进第 4 级、峰值 ~229 MiB。支线 C1 在**进程内**模拟同样 3 万/秒、持续 11 分钟：
纯服务端稳态 RSS 增量仅 ~97 MB。两者差 ~90 MB。本任务查清这 90 MB 是碎片、
并发瞬时峰值叠加、还是泄漏，并给出解决方法。

## 方法与工具

1. **探测脚本 `tools/mem_probe.py`**：容器内用 `create_app`（真实 memstat）启动
   与 `python -m flowlens` 相同配置的服务，逐 10s 采样三层读数——
   - cgroup：`memory.current`、`memory.stat` 的 anon/file/active_file/inactive_file、
     工作集（=current−inactive_file，与降级同口径）、limit；
   - 进程：`/proc/self/status` 的 VmRSS、RssAnon（堆 anon 部分，碎片判断核心信号）；
   - Python 层：`sys.getallocatedblocks()`（逐 10s）；`gc.get_objects()` 前 10 类 +
     总对象数、C1 `MemWalker` 各结构/缓存总量（逐 30s，记录 walk 自身耗时）。
   - 每采样也记录 `/healthz` 的降级级别与已写事件数。
   - 四种运行方式：基线 / `MALLOC_ARENA_MAX=2` / 每周期 `libc.malloc_trim(0)`
     （记录前后 RssAnon）/ `PYTHONMALLOC=malloc`。
2. **施压**：WSL 宿主 `tools/loadgen.py` 打容器，参数同 T10b S3（长尾、3 万/秒、
   每批 5000、4 进程×8 并发、5 分钟）；单连接对照用 1 进程×1 并发。
3. **分析**：`results/c2/analyze.py` 读 `probe.jsonl` 出逐分钟表 + 派生量（空载
   基线、负载结束、停发后回落、RssAnon 增量、walk 存活峰值、工作集−存活差、
   malloc_trim 回收量、降级首现时刻）。

**记账安全阈值（探针自身保护）**：满负载下服务 anon-rss 已爬到 ~238 MiB、
memory.current 峰值 243 MiB，此时 `gc.get_objects()` + MemWalker 深度遍历的瞬时
对象会把 current 推过 256 MiB 上限 → 内核 OOM 杀掉探针本身（baseline 实测在
t≈264s 被 OOM，dmesg 确认 `Memory cgroup out of memory: Killed process (python)
... anon-rss:243700kB`）。故 `memory.current > 上限×0.80` 时**跳过** gc 扫描与
MemWalker 记账（只损失峰值时刻的 walk 明细，逐 10s 的 cgroup/proc/allocated_blocks
仍照常采），保探针活着采完全程。

## 关键实测数字（长尾 3 万/秒，256 MB 容器，服务固定 1 核）

| 运行方式 | 接收率 | RssAnon 平台 | 工作集峰值 | 降级 | walk 存活峰值 | 停发后 RssAnon 回落 |
|---|---|---|---|---|---|---|
| 空载（无流量） | — | 77 MiB | 89 MiB | 0 级 | 51.9 MiB（空引擎） | — |
| 基线 4w×8c | 60.6%（80s 起 L4 拒写） | ~210 MiB | 232 MiB | L3@40s、L4@80s | 85.6 MiB | **0 MiB**（不回落） |
| 单连接 1w×1c | **100%**（9M 条全收） | ~169 MiB | 204 MiB | L3@193s | 92.2 MiB | ~11 MiB |
| arena2 4w×8c | 35.8% | ~193 MiB | 238 MiB | L3@50s、L4@110s | 51.9（仅空载采到） | ~0.6 MiB |
| trim 4w×8c | **100%**（9M 条全收） | ~210 MiB | 232 MiB | L3@101s、L4@292s | 92.1 MiB | **~60 MiB**（213.9→154.2） |
| pymalloc 4w×8c | 40.7% | ~184 MiB | 229 MiB | L3@50s、L4@131s | 51.9（仅空载采到） | 0 MiB |

（arena2/pymalloc 的 walk 只在空载采到——满负载时 memory.current 超过上限 80% 阈值，
探针按自保规则跳过记账。arena2 与 pymalloc 的 RssAnon 平台 ~184–193 MiB 与基线
~210 MiB 同量级（两者接收事件更少 3.2M/3.7M vs 5.5M），**均无改善**：碎片不是
arena 计数、也不是 CPython 私有块分配器的问题。）

## 结论：原因判定

**不是泄漏。是"分配器保留已释放页（碎片）"与"并发瞬时峰值叠加"的组合。**

1. **不是泄漏**。`gc.get_objects()` 总对象数全程平稳（~44k–58k，无趋势）；
   `getallocatedblocks()` 在 54 万–69 万间**震荡**而非单调上升；`MemWalker` 统计的
   全部结构（Bloom、活跃环、线段树、快照、查询缓存、归一化缓存）`walked` **平台化**
   在 ~85–92 MiB、甚至随保留期滚动而略降（4w×8c：85.6→84.8→82.8 MiB）。没有
   任何 Python 对象结构在持续增长。Bloom 是 8 代固定容量、活跃环固定 66 槽、线段树
   受 600s 保留期界定，本就应有上界。
2. **碎片（分配器保留）是"只涨不跌"的直接证据**。负载中 RssAnon 持续爬升、**停发后
   基线 0 回落**，但 `malloc_trim` 一停发就回收 ~60 MiB（213.9→154.2 MiB）——正是
   "对象释放了、glibc 把已释放页留住没还给 OS"的碎片特征。`MALLOC_ARENA_MAX=2` 与
   `PYTHONMALLOC=malloc` 均**无改善**：保留来自 glibc 主分配器对 per-request 解析
   churn（orjson 5000 dict + numpy 临时列，瞬时 ~2.5 MiB/请求）的普通页/堆顶保留，
   不是 arena 计数、也不是 CPython 私有块分配器。
3. **并发瞬时峰值叠加是"C1 进程内 vs 容器真实 HTTP"差距的最大单一成分**。单连接
   （顺序流、100% 收满 3 万/秒）非存活 churn 保留 ~53 MiB；32 连接 ~103 MiB——
   多出的 ~45 MiB 是 in-flight 请求解析临时对象叠加 + 每连接 aiohttp/TCP 栈
   （~1.4 MiB × 32）。C1 进程内直驱 ingest、顺序流、无 HTTP，这两项都测不到。

## 与 C1 账本的 90 MB 差距来源（口径对齐后）

C1 的 ~97 MB 是**增量**口径（满载 RSS − 空载 import RSS，Windows MSVC、无 HTTP、
进程内顺序批）；容器 ~229 MiB 是**绝对值**（含空载基线 83）。**"90 MB"= 229 − 137
是"绝对值 vs C1 估算"的口径混用**。对齐到增量口径（容器 146 − C1 97）真实差距
**~49 MiB**，分解（详见 C2-report.md 第 3 节）：① 并发瞬时峰值叠加 ~45 + ② glibc
保留比 MSVC 高 ~21 − ③ 存活结构差 −30（本支线 L4 拒写早、保留期没填满、事件数
5.45M < C1 的 19.8M，方向相反抵消）+ ④ 工作集含 file 缓存 ~13。绝对口径的剩余
~43 是空载基线固有差（⑤），与负载无关。

## 解决方法（推荐，详见 C2-report.md 第 4 节）

- **做：定期 `libc.malloc_trim(0)`**（Ticker 每 10–30s，或负载回落时）——实测停发后
  回收 ~60 MiB、防棘轮式累积；满载中压不住峰值（每 10s 追不上 ~30 MiB 新 churn）。
  改动仅 Ticker 一处（T9b 主线实施），无新依赖，0.4–6.8 ms/次。
- **治本：降低单请求解析峰值**（单批 5000→1000 条或流式解析）——解析高水位
  ~58→~12 MiB。预期满载工作集 ~229→~179 MiB（89%→70%）。
- **不做**：`MALLOC_ARENA_MAX=2`、`PYTHONMALLOC=malloc`（均实测无改善）。
- **可选**：`M_MMAP_THRESHOLD`/`MALLOC_TRIM_THRESHOLD_` 调低（让解析临时列走 mmap，
  free 即还 OS）。
- **3 万/秒×5 分钟是否保持 0 级**：仅 trim 否（实测 101s 进 3 级）；trim + 降单批
  后稳态 ~70%（0 级）、瞬时尖峰偶发接近 78%；要稳定 0 级还需客户端降并发连接数
  或 mmap 阈值调整。C2 未直接跑组合，T9b 实施后需复测。

## 待确认的问题

1. **定期 malloc_trim 的调用频率与触发条件**（T9b 定）：实测每次回收 20–42 MiB（满载）
   / 停发后 ~60 MiB、耗时 0.4–6.8 ms，每 10s 一次就够用且开销可忽略；也可做成
   "工作集 < 第 3 级阈值且距上次 > N 秒"的条件触发。注意 trim 只回收堆顶连续可归还
   段，满载中追不上 churn（每 10s 新 churn ~30 MiB），所以**定位是"负载后回落 +
   防棘轮"，不是"压满载峰值"**。
2. **"降单批 5000→1000"是建议、未实测**：解析高水位 ~58→~12 MiB 是按单批线性外推，
   还需考虑请求数增 5× 后 aiohttp 协议对象 churn 是否变高。T9b 实施后必须复测
   "1000 条/批 + trim"组合的满载工作集，确认"稳态 0 级、尖峰偶触 3 级"的预判。
3. **每连接 aiohttp/TCP 栈 ~1.4 MiB × 32 连接 ≈ 45 MiB 是服务端治不了的**（取决于
   客户端并发连接数）——要稳定 0 级，可能需要**对客户端给出并发建议**或调低
   `M_MMAP_THRESHOLD` 让解析临时列走 mmap。是否作为产品要求，留给 T9b/用户定。
4. **最坏瞬时接近 78% 第 3 级阈值**（C1 §2）：满载工作集 ~229 MiB 在 256 MB 下就是
   ~89%——**降级本身是正确自我保护**，本任务查的是"为何常驻偏高"，而非"降级阈值设得
   对不对"。trim + 降单批后常驻平台预期下移到 ~70%、进级更晚。
4. **单核争抢**：服务固定 1 核 + 压测固定其他核，3 万/秒下单核已近饱和（client_cpu
   72–77%、服务单核打满），4w×8c 时 80s 就进 L4 拒写（接受率 60.6%），故 4w×8c 的
   接收事件数（5.45M）少于单连接（9M）。这不影响"碎片判定"（非存活 anon 平台高度
   一致），但 4w×8c 与 T10b S3 的时间线不完全同（T10b 到 200s 才进 4 级，本次
   80s）——差异来自本支线压测机（WSL2 单核争抢）比 T10b 当时的更紧张，非服务
   行为变化。**报告里已标注此 caveat。**

## 设计文档待改项

（合并时追加进 `docs/DESIGN_TODO.md`）

1. DESIGN.md 第 5 节"内存预算"：本任务把"容器里 3 万/秒满载 256 MiB 工作集"拆成了
   实测分量（空载基线 83 + 存活结构 ~40 增长 + 非存活 churn 保留 ~100，见
   C2-report.md 第 3 节），建议按此替换"估算 195 MB"的逐项数字，并标注测量方法
   （`tools/mem_probe.py`，容器 256 MB 实测）。
2. DESIGN.md §3.6/§3.7（过载降级）："满载 + 32 连接时工作集 ~89%、长期贴 3–4 级"
   是**预期行为**（C1 §2 的"最坏瞬时"被本任务证实），不是故障；但"负载后常驻高水位
   不回落"（碎片）导致低负载时仍可能贴着阈值，建议补一句：T9b 若采纳定期
   `malloc_trim`，负载回落后的常驻会下移 ~60 MiB。
3. 若 T9b 采纳"单批 5000→1000 条 / 流式解析"（C2-report 第 4 节治本建议），需更新
   DESIGN.md §3.1（写入路径：解析峰值从 ~2.5 MiB/请求降到 ~0.5）与 §5（内存预算的
   "解析高水位"分量），并说明 5000 条/批是压测口径还是服务端推荐上限。
4. DESIGN.md §3.2/§5："每连接 aiohttp/TCP 栈 ~1.4 MiB"是新实测分量（C1 进程内模拟
   测不到），若设计文档要给出"客户端并发连接数建议"，此处补数字依据。

## AI 使用记录

- 本任务由 Claude（claude-code）执行。方案要点：
  - 三层读数设计（cgroup 工作集 / 进程 RssAnon / Python 层 gc+allocated_blocks）
    是为了能**区分**碎片 vs 泄漏 vs 并发峰值：泄漏→Python 层一起涨；碎片→Python
    层平稳而 RssAnon 涨；并发→单连接对照不涨。
  - 用 C1 的 `MemWalker` 直接统计"存活结构"，与工作集对照——两者之差就是
    "非存活 anon"（分配器保留 + 解释器 + HTTP 层），这是判定碎片的直接证据。
  - **探针自身 OOM 事故**：baseline 满负载下探针的 30s `gc.get_objects()` +
    MemWalker 遍历把 memory.current 从 243 MiB 推过 256 MiB 上限，内核 OOM 杀掉
    探针（t≈264s，dmesg 确认）。修复：`memory.current > 上限×0.80` 时跳过记账。
    教训——**探针自己也是容器里的一个进程，其测量开销会挤占被测进程本就紧张的上限**；
    被测系统已近上限时，测量必须让位于保命。
  - `taskset` 的 env 前缀踩坑：`taskset -c 1-6 NO_PROXY=... python ...` 会把
    `NO_PROXY=...` 当命令名执行（"failed to execute NO_PROXY=..."）。必须在 taskset
    之前以 shell 环境变量前缀设置。
  - `MALLOC_ARENA_MAX=2` 假设（"多线程 arena 计数导致保留"）被实测**否决**：单连接
    也复现 ~53 MiB churn 保留、arena2 无改善——glibc 单 arena 的 per-request 峰值页
    保留才是主因。
- 被否决/修改的 AI 建议：
  - 初版报告把"90 MB 差距"直接归因为"glibc 保留 + HTTP 层"（两成分、不拆口径）。
    复盘时发现 C1 的 97 是**增量**口径、容器 229 是**绝对值**口径，直接相减本身
    混了口径——改为先对齐口径（增量 vs 增量 = 49 MiB 真实差距，绝对差里 ~43 是空载
    基线固有差），结论从"碎片为主因"修正为"并发瞬时峰值叠加 ~45 是 C1 差距的最大
    单一成分、碎片 ~21 被存活结构差 −30 部分抵消"。教训：**跨实验对比内存数字前
    先核对口径（增量 vs 绝对值、事件数、平台）**。
  - 初版 `run_experiment.sh` 把 `NO_PROXY=...` 写成 `taskset` 的**参数**，taskset 把
    它当命令名执行、loadgen 根本没跑（第一次"基线"实际是无流量空跑，浪费了 340s）。
    排查线索是 `events_ingested` 恒 0 + loadgen.err 的 "failed to execute NO_PROXY"。
    已修（env 前缀放 taskset 之前）并把那次无流量数据留作"空载基线"参考。
  - 曾考虑用 `tracemalloc` 逐次分配计数来更精确定位 churn，被否决：tracemalloc 有
    显著性能开销（C1 实测 ~3×）、会改变被测行为，且"分配-释放配对"在跨协程场景下
    归因困难；`malloc_trim` 的回收量 + RssAnon 停发回落已经足够定性。
