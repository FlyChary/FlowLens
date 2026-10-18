# FlowLens：给 Claude Code 的工作说明

先读 `docs/DESIGN.md`（设计）和 `docs/TASKS.md`（任务清单），再动手。每次只做一个任务，做完跑通验收再进入下一个。

## 环境与命令

Python 3.11+。依赖只有 numpy、orjson、aiohttp，测试用 pytest。项目根目录是 `D:\dhy\code\flowlens`（里面有 pyproject.toml），包源码在 `D:\dhy\code\flowlens\flowlens`，以下命令都在根目录执行。开发环境是 Windows，命令按 PowerShell 写。

Python 用 conda 环境 `flowlens_env`（`D:\dhy\miniconda\envs\flowlens_env`），先激活再执行下面的命令：

```
conda activate flowlens_env
pip install -e ".[dev]"
pytest -q                                            # 每次提交前必须通过（默认跳过 slow 慢测试）
$env:PYTHONHASHSEED="0"; python -m flowlens          # 本地启动，监听 :8080
python tools/bench_core.py 长尾                      # 统计核心吞吐基准
docker build -t flowlens .
docker run --rm -p 8080:8080 --memory=256m flowlens  # 模拟评测的内存限制
```

慢测试（`@pytest.mark.slow`，等价性对照）日常默认跳过；修改 `flowlens/bounds.py` 或 `flowlens/sketch/heavy.py` 后，必须额外运行 `pytest -q -m slow` 确认输出不变。

写脚本和测试时注意跨平台：路径用 `pathlib`，不要写死 `/` 或 `\`；读 cgroup 等 Linux 专有文件时要能在 Windows 上优雅地退回（例如 memstat 读不到就返回进程内存）。

## 运行模型：单进程、单写者、按批向量化

- 整个服务是一个进程、一个 asyncio 事件循环（aiohttp）。所有统计状态归一个 `Engine` 对象所有，只在事件循环里读写，因此**不需要任何锁**，也不要引入线程去改统计状态。
- 不做多进程分片：每个 Python 进程有几十 MB 基础内存，256 MB 里开不了几个。性能靠"按批处理 + numpy 向量化"获得。
- 热路径里禁止对每条事件写 Python 循环调用复杂逻辑。正确做法：先把一批事件拆成 numpy 数组，用向量化操作分类、去重、聚合，再把聚合后的少量结果交给对象处理。参照 `HeavyHitters.observe` 和 `tools/bench_core.py`。
- 耗时操作（解析大请求、合并大区间）要分块，块之间 `await asyncio.sleep(0)` 让出事件循环，否则查询延迟会超过 50 ms。

## 目录与归属

```
flowlens/config.py    ✅ 全部参数；生成各组件的规格对象
flowlens/dims.py      ✅ 维度 url / user
flowlens/hashing.py   ✅ 64 位哈希（需固定 PYTHONHASHSEED）与派生
flowlens/timewin.py   ✅ watermark、可接收范围、区间拆分、假时钟
flowlens/sketch/      ✅ Count-Min、TopK、带索引堆、HeavyHitters、HLL、DDSketch、Stats
flowlens/dedup.py     ✅ 按 ts 分代的 Bloom 去重
flowlens/bounds.py    ✅ 多个时间片合并成 Top-K，上下界剪枝
flowlens/segtree.py   ✅ 稀疏线段树（冻结历史）
flowlens/ring.py      ✅ 活跃环、秒桶、维度桶
flowlens/model.py        事件解析、校验、URL 归一化
flowlens/engine.py       统计引擎：拥有环、线段树、去重；写入、tick、组装时间片
flowlens/ingest.py       写入请求处理
flowlens/query.py        回放与统计查询、结果缓存
flowlens/api/            HTTP 路由、参数、错误、SSE
flowlens/degrade.py      降级控制器
flowlens/memstat.py      内存读数
flowlens/intern.py       字符串驻留表
flowlens/__main__.py     启动入口
tools/                   bench_core.py ✅、exact.py、loadgen.py
tests/                   单元测试；tests/e2e/ 端到端用例
```

标 ✅ 的模块是已经定稿的基础代码，不得修改（包括它们的测试文件和 tools/bench_core.py）。上层模块只能通过它们的公开接口使用它们。如果发现基础代码有 bug 或缺少必要的能力，停下来在回复里说明问题和建议的改法，等确认后再动，不要自行修改，也不要在上层复制一份改过的版本来绕开。

## 代码组织：面向对象，但不过度设计

1. 类型自带行为。数据和操作它的方法放在一起，调用方不拆开字段自己拼。例如 `DimBucket` 自己负责记录事件（`observe`）、转成可合并的时间片（`part`）、交给线段树（`leaf`）。
2. 用组合搭出复杂对象：`HeavyHitters` = Count-Min + `TopK`；`DimBucket` = `HeavyHitters` + `Stats`；`Bucket` = 按维度存放的 `DimBucket`；`Engine` = 环 + 线段树 + 去重。
3. url 和 user 用维度下标处理（`bucket.dims[d]`，遍历 `ALL_DIMS`）。同一段逻辑不允许写成 `if by == "url": … else: …` 两份。差异由对象自己承担：url 的 `Stats` 带 DDSketch，user 的带 HLL。
4. 规格对象负责创建实例：`StatsSpec.new()`、`Config.dim_spec()`、`Config.tree_config()`。参数只在 `config.py` 里定义。
5. 抽象基类和协议只在两种情况下使用：确实有多个实现，或测试需要替身（如 `timewin.Clock`）。不要给 CMS、HLL、DDSketch 定统一的基类。
6. 同一段逻辑出现第二次就抽出来；"将来可能用到"的抽象不做。 
7. 简单的数据载体用 `@dataclass`（不可变的加 `frozen=True`）；类型注解写全。

下面这些能力已经存在，直接用，不要另写：

| 要做的事 | 用这个 |
|---|---|
| 判断迟到、未来事件 | `timewin.classify` |
| 毫秒区间拆成整秒加边缘秒 | `timewin.split_range` |
| 字符串哈希 | `hashing.key_hashes`（批量）/ `key_hash` |
| 解析 by 参数 | `Dim.parse` |
| 秒桶转成可合并的时间片 | `DimBucket.part` |
| 线段树区间转成时间片 | `Tree.cover(…).parts()` |
| 多个时间片合并成 Top-K | `bounds.top_k` |
| 查某个 key 的上下界 | `bounds.for_keys` |
| 合并统计概要 | `Stats.merge`、`Cover.merge_stats_into` |
| 带索引的小顶堆 | `sketch.IndexedMinHeap` |

## 硬性规则

1. 不得引入现成的 Heavy-Hitter、HyperLogLog、分位数库。新增任何依赖都要在 AI_LOG.md 写明理由。
2. 写入请求要么整体接受，要么整体返回 503，并且必须在写 Bloom 之前就确定：先为整个请求预留待处理队列的容量，预留成功后才去重。否则被拒绝的请求里的 event_id 已经写进 Bloom，调用方重试时会被当成重复事件丢掉。
3. 所有缓冲都要有上限（请求体大小、待处理事件数、SSE 连接数、缓存条目数）。新增任何会随流量增长的结构，先写清楚它的上限。
4. 时间一律用 epoch 毫秒；需要"现在"的地方注入 `timewin.Clock`，测试用 `FakeClock`。
5. 启动时检查 `PYTHONHASHSEED` 已固定，未固定时打印警告。
6. 注释、文档字符串、错误信息用中文，标识符用英文。
7. 执行instructionsz2任务时，先建任务清单（计划），然后逐项执行

## 每个任务结束时

- `pytest -q` 通过；涉及热路径的改动跑一次 `tools/bench_core.py`，吞吐不能明显下降；
- **两个平台都要跑全量 `pytest -q`**：Windows（本机）之外，还必须在 WSL 中跑一次全量测试（评测在 Linux 上进行，两边结果都必须全部通过，汇报时分别给出）。WSL 项目路径 `/mnt/d/dhy/code/flowlens`，用 T6c 建的虚拟环境 `/home/user/flowlens-venv`（缺依赖先 `pip install -e ".[dev]"`）：`wsl -e bash -lc 'cd /mnt/d/dhy/code/flowlens && PYTHONHASHSEED=0 /home/user/flowlens-venv/bin/python -m pytest -q'`。
- 在 AI_LOG.md 末尾追加几行：做了什么、AI 给的方案哪里被修改或否决、为什么。
