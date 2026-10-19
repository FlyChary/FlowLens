# P1b 过程记录

> 每个任务节点的结果都追加到这里（P1b 一.2）：即使会话中断，结果也不丢。

## 0. 现场保存（P1b 一.1）

- 提交 879f49b：上个会话的未提交改动（FLOWLENS_RAW_BUDGET_BYTES 环境变量化、
  /healthz 拒绝计数 ingest_rejected/raw_rejected、experiments.py 逐进程 CPU 采样与
  多核核分配参数）。全量快测试通过（362 passed）。

## 1. 本地复测 ingest_parsed 每条耗时（块 2000 vs 10000，P1b 三.3 前置）

方法：长尾 40 万条，逐块调 `engine.ingest_parsed`（写入核心，与容器主进程同路径），
计时取"总耗时/总条数"。本地 Python 3.11（比容器 Python 3.12 略慢）。

| 每块条数 | 每条耗时 | 单块同步耗时 |
|---|---|---|
| 2000（改前） | 1.99 μs | 6.6 ms |
| 5000 | 1.84 μs | 11.8 ms |
| 10000（改后 POOLED_CHUNK_EVENTS） | 1.75 μs | 26.3 ms |

结论：块越大主进程每条越省（2000→10000 降约 12%），单块同步耗时相应增长
（6.6→26.3 ms），方向与 P1b 二点五审查方表（2.98→1.88 μs/条、6.0→18.8 ms/块）一致，
确认 `POOLED_CHUNK_EVENTS=10000` 改动生效。

## 2. 块大小改动与等价性测试口径（提交 c850366）

- `ingest.py`：新增 `POOLED_CHUNK_EVENTS=10000`，仅进程池路径用它切块；单进程路径
  仍用 `CHUNK_EVENTS=2000`。
- 等价性测试（test_ingest_parsed_equals_ingest / test_pool_path_full_equivalence）：
  两条路径改用**同一块大小**切块（把块大小这个性能旋钮固定、只留"路径"变量）。
  原因：Top-K 名单是近似结构、堆成员对"逐秒 observe 的分批顺序"敏感，块大小不同会在
  高基数流量的名单深尾部产生少量差异（sketch 固有性质、非 bug）；改前池路径用 2000、
  in-proc 也用 2000 故相等，池路径改 10000 后两条路径切块不同、深尾不等。全量
  363 passed。
