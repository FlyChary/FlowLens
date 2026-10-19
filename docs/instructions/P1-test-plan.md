# P1 测试计划：解析进程池与写入并发限制

对应 `docs/instructions/P1.md` 第三节（等价性与正确性）。原则：**核心正确性测试不依赖子进程**（可在 Windows 本地与 Docker 都稳定跑），**真子进程测试**（spawn、崩溃重建、关闭）依赖固定 `PYTHONHASHSEED`，无固定种子时跳过——Docker 测试阶段继承了 base 的 `ENV PYTHONHASHSEED=0`，所以 Docker 里会真正跑。

## 前置：种子判定

- 判定函数 `_hash_seed_fixed()`：`os.environ["PYTHONHASHSEED"]` 存在且不是 `random`。
- **为什么**：spawn 子进程用固定种子重算 `key_hash("flowlens-selftest")` 与主进程比对（parse_pool 自检）。主进程（pytest）的解释器种子在启动时定，测试中途设 env 改不了主进程哈希，所以"真子进程"测试必须靠**启动 pytest 时就固定了种子**。
- Windows 本地跑：`$env:PYTHONHASHSEED="0"; python -m pytest`（真子进程测试生效）。Docker：`docker build --target test .`（test 阶段继承 `PYTHONHASHSEED=0`，生效）。

## 测试文件与用例

### A. `tests/test_parse_pool.py`（新建）

| 用例 | 类型 | 验证点（P1 依据） | 通过标准 |
|---|---|---|---|
| `test_parse_body_matches_validate` | 进程内 | `parse_body(body)` 与 `validate(load_array(body))` 的 ts/nbytes/urls/users/invalid 一致，三列哈希 == `key_hashes(各列)` | 逐列相等 |
| `test_parsedbatch_pickle_roundtrip` | 进程内 | ParsedBatch 可 pickle（跨进程传输的前提） | `pickle.loads(pickle.dumps(pb))` 各字段逐列相等 |
| `test_parse_body_bad_request` | 进程内 | 全非法 JSON → 抛 BadRequest（异常原样回传，决定一） | `pytest.raises(BadRequest)` |
| `test_resolve_workers_from_cpus` | 纯函数 | `min(3, 可用核−1)`，<1 不启用 | monkeypatch `available_cpus`：4→3、2→1、1→0 |
| `test_workers_env_override` | 纯函数 | `FLOWLENS_PARSE_WORKERS`：合法生效、0 禁用、非法→0、夹到 [0,3] | 各分支断言 |
| `test_workers_one_core_disabled` | 纯函数 | 可用核=1 时不启用（§3.4） | `available_cpus=1 → resolve=0` |
| `test_pool_spawn_end_to_end` | 真池 | §3.1：同一 body 经**真子进程** parse → `engine.ingest_parsed`，与 `engine.ingest(parse_events)` 两全新引擎，同区间 topk(url/user)、stats、计数器**逐项相等** | 两引擎快照与计数完全相同 |
| `test_pool_selfcheck_mismatch` | 真池 | 决定三：种子不一致自检失败 | 故意传错期望值 → 构造时抛 `BrokenProcessPool` |
| `test_pool_rebuild_after_crash` | 真池 | §3.3：崩溃后重建，后续请求正常 | `rebuild()` 后 `submit` 正常返回 |
| `test_pool_close_no_residual` | 真池 | §3.5：close 后无残留子进程 | close 后 `_executor is None` 且 worker `Process` 都 `is_alive()==False` |

> 真池用例统一 `@pytest.mark.skipif(not _hash_seed_fixed(), ...)`。

### B. `tests/test_ingest.py`（改：新增/改既有）

| 用例 | 类型 | 验证点 | 通过标准 |
|---|---|---|---|
| `test_ingest_parsed_equals_ingest`（新增） | 进程内 | §3.1 核心：同一 body，`ingest(parse_events)` 与 `ingest_parsed(ParsedBatch)` 写两个全新引擎，同区间 topk(url/user)/stats/计数器逐项相等。覆盖长尾、全随机、含非法、批内重复 | 完全相同 |
| `test_ingest_chunked_write_2000`（新增） | 进程内 | 决定五：写入按 2000 条分块、块间让出事件循环 | 探针任务在处理中执行过（复用既有 `test_chunking_yields` 手法），且计数正确 |
| `test_503_retry_not_deduped`（既有 `test_overloaded_then_retry_not_deduped` 保留） | 进程内 | §3.2 / CLAUDE 规则 2：在途满→503，未写去重，释放后重试不判重。预留在解析与池提交**之前**，与是否用池无关 | accepted=1, deduped=0 |
| `test_engine_write_semaphore_serializes`（新增） | 进程内 | 决定五：Semaphore(1) 保护"写引擎" | 两并发写入串行（用打桩计数进入/离开信号量区间，不出现重叠） |
| `test_arr_dicts_released`（**改**既有 `test_arr_elements_released_after_processing`） | 进程内 | 单进程路径 validate 后 dict 尽早释放（新设计是 `del arr` 而非原位置 None） | ingest 后指向 dict 的 weakref 已失效 |

> 既有 `test_ingest_all_invalid_chunked_counted` / `test_ingest_normal_batch_counts` 等应继续全过（ingest 对外签名与计数不变）。若某既有用例因"分块口径从 5000→2000、或 arr 释放方式改变"而失效，属**预期改动**，按上表改法更新，并在 AI_LOG 记录。

### C. `tests/e2e/test_parse_pool_e2e.py`（新建）

| 用例 | 类型 | 验证点 | 通过标准 |
|---|---|---|---|
| `test_app_shutdown_closes_pool` | 真池+真服务 | §3.5：应用关闭时进程池被关、无残留 | 起 app（带真池）→ 收 SIGTERM/关 runner → `pool._executor is None` 且 worker 进程不存活 |
| `test_pool_path_http_ingest` | 真池+真服务 | 走 HTTP 的 /v1/ingest（进程池路径）计数正确、与直连引擎一致 | 202 且计数正确；等价于单进程结果 |

## 执行顺序与收尾

1. 本地（Windows，`PYTHONHASHSEED=0`）：先跑 A+B（`pytest tests/test_parse_pool.py tests/test_ingest.py -q`），再 e2e C。
2. Docker：`docker build --target test .`（test 阶段 3.12 + `PYTHONHASHSEED=0`），全量快测试（含真池用例）必须全过。
3. 慢测试 `pytest -q -m slow`：本次**未改** bounds.py / sketch/heavy.py，但改了 `engine.py`（非冻结），按 CLAUDE.md 收尾规则，engine 变更不强制 slow，仍随 Docker 阶段确认无回归。
4. 性能基准：改动了写入热路径（engine.ingest 拆分、CHUNK_EVENTS 2000、信号量），需跑 `bench_core`、`bench_engine`（tools/ 冻结基准），与 PROGRESS.md 上一列对比，退化 >10% 才重跑 3 次取中位分析。

## 已知风险与对策

- **真子进程慢**：spawn 一次约 0.4s，真池用例共约 4 个，Docker 里增加数秒，可接受。
- **`_executor._processes` 是私有属性**：`test_pool_close_no_residual` 用它断言 worker 不存活，用 `getattr` 守卫，aiohttp/multiprocessing 改名时降级为"只断言 executor 已置 None"。
- **多并发请求同时撞 BrokenProcessPool**：各自调 `rebuild()`，最后一次重建生效，无死锁（rebuild 是同步、事件循环串行）；接受少量冗余重建，注释说明。
- **全非法 body（0 条有效）**：分块写入保证"至少调用一次 `_ingest_core`"，invalid 计数不丢（对应既有 `test_ingest_all_invalid_chunked_counted`）。
