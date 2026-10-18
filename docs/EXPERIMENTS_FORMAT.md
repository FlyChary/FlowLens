# 实验结果文件格式（T10b 第二节）

`tools/experiments.py` 把每个场景的结果写进 `results/<日期时间>/<场景>/`。
`tools/summarize.py <results 根目录>` 读取这些文件、打印 Markdown 汇总表。
T10c 的控制台回放直接读这些文件。

## 目录结构

```
results/<YYYYmmdd_HHMMSS>/
├── S1_plain/          # S1 不压缩
│   ├── meta.json
│   ├── timeline.jsonl
│   └── summary.json
├── S1_gzip/           # S1 gzip（同结构）
├── S2/
│   ├── meta.json
│   ├── timeline.jsonl
│   ├── queries.jsonl
│   └── summary.json
├── S3/
│   ├── meta.json
│   ├── timeline.jsonl
│   ├── accuracy.jsonl
│   ├── archive/       # loadgen --archive 的整数列存档（npz + meta.json）
│   └── summary.json
└── S5/
    ├── meta.json
    ├── timeline.jsonl
    └── summary.json
```

`<场景>` 名：`S1_plain` / `S1_gzip` / `S2` / `S3` / `S5`。每个场景启动一个全新
服务（docker 容器或本机进程），结束前采样停止、服务干净退出。

## meta.json（每个场景一个）

```json
{
  "scenario": "S2",
  "git_commit": "abc1234",
  "machine": {"cpu_model": "...", "cpus": 16, "os": "...", "python": "3.12"},
  "service": {
    "target": "docker",        // 或 "local"
    "port": 9000,
    "cpu_alloc": {"service": "0", "loadgen": "1-6", "aux": "7"},
    "image": "flowlens:latest"  // local 目标为 null
  },
  "params": { "..." : "该场景的全部参数" },
  "created_ms": 1792316804000
}
```

`cpu_alloc` 记录核的分配：服务固定在一个核（`--cpuset-cpus`），压测进程用
`taskset` 固定在其他核，避免互相争抢。

## timeline.jsonl（每秒一行）

该秒 loadgen 的发送/接受/拒绝/延迟 + 当时 `/healthz` 的**完整内容**（含工作集
`memory_bytes`、原始读数 `memory_current_bytes`、上限、`degrade.level` 等）。

```json
{"t": 1792316804, "sent": 5000, "accepted": 5000, "rejected": 0, "errors": 0,
 "lat_p50_ms": 94.5, "lat_p95_ms": 128.9, "lat_p99_ms": 131.0,
 "healthz": {"ok": true, "events_ingested": 5000, "memory_bytes": 84779008,
             "memory_current_bytes": 163663872, "memory_limit_bytes": 268435456,
             "degraded": false, "degrade": {"level": 0, "sample_weight": 1,
             "occupancy": 0.0, "tick_lag_ms": 0.001, "memory_bytes": 84516864,
             "memory_limit_bytes": 268435456}, "deduped": 0, "late": 0,
             "invalid": 0, "tick_errors": 0, "last_tick_ms": 2.15,
             "sse": {"groups": 0, "subscribers": 0, "dropped_frames": 0}}}
```

`healthz` 可能为 `null`（该秒采样失败）。`memory_bytes` 是工作集（降级判断用它），
`memory_current_bytes` 是原始 cgroup 读数（对照用）。

## queries.jsonl（S2、S3）

每个查询一行：

```json
{"t_ms": 1792316804123, "type": "topk_replay",
 "params": {"type": "topk_replay", "by": "url", "window_s": 120, "at_ms": 1792316300000},
 "status": 200, "lat_ms": 12.3}
```

`type` 取值：`topk_default`（默认 60s 窗口 topk，url/user）、`topk_replay`（随机
`at`+`window` 回放 topk）、`stats`。`status` 是 HTTP 状态码（422/503 也记录，供
判读降级行为）。

## accuracy.jsonl（S3）

两类行，用 `type` 区分：

- `{"type": "per_second", "t_ms": ..., "topk_url": {"status","lat_ms","body"},
   "topk_user": {...}, "stats": {...}}` —— 运行期每秒一次的服务响应（默认窗口、
   k=10），供控制台回放做"估计与真实对比"。
- `{"type": "sample_comparison", "from_ms": ..., "to_ms": ..., "status": ...,
   "degraded": ..., "bucket": ..., "topk_url": {"recall","mean_rel_err",
   "max_rel_err","in_bounds","k"}, "topk_user": {...}, "stats":
   {"distinct_users": {"service","exact"}, "p95": {"service","exact"}}}` ——
   结束时（数据仍在保留期内）随机抽 200 个已定稿区间，与离线精确解
   （`tools/exact.py`）比较：召回（并列不算错）、计数相对误差、真实值是否落
   `[lower,upper]`、stats 去重人数与 p95 的 service/exact。

   每条抽样记录服务当时的 `status`（状态码）与 `degraded`（响应体里的降级标志），
   并分桶到 `bucket`，精度只由 valid 桶统计（`summarize.py` 分开计数三类）：

   - `valid`：200 且非降级——真正可比的精度样本（含 topk_url/topk_user/stats 对比）；
   - `degraded`：200 但 `degraded=true`——降级期服务返回空/不完整 topk，**不能**
     用来算精度（否则空响应会被误判成完美召回），只计数；
   - `rejected`：非 200（503/422）——回放查询在降级期被暂停，只计数。

## summary.json（每个场景一个）

该场景的汇总数字，供 `summarize.py` 读。关键字段：

- 通用：`memory.working_set.{start_mb,peak_mb,end_mb}`、`memory.raw_current.*`、
  `memory.degrade_changed`、`memory.degrade_levels_seen`、`memory.peak_degrade_level`。
- S1：`levels[]`（每级的 rate/accept_ratio/ratio_503/lat_p50/p95/p99/
  client_cpu_pct/svc_cpu_pct_median）、`max_sustainable`。
- S2：`write_rate`、`query_stats{type:{n,p50,p95,p99}}`、`sse{subscribers,groups,
  frames,errors}`。
- S3：`rate`、`mode`、`archive_events`、`n_intervals`、`valid`/`degraded`/
  `rejected`（抽样区间三桶计数，精度仅由 valid 统计）。
- S5：`overload_rate`、`overload_start_ms`、`overload_end_ms`、`recover_seconds`、
  `degrade_level_seq[[t_ms,level],...]`。

## 汇总判读口径

- **最大可持续吞吐**：S1 里满足"接受率≥99%、无 503、写入 p99<500 ms"的**最高**
  发送速率。
- **正常负载进 3 级**：S1/S2/S3（非过载场景）若 `peak_degrade_level ≥ 3`，
  `summarize.py` 会单独标出（⚠），并给出当时的峰值工作集。过载（S5）进 3/4 级
  是预期的，不算异常。
- 所有时间均为 epoch 毫秒；延迟单位为毫秒。
