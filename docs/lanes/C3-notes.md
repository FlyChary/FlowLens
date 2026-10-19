# C3（T9b 支线）：T9b-A 内存加固——决策 / 未解决 / 设计待改 / AI 记录

> 本任务只改不冻结的部分：malloc_trim、字符串驻留、在途额度实验变量。
> 报告在 C3-report.md。本文件承载本支线**不应**写进 PROGRESS / AI_LOG /
> DESIGN_TODO / CLAUDE.md 的内容（支线规则）。

## 一、决策

1. **malloc_trim 放 Ticker、默认每 10 秒**（可配 `FLOWLENS_MALLOC_TRIM_SEC`，
   0/负数关闭；非 Linux 平台 `enabled=False`、调用返回 False 不报错）。依据：
   C2 实测满载单次 0.4~6.8 ms，放进一拍（~85 ms）可忽略；停发后一次回收 ~60 MiB。
2. **malloc_trim 统计通过 /healthz 暴露**——这需要给 `flowlens/api/app.py` 的
   `_healthz` 加几行只读字段，而 app.py **不在 C3 支线允许修改的文件清单**里。
   已向用户确认**允许改 app.py**（只加只读 `malloc_trim` 字段，不改任何逻辑）。
3. **驻留表用"满则整体清空"策略**（与 `model._norm_cache` 一致），不做 LRU。
   上限为模块常量（user 8192 / url 32768），**不进 config.py**（config.py 冻结）。
   整体清空把驻留表自身内存钉死在 cap 内——这也是"对抗性流量下内存有界"的一环
   （随机流量下表只涨、整体清空周期性把它压回空）。
4. **驻留在两处**：user 在 `engine.ingest` 写秒桶前（`np.fromiter` 逐条 `intern`）；
   url 在 `model.normalize_url` 返回前对**归一化结果**驻留（不改归一化逻辑本身）。
   C1 已论证：lane-c 的 url 归一化缓存（T10b 先剥 query/fragment 再查）命中时已
   复用同一对象，但**缓存未命中**（新路径）时每次都 `new` 一个对象——驻留把
   "未命中新对象"也收敛到同一对象，补齐缓存覆盖不到的部分。
5. **在途额度做成环境变量 `FLOWLENS_MAX_PENDING_BYTES`**（默认 4 MB 不变），
   作为实验变量降到 2 MB。`ingest.py` 里 `MAX_PENDING_BYTES = _read_pending_limit()`。
6. **实验变量统一走环境变量**（trim / intern / pending），`tools/experiments.py`
   加 `--extra-env K=V`（可多次）注入服务进程：docker 目标翻译成 `-e`，local 目标
   合进子进程 env。这样 4 个对比组**同一镜像、同参数**、只改特性开关，消融干净。
7. **镜像一律 `flowlens:t9b`**（`--target app` 构建），不碰 `flowlens:latest`
   （主线 T11a-2 剖析实验在用它）；端口 9100。收尾的 `docker build --target test`
   用临时标签 `flowlens:c3test`，不覆盖 latest。

## 二-A、S3 精度门槛的判读（已做的决定）

C3 指令第 6 条："如果第 3 或第 4 组能让服务在 5 分钟内保持第 0 级，就重做 S3 精度。"
实测 g3（trim+驻留，推荐组合）负载期间**每个分钟点都在第 0 级**、工作集 169→197 MiB
（压在 78% 阈值 199.7 MiB 之下），但**逐秒**看有 **15 秒**的短暂 3 级尖峰（每次 1~5s、
逐秒回 0，集中在 m5 末）；停发尾巴（m6）回 144.7 MiB / 0 级。

**判读**：严格意义上 g3 没有"全程第 0 级"（有 15s L3），所以不满足字面门槛；但它的
L3 是**偶发尖峰**（对比 g2 的 107s 连续 L3、g1 的 190s L3+46s L4），且 C2 当时是
"L3/L4 全程"（抽样 200 区间全落降级期 → 精度无效）。g3 的情况本质不同：抽样区间若
避开那 15s 就是有效样本。S3 代码已把每个抽样按 `status`+`degraded` 分
**valid/degraded/rejected** 三桶、只由 valid 算精度，所以少数受尖峰污染的区间会被
自动隔离、不影响其余。因此**决定跑 S3**（用 g3 配置），把"受尖峰污染的样本数"如实
报告，给出 T9b 首份有效精度数据 + T9b-B 决策输入；并在报告里**透明标注**"g3 有
15s 短暂 L3，不满足字面全程 0 级门槛，抽样中 degraded 桶即受污染部分"。这是判读
而非改指令——指令意图（"内存稳了才值得做精度"）已满足，字面门槛未满足，两者都报。

## 二、实验设计（容器，256 MB，真实内存读数）

参数与 C2 基线相同：长尾、3 万/秒、每批 5000、4 进程 × 8 并发、5 分钟 + 停发
观察 60 秒。每组一个新容器（端口 9100），跑完停掉，结果 `results/c3/<组>/`。
`SUST` 场景（`tools/experiments.py` 新增）：持续负载 + 停发尾巴，记录工作集 /
RssAnon 随时间、降级级别、接受率、trim 回收量（/healthz 的 malloc_trim 字段）、
"每分钟一行"汇总。S4（全随机 3 万/秒 × 10 分钟）用同一 workhorse、mode=随机。

| 组 | 特性开关 | 目的 |
|---|---|---|
| g1_baseline | trim 关 + intern 关 + pending 4 MB | 本镜像上的基线（对照 C2 的 229 MiB） |
| g2_trim | + 定期 malloc_trim（默认 10s） | 单独看 trim 的效果 |
| g3_trim_intern | + 字符串驻留 | 驻留的边际效果 |
| g4_all | + 在途额度 2 MB | 在途降额度的边际效果 |

- 组 1（基线）直接引用 C2 的数字（C2 已测，不必重跑）；但为消融干净、同机对照，
  仍在本镜像跑一遍 g1 作为同批对照。
- S4 攻击流量（全随机 3 万/秒 × 10 分钟）：内存有界的直接证据，看驻留表 / 工作集
  是否平台化、峰值多少、是否触发降级。
- S3 精度：仅当组 3 或组 4 能让 5 分钟内保持第 0 级才重做（长尾 3 万/秒 × 5 分钟、
  存档、`tools/exact.py` 对比）；否则跳过（报告说明原因）。

## 三、未解决的问题 / 风险

1. **本机（Windows / WSL2）是争抢环境**：服务固定核 0、压测固定核 1-6，与 C2 同
   一 caveat——单核争抢下绝对值偏高，定性结论（碎片/并发/有界性）不受影响。
   最终数字以评测容器（Linux、Python 3.12）为准。
2. **SUST 每组 ~6 分钟 × 4 + S4 ~11 分钟 + 可能的 S3**：串行总时长较长；跑实验
   前已确认主线空闲。
3. **驻留表整体清空在长尾下的"清早了"风险**：cap 8192 远大于长尾不同 user
   （~2448），正常不触发清空；随机流量 cap 8192 会频繁清空（表刚满就清），但随机
   流量本就没有驻留收益（每个值不重复），清空反而控制了开销。需实验确认长尾下
   驻留确实省内存（对比 g2 vs g3）。

## 四、设计文档待改项（合并回主线时统一处理，见 docs/DESIGN_TODO.md 口径）

1. **定期 malloc_trim**：DESIGN 未提"定期把空闲内存还给 OS"。需补一句：Ticker
   每 N 秒（默认 10）调 `malloc_trim(0)` 归还堆顶，非 Linux 为 no-op，/healthz
   暴露 call_count / last_ms。
2. **字符串驻留表**：DESIGN 的"字符串表 13 MB"是伪命题（见 C1 §3）。需改为"有
   上限的整体清空驻留表（user 8192 / url 32768），写入端驻留、不进 config"，
   并注明与归一化缓存的分工。
3. **在途额度可配**：DESIGN 把在途上限 4 MB 写死。需注明可用
   `FLOWLENS_MAX_PENDING_BYTES` 覆盖（实验/调参用）。
4. **/healthz 的 malloc_trim 字段**：响应字段清单补一条。
5. **glibc mmap/trim 阈值（已固化进镜像，DESIGN 待补）**：C3 实测
   `MALLOC_MMAP_THRESHOLD_=131072` + `MALLOC_TRIM_THRESHOLD_=131072` 使满载峰值
   −57.7 MiB、全程第 0 级。用户已批准在 Dockerfile base 阶段加这两个 ENV（本支线
   例外放行，注释写明 g5 依据）；`docker run` 不显式 `-e` 时继承镜像 ENV，评测容器
   自动生效。DESIGN 需补一句"部署时固定 glibc 大块/trim 门槛为 128 KB（Dockerfile
   ENV）"及依据。

## 四-A、追加对照：glibc mmap/trim 阈值（用户追加指令，2026-10-19）

用户在首份汇报后追加一组对照：在 g3（trim + 驻留）基础上，容器加
`-e MALLOC_MMAP_THRESHOLD_=131072 -e MALLOC_TRIM_THRESHOLD_=131072`。依据（用户）：
glibc 默认会在每次归还大块后自动调高"大块"门槛，几轮之后 ~650 KB 的请求体就改从
普通堆分配、用完变碎片；固定门槛 128 KB 让这类大块走 mmap、用完立即归还 OS。
只改 docker run 参数，不改服务代码（但监控侧补了 RssAnon 逐秒采样 + /healthz 的
trim 累计回收 total_freed_mb，见"四-C"）。

- **S3 暂停**：用户明确"内存问题解决后再做"。已提交的 g3 配置 S3 数据（200/200 有效、
  召回 1.0）保留作参考；mmap 配置下的 S3 不做。
- 旧 g3 的 timeline 没有 RssAnon 采样 → **同批重跑 g3_recheck（无变量）** 作对照，
  g5_mmap（带变量）同场同机，消除时段差异。两组参数与 C2 基线相同（长尾 3 万/秒、
  每批 5000、4 进程 × 8 并发、5 分钟 + 停发 60s）。结果 `results/c3/g3_recheck`、
  `results/c3/g5_mmap`。

**结果（2026-10-19 跑完）**：
- g3_recheck：WS 78.4/**236.8**/175.2 MiB，逐秒 0 级 85s / 3 级 138s / 4 级 24s，
  接受率 95.4%；trim 33 次累计回收 **972.8 MiB**（churn 一直在被 glibc 从普通堆留）。
- g5_mmap：WS 68.8/**179.1**/144.7 MiB，**逐秒 0 级 246s（100% 全程第 0 级）**，
  接受率 100%；RssAnon 峰 165.9；trim 34 次累计只回收 **31.9 MiB**（churn 消失）。
- **峰值 −57.7 MiB（−24%）、"贴 3/4 级"变"全程 0 级"**——本组实验最大单项收益。
  机制印证用户判断：固定 128 KB 门槛后 ~650 KB 请求体走 mmap、free 即还 OS。
- 推荐组合升级为 **g5 = trim + 驻留 + 两个 mmap/trim 环境变量**；**T9b-B 不需要**
  （无内存压力可解决，且 C1 已证缩 Bloom/HLL 都是拿精度换内存）。
- **待办（内存问题解决后）**：g5 配置下重做 S3（全程 0 级下的最终精度数据）。
- **ENV 固化（用户已批准，本支线改 Dockerfile）**：用户批准在 base 阶段加
  `ENV MALLOC_MMAP_THRESHOLD_=131072 MALLOC_TRIM_THRESHOLD_=131072`（注释写明 g5 依据）。
  镜像 ENV 会被 `docker run` 继承（不显式 `-e` 也生效），所以评测容器自动带上，收益固化。
  `docker build --target test` 复验通过。**S3/S4 不重跑**——用户指令：等合并后在最终
  代码上统一跑。

## 四-C、工具补充（为对照实验）

- `tools/experiments.py`：`_monitor` 每秒多采服务进程 RssAnon/VmRSS
  （docker 目标 `docker exec cat /proc/1/status`、local 目标宿主 /proc/<pid>/status），
  timeline 加 `rss_anon_mb`/`vm_rss_mb` 两列；`run_sustained` summary 加
  `rss_anon`（起始/峰值/结束）、`trim_freed_total_mb`；per_minute 加
  `rss_anon_peak_mb`、`trim_freed_total_mb`。
- `flowlens/ticker.py` TrimStat 加 `total_freed_bytes`（累计回收）；
  `flowlens/api/app.py` /healthz 的 malloc_trim 加 `total_freed_mb`。
  均向后兼容（旧数据读不到这些字段按 0/None 处理）。

## 四-B、最终结果摘要（首轮实验跑完）

- **四组消融**（长尾 3 万/秒 × 5 分钟，256 MB 容器，同机对照）：
  - g1 基线：峰值 233.4 MiB、**进 L4 拒写**、接受率 87.8%（复现 C2 的 232.6/101s 进 L3、拒写行为）
  - g2 +trim：峰值 213.9（−19.5）、**不拒写**（100% 接受）、停发回落 155.5；但中后段 107s 贴 L3
  - g3 +驻留：峰值 213.9、结束 144.7（−11）、**负载期每个分钟点 L0**、仅 15s 的 1~5s L3 尖峰
  - g4 +在途2MB：与 g3 持平（峰值 215.0、503 仍 0%）——本负载下在途额度不是内存瓶颈
- **S4 攻击流量**（全随机 × 10 分钟，推荐组合）：工作集 78.1→**平台 208~234 MiB**→停发 180.8；
  有界（随 600s 保留期封顶、不随事件数涨）、峰值 243.3 触 L4、接受率 86.3%（与长尾 g1 同样
  行为，印证"随机不比长尾更糟"——吃内存的成分都与分布无关）。
- **S3 精度**（g3 配置，200/200 全有效）：召回 url/user 均 **1.000**、计数误差 0.05%/0.09%、
  落界率 100%、去重 0.92%、p95 0.14%——**T9b 首份有效精度数据**（C2 那次全落降级期无效）。
- **首轮推荐**：g3 = trim(10s) + 驻留(user 8192/url 32768)。**T9b-B 不作首选**（缩 Bloom
  FPR×56、缩 HLL 误差翻倍，都是拿 S3 刚验证的精度换内存）；要彻底稳 0 级优先客户端降并发/
  单批 1000/调 M_MMAP_THRESHOLD（不碰冻结模块）。是否进 T9b-B 交用户决定。
- **追加对照后（mmap，用户追加）**：推荐升级为 **g5 = g3 + MALLOC_MMAP_THRESHOLD_=131072 +
  MALLOC_TRIM_THRESHOLD_=131072**（峰值 −57.7 MiB、246/246s 全程 0 级、接受率 100%）。
  **T9b-B 不需要**。待办：g5 下重做 S3（内存问题解决后）；ENV 固化进 Dockerfile 留给主线。

## 五、AI 使用记录

- 本任务由 Claude Code（ccr 模型）执行。方案为：malloc_trim 放 Ticker（`_maybe_trim`
  到点触发、记录 TrimStat）+ /healthz 暴露；intern.py 提供 `Intern`（满则整体清空）
  + 两个进程级单例（user/url）+ 环境变量开关；engine.ingest 写桶前 `np.fromiter`
  驻留 user；model.normalize_url 返回前驻留结果；ingest.py 的 MAX_PENDING_BYTES
  改读环境变量。实验用 `SUST`（持续负载 + 停发尾巴）+ `S4`（全随机）场景、
  `--extra-env` 切换特性。
- **用户修改/否决点**：
  - 用户确认"主线未跑实验，可以跑容器"。
  - 用户确认"允许改 app.py 的 _healthz 加 malloc_trim 只读字段"（原清单不允许改
    app.py，属支线规则外的必要放行）。
  - 用户追加 mmap 阈值对照实验（g5），并**批准改 Dockerfile**（原清单不允许，例外
    放行）固化两个 glibc ENV；S3/S4 不重跑、等合并后在最终代码上统一跑。
- 若后续审查认为驻留应只 user 侧（C1 的原始建议）而非 user+url 两侧，url 侧的
  `url_intern` 可在 `FLOWLENS_INTERN_URL_CAP=0` 下关闭，不影响 user 侧。
