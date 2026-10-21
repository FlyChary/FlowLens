# P2b：Bloom 去重提速与 tick 剖析

## 背景

P2a 的剖析（docs/lanes/P2-notes.md）：进程池配置下主进程每条 7.5 μs，其中 Bloom 去重 1.67 μs（22.2%）、tick 1.18 μs（15.7%）、进程池通信 1.76 μs（23.5%）。本任务处理前两项；进程池通信留给下一步的前端进程架构。

**用户已批准解冻 flowlens/dedup.py**，用于性能优化。其他冻结模块（含 config.py、hashing.py、segtree.py、bounds.py）仍不得修改。在 docs/PROGRESS.md 的"已定的决策"中记录：dedup.py 经用户批准解冻用于 P2b 的性能优化，本任务结束后重新冻结。

结果逐项追加进 docs/lanes/P2-notes.md 的新一节并提交；命令只打印汇总行。

## 一、Bloom：先做输出完全不变的优化

审查方读代码时发现 check_and_set 中有两处写法本身很慢（与缓存未命中无关）：

1. 判断每条事件的代是否就绪时，用的是逐条的 Python 列表推导：
   active = np.array([self.gens[g % len(self.gens)].id == g for g in gen_ids.tolist()])
   改为 numpy 数组运算：维护一个各代 id 的数组（或在调用时构造一个长度为代数的数组），用 gen_ids % 代数 取出对应代的 id 与 gen_ids 比较
2. 写入位时用的是 np.bitwise_or.at。numpy 的 ufunc.at 为了正确处理重复下标而逐个元素处理，非常慢。改为：把待写入的（字下标，掩码）按字下标排序，用 np.bitwise_or.reduceat 把同一个字的掩码合并，再对去重后的字下标一次性执行 words[唯一下标] |= 合并后的掩码

步骤：
1. 新建 tools/bench_bloom.py：在一代已写入约 200 万个 id（接近设计容量）的稳态下，每批 1 万条，分别计时 check_and_set 的各个子步骤（判断代是否就绪、批内去重 np.unique、计算探测位置 _probe、读取并判断、写入位），报告每条的 μs
2. 实施上面两项优化
3. 等价性测试（tests/test_dedup.py）：把修改前的 GenerationalBloom 原样复制进测试作为参考实现。用相同的随机 id 序列（含批内重复、跨批重复、跨代、未就绪的代、代的轮换与清空）分别驱动新旧实现，每一批之后比较：返回的 dup 与 active 完全相同，每一代的 words 数组逐位相同，n 相同
4. 用 bench_bloom.py 报告优化前后各子步骤的耗时

## 二、Bloom：减少哈希个数（会改变误判率）

1. BloomConfig 增加字段 k: int | None = None。为 None 时沿用现有公式（行为不变）；为整数时直接使用该值。不修改 config.py（Config 通过 default_factory=BloomConfig 使用这里的默认值）
2. 在 tools/bench_bloom.py 中实测误判率：在一代中写入 per_gen（200 万）个不同的 id，再查询 100 万个从未写入的 id，统计被误判为"见过"的比例。分别对 k = 12（现有）、8、6、4 测量误判率与 check_and_set 每条的耗时
3. 选择默认值：在"误判率不超过 0.1%"的前提下取耗时最低的 k，把 BloomConfig 的默认 k 设为这个值。在 dedup.py 的注释中写明各 k 的实测误判率与耗时
4. 更新受影响的测试；在 docs/DESIGN_TODO.md 中记一条：去重的哈希个数与实测误判率需要更新到设计文档 §3.1 与 §4

## 三、tick 的剖析（只诊断，不修改）

P2a 发现 tick 的开销随吞吐上涨：每秒 5 万条时约 40 ms/秒，每秒 12 万条时约 141 ms/秒，而按设计它应该基本固定（名单上限 512、Count-Min 大小固定）。
1. 新建 tools/bench_tick.py：用 FakeClock 驱动一个 Engine，按长尾与全随机两种分布，分别以每秒 5 万、12 万、20 万条的速率写入（直接调用 engine.ingest，每个模拟秒调用一次 tick），在保留期填满后的稳态下测量 advance（定稿）与 refresh_snapshot（快照）每次的耗时
2. 把两者再细分到子步骤（定稿：叶子摘要、Count-Min 加入各层累加器、节点完成；快照：组装时间片、合并候选、Top-K 计算、HLL 合并、DDSketch 合并），报告每一项在三种速率下的耗时
3. 解释耗时随速率上涨的原因（例如：候选集合的大小是否随速率增长、名单的更替是否更频繁），给出针对性的优化建议，注明每项建议是否涉及冻结模块。只诊断，不修改

## 四、端到端验证

配置 A（lane-c 当前代码、进程池 3 个解析进程、块 10000、两个额度 16 MB、容器 512 MB），使用 P2a 的快速压测模式（--fast），用 tools/s1_measure.py 跑 12 万、16 万、20 万、24 万四档：
1. 修改前（本任务开始时的 HEAD）跑一次
2. 第一、二节完成后再跑一次
每档报告：接受率、主进程 CPU%、主进程每条 μs、解析进程合计 CPU%、工作集峰值。再用 py-spy 对修改后的主进程剖析 30 秒，确认 Bloom 的占比下降了多少。

## 收尾

1. pytest -q、pytest -q -m slow、docker build --target test 全部通过；跑 bench_core 一次
2. 在 docs/PROGRESS.md 中记录：dedup.py 本任务结束后重新冻结
3. 提交并推送 lane-c，汇报：第一节各子步骤优化前后的耗时与等价性测试结果；第二节各 k 的误判率与耗时及选定值；第三节 tick 的剖析结论与优化建议；第四节端到端的前后对比
