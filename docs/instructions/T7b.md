# T7b：SSE 修正与 T7 收尾补做

不得修改冻结的基础模块。engine.py、api/ 下的文件不属于冻结范围，可以修改。

## 一、Bug：长窗口的帧因"早于保留期"被跳过

现象（审查时实测）：window=600s 的 SSE，interval 为 1.5 s 时约一半的帧、5 s 时约 80% 的帧、60 s 时几乎全部帧都因 TooOld 被跳过。普通查询同样受影响：at 为 1 秒前、window 为 600 s 的查询会返回 422。

原因：帧时刻 T 对齐到 interval 网格，往往比 now 早；区间起点 T − window 因此早于 now − 600 s。而实际上数据是有的：线段树保留的是"最新定稿秒往前 retention_sec 秒"，最新定稿秒本身约为 now − 60 s，所以实际能查到约 now − 660 s 的数据。

修改：engine._plan 的保留期检查改为与实际可用范围一致：区间起点早于 now_sec − retention_sec − 60 时才抛 TooOld（60 即 timewin 中的迟到上限，用常量，不要写死）。在 _plan 的文档字符串里说明原因。

测试：
- window=600s，interval 为 1 s、1.5 s、5 s、60 s 时，在一分钟内的各个时刻，帧计算都不会抛 TooOld
- /v1/topk：window=600s、at 为 60 秒前时正常返回；起点早于 now − 660 s 时返回 422

## 二、补帧不重算：订阅组保留最近的帧

现状：每个重连的订阅者各自把错过的帧逐帧重新计算。同组订阅者的帧完全相同，网络抖动时大量订阅者同时重连，会产生"订阅者数 × 30"次回放计算。

修改：
- SubscriptionGroup 保留最近 30 帧的 (帧时刻, payload)（collections.deque(maxlen=30)），生产者每产出一帧就追加
- 补帧时优先从这里取；只有不在其中的帧才调用 topk 计算
- 补帧时需要现场计算的部分，全局用一个 asyncio.Semaphore(1) 串行化，避免多个订阅者同时重连时并发计算

测试：同组 10 个订阅者同时带 Last-Event-ID 重连，后台调用 topk 的次数不随订阅者数增加。

## 三、新订阅者立即收到最近一帧

现状：新订阅者要等到下一个网格点才有第一帧，interval 为 60 s 时最多要等 60 s。

修改：没有 Last-Event-ID 的订阅者加入已有订阅组时，如果组里已有最近一帧，立即发送它；之后照常跟随实时帧，id 依然连续、严格递增。

测试：interval=60s 时，第二个订阅者加入后 1 秒内收到第一帧。

## 四、关闭时立即结束所有流

现状：关闭时只取消了各组的生产任务，SSE 处理函数还在等待队列。aiohttp 默认等待它们最多 60 秒才强制取消，而 docker stop 只等 10 秒就会杀掉进程。

修改：StreamManager.close() 通知每个订阅者立即结束（例如向队列放入一个结束标记，pump 收到后返回），让所有 SSE 处理函数及时退出。

测试：有多个活跃订阅者时，应用清理在 2 秒内完成，所有客户端都看到流正常结束。

## 五、两处小修

1. Last-Event-ID 不在当前 interval 网格上时（例如客户端换了 interval），起始游标向上对齐到网格，保证补帧与实时帧都在网格上。
2. 验证重算节流的行为：interval=1500 ms（大于 1 秒）时，每一帧都应重新计算，帧的 data.to 应等于 id。用测试固定下来。interval 小于 1 秒、复用上一次结果时，data 中的 from/to 保持实际计算时的值（不改写），在 docs/DESIGN_TODO.md 里记一条：SSE 帧的 data.to 可能比 id 早不到 1 秒。

## 六、补做 T7 收尾中遗漏的部分

T7 执行时用的是旧版指令，以下几项没有完成：
1. 在 docs/PROGRESS.md 的"已定的决策"中追加用户的两项决定：
   - hashing.key_hashes 中 hash(str(k)) 的改动保留：所有调用位置传入的都是校验过的字符串，结果与 hash(k) 完全一致，开销约每条事件 57 ns（约 1%）
   - sketch/hll.py 保留支持分段的版本，不换成简化版
2. 同样追加：内存读数保持 memory.current 原值，不扣除页缓存。理由：评测以是否超出 --memory 上限判定，内核判断 OOM 用的就是 memory.current，这个口径更保守，适合做降级判断。
3. 同样追加：SSE 默认 60 秒窗口的帧暂时走回放；是否改为复用快照，等 T10 压测看过 CPU 占用后在 T11 决定（同时记入"未解决的问题"）。
4. docs/PROGRESS.md 的任务列表和"下一步"按 docs/TASKS.md 开头的新顺序更新：T7 → T8 → T10 → T9 → T11 → 收尾。

## 收尾

1. 重跑全部测试与性能基准（以后每个任务都一样）：
   - pytest -q
   - pytest -q -m slow
   - python tools/bench_core.py 长尾
   - python tools/bench_model.py
   - python tools/bench_engine.py
2. 在 docs/PROGRESS.md 中建立"基准数据"一节（放在"未解决的问题"之前）：一张表，列出上面每一项的结果并注明提交号。先把下面这些上一版的数字作为对照行，再追加本次的数字：
   | 项目 | 上一版（T6c 前后） |
   |---|---|
   | pytest -q | 197 passed, 5 deselected |
   | pytest -q -m slow | 5 passed |
   | bench_core 长尾 | 约 20 万～30 万 事件/秒（不同次运行波动较大） |
   | bench_model（缓存命中） | 约 1.68 μs/条 |
   | bench_engine 每批 1000 条合计 | 约 6.3 μs/条（约 15.9 万/秒） |
   | bench_engine 每批 5000 条合计 | 约 4.9 μs/条（约 20.5 万/秒） |
   | 单次定稿 | 中位约 23 ms，最大约 47 ms |
   | 满载快照 refresh_snapshot | 中位约 59 ms，最大约 81 ms |
   | 完整 tick | 中位约 108 ms，最大约 137 ms |
   任何一项比上一版差 10% 以上时，在汇报里单独指出并分析原因。
3. 更新 docs/PROGRESS.md 的其他各节；实现与 DESIGN.md 不一致的地方追加到 docs/DESIGN_TODO.md；在 AI_LOG.md 末尾追加本任务的记录。
4. 提交到 git（不要推送，推送由用户完成），然后按下面的格式汇报，停下等待审查：
   1. 第一～六节的完成情况
   2. 第 1 步全部测试与基准的结果，以及与上一版的对比
   3. 和指令不一致的地方，以及原因
   4. 需要确认的问题
   5. 本任务新建或修改的文件清单

如果上下文快满、任务做不完：先把已完成和未完成的部分写进 docs/PROGRESS.md 的"进行中"一节，然后停下汇报。
