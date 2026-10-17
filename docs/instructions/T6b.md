# T6b：补齐 HTTP 接口

背景：T6 的上一个会话中断，有几项要求没有落实。create_app 保持现有的参数形式。aiohttp 的两个发现（@web.middleware、auto_decompress=False）要保留，并确认 __main__.py 的生产启动路径也关闭了自动解压。

## 一、和题目要求不符，必须修改

1. /v1/stats 的字段名必须与题目一致：approx_distinct_users（四舍五入为整数）、approx_p95_bytes，并且必须包含 memory_bytes。保留 window_ms、from、to、degraded。
2. 所有 JSON 响应都不能出现 NaN 或 Infinity：p95 没有数据时输出 null。用 orjson 序列化（它会把 nan 输出为 null），统一写一个 json_response 辅助函数，所有处理函数都用它。

## 二、补齐遗漏的部分

3. 新建 flowlens/memstat.py：memory_bytes(root: str = "/") -> int，依次读取 sys/fs/cgroup/memory.current（cgroup v2）、sys/fs/cgroup/memory/memory.usage_in_bytes（v1）、proc/self/statm 的第二列（常驻页数）乘以 os.sysconf("SC_PAGE_SIZE")；全部读不到时（例如 Windows）返回 0，任何情况下都不抛异常。/stats 和 /healthz 改为调用它。
4. 原始请求体的内存保护：在途预留只管解压之后的数据，但读取请求体本身也占内存。
   - 在 app 上挂一个 RawBudget（复用 Admission 类，limit 为 8 MB），表示所有正在读取或持有的原始请求体字节数之和
   - 有 Content-Length 时：超过 4 MB 直接返回 413，不读请求体；否则先预留 Content-Length 字节，失败返回 503 + Retry-After: 1
   - 没有 Content-Length 时（分块传输）：先预留 4 MB，用 request.content 分块读取，累计超过 4 MB 立即返回 413
   - 预留在整个请求处理完毕后释放（finally）
5. 错误中间件补全：aiohttp 的 HTTPException（404、405、413 等）也转成 {"error": ...} 的 JSON，保持原状态码与原有响应头（例如 405 的 Allow）；其他未预期的异常用 logging.exception 记录完整信息，返回 500 和 {"error": "internal error"}。
6. /healthz 补充 tick_errors（Ticker.error_count）和 last_tick_ms（Ticker.last_tick_duration_ms），memory_bytes 改用 memstat。
7. params.py：
   - at 只接受非负整数，负数返回 400
   - 增加 interval 参数（T7 用）：接受 "500ms"、"1s"、"2"（纯数字表示秒）等写法，转成毫秒，范围 200 ms 到 60 s，默认 1 s
8. topk 响应里的 count 和 err：数值是整数时输出整数，否则保留两位小数。

## 三、补齐测试（tests/test_api.py、新建 tests/test_memstat.py）

- Content-Length 超过 4 MB 时返回 413，并验证服务端没有读取请求体（例如只发送请求头、不发送请求体，服务端也能立即返回 413）
- 分块传输的请求体超过 4 MB 时返回 413
- RawBudget 已满时返回 503，带 Retry-After；释放后用同一批事件重试，被正常接受、不被判为重复
- 404、405、413、500 的响应体都是 {"error": ...} 的 JSON；500 通过让某个依赖抛出意外异常来制造，并检查日志里有完整的异常信息
- 区间内没有数据时，/v1/stats 返回的 approx_p95_bytes 是 null，并且整个响应能被严格的 JSON 解析器解析（例如 json.loads 配合 parse_constant，遇到 NaN 时报错）
- /v1/stats 的字段名与题目一致；/healthz 包含所有字段
- at 为负数、interval 格式错误或越界时返回 400
- 应用关闭时 Ticker 正常结束
- memstat：用临时目录分别伪造 cgroup v2、cgroup v1、statm 三种情况，验证读取顺序与换算；全部不存在时返回 0

## 四、文档

README.md 补充本地启动方式（Windows PowerShell 写法），以及写入、topk、stats、healthz 的 curl 示例。

## 收尾（每个任务都一样）

1. pytest -q 必须通过；涉及 bounds.py 或 sketch/heavy.py 时还要运行 pytest -q -m slow。
2. 更新 docs/PROGRESS.md：当前进度、进行中、未解决的问题、下一步；本任务中新定下的决策追加到"已定的决策"。
3. 本任务中实现与 DESIGN.md 描述不一致的地方，追加到 docs/DESIGN_TODO.md。
4. 在 AI_LOG.md 末尾追加本任务的记录。
5. 按下面的格式汇报，然后停下，等待审查：
   1. 逐项说明上面 1~8 的完成情况
   2. pytest -q 和 pytest -q -m slow 的结果
   3. 本地实际启动服务后，用 curl 依次调用四个接口的完整输出
   4. 和指令不一致的地方，以及原因
   5. 需要确认的问题
   6. 本任务新建或修改的文件清单

如果上下文快满、任务做不完：先把已完成和未完成的部分写进 docs/PROGRESS.md 的"进行中"一节，然后停下汇报。
