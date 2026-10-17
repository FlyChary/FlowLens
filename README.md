# FlowLens

有界内存的实时流量分析服务。设计见 [docs/DESIGN.md](docs/DESIGN.md)。

## 启动

Docker：

```
docker build -t flowlens .
docker run --rm -p 8080:8080 --memory=256m flowlens
```

本地直接运行（Windows PowerShell，监听 :8080）：

```powershell
conda activate flowlens_env        # 依赖 numpy / orjson / aiohttp
$env:PYTHONHASHSEED="0"            # 必须固定，否则哈希每次启动不同
python -m flowlens
```

## 接口示例

写入一批事件（JSON 数组；支持 `Content-Encoding: gzip`；请求体 ≤ 4 MB）：

```powershell
curl.exe -X POST http://localhost:8080/v1/ingest `
  -H "Content-Type: application/json" `
  --data '[{"event_id":"e1","user_id":"u1","url":"/api/item/42","ts":1735689600000,"bytes":128},{"event_id":"e2","user_id":"u2","url":"/api/item/43","ts":1735689600123,"bytes":256}]'
```

最近 60 秒 Top-10（`by` 取 `url` / `user`；`at` 为毫秒可回放历史区间；不带 `at` 的默认窗口直接读快照）：

```powershell
curl.exe "http://localhost:8080/v1/topk?window=60s&k=10&by=url"
```

统计（去重人数与 p95 响应字节数）：

```powershell
curl.exe "http://localhost:8080/v1/stats?window=60s"
```

健康检查：

```powershell
curl.exe "http://localhost:8080/healthz"
```

## 测试

在项目根目录（含 pyproject.toml 的目录）执行：

```
pip install -e ".[dev]"
pytest -q
```
