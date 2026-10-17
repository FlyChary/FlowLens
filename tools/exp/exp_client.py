"""aiohttp 客户端：直接 POST 5 MB 请求体到 /v1/ingest，测 413 稳定性与耗时。

用法：python exp_client.py <big_body_file> [expect100=0|1]
每次试验用全新的 session（独立 TCP 连接），模拟真实客户端"被拒后换新连接"。
结果以单行 JSON 打印到 stdout，供汇总：
  {"trial": i, "got_413": bool, "error": <str|null>, "dt_ms": <float>,
   "follow_ok": bool, "follow_status": int}
error 分类：connection_reset（ServerDisconnectedError / ClientOSError /
ConnectionResetError）或其他异常类型名。
"""

import asyncio
import json
import os
import statistics
import sys
import time

import aiohttp
from aiohttp.client_exceptions import (
    ClientOSError,
    ClientConnectionError,
    ServerDisconnectedError,
)

URL = "http://127.0.0.1:8080/v1/ingest"
TIMEOUT = aiohttp.ClientTimeout(total=10.0)
N = int(os.environ.get("EXP_TRIALS", "20"))  # 试验次数（Windows 对照用少量可覆盖）


def _normal_body() -> bytes:
    now_ms = int(time.time() * 1000)
    evs = [{"event_id": f"n{i}", "user_id": "u", "url": "/n",
            "ts": now_ms - 1000, "bytes": 100} for i in range(5)]
    return json.dumps(evs).encode()


async def _one(session, big: bytes, expect100: bool, idx: int) -> dict:
    # 1. 发 5 MB 大请求体，记录 413 / 连接错误 / 耗时
    t0 = time.perf_counter()
    got_413, err = False, None
    try:
        async with session.post(URL, data=big, expect100=expect100) as r:
            body = await r.read()
            if r.status == 413:
                got_413 = True
    except ServerDisconnectedError as e:
        err = "connection_reset"
    except (ClientOSError, ConnectionResetError, ConnectionAbortedError) as e:
        err = "connection_reset"
    except ClientConnectionError as e:
        err = f"conn_error:{type(e).__name__}"
    except Exception as e:  # 其他异常单独归类
        err = type(e).__name__
    dt_ms = (time.perf_counter() - t0) * 1000.0

    # 2. 被拒（或任何结果）后，用新连接发一个正常写入请求，验证链路没坏
    follow_ok, follow_status = False, 0
    try:
        async with session.post(URL, data=_normal_body()) as r:
            follow_status = r.status
            await r.read()
            follow_ok = (r.status == 202)
    except Exception:
        follow_status = -1
    return {"trial": idx, "got_413": got_413, "error": err,
            "dt_ms": round(dt_ms, 2), "follow_ok": follow_ok,
            "follow_status": follow_status}


async def main() -> None:
    big_path = sys.argv[1]
    expect100 = len(sys.argv) > 2 and sys.argv[2] == "1"
    with open(big_path, "rb") as f:
        big = f.read()
    assert len(big) > 4 * 1024 * 1024, f"big body 应 >4MB，实际 {len(big)}"

    results = []
    for i in range(N):
        conn = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=conn, timeout=TIMEOUT) as s:
            results.append(await _one(s, big, expect100, i))

    dts = [r["dt_ms"] for r in results if r["error"] is None]
    n413 = sum(1 for r in results if r["got_413"])
    nreset = sum(1 for r in results if r["error"] == "connection_reset")
    other_err = {r["error"] for r in results if r["error"] not in (None, "connection_reset")}
    nfollow = sum(1 for r in results if r["follow_ok"])
    summary = {
        "client": "aiohttp", "expect100": expect100,
        "n413": n413, "n_reset": nreset, "other_errors": sorted(other_err),
        "median_ms": round(statistics.median(dts), 2) if dts else None,
        "max_ms": round(max(dts), 2) if dts else None,
        "follow_ok": nfollow,
    }
    print("SUMMARY", json.dumps(summary, ensure_ascii=False))
    for r in results:
        print("TRIAL", json.dumps(r, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
