"""边界自测（T10a）：对一个正在运行的服务地址逐项检查边界用例。

与 tests/e2e/test_boundaries.py 共用 tools/boundary_cases.py 的同一套用例，
输出"用例 / 期望 / 实际 / 通过与否"的表格并给出通过数；任何一项失败时退出码
非 0（供 CI / 答辩演示直接判断）。

用法：python tools/selftest.py --url http://127.0.0.1:8080
"""

import argparse
import asyncio
import sys

import aiohttp

import boundary_cases as bc


async def _run(url: str) -> list[bc.CaseResult]:
    host = url.split("//", 1)[1].split("/", 1)[0]
    if ":" in host:
        host, port = host.split(":", 1)
        port = int(port)
    else:
        host, port = host, 80
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=16, force_close=False, ttl_dns_cache=300)
    async with aiohttp.ClientSession(trust_env=False, timeout=timeout,
                                     connector=connector) as session:
        t = bc.Target(client=session, host=host, port=port,
                      base=url.rstrip("/"))
        return await bc.run_all(t)


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(description="FlowLens 边界自测")
    p.add_argument("--url", default="http://127.0.0.1:8080", help="服务地址")
    args = p.parse_args()
    results = asyncio.run(_run(args.url))
    name_w = max(len(r.name) for r in results) + 2
    exp_w = max(len(r.expected) for r in results) + 2
    for r in results:
        mark = "通过" if r.passed else "失败"
        print(f"{r.name:<{name_w}}| 期望: {r.expected:<{exp_w}}| 实际: {r.actual} | {mark}")
    passed = sum(1 for r in results if r.passed)
    print(f"\n通过 {passed}/{len(results)}")
    raise SystemExit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
