"""P1c 空闲内存测量：启动容器、等 5 秒，读主进程与每个解析进程的 RssAnon 与容器工作集。

运行在 WSL 宿主机（读 /proc 不进容器，与 s1_measure.py 同口径）：
- docker inspect 拿主进程宿主 PID，扫宿主 /proc/*/status 的 RssAnon（ppid==主进程
  的解析进程，排除 resource_tracker 与健康检查）；
- 工作集 = /healthz 的 memory_bytes（cgroup 工作集口径），原始读数 = memory_current_bytes。

用法（WSL）：
  ~/.flowlens_venv/bin/python tools/idle_mem.py --image flowlens:p1c-baseline --pool on --workers 3
  ~/.flowlens_venv/bin/python tools/idle_mem.py --image flowlens:p1c-baseline --pool off
  --env K=V 可额外注入环境变量（可多次）。
"""
import argparse
import asyncio
import json
import os
import socket
import sys
import time


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _run(cmd: list[str]) -> str:
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    out, _ = await proc.communicate()
    return out.decode("utf-8", errors="replace")


def _rss_anon_kb(pid: int) -> int | None:
    """宿主 /proc/<pid>/status 的 RssAnon（KB）；读不到返回 None。"""
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("RssAnon:"):
                    return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return None


def _cmdline(pid: int) -> str:
    try:
        return open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode("utf-8", "replace")
    except OSError:
        return ""


def _stat_ppid(pid: int) -> int | None:
    try:
        t = open(f"/proc/{pid}/stat", encoding="utf-8").read()
    except OSError:
        return None
    r = t.rfind(")")
    if r < 0:
        return None
    f = t[r + 1:].split()
    try:
        return int(f[1])
    except (ValueError, IndexError):
        return None


def find_service_pids(main_pid: int) -> dict[str, list[int]]:
    """按 ppid 找主进程的直接子进程，排除 resource_tracker 与健康检查进程。"""
    parse, other = [], []
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        pid = int(d)
        if _stat_ppid(pid) != main_pid:
            continue
        cl = _cmdline(pid)
        if "resource_tracker" in cl:
            continue
        if "healthz" in cl or "urllib.request" in cl:
            continue
        (parse if "python" in cl else other).append(pid)
    return {"parse": parse, "other": other}


async def wait_ready(url: str, timeout: float = 40.0) -> dict:
    import aiohttp
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with aiohttp.ClientSession(trust_env=False,
                                             timeout=aiohttp.ClientTimeout(total=5)) as s:
                async with s.get(url.rstrip("/") + "/healthz") as r:
                    if r.status == 200:
                        return await r.json()
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return {}


async def main_async(args) -> dict:
    port = _free_port()
    name = f"flowlens-idle-{int(time.time() * 1000)}"
    url = f"http://127.0.0.1:{port}"
    workers = args.workers if args.pool == "on" else 0
    env = ["-e", "PYTHONHASHSEED=0", "-e", f"FLOWLENS_PARSE_WORKERS={workers}"]
    for kv in args.env:
        if "=" not in kv:
            raise SystemExit(f"--env 需要 K=V 形式，收到 {kv!r}")
        env += ["-e", kv]
    await _run(["docker", "run", "--rm", "-d", "--name", name,
                f"--memory={args.mem}", f"--memory-swap={args.mem}",
                f"--cpuset-cpus={args.svc_cp}", *env,
                "-p", f"{port}:8080", args.image])
    try:
        hz = await wait_ready(url)
        if not hz.get("ok"):
            raise RuntimeError("服务未就绪：\n" + (await _run(["docker", "logs", name]))[-2000:])
        await asyncio.sleep(5.0)  # 等 5 秒（P1c 指令：启动后等 5 秒再读数）
        main_pid = int((await _run(["docker", "inspect", "-f", "{{.State.Pid}}", name])).strip() or 0)
        if not main_pid:
            raise RuntimeError("拿不到容器主进程 PID")
        pids = find_service_pids(main_pid)
        main_rss = _rss_anon_kb(main_pid)
        parse_rss = {p: _rss_anon_kb(p) for p in pids["parse"]}
        if len(pids["parse"]) != (workers if args.pool == "on" else 0):
            raise RuntimeError(f"解析进程数不符：期望 {workers if args.pool == 'on' else 0}，"
                               f"实得 {len(pids['parse'])}")
        hz = await wait_ready(url)
        MB = 1048576
        result = {
            "label": args.label or (f"pool_{args.pool}_w{workers}"),
            "image": args.image, "mem": args.mem, "svc_cp": args.svc_cp,
            "main_rss_anon_mb": round(main_rss / 1024, 1) if main_rss else None,
            "parse_rss_anon_mb": [round(v / 1024, 1) if v else None
                                  for v in parse_rss.values()],
            "ws_mb": round(hz.get("memory_bytes", 0) / MB, 1),
            "memcur_mb": round(hz.get("memory_current_bytes", 0) / MB, 1),
        }
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return result
    finally:
        await _run(["docker", "rm", "-f", name])


def main() -> None:
    ap = argparse.ArgumentParser(description="P1c 空闲内存测量（WSL 宿主运行）")
    ap.add_argument("--image", required=True)
    ap.add_argument("--pool", choices=["on", "off"], default="off")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--mem", default="256m")
    ap.add_argument("--svc-cp", default="0,2,4,6")
    ap.add_argument("--env", action="append", default=[], metavar="K=V")
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
