"""P2d-1 第六节：前端进程架构的端到端测试（仅 Linux，SO_REUSEPORT 依赖）。

用真实子进程 ``python -m flowlens``（FLOWLENS_FRONTENDS=2）启动服务，/healthz
等就绪后验证：

- 写入后 events_ingested 正确；
- 等价性：同一组请求分别写入前端模式与单进程模式（FLOWLENS_FRONTENDS=0）的
  两个服务，同一区间的 topk（url、user）、stats 逐项一致（近似字段用容差）；
- 状态码与错误体：400/404/405（含 GET /v1/ingest）/413（含 Expect 路径）/
  415/422/503 与单进程模式一致；
- SSE 经转发：收到帧；Last-Event-ID 重连 id 连续不重发；断开后引擎侧订阅数
  几秒内减少；
- 第 4 级：FLOWLENS_TEST_MEM（__main__ 的测试用内存读数注入）让引擎进入第 4
  级，前端在读取请求体之前 503，与单进程模式同状态码同错误体；
- SIGTERM：几秒内全部进程退出、无残留；kill -9 一个前端后几秒内被重新拉起，
  写入正常且引擎侧在途占用回到 0（被 kill 的前端预留已清零）；
- tools/selftest.py 的边界用例对前端模式服务全部通过。

kill -9 / 子进程发现需要读 /proc，用一个本地 wrapper（_WRAPPER）包住服务：
wrapper 起服务子进程，并在 stdin/stdout 上提供 JSON 命令（children /
front_children）把 /proc 的子进程信息回读出来——wrapper 与服务同机，不需要
跨进程共享内存映射。**引擎子进程退出时 wrapper 也会退出**（不依赖 stdin EOF），
否则 SIGTERM 测试里引擎死了、wrapper 还阻塞在读 stdin、测试又阻塞在 wait 上。
"""

import asyncio
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest
from aiohttp import ClientSession, ClientTimeout

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "tools"

pytestmark = pytest.mark.skipif(sys.platform != "linux",
                                reason="前端模式需要 Linux（SO_REUSEPORT 与 /proc）")


# ---- wrapper：起服务子进程 + stdin/stdout JSON 命令通道 ----

_WRAPPER = r'''
import json, os, re, select, subprocess, sys, threading

def children(pid):
    try:
        p = f"/proc/{pid}/task/{pid}/children"
        return [int(x) for x in open(p).read().split() if x]
    except OSError:
        return []

def front_children(engine_pid):
    """引擎的直接子进程里排除 resource_tracker 的 = 前端进程。

    spawn 子进程的 cmdline 是 python -c "from multiprocessing.spawn import
    spawn_main; spawn_main(...)"（不含 "flowlens"），所以只能按"父进程是引擎
    + 不是 resource_tracker"识别（与 s1_measure.py 的口径一致，P2d-1 第七节）。
    """
    res = []
    for c in children(engine_pid):
        try:
            cl = open(f"/proc/{c}/cmdline", "rb").read()
        except OSError:
            continue
        if b"resource_tracker" not in cl:
            res.append(c)
    return res

def main():
    port, fronts, test_mem, raw_budget, debug, sockfile = sys.argv[1:7]
    env = dict(os.environ, PYTHONHASHSEED="0", FLOWLENS_FRONTENDS=fronts)
    if test_mem:
        # P2d-2 五：FLOWLENS_TEST_MEM 只在 FLOWLENS_DEBUG=1 时生效，故一并开启调试
        env["FLOWLENS_TEST_MEM"] = test_mem
        env["FLOWLENS_DEBUG"] = "1"
    if debug:
        env["FLOWLENS_DEBUG"] = "1"
    if raw_budget:
        env["FLOWLENS_RAW_BUDGET_BYTES"] = raw_budget
    child = subprocess.Popen([sys.executable, "-m", "flowlens",
                              "--host", "127.0.0.1", "--port", port],
                             cwd=os.environ["FLOWLENS_ROOT"], env=env,
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.PIPE)
    # 引擎启动日志里有 "内部 HTTP <query.sock>、写入通道 <ingest.sock>"，把它
    # 解析出来写到 sockfile，测试据此直连引擎内部 Unix 套接字（路径在临时目录）
    sock_re = re.compile(r"内部 HTTP (\S+)、写入通道 (\S+)")
    def _tail():
        q = i = None
        for line in child.stderr:
            line = line.decode("utf-8", "replace")
            m = sock_re.search(line)
            if m and q is None:
                q, i = m.group(1), m.group(2)
                try:
                    with open(sockfile, "w") as f:
                        f.write(json.dumps({"query": q, "ingest": i}))
                except OSError:
                    pass
    threading.Thread(target=_tail, daemon=True).start()
    while True:
        # 引擎子进程退出就结束（不依赖 stdin EOF，避免 SIGTERM 测试死锁）
        if child.poll() is not None:
            break
        r, _, _ = select.select([sys.stdin], [], [], 0.2)
        if r:
            line = sys.stdin.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            if msg.get("cmd") == "children":
                reply = {"ok": True, "pid": child.pid,
                         "children": children(child.pid)}
            elif msg.get("cmd") == "front_children":
                reply = {"ok": True, "pid": child.pid,
                         "children": front_children(child.pid)}
            else:
                reply = {"ok": False, "error": "unknown " + str(msg.get("cmd"))}
            sys.stdout.write(json.dumps(reply) + "\n")
            sys.stdout.flush()
    rc = child.poll()
    if rc is None:
        child.kill()
        rc = child.wait()
    sys.exit(0 if rc == 0 else 1)

main()
'''


class _Wrapper:
    """wrapper 进程：stdin 发 JSON 命令、stdout 收 JSON 回复。

    服务子进程的环境变量（FLOWLENS_FRONTENDS / FLOWLENS_TEST_MEM /
    FLOWLENS_RAW_BUDGET_BYTES / FLOWLENS_DEBUG）在这里传给子进程。
    前端模式下引擎把两个内部 Unix 套接字路径写在启动日志里（P2d-2 六），
    wrapper 的 tail 线程解析出来写到 self._sockfile，query_sock 属性读回。
    close() 时 terminate wrapper（wrapper 退出前把服务子进程一并 kill）。
    """

    def __init__(self, port: int, fronts: str = "2", test_mem: str = "",
                 raw_budget: str = "", debug: str = "1"):
        import tempfile
        fd, self._sockfile = tempfile.mkstemp(prefix="flowlens-sock-")
        os.close(fd)
        env = dict(os.environ, FLOWLENS_ROOT=str(ROOT))
        self.proc = subprocess.Popen(
            [sys.executable, "-c", _WRAPPER, str(port), fronts, test_mem,
             raw_budget, debug, self._sockfile],
            env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, encoding="utf-8",
            errors="replace", bufsize=1)
        self.port = port

    def ask(self, cmd: str) -> dict:
        self.proc.stdin.write(json.dumps({"cmd": cmd}) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        assert line, "wrapper 已退出（无回复）"
        out = json.loads(line)
        assert out.get("ok"), out
        return out

    @property
    def engine_pid(self) -> int:
        return self.ask("children")["pid"]

    def front_pids(self) -> list[int]:
        return self.ask("front_children")["children"]

    @property
    def query_sock(self) -> str:
        """引擎内部 HTTP 的 Unix 套接字路径（前端模式；轮询等 wrapper 解析日志）。"""
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                with open(self._sockfile, encoding="utf-8") as f:
                    d = json.load(f)
                if d.get("query"):
                    return d["query"]
            except (OSError, ValueError):
                pass
            time.sleep(0.1)
        raise AssertionError("未从引擎日志解析出内部 Unix 套接字路径")

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


# ---- 通用辅助 ----

def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_healthz(port: int, timeout: float = 40.0) -> bool:
    url = f"http://127.0.0.1:{port}/healthz"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _healthz(port: int) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz",
                                timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))


def _start_front_service(fronts: int = 2, test_mem: str = "",
                         raw_budget: str = "") -> _Wrapper:
    w = _Wrapper(_free_port(), str(fronts), test_mem, raw_budget)
    try:
        assert _wait_healthz(w.port), "服务未就绪"
        if fronts > 0:
            deadline = time.time() + 20
            while time.time() < deadline:
                if len(w.front_pids()) == fronts:
                    break
                time.sleep(0.2)
            assert len(w.front_pids()) == fronts, "前端进程数不符"
    except BaseException:
        w.close()
        raise
    return w


def _http_json(port: int, method: str, path: str, body: bytes = b"",
               headers: dict | None = None,
               timeout: float = 10.0) -> tuple[int, dict, dict]:
    """发一个原始 HTTP 请求，返回 (状态码, JSON 体或 {}, 小写响应头 dict)。"""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    hdrs = dict(headers or {})
    if body or method in ("POST", "PUT", "PATCH"):
        hdrs.setdefault("Content-Length", str(len(body)))
    conn.request(method, path, body=body or None, headers=hdrs)
    r = conn.getresponse()
    data = r.read()
    out = {}
    if data:
        try:
            out = json.loads(data.decode("utf-8"))
        except ValueError:
            out = {"_raw": data.decode("utf-8", "replace")}
    conn.close()
    return r.status, out, {k.lower(): v for k, v in r.getheaders()}


def _mk_body(n: int, tag: str, seed: int = 7) -> bytes:
    """n 条合法事件 + 1 条批内重复（deduped）+ 1 条缺字段（invalid）。

    ts 用**现在附近**（watermark = now − 60s 以内）：太早的事件会被判迟到
    （LATENESS_MS=60s），不算 accepted。
    """
    import orjson
    import random
    r = random.Random(seed)
    now = int(time.time() * 1000)
    evs = [{"event_id": f"{tag}-{now}-{i}", "user_id": f"u{r.randrange(40)}",
            "url": f"/{tag}/{r.getrandbits(28)}" if i % 3 else f"/{tag}/hot{i % 5}",
            "ts": now - (i % 300), "bytes": 10 + (i % 90)} for i in range(n)]
    evs[5] = dict(evs[0])              # 批内重复 → deduped=1
    evs[6] = {"user_id": "x"}          # 缺字段 → invalid=1
    return orjson.dumps(evs)


# ---- 1. 写入计数 ----

def test_fronts_ingest_counted():
    w = _start_front_service(fronts=2)
    try:
        body = _mk_body(300, "cnt")
        st, out, _ = _http_json(w.port, "POST", "/v1/ingest", body)
        assert st == 202, out
        assert out["accepted"] == 298 and out["deduped"] == 1 and out["invalid"] == 1
        hz = _healthz(w.port)
        assert hz["events_ingested"] == 298
        assert hz["deduped"] == 1 and hz["invalid"] == 1
    finally:
        w.close()


# ---- 2. 等价性：前端模式 vs 单进程模式 ----

def _compare_topk(a: dict, b: dict, path: str) -> None:
    """两个 /v1/topk 响应逐项一致：顺序 + key + 次数（精确）；err 与
    unseen_upper 是"上界/近似"，允许小容差（TopK 堆成员对写入分批顺序敏感，
    前端 write_parsed 块 10000 与单进程 _ingest_inproc 块 2000 可能不同）。"""
    assert a["final"] == b["final"], path
    ai, bi = a["items"], b["items"]
    assert len(ai) == len(bi), (path, len(ai), len(bi))
    for xa, xb in zip(ai, bi):
        assert xa["key"] == xb["key"], (path, xa, xb)
        assert int(xa["count"]) == int(xb["count"]), (path, xa, xb)
        assert abs(xa["err"] - xb["err"]) <= 2.0, (path, xa, xb)
    assert abs(a["unseen_upper"] - b["unseen_upper"]) <= 2.0, (
        path, a["unseen_upper"], b["unseen_upper"])


def test_fronts_equivalence_with_single():
    """同一组请求分别写入前端模式与单进程模式，同一区间的统计应一致。

    用"5 个明确的重命中 URL/用户 + 长尾"的负载、只查 top-5：这样 top-5 是
    无歧义的（count 远大于长尾的 1），不受 TopK 堆成员对写入分批顺序敏感
    （前端 write_parsed 块 10000 vs 单进程 _ingest_inproc 块 2000，会改变名单
    **深尾部**成员）的影响。事件 ts 用现在附近（watermark = now − 60s 内，否则
    判迟到），at 取事件之后不久（保留期内），窗口覆盖事件所在秒。
    """
    import orjson
    import random
    r = random.Random(11)
    now = int(time.time() * 1000)
    # 5 个热 URL / 热用户各 400 次（共 2000 条）+ 3000 条长尾各 1 次 = 5000 条。
    # 热键明确（400 >> 长尾 1），top-5 无歧义，不受 TopK 堆成员对写入分批顺序
    # 敏感（前端 write_parsed 块 10000 vs 单进程 _ingest_inproc 块 2000 会改变名单
    # 深尾部成员）的影响。ts 在事件前 2 秒内（watermark = now − 60s 内，都 accepted）。
    #
    # URL 必须避免"纯数字路径段"——normalize_url 会把它归一成 {id}（model.py），
    # 那会把所有长尾 URL 折成同一个 key。热键 /eq/hot{0..4}、长尾 /eq/t/x{i} 都含
    # 字母、不会被归一化，且互不相同。user_id 不做 URL 归一化。
    evs = []
    for i in range(5000):
        if i < 2000:
            hot = i // 400                    # 0..4：第几个热键
            evs.append({"event_id": f"eq-{now}-{i}", "user_id": f"hu{hot}",
                        "url": f"/eq/hot{hot}", "ts": now - (i % 2000),
                        "bytes": 100 + (i % 50)})
        else:
            evs.append({"event_id": f"eq-{now}-{i}",
                        "user_id": f"u{r.randrange(500)}",
                        "url": f"/eq/t/x{i}",
                        "ts": now - (i % 2000), "bytes": 100 + (i % 50)})
    body = orjson.dumps(evs)
    hot_urls = {f"/eq/hot{j}" for j in range(5)}
    hot_users = {f"hu{j}" for j in range(5)}

    wf = _start_front_service(fronts=2)
    ws = _start_front_service(fronts=0)
    try:
        for svc in (wf, ws):
            st, out, _ = _http_json(svc.port, "POST", "/v1/ingest", body)
            assert st == 202, out
            assert out["accepted"] == 5000
        # 计数器逐项一致（精确值）
        hf, hs = _healthz(wf.port), _healthz(ws.port)
        for k in ("events_ingested", "deduped", "late", "invalid"):
            assert hf[k] == hs[k], (k, hf[k], hs[k])
        # at 取查询**当时** now − 1s（必须 ≤ 引擎当前时刻，否则 _parse_at 判"在未来"
        # 返回 400）；窗口 60s 覆盖事件所在秒（事件在启动 now − 2s 内，查询在启动后
        # ~2s，[at−60s, at] ⊇ [now0−2s, now0]）。
        time.sleep(1.0)
        at = int(time.time() * 1000) - 1000
        # top-5：两个模式逐项一致（key 顺序 + 次数精确；err/unseen_upper 容差）
        _compare_topk(
            _http_json(wf.port, "GET", f"/v1/topk?window=60s&k=5&by=url&at={at}")[1],
            _http_json(ws.port, "GET", f"/v1/topk?window=60s&k=5&by=url&at={at}")[1],
            "topk/url")
        _compare_topk(
            _http_json(wf.port, "GET", f"/v1/topk?window=60s&k=5&by=user&at={at}")[1],
            _http_json(ws.port, "GET", f"/v1/topk?window=60s&k=5&by=user&at={at}")[1],
            "topk/user")
        # 非空校验：top-5 应恰好是那 5 个热键（count 大），确认比较不是空转
        iu = _http_json(wf.port, "GET", f"/v1/topk?window=60s&k=5&by=url&at={at}")[1]
        assert {it["key"] for it in iu["items"]} == hot_urls, iu["items"]
        assert all(it["count"] >= 350 for it in iu["items"]), iu["items"]
        iu2 = _http_json(wf.port, "GET", f"/v1/topk?window=60s&k=5&by=user&at={at}")[1]
        assert {it["key"] for it in iu2["items"]} == hot_users, iu2["items"]
        assert all(it["count"] >= 350 for it in iu2["items"]), iu2["items"]
        # stats：近似（HLL / DDSketch），distinct_users 与 p95 用相对容差
        sf = _http_json(wf.port, "GET", f"/v1/stats?window=60s&at={at}")[1]
        ss = _http_json(ws.port, "GET", f"/v1/stats?window=60s&at={at}")[1]
        sf.pop("memory_bytes", None)
        ss.pop("memory_bytes", None)
        assert sf["approx_distinct_users"] == ss["approx_distinct_users"], (sf, ss)
        pf, ps = sf["approx_p95_bytes"], ss["approx_p95_bytes"]
        assert pf is not None and ps is not None, (sf, ss)
        hi = max(abs(pf), abs(ps), 1.0)
        assert abs(pf - ps) / hi <= 0.05, (sf, ss)
    finally:
        wf.close()
        ws.close()


# ---- 3. 状态码与错误体与单进程一致 ----

def _raw_req(port: int, request: bytes, timeout: float = 10.0) -> tuple[float, str]:
    """原始 socket 发一段请求字节，读到响应头 + 一点体，返回 (耗时秒, 原始响应)。"""
    t0 = time.time()
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    try:
        s.sendall(request)
        buf = b""
        while b"\r\n\r\n" not in buf:
            part = s.recv(65536)
            if not part:
                break
            buf += part
        # 响应头已到，再尝试读一点体（短超时即止）
        s.settimeout(0.5)
        try:
            buf += s.recv(65536)
        except socket.timeout:
            pass
        return time.time() - t0, buf.decode("latin-1")
    finally:
        s.close()


def test_fronts_error_parity_with_single():
    wf = _start_front_service(fronts=2)
    ws = _start_front_service(fronts=0)
    try:
        port_f, port_s = wf.port, ws.port
        now = int(time.time() * 1000)
        cases = [
            # (方法, 路径, 体, 头) —— 前端与单进程必须同状态码、同错误体
            ("POST", "/v1/ingest", b"not json at all",
             {"Content-Type": "application/json"}),                          # 400
            ("GET", "/v1/ingest", b"", {}),                                  # 405
            ("POST", "/v1/topk", b"", {"Content-Type": "application/json"}), # 405
            ("POST", "/v1/ingest", b"[]",
             {"Content-Encoding": "br", "Content-Type": "application/json"}),  # 415
            ("GET", f"/v1/topk?window=60s&k=10&by=url&at={now - 3_600_000}",
             b"", {}),                                                       # 422
            ("GET", "/no/such/path", b"", {}),                               # 404
            ("GET", "/v1/topk?window=60s&k=0&by=url", b"", {}),              # 400
        ]
        for method, path, bdy, headers in cases:
            st_f, out_f, _ = _http_json(port_f, method, path, bdy, headers)
            st_s, out_s, _ = _http_json(port_s, method, path, bdy, headers)
            assert st_f == st_s, (path, st_f, st_s)
            assert out_f == out_s, (path, out_f, out_s)
        # 413（Expect 路径）：Content-Length 超 4 MB 且带 Expect: 100-continue，
        # **不发送请求体**——服务端必须在读体之前就 413
        req = (b"POST /v1/ingest HTTP/1.1\r\nHost: 127.0.0.1\r\n"
               b"Content-Type: application/json\r\n"
               b"Content-Length: " + str(5 * 1024 * 1024).encode() + b"\r\n"
               b"Expect: 100-continue\r\n\r\n")
        for port in (port_f, port_s):
            dt, resp = _raw_req(port, req)
            head = resp.split("\r\n", 1)[0]
            assert "413" in head and dt < 5.0, (port, head, dt)
        # 413（分块路径）：分块传输累计超 4 MB 立即 413
        big = b"x" * (5 * 1024 * 1024)

        async def chunked_413(port: int):
            async with ClientSession(
                    timeout=ClientTimeout(total=30)) as sess:
                async with sess.post(f"http://127.0.0.1:{port}/v1/ingest",
                                     data=big, chunked=True) as resp:
                    data = await resp.read()
                    return resp.status, json.loads(data.decode())
        st_f, out_f = asyncio.run(chunked_413(port_f))
        st_s, out_s = asyncio.run(chunked_413(port_s))
        assert st_f == st_s == 413, (st_f, st_s)
        assert out_f == out_s, (out_f, out_s)
    finally:
        wf.close()
        ws.close()


# 注：额度满的 503（Overloaded "overloaded"）不单独做端到端用例——
# 前端模式每前端有独立额度、SO_REUSEPORT 把并发请求分到不同前端，"额度满"
# 无法跨两个进程确定性地复现（每前端原始体额度又有 4 MB 下限）。503 的状态码/
# 错误体/Retry-After 一致性由第 4 级用例（test_fronts_level4_reject_before_body）
# 确定性地覆盖：两种模式同状态码、同 {"error": ...} 体、同 Retry-After:1。


# ---- 4. SSE 经转发 ----

def test_fronts_sse_forwarded():
    w = _start_front_service(fronts=2)
    try:
        url = (f"http://127.0.0.1:{w.port}/v1/topk/stream?"
               "window=60s&k=10&by=url&interval=200ms")
        base_sub = _healthz(w.port)["sse"]["subscribers"]

        async def run() -> list[int]:
            timeout = ClientTimeout(total=None)
            async with ClientSession(timeout=timeout) as sess:
                async with sess.get(url) as resp:
                    assert resp.status == 200
                    assert resp.headers.get("Content-Type", "").startswith(
                        "text/event-stream")
                    block = await resp.content.readuntil(b"\n\n")
                    assert block.startswith(b"retry:")
                    got = []
                    t0 = time.monotonic()
                    while len(got) < 2 and time.monotonic() - t0 < 10:
                        blk = await resp.content.readuntil(b"\n\n")
                        for ln in blk.decode().split("\n"):
                            if ln.startswith("id:"):
                                got.append(int(ln[3:]))
                    # 连接期间引擎侧订阅数 +1（经前端建立的连接）
                    assert _healthz(w.port)["sse"]["subscribers"] >= base_sub + 1
            # 断开后引擎侧订阅数几秒内回落
            deadline = time.time() + 8
            while time.time() < deadline:
                if _healthz(w.port)["sse"]["subscribers"] <= base_sub:
                    return got
                await asyncio.sleep(0.3)
            raise AssertionError("断开后引擎侧订阅数未回落")
        got = asyncio.run(run())
        # 帧能经前端转发到达（id 严格递增；精确的 500/200 ms 网格连续性由
        # 边界用例 c18 经 selftest 覆盖，这里不重复断言以免因代理缓冲丢帧而误报）
        assert len(got) >= 2 and got[1] > got[0], got
    finally:
        w.close()


def test_fronts_sse_last_event_id_reconnect():
    """Last-Event-ID 重连经前端转发：不重发已见帧、id 连续（500 ms 网格）。

    与边界用例 c18 同逻辑（tolerant）：首帧落在 last+500~last+2000 的网格上
    （若旧组已释放、新组从当前网格点起产帧，首帧可能跳过若干格，但不重发已见帧、
    之后连续 +500 是硬要求）。
    """
    w = _start_front_service(fronts=2)
    try:
        url = (f"http://127.0.0.1:{w.port}/v1/topk/stream?"
               "window=60s&k=5&by=url&interval=500ms")

        async def run() -> None:
            timeout = ClientTimeout(total=None)
            async with ClientSession(timeout=timeout) as sess:
                async with sess.get(url) as resp:
                    await resp.content.readuntil(b"\n\n")  # retry
                    last = None
                    t0 = time.monotonic()
                    while last is None and time.monotonic() - t0 < 10:
                        blk = (await resp.content.readuntil(b"\n\n")).decode()
                        for ln in blk.split("\n"):
                            if ln.startswith("id:"):
                                last = int(ln[3:])
                    assert last is not None, "第一连接没收到带 id 的帧"
                # 带 Last-Event-ID 重连
                async with sess.get(url, headers={"Last-Event-ID": str(last)}) as r2:
                    await r2.content.readuntil(b"\n\n")
                    got = []
                    t0 = time.monotonic()
                    while len(got) < 3 and time.monotonic() - t0 < 10:
                        blk = (await r2.content.readuntil(b"\n\n")).decode()
                        for ln in blk.split("\n"):
                            if ln.startswith("id:"):
                                got.append(int(ln[3:]))
                    assert len(got) >= 3, got
                    assert got[0] > last, (got[0], last)          # 不重发已见帧
                    assert got[0] % 500 == 0 and got[0] <= last + 2000, got  # 落网格、不远
                    assert all(b - a == 500 for a, b in zip(got, got[1:])), got
        asyncio.run(run())
    finally:
        w.close()


# ---- 5. 第 4 级：前端在读取请求体之前拒绝 ----

def _wait_level4(port: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _healthz(port)["degrade"]["level"] >= 4:
            return True
        time.sleep(0.2)
    return False


def test_fronts_level4_reject_before_body():
    # 95% × 256 MB > 0.90 × 256 MB → 引擎进入第 4 级（occupancy 为 0，不触发 1/2 级）
    w = _start_front_service(fronts=1, test_mem="251658240,268435456")
    try:
        assert _wait_level4(w.port), "未进入第 4 级"
        body = json.dumps([{"event_id": "l4-1", "user_id": "u", "url": "/l4",
                            "ts": int(time.time() * 1000), "bytes": 10}]).encode()
        # Expect 路径：前端在客户端发送请求体**之前**就 503（不读体、立即返回）
        t0 = time.time()
        st, out, hdrs = _http_json(w.port, "POST", "/v1/ingest", body,
                                   headers={"Content-Type": "application/json",
                                            "Expect": "100-continue"})
        assert st == 503 and time.time() - t0 < 3.0
        assert hdrs.get("retry-after") == "1"
        assert out == {"error": "降级期间暂停写入"}
        # 普通路径同样 503（在读取请求体之前），引擎未写入
        st2, out2, _ = _http_json(w.port, "POST", "/v1/ingest", body,
                                  headers={"Content-Type": "application/json"})
        assert st2 == 503 and out2 == {"error": "降级期间暂停写入"}
        assert _healthz(w.port)["events_ingested"] == 0
        # 与单进程模式的错误体一致（同样的 FLOWLENS_TEST_MEM 让引擎进第 4 级）
        ws = _start_front_service(fronts=0, test_mem="251658240,268435456")
        try:
            assert _wait_level4(ws.port), "单进程未进第 4 级"
            st_s, out_s, _ = _http_json(ws.port, "POST", "/v1/ingest", body,
                                        headers={"Content-Type": "application/json"})
            assert (st, out) == (st_s, out_s), (st, out, st_s, out_s)
        finally:
            ws.close()
    finally:
        w.close()


# ---- 6. SIGTERM：全部进程几秒内退出、无残留 ----

def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)  # 信号 0：只探测存在性
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def test_fronts_sigterm_clean_exit():
    w = _start_front_service(fronts=2)
    try:
        fps = w.front_pids()
        assert len(fps) == 2
        engine = w.engine_pid
        t0 = time.time()
        os.kill(engine, signal.SIGTERM)
        rc = w.proc.wait(timeout=15)
        elapsed = time.time() - t0
        assert rc == 0, f"退出码 {rc}"
        assert elapsed < 10, f"退出耗时 {elapsed:.1f}s"
        # 前端进程无残留（给 reap 一点时间）
        deadline = time.time() + 5
        while time.time() < deadline:
            if any(_pid_alive(p) for p in fps):
                time.sleep(0.3)
                continue
            break
        alive = [p for p in fps if _pid_alive(p)]
        assert not alive, f"仍有前端进程存活: {alive}"
    finally:
        w.close()


# ---- 7. kill -9 一个前端：几秒内被重新拉起，写入正常，预留清零 ----

def test_fronts_kill9_restarts_and_clears_slot():
    w = _start_front_service(fronts=2)
    try:
        fps = w.front_pids()
        victim = fps[0]
        os.kill(victim, signal.SIGKILL)
        # 几秒内被重新拉起（monitor 每秒检查一次）
        deadline = time.time() + 15
        new_fps = fps
        while time.time() < deadline:
            new_fps = w.front_pids()
            if len(new_fps) == 2 and victim not in new_fps:
                break
            time.sleep(0.3)
        assert len(new_fps) == 2 and victim not in new_fps, new_fps
        # 重启后写入正常（新前端重新连上写入通道）：50 条 = 48 合法 + 1 批内重复
        # + 1 缺字段 → accepted=48、deduped=1、invalid=1
        body = _mk_body(50, "k9")
        st, out, _ = _http_json(w.port, "POST", "/v1/ingest", body)
        assert st == 202, out
        assert out["accepted"] == 48, out
        assert _healthz(w.port)["events_ingested"] == 48
        # 被 kill 的前端没有释放它的预留：monitor 重启前 reset_slot，
        # 引擎侧（FrontsBudgetView 读共享数组的）在途占用应回到 0
        deadline = time.time() + 10
        while time.time() < deadline:
            if _healthz(w.port)["degrade"]["occupancy"] == 0.0:
                break
            time.sleep(0.3)
        assert _healthz(w.port)["degrade"]["occupancy"] == 0.0
    finally:
        w.close()


# ---- 8. tools/selftest.py 边界用例对前端模式全部通过 ----

def test_fronts_selftest_boundary_cases():
    if str(TOOLS) not in sys.path:
        sys.path.insert(0, str(TOOLS))
    import boundary_cases as bc

    w = _start_front_service(fronts=2)
    try:
        async def run() -> list[bc.CaseResult]:
            session = ClientSession(trust_env=False,
                                    timeout=ClientTimeout(total=30))
            try:
                t = bc.Target(client=session, host="127.0.0.1", port=w.port,
                              base=f"http://127.0.0.1:{w.port}")
                return await bc.run_all(t)
            finally:
                await session.close()
        results = asyncio.run(run())
        failed = [r for r in results if not r.passed]
        for r in failed:
            print(f"边界用例失败：{r.name} 期望 {r.expected} 实际 {r.actual}")
        assert not failed, f"{len(failed)} 个边界用例失败"
    finally:
        w.close()


# ---- 9. P2d-2：默认窗口查询由前端本地回答 ----

def _unix_http(sock_path: str, path: str,
               timeout: float = 10.0) -> tuple[int, bytes]:
    """对某 Unix 套接字发一个 GET，返回 (状态码, 响应体)。

    引擎只监听 Unix 套接字（前端模式不监听 TCP），测试用它绕过前端直查引擎，
    与前端（对外端口）的响应做逐字节对照。
    """
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(sock_path)
        s.sendall((f"GET {path} HTTP/1.1\r\nHost: engine\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            part = s.recv(65536)
            if not part:
                break
            buf += part
        head, _, body = buf.partition(b"\r\n\r\n")
        head_txt = head.decode("latin-1")
        status = int(head_txt.split(" ", 2)[1])
        for line in head_txt.split("\r\n"):
            if line.lower().startswith("content-length:"):
                cl = int(line.split(":", 1)[1].strip())
                while len(body) < cl:
                    part = s.recv(65536)
                    if not part:
                        break
                    body += part
                break
        return status, body
    finally:
        s.close()


def _engine_info(w: _Wrapper) -> dict:
    """引擎内部 HTTP 的 /debug/info：query 计数（topk/stats）与 query_sock 路径。

    引擎的 Unix 套接字路径在临时目录里（不对外暴露），从它自己内部 HTTP 的
    /debug/info 读回（wrapper 以 FLOWLENS_DEBUG=1 起服务，故该端点存在）。
    """
    st, body = _unix_http(w.query_sock, "/debug/info")
    assert st == 200, (st, body[:200])
    return json.loads(body.decode("utf-8"))


def _body_without_mem(body: bytes) -> dict:
    """响应体去掉 memory_bytes 后的 JSON（前端与引擎的 memory_bytes 口径不同）。"""
    d = json.loads(body.decode("utf-8"))
    d.pop("memory_bytes", None)
    return d


def test_fronts_default_window_answered_locally():
    """默认窗口查询由前端本地回答：

    - 一致性：同一时刻对前端（对外端口）与引擎（内部 Unix HTTP）请求同一
      /v1/topk（url、user，k=1/10/100）与 /v1/stats，除 memory_bytes 外
      响应体一致（两次请求要在同一秒内完成，否则 from/to 可能不同——重试
      几次取 from/to 对齐的一次）；
    - 引擎侧计数：默认窗口查询不增引擎的 query 计数（前端本地回答），
      带 at / 非 60 秒窗口的查询（转发）才增。
    """
    w = _start_front_service(fronts=2)
    try:
        # 写入一批数据（热键明确，top 无歧义），等 2 秒让快照稳定
        st, out, _ = _http_json(w.port, "POST", "/v1/ingest", _mk_body(2000, "snap"))
        assert st == 202, out
        time.sleep(2.0)
        info = _engine_info(w)
        sock = info["query_sock"]
        assert sock == w.query_sock
        base = dict(info["query"])
        queries = [f"/v1/topk?by={d}&k={k}" for d in ("url", "user")
                   for k in (1, 10, 100)] + ["/v1/stats"]
        ok = False
        for _ in range(6):
            ok = True
            for path in queries:
                stf, bf, _ = _http_json(w.port, "GET", path)
                ste, be = _unix_http(sock, path)
                assert stf == ste == 200, (path, stf, ste)
                if _body_without_mem(bf) != _body_without_mem(be):
                    ok = False     # from/to 跨秒了，重试
                    break
            if ok:
                break
            time.sleep(0.3)
        assert ok, "前端与引擎的默认窗口响应未能对齐（from/to 跨秒）"
        # 引擎侧计数不增：全部默认窗口查询都由前端本地回答
        assert _engine_info(w)["query"] == base, (base, _engine_info(w)["query"])
        # 带 at 的 topk（转发）：引擎计数 topk +1
        at = int(time.time() * 1000) - 1000
        assert _http_json(w.port, "GET",
                          f"/v1/topk?window=60s&k=5&by=url&at={at}")[0] == 200
        aft2 = _engine_info(w)["query"]
        assert aft2["topk"] == base["topk"] + 1 and aft2["stats"] == base["stats"], (base, aft2)
        # 非 60 秒窗口的 stats（转发）：引擎计数 stats +1
        assert _http_json(w.port, "GET", "/v1/stats?window=30s")[0] == 200
        aft3 = _engine_info(w)["query"]
        assert aft3["stats"] == base["stats"] + 1 and aft3["topk"] == aft2["topk"]
    finally:
        w.close()


def test_fronts_local_default_matches_single_process():
    """前端本地回答的默认窗口查询与单进程模式（同一批数据）结果一致：
    topk 的 key 与 count 逐项相等，stats 的近似字段等值。"""
    import orjson
    import random
    r = random.Random(23)
    now = int(time.time() * 1000)
    evs = []
    for i in range(3000):
        if i < 1500:
            hot = i // 300
            evs.append({"event_id": f"ld-{now}-{i}", "user_id": f"hu{hot}",
                        "url": f"/ld/hot{hot}", "ts": now - (i % 1500),
                        "bytes": 100 + (i % 40)})
        else:
            evs.append({"event_id": f"ld-{now}-{i}", "user_id": f"u{r.randrange(300)}",
                        "url": f"/ld/t/x{i}", "ts": now - (i % 1500),
                        "bytes": 100 + (i % 40)})
    body = orjson.dumps(evs)
    wf = _start_front_service(fronts=1)
    ws = _start_front_service(fronts=0)
    try:
        for svc in (wf, ws):
            st, out, _ = _http_json(svc.port, "POST", "/v1/ingest", body)
            assert st == 202, out
            assert out["accepted"] == 3000
        time.sleep(2.0)   # 让两个服务的快照都稳定
        for by in ("url", "user"):
            for _ in range(6):
                _, df, _ = _http_json(wf.port, "GET", f"/v1/topk?by={by}&k=10")
                _, ds, _ = _http_json(ws.port, "GET", f"/v1/topk?by={by}&k=10")
                if (df["from"], df["to"]) == (ds["from"], ds["to"]):
                    break
                time.sleep(0.3)
            assert (df["from"], df["to"]) == (ds["from"], ds["to"]), (by, df, ds)
            assert [it["key"] for it in df["items"]] == \
                   [it["key"] for it in ds["items"]], (by, df, ds)
            assert [it["count"] for it in df["items"]] == \
                   [it["count"] for it in ds["items"]], (by, df, ds)
        for _ in range(6):
            _, sf, _ = _http_json(wf.port, "GET", "/v1/stats")
            _, ss, _ = _http_json(ws.port, "GET", "/v1/stats")
            if sf["from"] == ss["from"]:
                break
            time.sleep(0.3)
        assert sf["from"] == ss["from"], (sf, ss)
        assert sf["approx_distinct_users"] == ss["approx_distinct_users"], (sf, ss)
        assert sf["approx_p95_bytes"] == ss["approx_p95_bytes"], (sf, ss)
    finally:
        wf.close()
        ws.close()


def test_fronts_forward_cases_parity_with_single():
    """转发的情况（带 at、window=30s、非法参数 k=0 / window=abc）结果与单进程
    模式一致：状态码相等；错误体（400）逐字节相等（前端本地用同一 QueryParams
    校验，或转发后由引擎回）。"""
    now = int(time.time() * 1000)
    cases = [
        f"/v1/topk?window=60s&k=5&by=url&at={now - 5000}",  # 200（转发回放）
        "/v1/stats?window=30s",                             # 200（转发回放）
        "/v1/topk?window=60s&k=0&by=url",                   # 400
        "/v1/topk?window=abc",                              # 400
        "/v1/stats?window=abc",                             # 400
    ]
    wf = _start_front_service(fronts=2)
    ws = _start_front_service(fronts=0)
    try:
        for path in cases:
            stf, outf, _ = _http_json(wf.port, "GET", path)
            sts, outs, _ = _http_json(ws.port, "GET", path)
            assert stf == sts, (path, stf, sts)
            if stf == 400:
                assert outf == outs, (path, outf, outs)
            elif stf == 200:
                assert set(outf) == set(outs), (path, set(outf), set(outs))
                assert outf["k"] == outs["k"] and outf["by"] == outs["by"]
    finally:
        wf.close()
        ws.close()


def test_fronts_degraded_local_answer():
    """第 3 级降级：前端本地回答的默认窗口查询 degraded 为 true；sample_rate
    与引擎一致（shared.sample_weight / degraded 由引擎快照回调写入）。"""
    # 208037478/268435456 = 0.78×256MB < x < 0.90×256MB → 第 3 级（degraded、weight 1）
    w = _start_front_service(fronts=1, test_mem="208037478,268435456")
    try:
        assert _wait_level3(w.port), "未进入第 3 级"
        st, out, _ = _http_json(w.port, "GET", "/v1/topk?by=url&k=10")
        assert st == 200
        assert out["degraded"] is True
        wgt = _healthz(w.port)["degrade"]["sample_weight"]
        if wgt > 1:
            assert out["sample_rate"] == 1.0 / wgt
        else:
            assert "sample_rate" not in out
        st, s, _ = _http_json(w.port, "GET", "/v1/stats")
        assert st == 200 and s["degraded"] is True
    finally:
        w.close()


def _wait_level3(port: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _healthz(port)["degrade"]["level"] >= 3:
            return True
        time.sleep(0.2)
    return False


def test_fronts_stale_snapshot_forwards():
    """快照过期：暂停引擎（SIGSTOP）约 4 秒，前端的默认窗口查询转为转发
    （引擎冻结 → 内部 HTTP 无响应、查询挂起/超时）；恢复（SIGCONT）后前端
    重新收到快照、默认窗口查询恢复本地回答（快速 200）。

    这一项对真实进程时序敏感（指令："如果不稳定，改为单元测试
    FrontState.fresh_snapshot"——新鲜性判定本身已由 tests/test_hub.py 的
    test_front_state_freshness 与 tests/test_frontend.py 的
    test_front_topk_stale_snapshot_proxies 覆盖，本用例做真实进程的验证）。
    """
    w = _start_front_service(fronts=1)
    try:
        st, out, _ = _http_json(w.port, "POST", "/v1/ingest", _mk_body(500, "stale"))
        assert st == 202, out
        time.sleep(2.0)   # 让前端拿到快照（本地回答就绪）
        engine = w.engine_pid
        os.kill(engine, signal.SIGSTOP)
        try:
            time.sleep(4.0)   # 快照在超过 3 秒未收到 → fresh_snapshot 返回 None
            t0 = time.time()
            try:
                st, _, _ = _http_json(w.port, "GET", "/v1/topk?by=url&k=10",
                                      timeout=6.0)
                # 转发时引擎冻结：要么挂到超时、要么连接被拒。本地回答（<100ms）
                # 不应发生——它依赖 fresh_snapshot，此刻必为 None
                dt = time.time() - t0
                if st == 200 and dt < 0.05:
                    pytest.fail(f"快照过期后仍在 {dt*1000:.0f}ms 内本地回答")
            except (socket.timeout, TimeoutError, ConnectionError):
                pass   # 转发挂起 / 引擎无响应：符合预期
        finally:
            os.kill(engine, signal.SIGCONT)
        # 恢复后：前端重新收到快照，默认窗口查询恢复本地回答（快速 200）
        deadline = time.time() + 10
        ok = False
        while time.time() < deadline:
            try:
                t0 = time.time()
                st, _, _ = _http_json(w.port, "GET", "/v1/topk?by=url&k=10")
                if st == 200 and time.time() - t0 < 1.0:
                    ok = True
                    break
            except (socket.timeout, TimeoutError, ConnectionError):
                pass
            time.sleep(0.5)
        assert ok, "恢复后前端默认窗口查询未恢复本地回答"
        assert _healthz(w.port)["ok"] is True
    finally:
        w.close()
