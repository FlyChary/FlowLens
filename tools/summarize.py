"""读取实验结果目录，打印各场景的 Markdown 汇总（T10b 第二节第 3 点）。

用法：python tools/summarize.py results/<日期时间>

对每个场景子目录读 summary.json（必要时读 timeline.jsonl / accuracy.jsonl），
打印：
- 通用：工作集（memory_bytes）与原始读数的 起始/峰值/结束（MiB）、降级级别是否变化
  与到过哪些级别（T10b 追加要求）。正常负载（非 S5）若到过第 3 级或更高会**单独标出**。
- S1：各级速率表（接受率、503 比、写入 p50/p95/p99、客户端/服务 CPU）+ 最大可持续。
- S2：各查询类型延迟分位表（对照 p99 < 50 ms 目标）+ SSE 汇总。
- S3：与精确解的对比（召回、计数相对误差、是否落 [lower,upper]）+ stats 去重/p95 误差。
- S5：过载速率、恢复时间、降级级别随时间变化。
"""

import argparse
import json
import os
import sys

TOOLS = os.path.dirname(os.path.abspath(__file__))


def _load_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


def _fmt_ms(v):
    return "-" if v is None else f"{v:.1f}"


def _fmt_pct(v):
    return "-" if v is None else f"{v:.1%}"


def _fmt_cpu(v):
    """CPU 占用（已是百分数，可 >100% 表示多核），直接一位小数加 %。"""
    return "-" if v is None else f"{v:.0f}%"


def _mem_line(mem: dict) -> list[str]:
    """工作集与原始读数的起始/峰值/结束 + 降级变化，通用行。"""
    ws = mem.get("working_set", {})
    cur = mem.get("raw_current", {})
    lines = []
    lines.append(
        f"- 工作集：起始 {_f(ws.get('start_mb'))} / 峰值 {_f(ws.get('peak_mb'))} / "
        f"结束 {_f(ws.get('end_mb'))} MiB；原始读数（memory.current）峰值 "
        f"{_f(cur.get('peak_mb'))} MiB")
    lines.append(f"- 降级级别：{'有变化' if mem.get('degrade_changed') else '无变化'}，"
                 f"到过级别 {mem.get('degrade_levels_seen')}（峰值 "
                 f"{mem.get('peak_degrade_level')}）")
    return lines


def _f(v):
    return "-" if v is None else f"{v}"


def summarize_s1(d: dict, out: list) -> None:
    out.append(f"\n### S1 吞吐爬坡（gzip={d.get('gzip')}）\n")
    rows = []
    for lv in d.get("levels", []):
        rows.append([
            lv["rate"], _fmt_pct(lv.get("accept_ratio")),
            _fmt_pct(lv.get("ratio_503")),
            _fmt_ms(lv.get("lat_p50_ms")), _fmt_ms(lv.get("lat_p95_ms")),
            _fmt_ms(lv.get("lat_p99_ms")),
            _fmt_cpu(lv.get("client_cpu_pct")),
            _fmt_ms(lv.get("svc_cpu_pct_median")),
        ])
    out.append(_md_table(
        ["发送速率", "接受率", "503 比", "p50", "p95", "p99", "客户端 CPU", "服务 CPU"],
        rows))
    out.append(f"\n- **最大可持续吞吐：{d.get('max_sustainable')} 事件/秒**"
               f"（接受率≥99%、无 503、写入 p99<500ms 的最高级）\n")
    out += _mem_line(d.get("memory", {}))
    # 正常负载（S1 是纯写入、非过载）若到过第 3 级或更高，单独标出
    peak = d.get("memory", {}).get("peak_degrade_level", 0)
    if peak >= 3:
        out.append(f"- **⚠ 正常负载下进入第 {peak} 级**（S1 非过载场景，"
                   f"峰值工作集见上）\n")


def summarize_s2(d: dict, out: list) -> None:
    out.append(f"\n### S2 混合负载（写入 {d.get('write_rate')} 事件/秒，"
               f"{d.get('load', {}).get('wall_s')}s）\n")
    rows = []
    for t, s in sorted(d.get("query_stats", {}).items()):
        rows.append([t, s.get("n"), _fmt_ms(s.get("p50")),
                     _fmt_ms(s.get("p95")), _fmt_ms(s.get("p99"))])
    out.append(_md_table(["查询类型", "次数", "p50", "p95", "p99"], rows))
    out.append(f"\n- 对照目标：查询 **p99 < 50 ms**\n")
    sse = d.get("sse", {})
    out.append(f"- SSE：{sse.get('subscribers')} 订阅 / {sse.get('groups')} 组，"
               f"收到帧 {sse.get('frames')}，连接错误 {sse.get('errors')}\n")
    out += _mem_line(d.get("memory", {}))
    peak = d.get("memory", {}).get("peak_degrade_level", 0)
    if peak >= 3:
        out.append(f"- **⚠ 正常负载下进入第 {peak} 级**（S2 混合负载、非过载，"
                   f"峰值工作集见上）\n")


def summarize_s3(d: dict, out: list, dirpath: str) -> None:
    out.append(f"\n### S3 精度（{d.get('rate')} 事件/秒，{d.get('mode')}，"
               f"存档 {d.get('archive_events')} 条）\n")
    # 服务若在精度阶段前就进过 3/4 级，明确标注精度数据无效
    peak = d.get("memory", {}).get("peak_degrade_level", 0)
    if peak >= 3:
        out.append(f"- ⚠ 服务在运行中进过第 {peak} 级（内存增长所致）：精度抽样"
                   f"发生在降级期，**精度数据无效**（见 PROGRESS.md 未解决的问题）\n")
    out += _read_accuracy(dirpath)


def _read_accuracy(dirpath: str) -> list:
    """读 accuracy.jsonl 的抽样区间比较（type=sample_comparison），汇总成 Markdown。

    按 `bucket` 分三类（T10b）：valid（200 且非降级，真正可比）、degraded（200 但
    降级态空/不完整响应）、rejected（非 200，回放被暂停）。只有 valid 计入精度；
    其余单独计数。**兼容旧格式**（无 bucket 字段）：只有含 topk_url 结果的算有效、
    其余算拒绝——旧格式未记录 degraded，无法区分"200 空响应"，故旧数据的有效精度
    不可信，会在输出中提示。
    """
    path = os.path.join(dirpath, "accuracy.jsonl")
    recs = []
    try:
        with open(path, encoding="utf-8") as f:
            for ln in f:
                r = json.loads(ln)
                if r.get("type") == "sample_comparison":
                    recs.append(r)
    except (OSError, json.JSONDecodeError):
        return ["- （无 accuracy.jsonl 或读不到）\n"]
    if not recs:
        return ["- （无抽样区间比较）\n"]

    def _bucket(r):
        if "bucket" in r:                      # 新格式
            return r["bucket"]
        # 旧格式：有 topk_url 结果视为有效（但 degraded 未知，不可信）
        return "valid" if "topk_url" in r else "rejected"

    buckets = {"valid": [], "degraded": [], "rejected": []}
    for r in recs:
        buckets.setdefault(_bucket(r), []).append(r)
    valid, degraded, rejected = buckets["valid"], buckets["degraded"], buckets["rejected"]
    old_format = not any("bucket" in r for r in recs)

    lines = [f"\n抽样区间：有效 {len(valid)} / 降级 {len(degraded)} / "
             f"拒绝 {len(rejected)}（共 {len(recs)}）\n"]
    if old_format:
        lines.append("- ⚠ 旧格式未记录降级标记：以下有效精度可能混入降级期空响应，"
                     "**不可信**（本结果需重跑）\n")
    if degraded:
        lines.append(f"- ⚠ {len(degraded)} 个抽样区间在**降级态**返回空/不完整响应"
                     f"（未计入精度）\n")
    if rejected:
        lines.append(f"- {len(rejected)} 个抽样区间被 503/422 拒绝（回放暂停）\n")
    if not valid:
        lines.append("- （无有效精度样本，无法统计召回/误差）\n")
        return lines

    def _agg(key):
        vals, rel, bounds = [], [], []
        for r in valid:
            c = r.get(key)
            if not c:
                continue
            vals.append(c.get("recall", 0.0))
            rel.append(c.get("mean_rel_err", 0.0))
            bounds.append(1.0 if c.get("in_bounds") else 0.0)
        if not vals:
            return None
        import statistics
        return (statistics.fmean(vals), statistics.fmean(rel),
                sum(bounds) / len(bounds))

    rows = []
    for key in ("topk_url", "topk_user"):
        a = _agg(key)
        if a:
            rows.append([key, _fmt_pct(a[0]), _fmt_pct(a[1]), _fmt_pct(a[2])])
    if rows:
        lines.append(_md_table(["维度", "平均召回", "平均计数相对误差", "落界率"], rows))
    # stats：去重人数与 p95 的相对误差
    distinct_rels, p95_rels = [], []
    for r in valid:
        s = r.get("stats")
        if not s:
            continue
        du = s.get("distinct_users", {})
        se, ex_ = du.get("service"), du.get("exact")
        if ex_ and se is not None and ex_ > 0:
            distinct_rels.append(abs(se - ex_) / ex_)
        p = s.get("p95", {})
        se, ex_ = p.get("service"), p.get("exact")
        if ex_ and se is not None and ex_ > 0 and se == se:  # 非 nan
            p95_rels.append(abs(se - ex_) / ex_)
    import statistics
    if distinct_rels:
        lines.append(f"\n- stats 去重用户数：平均相对误差 {statistics.fmean(distinct_rels):.2%}（n={len(distinct_rels)}）")
    if p95_rels:
        lines.append(f"- stats p95 字节：平均相对误差 {statistics.fmean(p95_rels):.2%}（n={len(p95_rels)}）")
    return lines


def summarize_s5(d: dict, out: list) -> None:
    out.append(f"\n### S5 过载与恢复（过载 {d.get('overload_rate')} 事件/秒）\n")
    out.append(f"- 恢复时间（停发后回到 0 级）：**{d.get('recover_seconds')} 秒**\n")
    seq = d.get("degrade_level_seq", [])
    if seq:
        # 报告级别出现顺序与首次到达时间（相对过载开始），以及反复切换的次数
        order, first = [], {}
        t0 = seq[0][0]
        for t, lvl in seq:
            if lvl not in first:
                first[lvl] = t
                order.append(lvl)
        desc = " → ".join(f"{l}(+{(first[l] - t0) // 1000}s)" for l in order)
        # 级别跳变次数（相邻采样级别不同即算一次切换）
        switches = sum(1 for i in range(1, len(seq)) if seq[i][1] != seq[i - 1][1])
        out.append(f"- 降级级别轨迹：{desc}（期间级别切换 {switches} 次）\n")
    out += _mem_line(d.get("memory", {}))


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(description="实验结果汇总（Markdown）")
    p.add_argument("results_dir", help="实验结果根目录（含各场景子目录）")
    args = p.parse_args()

    print(f"# 实验结果汇总：{args.results_dir}\n")
    dirs = sorted(n for n in os.listdir(args.results_dir)
                  if os.path.isdir(os.path.join(args.results_dir, n)))
    # 机器/提交信息取第一个场景的 meta.json（每场景一份，内容相同）
    if dirs:
        m0 = _load_json(os.path.join(args.results_dir, dirs[0], "meta.json")) or {}
        mm = m0.get("machine") or {}
        svc = (m0.get("service") or {})
        print(f"git 提交：{m0.get('git_commit', '-')}；机器：{mm.get('cpu_model', '-')} "
              f"× {mm.get('cpus', '-')} 核；服务：{svc.get('target', '-')} "
              f"（核分配 {svc.get('cpu_alloc', '-')}）\n")

    for name in dirs:
        d = os.path.join(args.results_dir, name)
        s = _load_json(os.path.join(d, "summary.json"))
        if s is None:
            print(f"\n## {name}\n- （无 summary.json）\n")
            continue
        out = [f"\n## {name}\n"]
        sc = s.get("scenario")
        if sc == "S1":
            summarize_s1(s, out)
        elif sc == "S2":
            summarize_s2(s, out)
        elif sc == "S3":
            summarize_s3(s, out, d)
        elif sc == "S5":
            summarize_s5(s, out)
        else:
            out.append(f"- scenario={sc}")
        print("\n".join(out))


if __name__ == "__main__":
    main()
