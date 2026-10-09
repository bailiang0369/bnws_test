#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_spread.py —— 分析 spread_probe.py 采集的数据，回答两个问题：

Q1. 同一时间戳的事件，现货与合约的"到达时刻差"会不会比较大（如 >50ms）？多大比例？分布如何？
Q2. 同一时间戳的数据，是否"总是"某个市场先到？——用配对符号检验 + bootstrap 置信区间回答，
    而不是拍脑袋看一两次。

配对方法
--------
* 主口径（精确匹配）：bookTicker 每个更新都带 updateId(u)。同一笔簿内更新在两个市场
  通常各自独立生成 id，无法直接对齐；但**事件时间戳 T(微秒/毫秒) 相同的概率很低**，
  因此对"同一时间戳"采用两级匹配：
      level-1 完全相同 ev_us（现货微秒 vs 合约毫秒×1000，能精确相等说明同源打戳）
      level-2 最近邻：|Δev_us| <= tol (默认 2ms)，且要求两市场各自消息序列单调推进
* 到达差定义：d = t_mono_spot - t_mono_fut （单位 ms）。
      d > 0  => 合约先到；d < 0 => 现货先到。
* 滞后口径：latency_x = t_recv_x - ev_time_x（需先扣除时钟偏移 offset_x，来自 clock 记录）。

统计输出
--------
* 配对数、|d| 的分位数(p50/p90/p99/max)、|d|>50ms 占比
* 先到方计数、胜率、二项检验 p 值（双侧）、中位差的 bootstrap 95% CI
* 按分钟分桶的中位差走势（观察是否稳定偏向某市场）
* 各市场端到端延迟(latency)分布与时钟偏移汇总

用法：
    python3 analyze_spread.py data/probe_20261009_143000.jsonl [--tol-ms 2] [--gap-threshold 50]
"""

import argparse
import json
import math
import sys
from collections import defaultdict
from statistics import median, mean


def load(path):
    msgs = {"spot": [], "futures": []}
    clocks = {"spot": [], "futures": []}
    meta = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            t = r.get("type")
            # 注意：现货 bookTicker 不带任何时间戳字段(ev_us=None)，但含 b/a 价格字段，
            # 对口径B(买价跳变对齐)仍然有效，因此不能按 ev_us 过滤掉。
            if t == "msg" and r.get("market") in msgs and (r.get("ev_us") or r.get("b")):
                msgs[r["market"]].append(r)
            elif t == "clock" and r.get("market") in clocks:
                clocks[r["market"]].append(r)
            elif t == "meta":
                meta = r
    for m in msgs.values():
        m.sort(key=lambda x: x["t_mono"])
    return msgs, clocks, meta


def median_offset(clocks):
    out = {}
    for k, v in clocks.items():
        if v:
            out[k] = median([c["offset_ms"] for c in v])
        else:
            out[k] = 0.0
    return out


def pair_exact(spot, fut):
    """level-1: ev_us 完全相等的配对（现货微秒戳恰好等于合约毫秒戳×1000）。"""
    fmap = defaultdict(list)
    for r in fut:
        fmap[r["ev_us"]].append(r)
    pairs = []
    used = set()
    for s in spot:
        lst = fmap.get(s["ev_us"])
        if not lst:
            continue
        # 取到达时刻最接近的一条
        best = min(lst, key=lambda fr: abs(fr["t_mono"] - s["t_mono"]))
        key = best["t_mono"]
        if key in used:
            continue
        used.add(key)
        pairs.append((s, best))
    return pairs


def pair_nearest(spot, fut, tol_us):
    """level-2: 双指针最近邻匹配，|Δev|<=tol 且每个点最多用一次（贪心，按现货序扫描）。"""
    pairs = []
    j = 0
    used_f = set()
    for s in spot:
        # 移动 j 到不晚于 s.ev 的最后一个合约点附近
        while j < len(fut) and fut[j]["ev_us"] < s["ev_us"] - tol_us:
            j += 1
        best, bestd, bestk = None, None, None
        k = j
        while k < len(fut) and fut[k]["ev_us"] <= s["ev_us"] + tol_us:
            if k not in used_f:
                d = abs(fut[k]["ev_us"] - s["ev_us"])
                if bestd is None or d < bestd:
                    best, bestd, bestk = fut[k], d, k
            k += 1
        if best is not None:
            used_f.add(bestk)
            pairs.append((s, best))
    return pairs


def pair_trades(spot, fut):
    """口径A'（成交级精确配对）：两侧均为 trade/aggTrade 流时，用成交时间戳 T(毫秒) 对齐。

    同一笔成交在两个市场各自推送中的 T 字段是交易所打的同一时刻（毫秒级），
    因此 T 完全相等 <=> 同一事件。这是最严格意义的"同一时间戳"配对。
    """
    fmap = defaultdict(list)
    for r in fut:
        fmap[r["ev_us"]].append(r)
    pairs, used = [], set()
    for s in spot:
        lst = fmap.get(s["ev_us"])
        if not lst:
            continue
        best = min(lst, key=lambda fr: abs(fr["t_mono"] - s["t_mono"]))
        if best["t_mono"] in used:
            continue
        used.add(best["t_mono"])
        pairs.append((s, best))
    return pairs


def pair_by_price(spot, fut, max_gap_ms=500.0):
    """口径B：以"买价变化时刻"(b 字段跳变)作为同一市场事件的代理锚点对齐。

    bookTicker 两市场时间戳各自独立生成（现货 bookTicker 甚至不带时间戳），
    无法精确配对；但一次真实的盘口变动会先后反映到两个市场的买价上。
    对每次现货 b 跳变，找其后 max_gap 内第一次合约 b 跳到同一价格的推送，
    两条到达时刻(t_mono)之差即"同一事件的两市场到达差"。每个点最多用一次。
    """
    def jumps(stream):
        out, prev = [], None
        for r in stream:
            if r.get("b") is not None and r["b"] != prev:
                out.append(r)
                prev = r["b"]
        return out

    sj, fj = jumps(spot), jumps(fut)
    f_by_price = defaultdict(list)
    for i, r in enumerate(fj):
        f_by_price[r["b"]].append(i)
    used_f, pairs = set(), []
    ptr = defaultdict(int)          # 每价格一个游标，保证整体近似 O(n)
    for s in sj:
        lst = f_by_price.get(s["b"])
        if not lst:
            continue
        p = ptr[s["b"]]
        while p < len(lst) and (fj[lst[p]]["t_mono"] <= s["t_mono"] or
                                (fj[lst[p]]["t_mono"] - s["t_mono"]) * 1000.0 > max_gap_ms):
            p += 1
        ptr[s["b"]] = p
        if p < len(lst):
            i = lst[p]
            if i not in used_f:
                used_f.add(i)
                ptr[s["b"]] = p + 1
                pairs.append((s, fj[i]))
    return pairs


def sign_test_pvalue(n_pos, n_neg):
    """双侧精确二项检验 H0: P(先到)=0.5，n 大时用正态近似。"""
    n = n_pos + n_neg
    if n == 0:
        return 1.0
    k = min(n_pos, n_neg)
    if n <= 1000:
        p = sum(math.comb(n, i) for i in range(k + 1)) / (2 ** n) * 2
        return min(1.0, p)
    z = abs(n_pos - n / 2) / (math.sqrt(n) / 2)
    # erfc 近似双侧 p
    return math.erfc(z / math.sqrt(2))


def bootstrap_ci(vals, n=2000, alpha=0.05, seed=7):
    import random
    rnd = random.Random(seed)
    m = len(vals)
    if m == 0:
        return (float("nan"), float("nan"))
    if m < 30:
        n = 10000
    stats = sorted(median([vals[rnd.randrange(m)] for _ in range(m)]) for _ in range(n))
    return stats[int(alpha / 2 * n)], stats[int((1 - alpha / 2) * n)]


def pct(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    i = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[i]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl")
    ap.add_argument("--tol-ms", type=float, default=2.0, help="最近邻匹配容差(毫秒)")
    ap.add_argument("--gap-threshold", type=float, default=50.0, help="'较大时间差'阈值(毫秒)")
    ap.add_argument("--min-sep-s", type=float, default=5.0,
                    help="重连分段最小间隔(秒)：两条流接收时刻差超过该值视为不同连接段，剔除跨段配对")
    ap.add_argument("--max-event-gap-ms", type=float, default=500.0,
                    help="口径B：现货买价跳变后允许合约跟到的最大事件间隔(毫秒)")
    args = ap.parse_args()

    msgs, clocks, meta = load(args.jsonl)
    off = median_offset(clocks)
    print(f"== {args.jsonl}")
    print(f"   spot msgs={len(msgs['spot'])}  futures msgs={len(msgs['futures'])}")
    print(f"   时钟偏移(服务器-本机, ms): spot={off['spot']:+.1f}  futures={off['futures']:+.1f}"
          f"  => 两服务器间相对偏差≈{off['futures']-off['spot']:+.1f} ms")

    # ---- 重连分段：以每条流自身 t_mono 大间隙为界，只比较同一连接段内的配对 ----
    def segments(stream, gap_s):
        segs, cur = [], []
        prev = None
        for r in stream:
            if prev is not None and r["t_mono"] - prev > gap_s:
                segs.append(cur); cur = []
            cur.append(r); prev = r["t_mono"]
        if cur:
            segs.append(cur)
        return segs

    spot_segs = segments(msgs["spot"], args.min_sep_s)
    fut_segs = segments(msgs["futures"], args.min_sep_s)

    pairs = []
    for ss in spot_segs:
        for fs in fut_segs:
            lo = max(ss[0]["t_mono"], fs[0]["t_mono"])
            hi = min(ss[-1]["t_mono"], fs[-1]["t_mono"])
            if hi - lo <= 0:
                continue
            a = [r for r in ss if lo <= r["t_mono"] <= hi]
            b = [r for r in fs if lo <= r["t_mono"] <= hi]
            pairs += pair_exact(a, b)
            pairs += pair_nearest(a, b, int(args.tol_ms * 1000))
    # 去重（exact 与 nearest 可能重复命中同一对）
    seen, uniq = set(), []
    for s, f in pairs:
        key = (s["t_mono"], f["t_mono"])
        if key not in seen:
            seen.add(key)
            uniq.append((s, f))
    ts_pairs = uniq

    p_pairs = []
    for ss in spot_segs:
        for fs in fut_segs:
            lo = max(ss[0]["t_mono"], fs[0]["t_mono"])
            hi = min(ss[-1]["t_mono"], fs[-1]["t_mono"])
            if hi - lo <= 0:
                continue
            a = [r for r in ss if lo <= r["t_mono"] <= hi]
            b = [r for r in fs if lo <= r["t_mono"] <= hi]
            p_pairs += pair_by_price(a, b, max_gap_ms=args.max_event_gap_ms)

    report(ts_pairs, "口径A(时间戳最近邻匹配)", off, thr=args.gap_threshold)

    tr_pairs = []
    for ss in spot_segs:
        for fs in fut_segs:
            lo = max(ss[0]["t_mono"], fs[0]["t_mono"])
            hi = min(ss[-1]["t_mono"], fs[-1]["t_mono"])
            if hi - lo <= 0:
                continue
            a = [r for r in ss if lo <= r["t_mono"] <= hi]
            b = [r for r in fs if lo <= r["t_mono"] <= hi]
            tr_pairs += pair_trades(a, b)
    report(tr_pairs, "口径A'(成交时间戳 T 完全相等，仅 trade/aggTrade 流有效)", off, thr=args.gap_threshold)

    # 口径B 需要买价字段：只有 bookTicker 流才有（trade 流没有 b 字段则自动跳过）
    if any(r.get("b") is not None for r in msgs["spot"][:200]) and \
       any(r.get("b") is not None for r in msgs["futures"][:200]):
        report(p_pairs, "口径B(买价跳变事件对齐)", off, thr=args.gap_threshold)
    else:
        print("\n===== 口径B 跳过：当前数据流不含买价 b 字段（需 bookTicker 流） =====")


def report(pairs, label, off, thr):
    n = len(pairs)
    print(f"\n===== {label}: 配对样本 n={n} =====")
    if n == 0:
        print("   无配对样本：请确认两侧都在收流、或增大 --tol-ms / --max-event-gap-ms")
        return

    d = [(s["t_mono"] - f["t_mono"]) * 1000.0 for s, f in pairs]  # >0: 合约先到; <0: 现货先到
    ad = sorted(abs(x) for x in d)
    big = sum(1 for x in ad if x > thr)
    print(f"   |到达差| ms: p50={pct(ad,0.5):.1f} p90={pct(ad,0.9):.1f} "
          f"p99={pct(ad,0.99):.1f} max={ad[-1]:.1f}")
    print(f"   |到达差| > {thr:.0f}ms : {big}/{n} = {big/n*100:.2f}%")

    n_fut_first = sum(1 for x in d if x > 0)     # spot 后到 => 合约先到
    n_spot_first = sum(1 for x in d if x < 0)
    n_tie = n - n_fut_first - n_spot_first
    pv = sign_test_pvalue(n_fut_first, n_spot_first)
    lo, hi = bootstrap_ci(d)
    md = median(d)
    print(f"\n-- 先后顺序：合约先到 {n_fut_first} ({n_fut_first/n*100:.1f}%) | "
          f"现货先到 {n_spot_first} ({n_spot_first/n*100:.1f}%) | 同时 {n_tie}")
    print(f"   中位差(正=合约先到) = {md:+.2f} ms, bootstrap 95%CI [{lo:+.2f}, {hi:+.2f}]")
    print(f"   符号检验 p = {pv:.3g} -> " +
          ("存在系统性先后偏向" if pv < 0.01 else "无显著系统性偏向（差异主要是网络抖动）"))
    if md > 0:
        print(f"   方向解读：平均而言【合约】比现货早到约 {md:.1f} ms")
    elif md < 0:
        print(f"   方向解读：平均而言【现货】比合约早到约 {-md:.1f} ms")

    # ---- 端到端延迟（扣除时钟偏移后）----
    print("\n-- 端到端推送延迟 latency = t_recv_local - ev_ts - server_offset (ms)")
    for mk, idx in (("spot", 0), ("futures", 1)):
        lat = sorted((p[idx]["t_wall"] - p[idx]["ev_us"] / 1000.0 - off[mk]) for p in pairs)
        print(f"   {mk:8s}: p50={pct(lat,0.5):.1f} p90={pct(lat,0.9):.1f} "
              f"p99={pct(lat,0.99):.1f} max={lat[-1]:.1f}")

    # ---- 按分钟分桶看偏向是否稳定 ----
    buckets = defaultdict(list)
    for x, (s, f) in zip(d, pairs):
        buckets[int(s["t_mono"] // 60)].append(x)
    print("\n-- 每分钟中位差(ms, 正=合约先到) 走势：")
    line = []
    for k in sorted(buckets)[:60]:
        v = buckets[k]
        nf = sum(1 for x in v if x > 0)
        line.append(f"{median(v):+.0f}({nf}/{len(v)})")
    print("   " + "  ".join(line))


if __name__ == "__main__":
    main()
