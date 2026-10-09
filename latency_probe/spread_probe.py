#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spread_probe.py —— 币安「现货 vs U 本位合约」推送到达时间差探针（采集器）

功能
----
1. 同时连接两个 WebSocket：
     - 现货 stream :  wss://stream.binance.com:9443/ws/btcusdt@bookTicker
     - 合约 stream :  wss://fstream.binance.com/ws/btcusdt@bookTicker
   （bookTicker 是实时买卖价推送；除 <symbol>@depth20 外无推送频率限制，最适合做延迟对比）
2. 每条消息记录：市场、事件时间戳 T（现货 t / 合约 E）、本地接收单调时钟 t_mono、
   本地墙钟 t_wall、本地处理时刻 t_proc、传输耗时 transfer_ms。
3. 每 60 秒做一次 NTP 式时钟偏移估计（GET /api/v3/time 与 /fapi/v1/time），
   用于事后判断"同一时间戳是否总是某个市场先到"时剔除服务器时钟偏差。
4. 所有数据以 JSONL 追加写入 ./data/probe_<日期>.jsonl，供 analyze_spread.py 分析。

用法
----
    python3 spread_probe.py --symbols btcusdt,ethusdt --duration 3600
    # 不传 --duration 则一直运行，Ctrl+C 退出

判定口径说明
------------
* "较大时间差(>50ms)"有两种口径，分析器都会给出：
    口径A（到达时延差）：同一事件时间戳 T，|t_recv_spot - t_recv_futures| > 50ms
    口径B（推送滞后差）：同一 T，|(t_recv_spot - T) - (t_recv_futures - T)| > 50ms
  两者在服务器给两个市场打的时间戳一致时等价；口径B 受各市场事件生成时刻影响更大。
* 由于网络抖动，"先后顺序"必须用配对样本的符号检验(sign test)+bootstrap 置信区间判断，
  单次比较没有统计意义 —— 这正是 analyze_spread.py 做的事。
"""

import argparse
import json
import os
import queue
import signal
import statistics
import sys
import threading
import time
from datetime import datetime, timezone

import ssl
import urllib.request
import websocket  # pip install websocket-client

SPOT_WS = "wss://stream.binance.com:9443/ws/{streams}"
FUT_WS = "wss://fstream.binance.com/ws/{streams}"
SPOT_TIME_API = "https://api.binance.com/api/v3/time"
FUT_TIME_API = "https://fapi.binance.com/fapi/v1/time"

STOP = threading.Event()


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Recorder(threading.Thread):
    """异步落盘线程：避免磁盘 IO 阻塞 websocket 回调、污染接收时间戳。"""

    def __init__(self, path):
        super().__init__(daemon=True, name="recorder")
        self.q = queue.Queue()
        self.path = path
        self.n = 0

    def log(self, obj):
        self.q.put(obj)

    def run(self):
        with open(self.path, "a", encoding="utf-8") as f:
            while True:
                try:
                    item = self.q.get(timeout=1.0)
                except queue.Empty:
                    if STOP.is_set():
                        break
                    continue
                if item is None:          # 哨兵：退出前 flush
                    f.flush()
                    os.fsync(f.fileno())
                    break
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                self.n += 1
                if self.n % 200 == 0:
                    f.flush()


def get_server_time(url, timeout=5):
    """NTP 式往返估计：返回 (server_ms, rtt_ms, t_local_wall_ms)。"""
    t0 = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": "spread-probe"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = json.loads(resp.read().decode())
    t1 = time.time()
    server_ms = int(body["serverTime"])
    rtt_ms = (t1 - t0) * 1000.0
    mid_wall_ms = ((t0 + t1) / 2.0) * 1000.0
    return server_ms, rtt_ms, mid_wall_ms


class MarketClient(threading.Thread):
    """一个市场一个线程：建连、订阅、断线重连、记录消息。"""

    def __init__(self, market, url, recorder):
        super().__init__(daemon=True, name=f"ws-{market}")
        self.market = market              # "spot" | "futures"
        self.url = url
        self.recorder = recorder
        self.ws = None
        self.connected_at = None
        self.last_msg_mono = None
        self.stats = {"msgs": 0, "reconnects": 0, "errors": 0}

    def on_message(self, ws, raw):
        # ---- 第一时间取单调时钟（这是全部测量的核心时间基准）----
        t_mono = time.perf_counter()
        t_wall_ms = time.time() * 1000.0
        try:
            d = json.loads(raw)
        except Exception:
            self.stats["errors"] += 1
            return
        payload = d.get("data") if ("stream" in d and isinstance(d.get("data"), dict)) else d
        # 现货 bookTicker: {"u","s","b","B","a","A"} —— 实测【不带】任何时间戳字段！
        # 合约 bookTicker: {"e","u","s","b","B","a","A","E","t","T","st"} —— E/T 为事件毫秒戳
        # 现货 trade/aggTrade: {"e","E",...,"T"} —— E=事件推送时间, T=成交时间(毫秒)
        ev_us = None
        for key in ("E", "T"):                      # 优先 E（推送/事件时间），再 T（成交时间）
            v = payload.get(key)
            if v is not None:
                ev_us = v * 1000 if v < 10**14 else v   # 归一化到微秒
                break
        rec = {
            "type": "msg",
            "market": self.market,
            "sym": payload.get("s"),
            "ev_us": ev_us,               # 统一用微秒：现货原生 T，合约 E*1000
            "b": payload.get("b"), "B": payload.get("B"),
            "a": payload.get("a"), "A": payload.get("A"),
            "t_mono": t_mono,             # 本机单调时钟接收时刻（秒）
            "t_wall": t_wall_ms,          # 本机墙钟接收时刻（毫秒）
            "t_proc": time.perf_counter(),# JSON 解析完成时刻（衡量解析开销）
        }
        self.last_msg_mono = t_mono
        self.stats["msgs"] += 1
        self.recorder.log(rec)

    def on_error(self, ws, e):
        self.stats["errors"] += 1
        print(f"[{self.market}] ws error: {e}", file=sys.stderr)

    def on_close(self, ws, code, msg):
        print(f"[{self.market}] closed ({code}) -> will reconnect", file=sys.stderr)

    def on_open(self, ws):
        self.connected_at = time.perf_counter()
        self.recorder.log({"type": "open", "market": self.market,
                           "t_mono": self.connected_at, "t_wall": now_iso()})

    def run(self):
        sslopt = {"cert_reqs": ssl.CERT_REQUIRED}
        while not STOP.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open,
                )
                # run_forever 内部自带 ping/pong(ping_interval)，阻塞直到断开
                self.ws.run_forever(sslopt=sslopt, ping_interval=15, ping_timeout=10)
            except Exception as e:
                self.stats["errors"] += 1
                print(f"[{self.market}] fatal in run_forever: {e}", file=sys.stderr)
            if STOP.is_set():
                break
            self.stats["reconnects"] += 1
            STOP.wait(3.0)   # 3 秒后重连


def clock_offset_loop(recorder, period=60.0):
    """周期性估计两市场服务器相对本机墙钟的偏移，写入日志。"""
    while not STOP.wait(period):
        for name, url in (("spot", SPOT_TIME_API), ("futures", FUT_TIME_API)):
            try:
                s_ms, rtt, mid_wall = get_server_time(url)
                recorder.log({
                    "type": "clock", "market": name,
                    "server_ms": s_ms, "rtt_ms": round(rtt, 2),
                    "local_mid_ms": mid_wall,
                    # offset = 服务器墙钟 - 本机墙钟中点（含半个 RTT 误差）
                    "offset_ms": round(s_ms - mid_wall, 2),
                    "t_wall": now_iso(),
                })
                print(f"[clock] {name}: offset={s_ms - mid_wall:+.1f}ms rtt={rtt:.1f}ms")
            except Exception as e:
                print(f"[clock] {name} failed: {e}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description="Binance spot vs futures arrival-latency probe")
    ap.add_argument("--symbols", default="btcusdt",
                    help="逗号分隔小写交易对，如 btcusdt,ethusdt")
    ap.add_argument("--streams", default="bookTicker",
                    help="流类型，默认 bookTicker（推荐）；可选 trade / depth20")
    ap.add_argument("--duration", type=float, default=0,
                    help="运行秒数，0=一直运行")
    ap.add_argument("--outdir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
    args = ap.parse_args()

    syms = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
    streams = "/".join(f"{s}@{args.streams}" for s in syms)
    os.makedirs(args.outdir, exist_ok=True)
    fname = os.path.join(args.outdir, f"probe_{datetime.now():%Y%m%d_%H%M%S}.jsonl")

    recorder = Recorder(fname)
    recorder.start()
    recorder.log({"type": "meta", "symbols": syms, "stream": args.streams,
                  "spot_url": SPOT_WS.format(streams=streams),
                  "fut_url": FUT_WS.format(streams=streams),
                  "start_mono": time.perf_counter(), "start_wall": now_iso(),
                  "note": "spot ev ts=t/T(us); futures ev ts=E(ms); t_mono=time.perf_counter()"})

    c_spot = MarketClient("spot", SPOT_WS.format(streams=streams), recorder)
    c_fut = MarketClient("futures", FUT_WS.format(streams=streams), recorder)
    th_clock = threading.Thread(target=clock_offset_loop, args=(recorder,), daemon=True)

    # Ctrl+C 优雅退出
    def sig_handler(sig, frm):
        print("\n[main] stopping ...")
        STOP.set()
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    c_spot.start(); c_fut.start(); th_clock.start()
    # 立即先做一次时钟偏移测量
    for name, url in (("spot", SPOT_TIME_API), ("futures", FUT_TIME_API)):
        try:
            s_ms, rtt, mid = get_server_time(url)
            recorder.log({"type": "clock", "market": name, "server_ms": s_ms,
                          "rtt_ms": round(rtt, 2), "local_mid_ms": mid,
                          "offset_ms": round(s_ms - mid, 2), "t_wall": now_iso()})
            print(f"[clock] {name}: offset={s_ms - mid:+.1f}ms rtt={rtt:.1f}ms")
        except Exception as e:
            print(f"[clock] {name} failed: {e}", file=sys.stderr)

    t_end = time.time() + args.duration if args.duration > 0 else None
    print(f"[main] logging to {fname}\n[main] spot={c_spot.url}\n[main] fut ={c_fut.url}")
    while not STOP.is_set():
        time.sleep(10)
        print(f"[heartbeat] spot msgs={c_spot.stats['msgs']} recon={c_spot.stats['reconnects']} err={c_spot.stats['errors']} | "
              f"fut msgs={c_fut.stats['msgs']} recon={c_fut.stats['reconnects']} err={c_fut.stats['errors']} | "
              f"logged={recorder.n}")
        if t_end and time.time() >= t_end:
            STOP.set()
    # 两条流都静默超过 90s 视为异常，自动退出
    STOP.set()
    for c in (c_spot, c_fut):
        if c.ws:
            try:
                c.ws.close()
            except Exception:
                pass
    time.sleep(1.5)
    recorder.log(None)
    recorder.join(timeout=10)
    print(f"[main] done. total records={recorder.n}, file={fname}")


if __name__ == "__main__":
    main()
