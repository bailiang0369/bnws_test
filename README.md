# bnws_test —— 币安现货 / U本位合约 WebSocket 推送时序差探针

同时连接币安**现货**与 **U 本位合约**的 WebSocket 行情流，记录每条消息的本地高精度到达时刻，
用于回答两个问题：

1. 同一时间戳（同一次成交事件）的推送，两市场到达本地的时间差会不会很大（例如 >50ms）？占比多少？
2. 同一时间戳的数据，是否总是某一个市场先到？偏向是否具有统计显著性？

## 目录结构

```
latency_probe/spread_probe.py    # 采集器：双 WS 线程 + 时钟偏移测量 + JSONL 落盘
latency_probe/analyze_spread.py  # 分析器：配对、分位数、符号检验、bootstrap CI
docs/结果总结.md                  # 本次实测结论（含数据表格）
latency_probe/data/*.jsonl       # 原始数据（体积大，已 .gitignore，不入库）
```

## 快速开始

```bash
pip install websocket-client

# 采集 1 小时（trade 流可严格对齐；bookTicker 流现货侧无时间戳，用价格跳变代理对齐）
python3 latency_probe/spread_probe.py --symbols btcusdt --streams trade      --duration 3600
python3 latency_probe/spread_probe.py --symbols btcusdt --streams bookTicker --duration 3600

# 分析
python3 latency_probe/analyze_spread.py latency_probe/data/probe_XXXX.jsonl --tol-ms 2 --gap-threshold 50
```

## 端点

| 市场 | URL |
|---|---|
| 现货 | `wss://stream.binance.com:9443/ws/<symbol>@<stream>` |
| U本位合约 | `wss://fstream.binance.com/ws/<symbol>@<stream>` |

支持 `<stream>` = `trade` / `aggTrade` / `bookTicker`。

## 方法要点

- 每条消息在回调第一时刻打 `time.perf_counter()`（单调钟），避免磁盘 IO 污染时间戳；
- 每 60s 向 `/api/v3/time`、`/fapi/v1/time` 做 NTP 式往返，估计各市场服务器相对本机时钟的偏移，
  用于计算端到端延迟；**两市场到达差比较只用同一台机器的单调钟相减，与时钟同步无关**；
- 配对口径：
  - **A'** 两侧成交时间戳 `T` 完全相等（最严格，仅 trade/aggTrade 可用）
  - **A** 事件时间戳最近邻 ±tol（默认 2ms）
  - **B** 最优买价档位跳变作为"同一冲击事件"的两市响应（bookTicker 专用）
- 先后偏向用**符号检验 + bootstrap 95% 置信区间**判定，而非看单次样本。

## 实测结论摘要（BTCUSDT，2026-10-09，详见 docs/结果总结.md）

- trade 流：|到达差| p50 <1ms，p99 ≈43ms，>50ms 占比 0~0.08%；现货系统性比合约早到约 0.3~1.7ms（p<1e-5）。
- bookTicker 流：现货侧不带任何时间戳字段，无法按时间戳配对；用价格跳变代理对齐后错位明显更大
  （p50≈28ms，p90≈217ms，>50ms 占 39.5%），且先后顺序无稳定偏向。
- 工程建议：跨市场"同一事件"以 `@trade/@aggTrade` 成交时间戳为锚；盘口数据当作带 age 标签的状态快照使用。
