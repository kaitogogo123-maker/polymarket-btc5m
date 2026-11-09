# Polymarket BTC 5 分钟盘口研究

个人量化研究项目（Python）。实时采集 Polymarket「BTC 5 分钟涨跌」市场的盘口、成交、Chainlink 和 Binance 价格，重建挂单簿，挖掘盘口价格规律，并用影子下单回放验证。**只做研究，不发送任何真实订单，代码里没有私钥或交易凭证。**

实时页面：https://polymarket-btc5m.kaitogogo123.workers.dev

Personal quant research in Python: live collection of Polymarket BTC 5-minute markets (order book, trades, Chainlink, Binance), order-book reconstruction from price-change events (99.96% match with official snapshots), rule mining with out-of-sample validation, and shadow-order replay. Research only, no real orders.

## 做了什么

- **结算规则复核**：从官方接口回填 23,590 个窗口，按 Chainlink 60 秒 TWAP 复算涨跌，与官方结果 0 不符。
- **实时采集**（`src/collect_live.py`）：WebSocket 常驻采集盘口、成交、Chainlink、Binance，按小时落盘并压缩，断线自动重连。排查到 WebSocket 压缩导致延迟 p99 6.5 秒和断线，关闭后降到 1.3 秒。
- **挂单簿重建**（`src/book_panel.py`）：由挂撤单事件实时重建挂单，与官方快照一致率 **99.96%**；做成每秒面板。
- **基线模型与闸门**（`src/model.py`、`src/eval_market.py`）：TWAP 感知的概率模型（Binance 代理 TWAP + 消基差 + EWMA 波动 + Student-t），样本外和市场价对比，并加入延迟检验。结论是：在考虑延迟后，没有可交易的 edge（详细数据见 [docs/research-notes.md](docs/research-notes.md)）。
- **规律挖掘**（`src/mine_rules.py`）：12 个特征 × 分位 × 时段 × 持有时间，前一半窗口挖掘、后一半验证；成本计入滑点和往返手续费，成交时点计入信号到达延迟。测试保证噪声和「只领先 1 秒」的信号不能通过、植入的规律必须通过。
- **影子下单**（`src/shadow.py`）：以数据到达时间为唯一时钟回放，订单在 285 ms 后按当时重建的盘口成交；订单时刻的重建盘口与官方报价一致 443/445。
- **实时页面**（`deploy/`）：本机每秒经一条出站 WebSocket 把状态推给 Cloudflare Worker（Durable Object），页面通过 WebSocket 拿约 1.4 KB 的增量，断流时退回轮询；本机不开放入站端口。每小时的面板和规律结果同步到 Postgres（Neon）。

## 技术栈

Python 3.12、pandas、NumPy、SciPy、PyArrow、websockets、psycopg · Cloudflare Workers + Durable Objects · Neon Postgres · launchd

## 目录

```
src/      采集、面板、模型、评估、规律挖掘、影子下单、推送、本地页面
tests/    规律挖掘和影子下单的测试
deploy/   Cloudflare Worker、部署和常驻脚本
docs/     研究记录（结果、数据说明、已知问题）
```

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python src/fetch_labels.py 2026-08-14 2026-11-03   # 回填官方结果
.venv/bin/python src/collect_live.py                         # 实时采集
./run.sh                                                      # 本地页面 http://127.0.0.1:8791
```

数据文件（每小时约 27 MB 压缩后）不在仓库内。推送实时页面需要在 `.push_token` 放一个自定义密钥并写入 Worker；同步 Postgres 需要在 `.neon_url` 放连接串。两者都已被 `.gitignore` 排除。
