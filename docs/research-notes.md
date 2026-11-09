# 研究记录

只采集和研究，不下单；代码里没有私钥或交易凭证。

## 结算规则(窗口起点 ≥ 2026-08-14 UTC)
`Up  ⇔  TWAP60(T+300) >= TWAP60(T)`,两者都取自 Chainlink BTC/USD 60s TWAP。

## 目录
| 文件 | 作用 |
|---|---|
| `src/fetch_labels.py A B` | Gamma 回填官方标签 + priceToBeat/finalPrice → `data/raw/labels.parquet` |
| `src/fetch_binance.py A B` | Binance BTCUSDT 1s K 线 → `data/binance/*.parquet` |
| `src/import_trades.py` | 把 11-02 调研抓到的 Data API 成交记录(1,621 窗,09-05~10-31)压成 `data/raw/trades.parquet` |
| `src/model.py` | TWAP-aware 基线:Binance 代理 TWAP + 官方开盘价消基差 + EWMA 波动 + Student-t |
| `src/eval_baseline.py` | 对齐检验 + 训练(08-14~09-30)+ 样本外(10-01~11-03)评估 |
| `src/eval_market.py [市场价窗口秒] [延迟秒]` | 闸门:模型对市场价的增量 `c(τ)` + 10 月 taker 模拟 |
| `src/collect_live.py` | 常驻只读采集：盘口/Chainlink 现货与 TWAP/Binance → `data/live/` |

更新数据：`.venv/bin/python src/fetch_labels.py 2026-08-14 <今天>`,`.venv/bin/python src/fetch_binance.py <起> <止>`。

## 2026-11-04 结果
- 标签 23,590 窗;`final>=open` 与官方结果不符 0 个。
- Binance 平均窗口 `[t-63, t-4]` 最贴近官方 TWAP(5 分钟内基差漂移 sd $2.84);仅用 Binance 判方向，错 2.2%。
- 基线样本外 log loss 全面优于点价 Φ(剩 60s:0.18 vs 0.29;剩 10s:0.05 vs 0.32)。拟合参数 k_sig=0.80, basis_sd=$2.2, ν=4.7。
- **对市场：单独看，市场价在每个 τ 的 log loss/Brier 都优于模型。**
  - 增量 `c` 在剩 60–30s 一度显著(z≈2.7–4.3),但给模型加 2 秒延迟、市场价改用 3 秒内成交后，剩 60s 的 c 掉到不显著(z 0.9–1.3)。大部分"信号"来自成交价陈旧 + 模型零延迟。
  - 剩 ≤30s 仍有 z≈2–2.6,但同时 b≫1(市场价末段还不够极端)。模型在 ≤20s 的 taker 模拟巨亏,但这一段不可信:末段成交稀少,所用"市场价"中位已过期 3–4 秒、p90 约 13 秒(历史约 4 成窗口在最后 10 秒无成交)。巨亏原因未查明,基差噪声只是猜测。
  - 10 月 taker 模拟 42 格里只有 1 格 CI 下界 >0(r=180, thr 5%),与多重比较的偶然命中一致。
- **结论：目前没有可交易的 edge。** 成交价只是可成交报价的粗代理;要做定论需要 `collect_live.py` 采到的真实盘口和 Chainlink 实时价。

## 下一步(数据够了再做)
1. 采集 ≥2 周后：用真实 best bid/ask 和 Chainlink 实时值重跑闸门(重点 r≤60s,带 150/250ms 延迟)。
2. 测 Chainlink 相对 Binance 的实际延迟，替换模型里的 Binance 代理。
3. 残差层特征(反转、flow、perp basis)只在第 1 步 c 稳定 >0 后再加。

## 采集器
- 常驻：`zsh deploy/autostart.sh install`(launchd，登录后自启、崩溃自动重启)；卸载：`zsh deploy/autostart.sh remove`
- 没装常驻时：`./run.sh` 启动三个进程
- 质量报告:`.venv/bin/python src/inspect_live.py`。11-04 09:35 UTC 前的数据每窗开头缺约 13 秒(已修)。服务端偶发 `slow consumer` 断线(35 分钟 2 次),会自动重连,原因未查明。
- Data API v1 已退役(410),以后抓成交要用 `/v2/trades?condition=`,单页上限 1000 且翻页未跑通。
- 约 135MB/小时未压缩，整点后自动 gzip;旧 RTDS 是 legacy,随时可能停(新 PolyBolt 要凭证，未接)。

## 实时页面
`./run.sh` 打开本地页面 http://127.0.0.1:8791 (会重启页面进程；没装常驻时也会启动采集器和推送进程)。页面的下单是模拟(记录只存浏览器，不发订单)，样式照 Polymarket 官网：主图可切换 BTC 和上涨概率，下面是每 5 秒成交量，右边是 Up/Down、订单簿和逐笔；模型不显示。
盘口延迟超过 2 秒时暂停显示最新价；历史线按交易所时间补齐，不留缺口。右侧是挂单板(只在成交后更新，页面标注快照时间)和成交明细。

## 公网页面
`polymarket-btc5m.kaitogogo123.workers.dev`：`src/push_state.py` 每秒把状态经一条出站 WebSocket 推给 Worker(`deploy/worker.js`)，Worker 把约 1.4KB 的增量转发给 `/live` 上的页面，页面断流时退回轮询 `/state`，Mac 不开放入站端口。Mac 关机或睡眠时页面会显示数据源断开。
部署：`deploy/deploy.sh`；推送密钥在 `.push_token`，用 `npx wrangler@4 secret put PUSH_TOKEN` 写入线上。

## 数据存在哪里
原始数据都是本机文件，没有备份；每秒面板另存一份到 Neon：
- `data/live/`：盘口、成交、Chainlink、Binance 原始消息，按小时 JSONL，结束后 gzip。
- `data/live_pc/`：挂撤单 price_change(Up 一侧)，同样按小时。
- `data/panel/`：每秒面板缓存，Parquet，可由原始数据重建。
- `data/raw/`、`data/binance/`：回填的官方标签和 Binance K 线，Parquet。
- `out/rules.json`：规律挖掘结果。
- Neon Postgres 项目 `polymarket-btc5m`(新加坡，与 tamga 同组织、不同项目)：`src/db_sync.py` 每小时上传每窗一行的每秒面板(约 16KB)和规律结果，`load_db()` 可读回。连接串在 `.neon_url`。原始小时文件太大，不进库。
Cloudflare 上只有展示状态：当前状态在 Durable Object 内存，最近 20 个窗口在它的 SQLite 存储，不是研究数据。

## 影子下单
`src/shadow.py`(每小时随规律挖掘运行)：回放已结束的小时数据，以数据到达时间为唯一时钟，算出每条规律本该发出的订单。订单在发出后 285 ms(网络 135 + 官方吃单延迟 150)按当时重建的盘口成交，入场最多差 2¢，超出部分撤销。结果写 `out/shadow/`，汇总同步到 Neon。不发送任何真实订单。探针规律只用来验证管线，不是策略。测试：`tests/test_shadow.py`。
订单时刻重建盘口与官方报价一致 443/445(2026-11-04 12 点 UTC 那一小时)。

## 盘口规律研究(当前目标)
目标不是预测收盘涨跌，而是预测盘口价格，找到规律后在其重现时交易。真实下单仍未开放，见开头。
1. `src/book_panel.py`：把 `data/live` 做成每秒面板。买卖价取 best_bid_ask，深度取 book(只在成交后才有，超过 5 秒作废)，断线时段标记为丢失。
2. `src/mine_rules.py`：12 个特征 × 分位 × 时段 × 持有时间 × 跟随/反向，前一半窗口挖掘，后一半验证。成本含吃 100 股滑点和往返手续费，成交时点 = 信号秒 + 1 秒 + 该信号的到达延迟，延迟超 2 秒的信号作废；Chainlink 特征按接收时间计。留出段至少 1500 窗才可能判通过。页面的进度来自 `out/rules.json`，每小时更新。
3. `tests/test_mine_rules.py`：噪声和只领先 1 秒的信号不能通过，植入规律必须通过。

数据要点(2026-11-04 实测)：
- `side` 是吃单方向；Down 盘口和 price_change 都是 Up 的镜像；每笔成交只在一个 token 上出现一次。
- WebSocket 压缩曾造成延迟 p99 6.5 秒和 slow consumer 断线，已关闭(p99 1.3 秒，并行对比 150 秒)。
- 未解决：关压缩后 11:00 UTC 窗口仍出现延迟最长 9.9 秒、断连 2 次(当时本机负载 6.5)，原因未定；回测已跳过延迟超 2 秒的信号，并把信号到达时间算进成交时点。
- price_change 约 1000 条/秒，只保存 Up 一侧到 `data/live_pc/`。11-04 10:47 UTC 前没有这份数据。
