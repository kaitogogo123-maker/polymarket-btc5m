"""Shadow orders: what each rule would have done with real orders, against the book as it stood. No orders are sent.

Replays finished hours of data/live + data/live_pc merged by arrival time, and that arrival time is the only clock, so
a decision can only use what had arrived by then. A rule is frozen the first time it is seen and only trades windows
that start after that. Decisions run at each second boundary with the same features as mine_rules.
An order reaches the book NET_MS + TAKER_DELAY_MS after it is sent and fills against the rebuilt book of that moment:
an entry only up to MAX_SLIP worse than the quote it saw (FAK, the rest is cancelled), an exit at any price.
Writes out/shadow/. Run: python src/shadow.py
"""
import fcntl, glob, gzip, heapq, json, math, os, sys, time, zlib
from pathlib import Path
import numpy as np, pandas as pd
from book_panel import SIZE, DEPTH_AGE, REF_AGE, book_row, build, load
from mine_rules import PARTS, MAX_LAG, LOOKBACK, features, clean_features, fee, pnl_tables, run_rule

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out/shadow"
NET_MS = 135            # half of the 271 ms median round trip to clob.polymarket.com, measured from this Mac on 2026-11-04
TAKER_DELAY_MS = 150    # official taker delay on crypto markets since 2026-09-04
MAX_SLIP = 0.02
SILENT_MS = 2000        # no book event for this long after the order's moment: the book did not change
GAP_S = 5
LAST_ENTRY = 295
# Fixed rules that exercise the pipeline before any mined rule exists. They are not strategies and never get a verdict.
PROBES = [dict(feature="成交流10s", threshold=.6, part="全程", hold=30, mode="跟随"),
          dict(feature="盘口动量5s", threshold=.03, part="全程", hold=10, mode="反向"),
          dict(feature="买卖盘失衡3¢", threshold=.5, part="全程", hold=0, mode="跟随")]
for _p in PROBES:
    _p.update(rule=f"探针 {_p['feature']} {_p['mode']} 持有{_p['hold'] or '到结算'}", probe=True, registered_at=0)
RAW = ["bid", "ask", "lag", "bd1", "ad1", "bd3", "ad3", "bd10", "ad10", "bn", "spot"]
LIMIT = {**dict.fromkeys(["bd1", "ad1", "bd3", "ad3", "bd10", "ad10"], DEPTH_AGE), "bn": REF_AGE, "spot": REF_AGE}
FLOWS = ["up_buy", "up_sell", "down_buy", "down_sell"]


def ffill(a, upto, limit=None):
    """Carry the last value forward over the seconds up to `upto`, like book_panel.carry, without its Python loop."""
    idx = np.where(np.isfinite(a), np.arange(a.size), -1)
    idx[upto + 1:] = -1
    idx = np.maximum.accumulate(idx)
    out = np.where(idx >= 0, a[idx], np.nan)
    if limit is not None:
        out[np.arange(a.size) - idx > limit] = np.nan
    out[upto + 1:] = np.nan
    return out


class Window:
    def __init__(self, T):
        self.T = T
        self.raw = {k: np.full(301, np.nan) for k in RAW}
        self.flow = {k: np.zeros(301) for k in FLOWS}
        self.bids, self.asks = {}, {}
        self.book_ts = None
        self.lost_until = -1

    def at(self, ts):
        i = ts // 1000 - self.T
        return i if 0 <= i <= 300 else None

    def view(self, i):
        P = {k: ffill(a, i, LIMIT.get(k))[None, :] for k, a in self.raw.items()}
        P.update({k: a[None, :] for k, a in self.flow.items()})
        return P


def walk(levels, want, price_of, limit):
    """Take up to `want` shares from levels ordered best first. Returns (shares, cash at token price, fee)."""
    got = cash = paid = 0.0
    for p, s in levels:
        px = price_of(p)
        if limit is not None and px > limit + 1e-9:
            break
        take = min(want - got, s)
        got += take
        cash += take * px
        paid += take * fee(px)
        if got >= want - 1e-9:
            break
    return got, cash, paid


class Shadow:
    def __init__(self, rules, tok, size=SIZE):
        self.rules, self.tok, self.size = rules, tok, size
        self.win, self.seen = {}, set()
        self.state = {r["rule"]: dict(busy_until=-1, busy_T=None, pos=None) for r in rules}
        self.pending, self.rows = [], []
        self.sec = None
        self.last_bn = None

    # ---- input: one record at a time, in arrival order
    def feed(self, r):
        src, rx, m = r["src"], r["rx"], r["m"]
        self.clock(rx)
        if src == "pc":
            self.on_change(m)
        elif src == "pm":
            self.on_market(m, rx)
        elif src == "bn":
            if m.get("k") == "t":
                self.last_bn = rx
                w = self.current()
                i = w.at(m["T"])
                if i is not None:
                    w.raw["bn"][i] = float(m["p"])
        elif src == "cl":
            p, topic = m.get("payload") or {}, m.get("topic") or ""
            for x in p.get("data") or [p]:
                if "value" in x and "timestamp" in x:
                    if "twap" not in topic:
                        w = self.current()
                        i = w.at(rx)
                        if i is not None:
                            w.raw["spot"][i] = x["value"]
        elif src == "err" and m.get("src") == "pm" and "t" in m:
            w = self.win.get(m["t"])
            if w and w.at(rx) is not None:
                w.lost_until = w.at(rx) + LOOKBACK

    def window(self, T):
        return self.win.setdefault(T, Window(T))

    def current(self):
        return self.window(self.sec // 300 * 300)

    def on_market(self, m, rx):
        who = self.tok.get(m.get("asset_id"))
        if not who:
            return
        T, side = who
        w, e = self.window(T), m.get("event_type")
        if e == "last_trade_price":
            d = (m.get("transaction_hash"), m["asset_id"], m["price"], m["size"], m["side"], m["timestamp"])
            if d in self.seen:             # reconnects and overlapping connections repeat events
                return
            self.seen.add(d)
            i = w.at(int(m["timestamp"]))
            if i is not None:
                w.flow[f"{side}_{'buy' if m['side'] == 'BUY' else 'sell'}"][i] += float(m["size"])
                self.late(w, i, rx - int(m["timestamp"]))
        elif side != "up":
            return
        elif e == "best_bid_ask":
            ts = int(m["timestamp"])
            i = w.at(ts)
            if i is not None:
                w.raw["bid"][i], w.raw["ask"][i] = float(m["best_bid"]), float(m["best_ask"])
                self.late(w, i, rx - ts)
        elif e == "book":
            ts = int(m["timestamp"])
            i = w.at(ts)
            if i is not None:
                row = book_row(m["bids"], m["asks"])
                if row:
                    for k, v in zip(RAW[3:9], row):
                        w.raw[k][i] = v
                self.late(w, i, rx - ts)
            if w.book_ts is None or ts >= w.book_ts:
                self.execute_due(ts, T)
                bids = {float(x["price"]): float(x["size"]) for x in m["bids"]}
                asks = {float(x["price"]): float(x["size"]) for x in m["asks"]}
                w.bids, w.asks, w.book_ts = bids, asks, ts

    @staticmethod
    def late(w, i, ms):
        v = w.raw["lag"][i]
        w.raw["lag"][i] = ms / 1000 if not np.isfinite(v) else max(v, ms / 1000)

    def on_change(self, m):
        w = self.win.get(m["T"])
        if not w or w.book_ts is None or m["ts"] < w.book_ts:
            return
        self.execute_due(m["ts"], m["T"])
        for price, size, side in m["c"]:
            book, p, s = (w.bids if side == "BUY" else w.asks), float(price), float(size)
            if s:
                book[p] = s
            else:
                book.pop(p, None)
        w.book_ts = m["ts"]

    # ---- the clock
    def clock(self, rx):
        s = rx // 1000
        if self.sec is None:
            self.sec = s
            return
        jump = s - self.sec > GAP_S        # checked first: during an outage silence says nothing about the book
        if not jump:
            for o in [o for o in self.pending if rx >= o["at"] + SILENT_MS]:
                self.fill(o)
        if s <= self.sec:
            return
        if jump:                           # the collector itself was down: nothing it would have sent can be trusted
            for o in list(self.pending):
                o["void"] = True
                self.fill(o)
            w = self.window((s - 1) // 300 * 300)
            if w.at((s - 1) * 1000) is not None:
                w.lost_until = max(w.lost_until, w.at((s - 1) * 1000) + LOOKBACK)
        for b in ([s] if jump else range(self.sec + 1, s + 1)):
            self.sec = b
            self.boundary(b)

    def boundary(self, s):
        """Second s - 1 has finished; anything sent now leaves at exactly s."""
        for r in self.rules:
            st = self.state[r["rule"]]
            pos = st["pos"]
            if pos and pos["T"] + 300 <= s:
                self.close(r, st)
            elif pos and pos["exit_at"] is not None and s >= pos["exit_at"] and not pos["exiting"]:
                self.send_exit(pos, s)
        T = (s - 1) // 300 * 300
        i = s - 1 - T
        w = self.win.get(T)
        if not w:
            return
        if self.last_bn is not None and s * 1000 - self.last_bn >= GAP_S * 1000:
            w.lost_until = max(w.lost_until, i + LOOKBACK)
        if i < w.lost_until:
            return
        live = [r for r in self.rules if r["registered_at"] <= T and PARTS[r["part"]][0] <= i < PARTS[r["part"]][1]
                and not self.blocked(r, T, s)]
        if not live:
            return
        P = w.view(i)
        bid, ask, lag = P["bid"][0, i], P["ask"][0, i], P["lag"][0, i]
        if not (np.isfinite(bid) and np.isfinite(ask)):
            return
        mid = (bid + ask) / 2
        if not .1 < mid < .9 or (np.isfinite(lag) and lag > MAX_LAG):
            return
        fs = features(P)
        for r in live:
            x = fs[r["feature"]][0, i]
            if not np.isfinite(x) or abs(x) < r["threshold"]:
                continue
            if s - T > LAST_ENTRY or (r["hold"] and s - T + r["hold"] > LAST_ENTRY):
                continue
            up = (x > 0) == (r["mode"] == "跟随")
            self.send_entry(r, T, i, s, "up" if up else "down", ask if up else 1 - bid)

    def blocked(self, r, T, s):
        st = self.state[r["rule"]]
        return st["pos"] is not None or (st["busy_T"] == T and s < st["busy_until"])

    # ---- orders
    def send_entry(self, r, T, i, s, side, quote):
        at = s * 1000 + NET_MS + TAKER_DELAY_MS
        pos = dict(rule=r["rule"], probe=bool(r.get("probe")), T=T, i=i, side=side, sent=s, quote=round(float(quote), 4), limit=round(float(quote) + MAX_SLIP, 4),
                   want=self.size, shares=0.0, open=0.0, cost=0.0, fees=0.0, proceeds=0.0, exit_at=None, exiting=False, status="发出")
        self.state[r["rule"]]["pos"] = pos
        self.pending.append(dict(kind="entry", pos=pos, r=r, T=T, at=at))

    def send_exit(self, pos, s):
        pos["exiting"] = True
        self.pending.append(dict(kind="exit", pos=pos, T=pos["T"], at=s * 1000 + NET_MS + TAKER_DELAY_MS))

    def execute_due(self, ts, T):
        for o in [o for o in self.pending if o["T"] == T and o["at"] <= ts]:
            self.fill(o)

    def fill(self, o):
        self.pending.remove(o)
        pos, w = o["pos"], self.win.get(o["T"])
        if o["kind"] == "entry":
            r = o["r"]
            st = self.state[r["rule"]]
            if o.get("void") or not w or w.book_ts is None:
                pos["status"] = "作废" if o.get("void") else "盘口未知"
                self.finish(st, pos)
                return
            up = pos["side"] == "up"
            levels = sorted(w.asks.items()) if up else sorted(w.bids.items(), reverse=True)
            got, cash, paid = walk(levels, pos["want"], (lambda p: p) if up else (lambda p: 1 - p), pos["limit"])
            if got <= 0:
                pos["status"] = "未成交"
                self.finish(st, pos)
                return
            pos.update(shares=round(got, 4), open=got, cost=cash, fees=paid, status="持仓")
            s = o["at"] // 1000
            st["busy_T"] = pos["T"]
            st["busy_until"] = s + (r["hold"] or 10 ** 9)
            if r["hold"]:
                pos["exit_at"] = pos["sent"] + r["hold"]
            return
        pos["exiting"] = False
        if o.get("void") or not w or w.book_ts is None:
            pos["exit_at"] = o["at"] // 1000 + 1      # we can't tell what happened: try again
            return
        up = pos["side"] == "up"                     # selling Up hits bids; selling Down equals buying Up from asks
        levels = sorted(w.bids.items(), reverse=True) if up else sorted(w.asks.items())
        got, cash, paid = walk(levels, pos["open"], (lambda p: p) if up else (lambda p: 1 - p), None)
        pos["open"] -= got
        pos["proceeds"] += cash
        pos["fees"] += paid
        if pos["open"] <= 1e-9:
            pos["open"] = 0.0
            pos["status"] = "已平"
            self.finish(self.state[pos["rule"]], pos)
        else:
            pos["exit_at"] = o["at"] // 1000 + 1

    def close(self, r, st):
        pos = st["pos"]
        self.pending = [o for o in self.pending if o["pos"] is not pos]
        pos["status"] = "到结算"
        self.finish(st, pos)

    def finish(self, st, pos):
        st["pos"] = None
        pos.pop("exiting")
        pos.pop("exit_at")
        for k in ("cost", "fees", "proceeds", "open"):
            pos[k] = round(pos[k], 6)
        self.rows.append(pos)

    def flush(self):
        """End of the input: what is still open waits for settlement."""
        for r in self.rules:
            st = self.state[r["rule"]]
            if st["pos"]:
                self.close(r, st)


def net_cents(row, up):
    """Net cents per share actually bought, or None while a share still open has no known outcome."""
    if not row["shares"]:
        return None
    pay = 0.0
    if row["open"] > 0:
        if up is None:
            return None
        pay = row["open"] * (up if row["side"] == "up" else 1 - up)
    return (row["proceeds"] + pay - row["cost"] - row["fees"]) / row["shares"] * 100


def lines(path):
    try:
        with (gzip.open(path, "rt") if str(path).endswith(".gz") else open(path)) as h:
            for raw in h:
                try:
                    yield json.loads(raw)
                except ValueError:
                    continue
    except (EOFError, OSError, zlib.error) as e:
        print("truncated", path, repr(e), flush=True)


def replay(key, rules, tok):
    s = Shadow(rules, tok)
    for r in heapq.merge(lines(ROOT / f"data/live/{key}.jsonl.gz"), lines(ROOT / f"data/live_pc/{key}.jsonl.gz"), key=lambda r: r["rx"]):
        s.feed(r)
    s.flush()
    return s


def registry():
    """Every rule ever carried to the hold-out, frozen as first seen, so a later re-fit cannot rewrite history."""
    path = OUT / "rules.json"
    reg = json.load(open(path)) if path.exists() else {}
    for p in PROBES:
        reg.setdefault(p["rule"], p)
    mined = ROOT / "out/rules.json"
    active = []
    if mined.exists():
        for c in json.load(open(mined)).get("candidates", []):
            active.append(c["rule"])
            reg.setdefault(c["rule"], dict(rule=c["rule"], feature=c["feature"], threshold=c["threshold"], part=c["part"], hold=c["hold"],
                                           mode=c["mode"], probe=False, registered_at=int(time.time())))
    tmp = OUT / "rules.part"
    json.dump(reg, open(tmp, "w"), ensure_ascii=False, indent=1)
    os.replace(tmp, path)
    return reg, active


def backtest(reg, windows):
    """The same rules priced by mine_rules on the same windows: the gap to the shadow is the cost of real execution."""
    P = load()
    if not len(P.get("T", [])):
        return {}
    keep = np.isin(P["T"], list(windows))
    if not keep.any():
        return {}
    Q = {k: v[keep] for k, v in P.items()}
    mid = (Q["bid"] + Q["ask"]) / 2
    tables, fs, out = pnl_tables(Q), clean_features(Q), {}
    for name, r in reg.items():
        registered = Q["T"] >= r["registered_at"]
        if not registered.any():
            continue
        t, n = run_rule(fs[r["feature"]][registered], r["threshold"], r["mode"], r["part"], r["hold"], mid[registered], Q["lag"][registered],
                        {h: (a[registered], b[registered]) for h, (a, b) in tables.items()})
        out[name] = dict(trades=n, cents=round(float(t.sum()) / n * 100, 3) if n else None)
    return out


def summarize(reg, active):
    rows = []
    done = sorted(glob.glob(str(OUT / "trades_*.jsonl")))
    for p in done:
        rows += [json.loads(x) for x in open(p)]
    outcome = {}
    secs = [pd.read_parquet(p, columns=["s", "twap"]) for p in glob.glob(str(ROOT / "data/panel/sec_*.parquet"))]
    twap = pd.concat(secs).dropna(subset=["twap"]).groupby("s")["twap"].last().to_dict() if secs else {}
    for T in {r["T"] for r in rows}:
        if T in twap and T + 300 in twap:
            outcome[T] = float(twap[T + 300] >= twap[T])
    bt = backtest(reg, {r["T"] for r in rows})
    out = []
    for name, r in reg.items():
        mine = [x for x in rows if x["rule"] == name]
        filled = [x for x in mine if x["shares"]]
        nets = [v for v in (net_cents(x, outcome.get(x["T"])) for x in filled) if v is not None]
        mean = float(np.mean(nets)) if nets else None
        se = float(np.std(nets, ddof=1) / math.sqrt(len(nets))) if len(nets) > 1 else None
        slip = [(x["cost"] / x["shares"] - x["quote"]) * 100 for x in filled]
        want = sum(x["want"] for x in mine)
        count = lambda s: sum(x["status"] == s for x in mine)
        out.append(dict(rule=name, probe=bool(r.get("probe")), active=bool(r.get("probe")) or name in active, registered_at=r["registered_at"], orders=len(mine),
                        filled=len(filled), unfilled=count("未成交"), void=count("作废"), no_book=count("盘口未知"), settled_pending=len(filled) - len(nets),
                        fill_rate=round(sum(x["shares"] for x in mine) / want, 3) if want else None,
                        net_cents=round(mean, 3) if mean is not None else None, ci95=round(1.96 * se, 3) if se is not None else None,
                        entry_slip_cents=round(float(np.mean(slip)), 3) if slip else None, backtest=bt.get(name)))
    res = dict(generated=int(time.time()), hours=[Path(p).stem.split("_", 1)[1] for p in done], assumed_ms=NET_MS + TAKER_DELAY_MS, size=SIZE, rules=out)
    tmp = OUT / "summary.part"
    json.dump(res, open(tmp, "w"), ensure_ascii=False, indent=1)
    os.replace(tmp, OUT / "summary.json")
    return res


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    instance = open(OUT / ".lock", "w")
    try:
        fcntl.flock(instance, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("shadow.py is already running")
    tok_path = ROOT / "data/panel/tokens.json"
    if not tok_path.exists():              # normally just refreshed by mine_rules, which runs right before this
        build()
    tok = {k: tuple(v) for k, v in json.load(open(tok_path)).items()}
    reg, active = registry()
    rules = list(reg.values())             # a rule that later drops out keeps being judged, or only the survivors would count
    for p in sorted(glob.glob(str(ROOT / "data/live_pc/*.jsonl.gz"))):
        key = Path(p).name.split(".")[0]
        dest = OUT / f"trades_{key}.jsonl"
        if dest.exists() or not (ROOT / f"data/live/{key}.jsonl.gz").exists():
            continue
        t0 = time.time()
        s = replay(key, rules, tok)
        tmp = dest.with_suffix(".part")
        with open(tmp, "w") as f:
            for row in s.rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp, dest)
        print(key, "orders", len(s.rows), "seconds", round(time.time() - t0), flush=True)
    res = summarize(reg, active)
    print(f"假设下单到撮合 {res['assumed_ms']} ms,每单 {res['size']} 股,已回放 {len(res['hours'])} 小时")
    for r in res["rules"]:
        bt = r["backtest"] or {}
        print(r["rule"], "| 发单", r["orders"], "成交", r["filled"], "未成交", r["unfilled"], "作废", r["void"], "盘口未知", r["no_book"], "待结算", r["settled_pending"],
              "| 成交率", r["fill_rate"], "| 净¢/股", r["net_cents"], "±", r["ci95"], "| 入场滑点¢", r["entry_slip_cents"], "| 回测", bt.get("trades"), "笔", bt.get("cents"), "¢")


if __name__ == "__main__":
    main()
