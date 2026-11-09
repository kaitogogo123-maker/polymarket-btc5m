"""Per-second order-book panel built from data/live (Up token, second = exchange timestamp).

bid/ask come from best_bid_ask, the only event that fires on every top-of-book change. `book` fires after
trades, so it supplies depth and the slippage of walking SIZE shares, and those go stale quickly.
`lag` is receive time minus exchange time: a signal could not have been seen before it arrived.

Finished hours are cached in data/panel; the current hour is reparsed on each run.
Run: python src/book_panel.py   prints coverage.
"""
import glob, gzip, json, os, zlib
from pathlib import Path
import numpy as np, pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LIVE, PANEL = ROOT / "data/live", ROOT / "data/panel"
SIZE = 100          # shares used to price entries and exits by walking the book
CLEAN_FROM = 1793784900  # 2026-11-04 09:35 UTC: earlier windows miss about 13 s at their start
REF_AGE = 5
DEPTH_AGE = 5           # `book` only arrives after trades: depth older than this says nothing about now
QUOTE = ["bid", "ask", "lag"]
DEPTH = ["bd1", "ad1", "bd3", "ad3", "bd10", "ad10", "buy_slip", "sell_slip"]
BOOK = QUOTE + DEPTH
FLOW = ["up_buy", "up_sell", "down_buy", "down_sell"]


def walk(levels, size, worst):
    """Average price to trade `size` shares through levels ordered best first; what the book can't fill pays `worst`."""
    left, cost = size, 0.0
    for p, s in levels:
        take = min(left, s)
        cost += take * p
        left -= take
        if left <= 0:
            return cost / size
    return (cost + left * worst) / size


def book_row(bids, asks):
    b = sorted(((float(x["price"]), float(x["size"])) for x in bids), reverse=True)
    a = sorted((float(x["price"]), float(x["size"])) for x in asks)
    if not b or not a:
        return None
    within = lambda side, best, w: sum(s for p, s in side if abs(p - best) < w / 100 + 1e-9)
    return [within(b, b[0][0], 1), within(a, a[0][0], 1), within(b, b[0][0], 3), within(a, a[0][0], 3),
            within(b, b[0][0], 10), within(a, a[0][0], 10), walk(a, SIZE, 1.0) - a[0][0], b[0][0] - walk(b, SIZE, 0.0)]


def parse(path, tok):
    """One hour file -> (book rows keyed by window and second, per-second reference prices)."""
    books, quotes, flows, secs, seen, gaps, lags = {}, {}, {}, {}, set(), set(), {}
    def late(T, ts, rx):
        key = (T, ts // 1000 - T)
        lags[key] = max(lags.get(key, 0.0), (rx - ts) / 1000)
    with (gzip.open(path, "rt") if str(path).endswith(".gz") else open(path)) as h:
        try:
            for raw in h:
                try:
                    r = json.loads(raw)
                    src, m = r["src"], r["m"]
                except (ValueError, KeyError, TypeError):
                    continue
                if src == "meta":
                    up, down = m["tokens"] if json.loads(m["outcomes"])[0] == "Up" else m["tokens"][::-1]
                    tok[up], tok[down] = (m["t"], "up"), (m["t"], "down")
                elif src == "bn" and m.get("k") == "t":
                    secs.setdefault(m["T"] // 1000, {})["bn"] = float(m["p"])
                elif src == "cl":
                    p, topic = m.get("payload") or {}, m.get("topic") or ""
                    for x in p.get("data") or [p]:
                        if "value" in x and "timestamp" in x:
                            # TWAP settles on its own stamp; spot is a signal, usable only once it has arrived (~1.5 s later).
                            s, key = (x["timestamp"] // 1000, "twap") if "twap" in topic else (r["rx"] // 1000, "spot")
                            secs.setdefault(s, {})[key] = x["value"]
                elif src == "err" and m.get("src") == "pm" and "t" in m:
                    gaps.add((m["t"], r["rx"] // 1000 - m["t"]))
                elif src == "pm":
                    who, e = tok.get(m.get("asset_id")), m.get("event_type")
                    if not who:
                        continue
                    T, side = who
                    if e == "book" and side == "up":
                        ts = int(m["timestamp"])
                        late(T, ts, r["rx"])
                        key = (T, ts // 1000 - T)
                        if key not in books or ts >= books[key][0]:
                            books[key] = (ts, m["bids"], m["asks"])
                    elif e == "best_bid_ask" and side == "up":
                        ts = int(m["timestamp"])
                        late(T, ts, r["rx"])
                        key = (T, ts // 1000 - T)
                        if key not in quotes or ts >= quotes[key][0]:
                            quotes[key] = (ts, float(m["best_bid"]), float(m["best_ask"]))
                    elif e == "last_trade_price":
                        d = (m.get("transaction_hash"), m["asset_id"], m["price"], m["size"], m["side"], m["timestamp"])
                        if d in seen:              # reconnects and overlapping connections repeat events
                            continue
                        seen.add(d)
                        late(T, int(m["timestamp"]), r["rx"])
                        f = flows.setdefault((T, int(m["timestamp"]) // 1000 - T), dict.fromkeys(FLOW, 0.0))
                        f[f"{side}_{'buy' if m['side'] == 'BUY' else 'sell'}"] += float(m["size"])
        except (EOFError, OSError, zlib.error) as e:
            print("truncated", path, repr(e), flush=True)
    rows = []
    for (T, i) in set(books) | set(quotes) | set(flows) | gaps:
        if not 0 <= i <= 300:
            continue
        b = book_row(*books[(T, i)][1:]) if (T, i) in books else None
        f = flows.get((T, i), dict.fromkeys(FLOW, 0.0))
        q = [*(quotes[(T, i)][1:] if (T, i) in quotes else (np.nan, np.nan)), lags.get((T, i), np.nan)]
        rows.append([T, i, (T, i) in gaps, *q] + (b or [np.nan] * len(DEPTH)) + [f[k] for k in FLOW])
    book = pd.DataFrame(rows, columns=["T", "i", "gap"] + BOOK + FLOW)
    sec = pd.DataFrame([(s, v.get("bn", np.nan), v.get("spot", np.nan), v.get("twap", np.nan)) for s, v in secs.items()],
                       columns=["s", "bn", "spot", "twap"])
    return book, sec


def hours():
    """Chronological source files; a finished .gz wins over a leftover plain file of the same hour."""
    gz = sorted(glob.glob(str(LIVE / "*.jsonl.gz")))
    done = {Path(p).name[:-3] for p in gz}
    plain = [p for p in sorted(glob.glob(str(LIVE / "*.jsonl"))) if Path(p).name not in done]
    return sorted(gz + plain, key=lambda p: Path(p).name)


def save_tokens(tok, path):
    tmp = path.with_suffix(".part")
    with open(tmp, "w") as f:
        json.dump(tok, f)
    os.replace(tmp, path)


def build():
    """Parse what is not cached. Tokens are kept on disk because a window's meta can sit in the previous hour."""
    PANEL.mkdir(exist_ok=True)
    tok_path = PANEL / "tokens.json"
    tok = {k: tuple(v) for k, v in json.load(open(tok_path)).items()} if tok_path.exists() else {}
    books, secs = [], []
    for p in hours():
        key = Path(p).name.split(".")[0]
        bp, sp = PANEL / f"book_{key}.parquet", PANEL / f"sec_{key}.parquet"
        if p.endswith(".gz") and bp.exists() and sp.exists() and os.path.getmtime(bp) >= os.path.getmtime(p):
            books.append(pd.read_parquet(bp)); secs.append(pd.read_parquet(sp))
            continue
        try:
            b, s = parse(p, tok)
        except FileNotFoundError:                  # compressed since hours() listed it; the next run reads the .gz
            continue
        books.append(b); secs.append(s)
        save_tokens(tok, tok_path)                 # before the cache: a cached hour is never reparsed for its metas
        if p.endswith(".gz"):
            for frame, path in ((b, bp), (s, sp)):
                part = path.with_suffix(".part")
                frame.to_parquet(part)
                os.replace(part, path)
    return pd.concat(books, ignore_index=True), pd.concat(secs, ignore_index=True)


def blanks(a, gap):
    """Seconds with no quote we can trust: from the last event before a lost connection to the first after it."""
    out = np.zeros(a.shape, bool)
    for w, g in zip(*np.nonzero(gap)):
        seen = np.nonzero(np.isfinite(a[w]))[0]
        p, n = seen[seen <= g], seen[seen > g]
        out[w, (p[-1] + 1 if len(p) else 0):(n[0] if len(n) else 301)] = True
    return out


def carry(a, bad=None, limit=None):
    """The feed only speaks when the book changes (a decided market can be silent for 25 s), so silence means
    unchanged; only seconds marked bad stay missing. `limit` caps the carry for values that age."""
    out = a.copy()
    age = np.zeros(a.shape[0], int)
    for i in range(1, a.shape[1]):
        have = np.isfinite(a[:, i])
        age = np.where(have, 0, age + 1)
        use = ~have & (True if bad is None else ~bad[:, i]) & (True if limit is None else age <= limit)
        out[use, i] = out[use, i - 1]
    return out


def load(now=None):
    """Complete windows as arrays of shape (windows, 301): quotes, depth, flow, reference prices and the outcome."""
    book, sec = build()
    now = now or pd.Timestamp.now(tz="UTC").timestamp()
    book = book[(book["T"] + 305 < now) & (book["T"] >= CLEAN_FROM)]
    sec = sec.groupby("s").last()
    good = []
    for T, g in book.groupby("T"):
        q = g.dropna(subset=["bid", "ask"]).sort_values("i")
        if not len(q) or q["i"].iloc[0] > 5:
            continue
        end_mid = (q["bid"].iloc[-1] + q["ask"].iloc[-1]) / 2
        if q["i"].iloc[-1] >= 295 or not 0.05 < end_mid < 0.95:    # a decided market goes quiet early
            good.append(T)
    W = np.array(sorted(good), dtype=int)
    out = dict(T=W)
    if not len(W):
        return out
    pos = {t: n for n, t in enumerate(W)}
    book = book[book["T"].isin(pos)].groupby(["T", "i"], as_index=False).agg(
        {"gap": "max", **{k: "last" for k in BOOK}, **{k: "sum" for k in FLOW}})
    wi, si = book["T"].map(pos).to_numpy(), book["i"].to_numpy()
    S = W[:, None] + np.arange(301)
    raw_bn = sec["bn"].reindex(S.ravel()).to_numpy().reshape(S.shape)
    gap = np.zeros(S.shape, bool)
    gap[wi, si] = book["gap"].to_numpy(bool)
    silent = np.isfinite(raw_bn).astype(int)       # no Binance trade for 5 s means the collector itself was down
    for r in range(len(W)):
        run = 0
        for i in range(301):
            run = 0 if silent[r, i] else run + 1
            if run == 5:
                gap[r, i] = True
    raw_bid = np.full(S.shape, np.nan)
    raw_bid[wi, si] = book["bid"].to_numpy()
    bad = blanks(raw_bid, gap)
    for k in BOOK + FLOW:
        a = np.full(S.shape, np.nan if k in BOOK else 0.0)
        a[wi, si] = book[k].to_numpy()
        out[k] = a if k in FLOW else carry(a, bad, DEPTH_AGE if k in DEPTH else None)
    out["lag"] = np.nan_to_num(out["lag"])
    slip = lambda k: carry(out[k], bad)            # the cost of size changes slowly; keep the last known value
    out["buy_px"], out["sell_px"] = out["ask"] + slip("buy_slip"), out["bid"] - slip("sell_slip")
    # These tick every second or so, so silence is an outage: go missing rather than fake a jump on recovery.
    out["bn"] = carry(raw_bn, limit=REF_AGE)
    out["spot"] = carry(sec["spot"].reindex(S.ravel()).to_numpy().reshape(S.shape), limit=REF_AGE)
    out["lost"] = bad
    tw = sec["twap"]
    a, b = tw.reindex(W).to_numpy(), tw.reindex(W + 300).to_numpy()
    out["up"] = np.where(np.isfinite(a) & np.isfinite(b), (b >= a).astype(float), np.nan)
    return out


if __name__ == "__main__":
    P = load()
    n = len(P["T"])
    print("complete windows", n)
    if n:
        print("window range UTC", pd.Timestamp(P["T"][0], unit="s"), pd.Timestamp(P["T"][-1], unit="s"))
        print("quote available", round(float(np.isfinite(P["bid"]).mean()), 4), "lost seconds", int(P["lost"].sum()), "walk-100 available", round(float(np.isfinite(P["buy_px"]).mean()), 4),
              "lag>2s seconds", round(float((P["lag"] > 2).mean()), 4))
        print("outcome known", int(np.isfinite(P["up"]).sum()), "up share", round(float(np.nanmean(P["up"])), 3))
