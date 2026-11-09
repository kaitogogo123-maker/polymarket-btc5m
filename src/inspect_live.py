"""Quality report for data/live: coverage, gaps, errors, spreads, Chainlink-vs-Binance lag."""
import glob, gzip, json, collections
from pathlib import Path
import numpy as np, pandas as pd

ROOT = Path(__file__).resolve().parents[1]
counts = collections.Counter()
errs = collections.Counter()
rx = {"pm": [], "cl": [], "bn": []}
token_t, up_tokens, bba, cl, twap, bn = {}, set(), [], {}, {}, []
files = sorted(glob.glob(str(ROOT / "data/live/*.jsonl*")))
for f in files:
    fh = gzip.open(f, "rt") if f.endswith(".gz") else open(f)
    with fh:
        for line in fh:
            try:
                d = json.loads(line)
            except ValueError:
                counts["bad_line"] += 1
                continue
            s, m, r = d["src"], d["m"], d["rx"]
            if s == "err":
                errs[(m["src"], m["e"][:70])] += 1
                continue
            if s == "meta":
                up_tokens.add(m["tokens"][0])
                token_t[m["tokens"][0]] = m["t"]
                continue
            rx[s].append(r // 1000)
            if s == "pm":
                k = m.get("event_type")
                counts["pm:" + str(k)] += 1
                if k == "best_bid_ask" and m["asset_id"] in up_tokens:
                    bba.append((r, token_t[m["asset_id"]], float(m["best_bid"]), float(m["best_ask"])))
            elif s == "cl":
                k = m.get("topic")
                counts["cl:" + str(k)] += 1
                p = m.get("payload") or {}
                if "value" in p:
                    (twap if "twap" in k else cl)[p["timestamp"] // 1000] = (p["value"], r)
            else:
                counts["bn:" + m["k"]] += 1
                if m["k"] == "q":
                    bn.append((r, (float(m["b"]) + float(m["a"])) / 2))

print("files", len(files), "| start", pd.to_datetime(min(rx["bn"]), unit="s"), "| end", pd.to_datetime(max(rx["bn"]), unit="s"))
print("\nmessages"); [print(f"  {k:28s}{v:>12,}") for k, v in sorted(counts.items())]
print("\nerrors", dict(errs) or "none")
print("\nlongest gaps (s), top 3 per stream")
for s, v in rx.items():
    a = np.unique(v); g = np.diff(a); o = np.argsort(g)[-3:][::-1]
    span = a[-1] - a[0] + 1
    print(f"  {s}: seconds covered {len(a)/span:.2%}; " + ", ".join(f"{g[i]}s @ {pd.to_datetime(a[i], unit='s')}" for i in o))

b = pd.DataFrame(bba, columns=["rx", "t", "bid", "ask"])
b["r"] = b.t + 300 - b.rx // 1000
live = b[(b.r > 0) & (b.r <= 300)]
print(f"\nwindows with Up quotes in-window: {live.t.nunique()} / meta {len(token_t)}")
live = live.assign(spread=live.ask - live.bid, bucket=pd.cut(live.r, [0, 10, 30, 60, 120, 240, 300]))
print(live.groupby("bucket", observed=True).spread.describe(percentiles=[.5, .9])[["count", "mean", "50%", "90%"]].round(4))

# Chainlink vs Binance lead/lag on a 1 Hz grid, returns correlation
c = pd.Series({k: v[0] for k, v in cl.items()}).sort_index()
q = pd.DataFrame(bn, columns=["rx", "mid"]); q["s"] = q.rx // 1000
bs = q.groupby("s").mid.last()
idx = np.arange(max(c.index[0], bs.index[0]), min(c.index[-1], bs.index[-1]))
C, B = c.reindex(idx).ffill(), bs.reindex(idx).ffill()
dc, db = C.diff(5), B.diff(5)
print(f"\nChainlink samples {len(c):,}; TWAP samples {len(twap):,}; level diff Binance-Chainlink mean ${(B-C).mean():.2f} sd ${(B-C).std():.2f}")
print("corr(Chainlink 5s return at t, Binance 5s return at t-lag)")
print("  " + "  ".join(f"{l:+d}s:{dc.corr(db.shift(l)):.3f}" for l in (-2, -1, 0, 1, 2, 3, 4, 6, 8)))
arr = np.array([v[1] / 1000 - k for k, v in cl.items()])
print(f"Chainlink arrival minus stamp: median {np.median(arr):.2f}s  p90 {np.percentile(arr, 90):.2f}s")
