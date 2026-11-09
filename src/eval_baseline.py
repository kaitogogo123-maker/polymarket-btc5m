"""Step 1: check TWAP alignment, fit baseline on Aug14-Sep30, evaluate out-of-sample Oct1-Nov3."""
import json, datetime as D
import numpy as np, pandas as pd
from model import ROOT, Prices, R_GRID, state, prob, naive_prob, logloss, slope, fit

SPLIT = int(D.datetime(2026, 10, 1, tzinfo=D.timezone.utc).timestamp())

px = Prices()
lab = pd.read_parquet(ROOT / "data/raw/labels.parquet").dropna(subset=["price_to_beat", "final_price"])
lab = lab[(lab.t - 400 > px.t0) & (lab.t + 400 < px.t0 + px.n)].reset_index(drop=True)
y = lab.up.to_numpy(int)
print(f"windows {len(lab)}  Up rate {y.mean():.4f}")

# 1. Which Binance averaging window best tracks the official TWAP? (basis drift over 5 min)
print("\nTWAP alignment: std of basis drift (final-B(E)) - (K-B(T)), USD")
best = None
for lo, hi in [(60, 1), (61, 2), (62, 3), (63, 4), (65, 6), (59, 0), (30, 1), (1, 1)]:
    T = lab.t.to_numpy(np.int64)
    drift = (lab.final_price - px.avg(T + 300, lo, hi)) - (lab.price_to_beat - px.avg(T, lo, hi))
    sd = float(np.std(drift))
    mis = float(np.mean(((px.avg(T + 300, lo, hi) >= px.avg(T, lo, hi)).astype(int)) != y))
    print(f"  [t-{lo}, t-{hi}]  drift sd {sd:7.3f}   label mismatch using Binance proxy {mis:.4f}")
    if best is None or sd < best[0]:
        best = (sd, lo, hi)
_, LO, HI = best
print(f"chosen window [t-{LO}, t-{HI}]")

tr, te = lab.t < SPLIT, lab.t >= SPLIT
S = {r: state(px, lab, r, LO, HI) for r in R_GRID}
sub = lambda s, m: {k: (v[m] if isinstance(v, np.ndarray) else v) for k, v in s.items()}
theta = fit([sub(S[r], tr.to_numpy()) for r in R_GRID], [y[tr] for _ in R_GRID])
print("\nfitted on train:", theta)

rows = []
for r in R_GRID:
    m = te.to_numpy()
    p = prob(sub(S[r], m), **theta)
    pn = naive_prob(px, lab[m], r)
    rows.append(dict(r=r, n=int(m.sum()), ll_model=logloss(y[m], p), ll_naive=logloss(y[m], pn),
                     brier_model=float(np.mean((p - y[m]) ** 2)), slope_model=slope(y[m], p),
                     slope_naive=slope(y[m], pn)))
res = pd.DataFrame(rows)
print("\nOUT-OF-SAMPLE (2026-10-01 .. 11-03)")
print(res.to_string(index=False, float_format=lambda v: f"{v:.4f}"))
json.dump(dict(theta=theta, lo=LO, hi=HI), open(ROOT / "out/baseline_params.json", "w"), indent=1)
res.to_csv(ROOT / "out/baseline_oos.csv", index=False)
