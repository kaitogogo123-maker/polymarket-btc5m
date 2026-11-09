"""Step 2 (the gate): does the model add information beyond the Polymarket price?

Market price m = last taker trade (Up-equivalent, size>=5) in the 20 s before decision time.
Model is fitted only on labels before 2026-10-01; per-r Platt calibration also pre-October.
Test: logit P(y) = a + b*logit(m) + c*(logit p - logit m). No edge unless c > 0 significantly.
"""
import json, sys, datetime as D
import numpy as np, pandas as pd
from scipy import optimize, stats
from model import ROOT, Prices, R_GRID, state, prob, logloss

SPLIT = int(D.datetime(2026, 10, 1, tzinfo=D.timezone.utc).timestamp())
FEE = lambda p: 0.07 * p * (1 - p)          # USDC per share, taker
MKT_WIN, LAT = (int(x) for x in (sys.argv[1:3] or (20, 0)))
SLIP = 0.005                                 # half-spread proxy, USDC per share
lg = lambda p: np.log(p / (1 - p))


def logit_fit(X, y):
    X = np.column_stack([np.ones(len(y)), X])
    f = lambda b: -np.sum(y * (X @ b) - np.logaddexp(0, X @ b))
    b = optimize.minimize(f, np.zeros(X.shape[1]), method="BFGS").x
    p = 1 / (1 + np.exp(-(X @ b)))
    H = X.T @ (X * (p * (1 - p))[:, None])
    return b, np.sqrt(np.diag(np.linalg.inv(H)))


px = Prices()
par = json.load(open(ROOT / "out/baseline_params.json"))
lab = pd.read_parquet(ROOT / "data/raw/labels.parquet").dropna(subset=["price_to_beat"])
lab = lab[(lab.t - 400 > px.t0) & (lab.t + 400 < px.t0 + px.n)].reset_index(drop=True)
lab["up"] = lab.up.astype(int)

# per-r Platt calibration on pre-October labels
cal = {}
for r in R_GRID:
    m = lab.t < SPLIT
    p = prob(state(px, lab[m], r, par["lo"], par["hi"]), **par["theta"])
    cal[r], _ = logit_fit(lg(p), lab.up[m].to_numpy())

json.dump({int(r): [float(v) for v in cal[r]] for r in R_GRID}, open(ROOT / "out/platt.json", "w"))

tr = pd.read_parquet(ROOT / "data/raw/trades.parquet")
tr = tr[(tr.taker == 1) & (tr["size"] >= 5)]
tr["pu"] = np.where(tr.outcome == 0, tr.price, 1 - tr.price)
tr = tr.sort_values("ts")
mw = lab[lab.t.isin(tr.t.unique())].reset_index(drop=True)
print(f"market windows with trades+labels: {len(mw)} ({mw.t.min()}..{mw.t.max()}), "
      f"Oct+ {int((mw.t >= SPLIT).sum())}")

g = {t: d for t, d in tr.groupby("t")}
out, sims = [], []
for r in R_GRID:
    st = state(px, mw, r, par["lo"], par["hi"], lat=LAT)
    a, b = cal[r]
    p = 1 / (1 + np.exp(-(a + b * lg(prob(st, **par["theta"])))))
    m = np.full(len(mw), np.nan)
    for i, t in enumerate(mw.t):
        d = g[t]; dt = t + 300 - r
        x = d.pu[(d.ts <= dt) & (d.ts > dt - MKT_WIN)]
        if len(x):
            m[i] = x.iloc[-1]
    ok = ~np.isnan(m) & (m > 0.005) & (m < 0.995)
    for name, sel in [("all", ok), ("oct", ok & (mw.t >= SPLIT).to_numpy())]:
        y = mw.up.to_numpy()[sel]; pm, pp = m[sel], p[sel]
        coef, se = logit_fit(np.column_stack([lg(pm), lg(pp) - lg(pm)]), y)
        out.append(dict(sample=name, r=r, n=int(sel.sum()), ll_mkt=logloss(y, pm), ll_model=logloss(y, pp),
                        brier_mkt=np.mean((pm - y) ** 2), brier_model=np.mean((pp - y) ** 2),
                        b=coef[1], c=coef[2], c_z=coef[2] / se[2]))
        if name == "oct":
            for thr in (0.02, 0.05, 0.10):
                buy_up, buy_dn = pp - pm > thr, pm - pp > thr
                cost = np.where(buy_up, pm, 1 - pm) + SLIP
                pay = np.where(buy_up, y, 1 - y)
                pnl = (pay - cost - FEE(cost)) / cost        # return per USDC staked
                k = buy_up | buy_dn
                if k.sum() < 20:
                    continue
                bs = [np.mean(np.random.default_rng(s).choice(pnl[k], k.sum())) for s in range(1000)]
                sims.append(dict(r=r, thr=thr, trades=int(k.sum()), ret=float(pnl[k].mean()),
                                 lo95=float(np.percentile(bs, 2.5)), hi95=float(np.percentile(bs, 97.5))))

res = pd.DataFrame(out)
ff = lambda v: f"{v:.4f}"
print("\nMODEL vs MARKET  (c_z>2 needed for real information)")
print(res.to_string(index=False, float_format=ff))
print("\nOCT TAKER SIM: return per USDC after fee + 0.5c slippage (95% bootstrap CI)")
print(pd.DataFrame(sims).to_string(index=False, float_format=ff))
res.to_csv(ROOT / f"out/model_vs_market_w{MKT_WIN}_lat{LAT}.csv", index=False)
pd.DataFrame(sims).to_csv(ROOT / f"out/taker_sim_oct_w{MKT_WIN}_lat{LAT}.csv", index=False)
