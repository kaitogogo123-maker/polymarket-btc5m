"""TWAP-aware baseline for Polymarket BTC 5m Up/Down.

Settlement (official, windows from 2026-08-14): Up iff TWAP60(T+300) >= TWAP60(T),
both from the Chainlink BTC/USD 60s TWAP stream. We approximate Chainlink with Binance
1s closes averaged over [t-LO, t-HI] and remove the level basis using the official
price_to_beat, which is public at T.
"""
from pathlib import Path
import numpy as np, pandas as pd
from scipy import stats, optimize

ROOT = Path(__file__).resolve().parents[1]
W = 300
R_GRID = np.array([285, 270, 240, 210, 180, 150, 120, 90, 60, 45, 30, 20, 10, 5])


class Prices:
    """Contiguous per-second Binance close array (forward-filled)."""

    def __init__(self):
        df = pd.concat([pd.read_parquet(f) for f in sorted((ROOT / "data/binance").glob("*.parquet"))])
        df = df.drop_duplicates("ts").set_index("ts").sort_index()
        self.t0 = int(df.index[0])
        idx = np.arange(self.t0, int(df.index[-1]) + 1)
        d = df.reindex(idx).ffill()
        self.p = d.close.to_numpy()
        self.buy = d.taker_buy_vol.fillna(0).to_numpy()
        self.vol = d.vol.fillna(0).to_numpy()
        self.cs = np.concatenate([[0], np.cumsum(self.p)])
        self.n = len(self.p)

    def i(self, t):
        return np.asarray(t, dtype=np.int64) - self.t0

    def avg(self, t, lo, hi):
        """mean close over seconds [t-lo, t-hi] inclusive."""
        a, b = self.i(t) - lo, self.i(t) - hi + 1
        return (self.cs[b] - self.cs[a]) / (b - a)

    def sigma2(self, t, halflives=(900, 3600), step=30):
        """Per-second variance of log price from EWMA of 30s returns, known before t."""
        if not hasattr(self, "_s2"):
            lp = np.log(self.p[::step])
            r2 = np.concatenate([[np.nan], np.diff(lp) ** 2]) / step
            s = pd.Series(r2)
            self._s2 = np.mean([s.ewm(halflife=h / step, min_periods=20).mean().to_numpy() for h in halflives], axis=0)
        k = (self.i(t) - 1) // step  # last grid point strictly before t
        return self._s2[k]


def sum_min(a, m):
    """sum_{i,j} min(u_i,u_j) for u = a+1..a+m (vectorised over arrays a, m)."""
    a = np.asarray(a, float); m = np.asarray(m, float)
    # sum_k (a+k)(2(m-k)+1), k=1..m
    s1 = m * (m + 1) / 2; s2 = m * (m + 1) * (2 * m + 1) / 6
    return (2 * m + 1) * (a * m + s1) - 2 * (a * s1 + s2)


def state(px, lab, r, lo=62, hi=3, lat=0):
    """Mean/variance of Binance proxy TWAP at T+300 given info at t = T+300-r-lat."""
    T = lab.t.to_numpy(np.int64)
    E = T + W
    t = E - r - lat
    St = px.p[px.i(t)]
    first, last = E - lo, E - hi  # window seconds
    n = lo - hi + 1
    known_hi = np.minimum(t, last)
    k = np.clip(known_hi - first + 1, 0, n)                       # known seconds
    i0 = np.minimum(px.i(first), px.n)                            # live view may end before the window starts
    known_sum = np.where(k > 0, px.cs[np.minimum(i0 + k, px.n)] - px.cs[i0], 0.0)
    m = n - k
    a = np.maximum(first, t + 1) - t - 1                          # gap before first unknown second
    mean = (known_sum + m * St) / n
    var_rel = sum_min(a, m) / n ** 2                              # in units of sigma^2 * S^2
    basis0 = lab.price_to_beat.to_numpy() - px.avg(T, lo, hi)
    s2 = px.sigma2(t)
    return dict(mean=mean, var_rel=var_rel, St=St, s2=s2, basis0=basis0,
                K=lab.price_to_beat.to_numpy(), BT=px.avg(T, lo, hi), t=t)


def prob(st, k_sig=1.0, basis_sd=0.0, nu=30.0):
    sd = np.sqrt(k_sig ** 2 * st["s2"] * st["St"] ** 2 * st["var_rel"] + basis_sd ** 2)
    z = (st["mean"] + st["basis0"] - st["K"]) / np.maximum(sd, 1e-9)
    return np.clip(stats.t.cdf(z, nu), 1e-6, 1 - 1e-6)


def naive_prob(px, lab, r):
    """Point-price Black-Scholes digital on Binance, start = Binance close at T."""
    T = lab.t.to_numpy(np.int64); t = T + W - r
    S, K = px.p[px.i(t)], px.p[px.i(T)]
    z = np.log(S / K) / np.sqrt(px.sigma2(t) * r)
    return np.clip(stats.norm.cdf(z), 1e-6, 1 - 1e-6)


def logloss(y, p):
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def slope(y, p):
    """Calibration slope: logistic regression of y on logit(p)."""
    x = np.log(p / (1 - p))
    f = lambda b: -np.mean(y * (b[0] + b[1] * x) - np.logaddexp(0, b[0] + b[1] * x))
    return float(optimize.minimize(f, [0, 1]).x[1])


def fit(states, ys):
    """Fit (k_sig, basis_sd, nu) by pooled log loss across r."""
    def f(th):
        k, b, lnu = th
        return np.mean([logloss(y, prob(s, k, abs(b), np.exp(lnu))) for s, y in zip(states, ys)])
    res = optimize.minimize(f, [1.0, 5.0, np.log(10)], method="Nelder-Mead", options=dict(maxiter=400))
    k, b, lnu = res.x
    return dict(k_sig=float(k), basis_sd=float(abs(b)), nu=float(np.exp(lnu)))
