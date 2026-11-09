"""Mine book rules on the earlier windows, judge them only on the later ones.

A rule: when |feature| >= threshold in a part of the window, follow or fade its sign, hold h seconds.
Costs: walk the book for SIZE shares both ways plus taker fee. Entry is one full second after the signal,
and a signal whose quote arrived more than MAX_LAG late is ignored because it could not have been seen in time.
Writes out/rules.json. Nothing here places orders.
Run: python src/mine_rules.py
"""
import fcntl, json, math, os, sys, time
from pathlib import Path
import numpy as np
from scipy.stats import norm

ROOT = Path(__file__).resolve().parents[1]
NEED_TEST = 1500                 # hold-out windows required before any rule can pass (about 5 days)
TRAIN = 0.5
LAT = 1                          # seconds between the signal and the quote it can trade at
MAX_LAG = 2
HOLDS = (10, 30, 60, 0)          # 0 = keep until settlement
PARTS = {"全程": (10, 290), "前段": (10, 120), "中段": (120, 240), "末段": (240, 290)}
QUANTS = (0.9, 0.95)
TOP = 20
MIN_FLOW = 100                   # shares; a ratio over a handful of shares says nothing
LOOKBACK = 30                    # features look back at most this far, so a lost stretch voids them for this long
fee = lambda p: 0.07 * p * (1 - p)


def features(P):
    mid = (P["bid"] + P["ask"]) / 2
    def back(a, n):
        out = np.full(a.shape, np.nan)
        out[:, n:] = a[:, n:] - a[:, :-n]
        return out
    def imb(k):
        b, a = P[f"bd{k}"], P[f"ad{k}"]
        with np.errstate(invalid="ignore", divide="ignore"):
            return (b - a) / (b + a)
    def flow(n):
        up = P["up_buy"] + P["down_sell"]
        dn = P["up_sell"] + P["down_buy"]
        cu, cd = np.cumsum(up, 1), np.cumsum(dn, 1)
        u, d = back(cu, n), back(cd, n)
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.where(u + d >= MIN_FLOW, (u - d) / (u + d), np.nan)
    return {"买卖盘失衡1¢": imb(1), "买卖盘失衡3¢": imb(3), "买卖盘失衡10¢": imb(10),
            "成交流10s": flow(10), "成交流30s": flow(30),
            "盘口动量5s": back(mid, 5), "盘口动量15s": back(mid, 15), "盘口动量30s": back(mid, 30),
            "Binance5s": back(P["bn"], 5), "Binance15s": back(P["bn"], 15),
            "Chainlink5s": back(P["spot"], 5), "Chainlink15s": back(P["spot"], 15)}


def clean_features(P):
    fs = features(P)
    if "lost" in P:                              # a lost stretch fakes a jump in every lookback that spans it
        recent = np.cumsum(P["lost"], 1)
        recent[:, LOOKBACK:] -= recent[:, :-LOOKBACK].copy()
        for x in fs.values():
            x[recent > 0] = np.nan
    return fs


def next_known(a):
    """Each second takes the first finite value at or after it."""
    out = a.copy()
    for i in range(a.shape[1] - 2, -1, -1):
        miss = ~np.isfinite(out[:, i])
        out[miss, i] = out[miss, i + 1]
    return out


def usable(mid, lag, part):
    """Seconds where a signal could be seen in time and the price still has somewhere to go."""
    lo, hi = PARTS[part]
    ok = (mid > 0.1) & (mid < 0.9) & (lag <= MAX_LAG)
    ok[:, :lo] = False
    ok[:, hi:] = False
    return ok


def pnl_tables(P):
    """Net result per share of going long or short Up at each entry second, for each holding time."""
    buy, sell = P["buy_px"], P["sell_px"]
    # A lost exit second must not erase the trade: use the next price, or the worst one if the window has none.
    exit_sell, exit_buy = np.nan_to_num(next_known(sell), nan=0.0), np.nan_to_num(next_known(buy), nan=1.0)
    out = {}
    for h in HOLDS:
        lg, sh = np.full(buy.shape, np.nan), np.full(buy.shape, np.nan)
        if h:
            lg[:, :-h] = exit_sell[:, h:] - buy[:, :-h] - fee(buy[:, :-h]) - fee(exit_sell[:, h:])
            sh[:, :-h] = sell[:, :-h] - exit_buy[:, h:] - fee(sell[:, :-h]) - fee(exit_buy[:, h:])
        else:                        # settlement pays 1 or 0 and has no exit fee
            up = P["up"][:, None]
            lg = up - buy - fee(buy)
            sh = sell - up - fee(sell)
        out[h] = lg, sh
    return out


def run_rule(x, th, mode, part, h, mid, lag, tables):
    """Per-window profit of one rule, taking trades one at a time. Returns (per-window sum, trades, sum of trades)."""
    lg, sh = tables[h]
    gap = h or 10 ** 6
    total = np.zeros(len(x))
    n = 0
    ws, ss = np.nonzero((np.abs(x) >= th) & usable(mid, lag, part))
    busy_w, busy_until = -1, -1
    for w, s in zip(ws.tolist(), ss.tolist()):
        if w == busy_w and s < busy_until:
            continue
        e = s + LAT + math.ceil(lag[w, s])       # the order goes out only after the signal has arrived
        if e > 295 or (h and e + h > 295):
            continue
        up = (x[w, s] > 0) == (mode == "跟随")
        v = lg[w, e] if up else sh[w, e]
        if not np.isfinite(v):
            continue
        total[w] += v
        n += 1
        busy_w, busy_until = w, e + gap
    return total, n


def t_stat(a):
    s = a.std(ddof=1)
    return a.mean() / s * np.sqrt(len(a)) if s > 0 else 0.0


def mine(P, need_test=NEED_TEST, seed=0):
    nW = len(P["T"])
    res = dict(generated=int(time.time()), windows=nW, need_test=need_test, need_total=int(need_test / (1 - TRAIN)),
               status="样本不足", tested=0, candidates=[], passed=[], first=int(P["T"][0]) if nW else None)
    if nW < 40:
        return res
    cut = int(nW * TRAIN)
    res.update(train=cut, test=nW - cut)
    ok_test = nW - cut >= need_test
    mid = (P["bid"] + P["ask"]) / 2
    lag = P.get("lag", np.zeros(mid.shape))
    tables = pnl_tables(P)
    fs = clean_features(P)
    slice_ = lambda a: a[:cut]
    tr_tables = {h: (slice_(a), slice_(b)) for h, (a, b) in tables.items()}
    te_tables = {h: (a[cut:], b[cut:]) for h, (a, b) in tables.items()}
    found = []
    for name, x in fs.items():
        for part in PARTS:
            ax = np.abs(x[:cut][usable(mid[:cut], lag[:cut], part)])
            ax = ax[np.isfinite(ax)]
            if not len(ax):
                continue
            ths = {}
            for q in QUANTS:                     # a tied distribution can give both quantiles the same value
                th = float(np.quantile(ax, q))
                if th > 0:
                    ths.setdefault(th, q)
            for th, q in ths.items():
                for h in HOLDS:
                    for mode in ("跟随", "反向"):
                        res["tested"] += 1
                        tot, n = run_rule(x[:cut], th, mode, part, h, mid[:cut], lag[:cut], tr_tables)
                        if n >= 30 and tot.mean() > 0:
                            found.append((t_stat(tot), name, q, th, part, h, mode, n, tot.mean()))
    found.sort(key=lambda r: -r[0])
    rng = np.random.default_rng(seed)
    rows = []
    for t, name, q, th, part, h, mode, n, m in found[:TOP]:
        if t < 2:
            continue
        tot, nt = run_rule(fs[name][cut:], th, mode, part, h, mid[cut:], lag[cut:], te_tables)
        boot = tot[rng.integers(0, len(tot), (2000, len(tot)))].mean(1)
        half = len(tot) // 2
        rows.append(dict(rule=f"{name} {part} {mode} 持有{h or '到结算'}{'秒' if h else ''} 前{round((1 - q) * 100)}%",
                         feature=name, threshold=th, part=part, hold=h, mode=mode,
                         train_t=round(float(t), 2), train_cents=round(float(m) * 100, 3),
                         test_trades=nt, test_cents=round(float(tot.mean()) * 100, 3),
                         ci_low=round(float(np.quantile(boot, .025)) * 100, 3), ci_high=round(float(np.quantile(boot, .975)) * 100, 3),
                         p=float(norm.sf(t_stat(tot))),
                         halves_positive=bool(tot[:half].mean() > 0 and tot[half:].mean() > 0)))
    # Benjamini-Hochberg over the rules carried to the hold-out.
    order = sorted(range(len(rows)), key=lambda i: rows[i]["p"])
    k_max = -1
    for k, i in enumerate(order, 1):
        if rows[i]["p"] <= 0.05 * k / len(rows):
            k_max = k
    for k, i in enumerate(order, 1):
        rows[i]["bh_pass"] = k <= k_max
    res["candidates"] = rows
    if ok_test:
        res["passed"] = [r for r in rows if r["bh_pass"] and r["ci_low"] > 0 and r["halves_positive"] and r["test_trades"] >= 200]
        res["status"] = "有规律通过" if res["passed"] else "没有规律通过"
    return res


if __name__ == "__main__":
    from book_panel import load
    instance = open(ROOT / "out/.mine.lock", "w")     # a restart can leave the previous run finishing
    try:
        fcntl.flock(instance, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("mine_rules.py is already running")
    R = mine(load())
    tmp = ROOT / "out/rules.part"
    json.dump(R, open(tmp, "w"), ensure_ascii=False, indent=1)
    os.replace(tmp, ROOT / "out/rules.json")
    print(R["status"], "| 窗口", R["windows"], "| 留出", R.get("test", 0), "/", R["need_test"], "| 试过", R["tested"], "条")
    for r in R["candidates"][:10]:
        print(r["rule"], "| 训练 t", r["train_t"], "| 留出 ¢/窗", r["test_cents"], "CI", r["ci_low"], r["ci_high"], "笔", r["test_trades"])
