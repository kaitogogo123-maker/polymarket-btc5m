"""Run: python tests/test_mine_rules.py  The gate must pass a planted edge and reject noise and a leak."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import numpy as np
import mine_rules as mr
from mine_rules import mine

N = 2000
real_fee = mr.fee


def panel(seed, shape=None, free=False):
    r = np.random.default_rng(seed)
    d = r.normal(0, .004, (N, 301))
    bd = r.uniform(400, 600, (N, 301)); ad = r.uniform(400, 600, (N, 301))
    if shape == "edge":            # strong bid depth in a burst drives the price up for the next 30 s
        for w in range(N):
            for s in r.integers(15, 240, 12):
                sign = r.choice([-1, 1])
                bd[w, s] = 5000 if sign > 0 else 50
                ad[w, s] = 50 if sign > 0 else 5000
                d[w, s + 2:s + 32] += sign * .08 / 30
    mid = np.clip(.5 + np.cumsum(d, 1), .12, .88)
    if shape == "leak":            # the signal knows only the move that happens before it can trade
        for w in range(N):
            e = np.exp(np.clip(np.diff(mid[w]) * 300, -3, 3))
            bd[w, :-1] = 500 * e; ad[w, :-1] = 500 / e
    half = 0 if free else .005
    P = dict(T=np.arange(N) * 300, bid=mid - half, ask=mid + half, buy_px=mid + half, sell_px=mid - half,
             up=(r.random(N) < mid[:, -1]).astype(float), up_buy=np.zeros((N, 301)), up_sell=np.zeros((N, 301)),
             down_buy=np.zeros((N, 301)), down_sell=np.zeros((N, 301)), bn=78000 + np.cumsum(r.normal(0, 3, (N, 301)), 1),
             spot=78000 + np.cumsum(r.normal(0, 3, (N, 301)), 1))
    for k in (1, 3, 10):
        P[f"bd{k}"], P[f"ad{k}"] = bd, ad
    return P


# With costs nothing reaches the hold-out, which would prove nothing. Remove them so the gate itself is exercised.
mr.fee = lambda p: 0 * p
null = [mine(panel(s, free=True), need_test=900, seed=s) for s in (1, 2, 3)]
assert all(x["candidates"] for x in null), "noise must reach the hold-out for this test to mean anything"
assert all(not x["passed"] for x in null), [x["passed"][:1] for x in null]
print("零成本噪声 3 组: 进入留出", [len(x["candidates"]) for x in null], "条，通过 0 条")

leak = mine(panel(4, "leak", free=True), need_test=900)
assert not leak["passed"], leak["passed"][:1]
mr.LAT = 0
cheat = mine(panel(4, "leak", free=True), need_test=900)
mr.LAT = 1
assert cheat["passed"], "without the delay the same signal must look profitable"
print("领先 1 秒的信号: 有延迟通过 0 条，去掉延迟通过", len(cheat["passed"]), "条")
mr.fee = real_fee

edge = mine(panel(5, "edge"), need_test=900)
assert edge["passed"] and all(x["mode"] == "跟随" for x in edge["passed"]), edge["candidates"][:2]
assert any("失衡" in x["feature"] for x in edge["passed"]), [x["rule"] for x in edge["passed"]]
print("植入规律: 通过", len(edge["passed"]), "条，第一条", edge["passed"][0]["rule"], edge["passed"][0]["test_cents"], "¢/窗")

late = panel(5, "edge"); late["lag"] = np.full((N, 301), 3.0)
blocked = mine(late, need_test=900)
assert not blocked["passed"], blocked["passed"][:1]
print("同一规律但信号都迟到 3 秒: 通过 0 条")

small = mine(panel(5, "edge"), need_test=10 ** 6)
assert small["status"] == "样本不足" and not small["passed"]
print("样本不足时: 不判通过")
