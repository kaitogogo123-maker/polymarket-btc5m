"""Run: python tests/test_shadow.py  Orders fill only against the book of their own moment, and the money adds up."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import shadow as sh
from shadow import Shadow, net_cents, NET_MS, TAKER_DELAY_MS, fee

T = 1800000000
DELAY = NET_MS + TAKER_DELAY_MS
RULE = dict(rule="r", feature="盘口动量5s", threshold=.03, part="全程", hold=10, mode="跟随", registered_at=0)


def bn(s):
    return dict(src="bn", rx=s * 1000, m=dict(k="t", p="100", T=s * 1000))


def quote(ms, bid, ask):
    return dict(src="pm", rx=ms + 10, m=dict(asset_id="U", event_type="best_bid_ask", best_bid=str(bid), best_ask=str(ask), timestamp=str(ms)))


def book(ms, bids, asks):
    f = lambda side: [dict(price=str(p), size=str(s)) for p, s in side]
    return dict(src="pm", rx=ms + 10, m=dict(asset_id="U", event_type="book", bids=f(bids), asks=f(asks), timestamp=str(ms)))


def pc(ms, *changes):
    return dict(src="pc", rx=ms + 10, m=dict(T=T, ts=ms, c=[[str(p), str(s), side] for p, s, side in changes]))


def run(extra, rule=RULE, size=100, until=100):
    """Up jumps 4c between seconds 15 and 20, so the follow rule fires when second 20 finishes: it sends at T+21."""
    s = Shadow([rule], {"U": (T, "up"), "D": (T, "down")}, size=size)
    recs = [bn(T + i) for i in range(until)]
    recs += [quote((T + 10) * 1000, .49, .50), quote((T + 20) * 1000, .53, .54), book((T + 5) * 1000, [(.53, 500)], [(.54, 500)])]
    recs += extra
    for r in sorted(recs, key=lambda r: r["rx"]):
        s.feed(r)
    s.flush()
    return s.rows


sent = (T + 21) * 1000
# 1. A fill uses the book at the order's moment: a change just after it is invisible, a change just before it counts.
after = run([pc(sent + DELAY + 1, (.54, 0, "SELL"), (.70, 500, "SELL"))])[0]
assert after["cost"] == 54 and after["shares"] == 100, after
before = run([pc(sent + DELAY - 1, (.54, 0, "SELL"), (.55, 500, "SELL")), bn(T + 22)])[0]
assert abs(before["cost"] - 55) < 1e-6, before
# 2. FAK: only up to two cents worse than the quote it saw, the rest is cancelled.
part = run([pc(sent + 1, (.54, 30, "SELL"), (.56, 30, "SELL"), (.57, 500, "SELL"))])[0]
assert part["shares"] == 60 and abs(part["cost"] - (30 * .54 + 30 * .56)) < 1e-6, part
gone = run([pc(sent + 1, (.54, 0, "SELL"), (.60, 500, "SELL"))])
assert gone[0]["status"] == "未成交" and gone[0]["shares"] == 0, gone[0]
# 3. Net per share is exit minus entry minus both fees.
sold = run([pc(sent + 2000, (.53, 0, "BUY"), (.60, 500, "BUY"))])[0]
assert sold["status"] == "已平" and abs(sold["proceeds"] - 60) < 1e-6, sold
want = (60 - 54 - 100 * fee(.54) - 100 * fee(.60)) / 100 * 100
assert abs(net_cents(sold, None) - want) < 1e-3, (net_cents(sold, None), want)
# 4. A fade buys Down at 1 - bid and sells it at 1 - ask.
down = run([pc(sent + 2000, (.54, 0, "SELL"), (.50, 500, "SELL"))], dict(RULE, mode="反向"))[0]
assert down["side"] == "down" and abs(down["cost"] - 47) < 1e-6 and abs(down["proceeds"] - 50) < 1e-6, down
# 5. Held to settlement: pays 1 or 0 per share, with no exit fee.
held = run([], dict(RULE, hold=0), until=320)[0]
assert held["status"] == "到结算" and held["open"] == 100, held
assert net_cents(held, None) is None
assert abs(net_cents(held, 1.0) - (100 - 54 - 100 * fee(.54))) < 1e-3
assert abs(net_cents(held, 0.0) - (0 - 54 - 100 * fee(.54))) < 1e-3
# 6. A rule frozen after the window started does not trade it.
assert run([], dict(RULE, registered_at=T + 1)) == []
# 7. Collector down when the order lands: the entry is void, no fill is invented, and lookbacks spanning it stay void.
s = Shadow([RULE], {"U": (T, "up")})
for r in sorted([bn(T + i) for i in range(22)] + [quote((T + 10) * 1000, .49, .50), quote((T + 20) * 1000, .53, .54), book((T + 5) * 1000, [(.53, 500)], [(.54, 500)])], key=lambda r: r["rx"]):
    s.feed(r)
for i in range(40, 80):
    s.feed(bn(T + i))
s.flush()
assert [x["status"] for x in s.rows] == ["作废"], s.rows
# 8. No signal from the future: the quote that makes the signal arrives after the boundary, so nothing is sent at T+21.
late = dict(quote((T + 20) * 1000, .53, .54), rx=sent + 500)
s = Shadow([RULE], {"U": (T, "up")})
for r in sorted([bn(T + i) for i in range(22)] + [quote((T + 10) * 1000, .49, .50), late, book((T + 5) * 1000, [(.53, 500)], [(.54, 500)])], key=lambda r: r["rx"]):
    s.feed(r)
assert not s.pending and s.state["r"]["pos"] is None, s.pending
print("ok")
