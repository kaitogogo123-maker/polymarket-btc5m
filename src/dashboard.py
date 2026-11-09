"""Local real-time dashboard. Read-only: tails data/live written by collect_live.py.

Uses the same model.state/prob and parameters as the offline evaluation; no orders, no credentials.
Run: python src/dashboard.py   then open http://127.0.0.1:8791
"""
import collections, datetime as D, glob, gzip, json, subprocess, sys, threading, time, traceback, zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
import numpy as np, pandas as pd, requests
from model import ROOT, Prices, state, prob

PORT, STEP, VIEW, REPLAY = 8791, 30, 4 * 3600, 3600
par = json.load(open(ROOT / "out/baseline_params.json"))
platt = {int(k): v for k, v in json.load(open(ROOT / "out/platt.json")).items()}
PR = sorted(platt)
PA, PB = [platt[r][0] for r in PR], [platt[r][1] for r in PR]
KEEP_WINDOWS = 25
MAX_LAG = 2
GAP, STALE = 60, 15                            # seconds without Binance trades: reseed / stop publishing
fee = lambda p: 0.07 * p * (1 - p)
lock = threading.Lock()


def depth(bids, asks):
    """Shares resting within 3 cents of the best price on each side."""
    if not bids or not asks:
        return None
    b, a = [(float(x["price"]), float(x["size"])) for x in bids], [(float(x["price"]), float(x["size"])) for x in asks]
    bb, ba = max(p for p, _ in b), min(p for p, _ in a)
    return dict(bid=sum(s for p, s in b if p > bb - .03 - 1e-9), ask=sum(s for p, s in a if p < ba + .03 + 1e-9))


def ladder(book, rows=9):
    """Nearest price levels, best first."""
    if not book:
        return None
    return dict(bids=sorted(book["bids"].items(), reverse=True)[:rows], asks=sorted(book["asks"].items())[:rows], ts=book["ts"])


def align(lad, bid, ask):
    """price_change reaches this page up to a moment after best_bid_ask. Levels the quote has moved past are gone,
    and a new best level has no known size yet (shown as a dash)."""
    if not lad or bid is None:
        return lad
    b = [x for x in lad["bids"] if x[0] <= bid]
    a = [x for x in lad["asks"] if x[0] >= ask]
    if bid > 0 and (not b or b[0][0] < bid):
        b.insert(0, [bid, None])
    if ask < 1 and (not a or a[0][0] > ask):
        a.insert(0, [ask, None])
    return dict(bids=b, asks=a, ts=lad["ts"])


_rules = (0, None)


def rules_status():
    global _rules
    f = ROOT / "out/rules.json"
    try:
        m = f.stat().st_mtime
        if m != _rules[0]:
            r = json.load(open(f))
            _rules = (m, {k: r.get(k) for k in ("status", "windows", "need_total", "first", "tested", "generated", "passed")})
    except (OSError, ValueError):
        pass
    return _rules[1]


def refresh_rules():
    """Hourly re-mining and database copy; each run reparses only the current hour."""
    while True:
        with open(ROOT / "out/mine_rules.log", "a") as log:
            for job in ("src/mine_rules.py", "src/shadow.py", "src/db_sync.py"):
                subprocess.run([sys.executable, str(ROOT / job)], cwd=ROOT, stdout=log, stderr=log)
        time.sleep(3600)


class Series:
    """Per-second Binance last-trade prices, forward-filled like the 1s kline closes used in training."""

    def __init__(self, now):
        self.base = (now - 2 * VIEW) // STEP * STEP
        self.p = np.full(4 * VIEW, np.nan)
        self.filled = self.base - 1

    def room(self, sec):
        """Slide (dropping the oldest half) until second sec fits, keeping the last price at index 0."""
        h = len(self.p) // 2
        while sec - self.base >= len(self.p):
            known = self.p[~np.isnan(self.p)]
            carry = known[-1] if len(known) else np.nan
            self.p = np.concatenate([self.p[h:], np.full(h, np.nan)])
            self.p[0] = carry
            self.base += h
            self.filled = max(self.filled, self.base)

    def put(self, sec, price):
        self.room(sec)
        if sec >= self.base:
            self.p[sec - self.base] = price

    def fill_to(self, s):
        self.room(s)
        for i in range(max(self.filled + 1 - self.base, 1), s - self.base + 1):
            if np.isnan(self.p[i]):
                self.p[i] = self.p[i - 1]
        self.filled = max(self.filled, s)

    def view(self, s):
        """Prices object containing only data up to second s (no look-ahead)."""
        t0 = (s - VIEW) // STEP * STEP
        px = Prices.__new__(Prices)
        px.t0, px.p = t0, self.p[t0 - self.base: s - self.base + 1]
        px.n = len(px.p)
        px.cs = np.concatenate([[0], np.cumsum(px.p)])
        return px


def seed(now):
    """Volatility needs history that predates the collector: public Binance 1s klines."""
    ser = Series(now)
    end = now * 1000
    for _ in range((VIEW + REPLAY) // 1000 + 2):
        r = requests.get("https://api.binance.com/api/v3/klines", timeout=20,
                         params=dict(symbol="BTCUSDT", interval="1s", limit=1000, endTime=end))
        r.raise_for_status()
        rows = r.json()
        for k in rows:
            ser.put(k[0] // 1000, float(k[4]))
        end = rows[0][0] - 1
    first = int(np.argmax(~np.isnan(ser.p)))
    ser.p[:first] = ser.p[first]
    ser.fill_to(now - 1)
    return ser


class Engine:
    def __init__(self, ser):
        self.ser = ser
        self.tok, self.twap, self.win, self.hist = {}, {}, {}, []
        self.down, self.depth, self.books = {}, {}, {}   # books: T -> live Up book, {price: size} on each side
        self.tape = collections.deque(maxlen=400)  # newest last: dict(T, ts, side, price, size), in Up terms
        self.flows = collections.defaultdict(dict) # T -> 5 s bucket -> [shares pushing Up, shares pushing Down]
        self.seen_tx = collections.deque(maxlen=4000)
        self.bid, self.ask, self.quote_ts, self.lag, self.quote_rx = {}, {}, {}, {}, {}
        self.quotes = collections.defaultdict(dict)  # T -> exchange second -> mid: late quotes still land in the right place
        self.last_rx = {"pm": 0, "cl": 0, "bn": 0}
        self.bn = self.spot = self.tw = None
        self.tw_stamp = 0
        self.bn_sec = int(time.time())             # seed() already covers everything up to here
        self.need_seed = False
        self.seed_try = 0
        self.replaying = True
        self.fatal = None
        self.sec = 0
        self.error = None
        self.error_at = 0
        self.gaps = []                             # (last second heard, next second heard): nothing was recorded between

    def note(self, e):
        """Transient failure: logged once per message, shown on the page until it ages out."""
        if self.error != repr(e):
            traceback.print_exception(e)
        self.error, self.error_at = repr(e), time.time()

    def advance(self, sec, tick_from):
        """Run ticks for finished seconds before applying anything received in a later second."""
        if sec <= self.sec:
            return
        prev, self.sec = self.sec, sec
        if prev and sec - prev > 5:
            self.gaps.append((prev, sec))
            del self.gaps[:-200]
        if prev:
            for s in range(max(prev, sec - 5, tick_from), sec):
                try:
                    self.tick(s)
                except Exception as e:
                    self.note(e)

    def line(self, raw, tick_from):
        if self.replaying and '"event_type":"book"' in raw:   # only the live seconds need the ladder
            rx = int(raw[raw.index('"rx":') + 5:raw.index(',', raw.index('"rx":'))])
            self.last_rx["pm"] = rx
            return
        try:
            d = json.loads(raw)
        except ValueError:
            return
        src, m, rx = d["src"], d["m"], d["rx"]
        self.advance(rx // 1000, tick_from)
        if src == "meta":
            self.tok[m["tokens"][0]] = m["t"]
            self.down[m["tokens"][1]] = m["t"]
        elif src == "pm":
            self.last_rx["pm"] = rx
            if m.get("event_type") == "best_bid_ask" and m["asset_id"] in self.tok:
                t = self.tok[m["asset_id"]]
                ts = int(m["timestamp"])
                self.bid[t], self.ask[t], self.quote_ts[t], self.lag[t], self.quote_rx[t] = float(m["best_bid"]), float(m["best_ask"]), ts, (rx - ts) / 1000, rx
                self.quotes[t][ts // 1000 - t] = (self.bid[t] + self.ask[t]) / 2
            elif m.get("event_type") == "book" and m["asset_id"] in self.tok:
                t = self.tok[m["asset_id"]]
                self.depth[t] = depth(m["bids"], m["asks"])
                self.books[t] = dict(bids={float(x["price"]): float(x["size"]) for x in m["bids"]},
                                     asks={float(x["price"]): float(x["size"]) for x in m["asks"]}, ts=int(m["timestamp"]))
            elif m.get("event_type") == "last_trade_price":
                self.trade(m, rx)
        elif src == "cl":
            self.last_rx["cl"] = rx
            p, topic = m.get("payload") or {}, m.get("topic") or ""
            if "twap" in topic:
                for x in p.get("data") or [p]:
                    if "value" in x:
                        stamp = x["timestamp"] // 1000
                        self.twap[stamp] = x["value"]
                        if stamp >= self.tw_stamp:
                            self.tw_stamp, self.tw = stamp, x["value"]
            elif "chainlink" in topic and "value" in p:
                self.spot = p["value"]
        elif src == "bn":
            self.last_rx["bn"] = rx
            if m["k"] == "t":
                self.bn = float(m["p"])
                sec = m["T"] // 1000
                if sec - self.bn_sec > GAP:            # sleep or outage: flat-filled prices would poison the EWMA
                    self.need_seed = True
                self.bn_sec = max(self.bn_sec, sec)
                self.ser.put(sec, self.bn)

    def change(self, m):
        """Sizes are absolute, so a change the snapshot already holds is harmless; older ones are skipped by time."""
        b = self.books.get(m["T"])
        if not b or m["ts"] < b["ts"]:
            return
        for price, size, side in m["c"]:
            levels, p, s = b["bids" if side == "BUY" else "asks"], float(price), float(size)
            if s:
                levels[p] = s
            else:
                levels.pop(p, None)
        b["ts"] = m["ts"]

    def trade(self, m, rx):
        """`side` is the taker's. Buying Down is selling Up at 1 - p, so the tape is written in Up terms."""
        a = m["asset_id"]
        if a not in self.tok and a not in self.down:
            return
        key = (m.get("transaction_hash"), a, m["size"], m["timestamp"])
        if key in self.seen_tx:                    # reconnects replay events
            return
        self.seen_tx.append(key)
        is_up, buy, price, size, ts = a in self.tok, m["side"] == "BUY", float(m["price"]), float(m["size"]), int(m["timestamp"])
        T = self.tok[a] if is_up else self.down[a]
        pushes_up = buy if is_up else not buy
        self.tape.append(dict(T=T, ts=ts, buy=pushes_up, price=price if is_up else 1 - price, size=size))
        i = (ts // 1000 - T) // 5
        if 0 <= i < 60:
            bar = self.flows[T].setdefault(i, [0.0, 0.0])
            bar[0 if pushes_up else 1] += size

    def twap_at(self, t, s):
        """TWAP stamped t. The stream drops ~1% of seconds: after 3 s use the nearest one, flagged approximate."""
        if t in self.twap:
            return self.twap[t], False
        if s >= t + 3:
            for d in (1, -1, 2, -2, 3, -3):
                if t + d in self.twap:
                    return self.twap[t + d], True
        return None, False

    def tick(self, s):
        """Compute the model at second s using data up to s only."""
        if self.need_seed or s - self.bn_sec > STALE:  # no honest price: leave a gap instead of a fake point
            T = s // 300 * 300
            self.settle(T - 300, s)
            return
        self.ser.fill_to(s)
        T = s // 300 * 300
        r = T + 300 - s
        w = self.win.setdefault(T, dict(T=T, points=[], at60=None))
        self.settle(T - 300, s)                    # TWAP(T) arrives ~1.5 s after T, so retry each tick
        self.refine()
        if not w.get("ptb") or (w.get("approx") and T in self.twap):
            w["ptb"], w["approx"] = self.twap_at(T, s)
        ptb = w["ptb"]
        if ptb is None:
            return
        lab = pd.DataFrame(dict(t=[T], price_to_beat=[ptb]))
        st = state(self.ser.view(s), lab, r, par["lo"], par["hi"])
        raw = float(prob(st, **par["theta"])[0])
        if not np.isfinite(raw):                   # no volatility yet; never send NaN to the page
            return
        x = np.log(raw / (1 - raw))
        pu = float(1 / (1 + np.exp(-(np.interp(r, PR, PA) + np.interp(r, PR, PB) * x))))
        bid, ask = self.bid.get(T), self.ask.get(T)
        mid = (bid + ask) / 2 if bid is not None and ask is not None else None
        if self.lagging(T, s):                     # the newest quote we hold is not the market now
            bid = ask = mid = None
        w["points"].append([300 - r, round(pu, 5), self.tw])
        w["now"] = dict(r=r, p=pu, bid=bid, ask=ask, mid=mid, tw=self.tw, spot=self.spot, binance=self.bn)
        if r == 60:
            w["at60"] = pu
        for t in [k for k in self.win if k < T - KEEP_WINDOWS * 300]:
            del self.win[t]
        for t in [k for k in self.flows if k < T - KEEP_WINDOWS * 300]:
            del self.flows[t]
        for t in [k for k in self.quotes if k < T - KEEP_WINDOWS * 300]:
            del self.quotes[t]
        for t in [k for k in self.books if k < T - 300]:
            del self.books[t]
        for t in [k for k in self.twap if k < s - 1200]:
            del self.twap[t]

    def settle(self, T, s):
        w = self.win.get(T)
        final, final_approx = self.twap_at(T + 300, s)
        if not w or not w.get("ptb") or final is None or w.get("done"):
            return
        w["done"] = True
        p, m = w["at60"], self.mid_at(T, 240)
        self.hist.append(dict(T=T, ptb=w["ptb"], final=final, up=int(final >= w["ptb"]), p60=p, m60=m,
                              ptb_approx=bool(w.get("approx")), final_approx=final_approx))
        del self.hist[:-500]

    def lagging(self, T, s):
        """The last quote arrived late, and recently: a quiet market afterwards does not make it fresh or stale."""
        return self.lag.get(T, 0) > MAX_LAG and s - self.quote_rx.get(T, 0) / 1000 < 5

    def mid_at(self, T, i):
        """Quote in force at second i of window T, by exchange time."""
        q = self.quotes.get(T, {})
        for k in range(i, -1, -1):
            if k in q:
                return q[k]
        return None

    def refine(self):
        """A late exact TWAP replaces the neighbouring second used for recent results."""
        for h in self.hist[-3:]:
            if h["ptb_approx"] and h["T"] in self.twap:
                h["ptb"], h["ptb_approx"] = self.twap[h["T"]], False
            if h["final_approx"] and h["T"] + 300 in self.twap:
                h["final"], h["final_approx"] = self.twap[h["T"] + 300], False
            h["up"] = int(h["final"] >= h["ptb"])

    def snapshot(self, view=None):
        now = time.time()
        T = int(now) // 300 * 300
        v = view if view in self.win else T
        w = self.win.get(v) or dict(T=v, points=[])
        n = w.get("now") or {}
        edge = {}
        if n.get("ask") is not None and n.get("bid") is not None:
            cu, cd = n["ask"], 1 - n["bid"]
            edge = dict(up=n["p"] - cu - fee(cu), down=(1 - n["p"]) - cd - fee(cd))
        if v == T and T in self.bid:               # the quote does not depend on the model having an opening price
            live = not self.lagging(T, now)
            bid, ask = (self.bid[T], self.ask[T]) if live else (None, None)
            n = {**n, "bid": bid, "ask": ask, "mid": (bid + ask) / 2 if live else None}
        settled = next((h for h in self.hist if h["T"] == v), None)
        known = self.quotes.get(v, {})
        last, mid, points = max(known, default=-1), None, []
        by_sec = {q[0]: q for q in w["points"]}
        fresh = now - self.last_rx["pm"] / 1000 <= STALE   # a quote holds until it changes, while the feed is alive
        holes = {i for a, b in self.gaps for i in range(a - v + 1, b - v)}
        for i in range(0, min(300, int(now) - v) + 1):     # [second, model, mid by exchange time, TWAP]
            mid = known.get(i, mid)
            q = by_sec.get(i)
            held = i <= last or fresh
            if q or held:
                points.append([i, q[1] if q else None, mid if held and i not in holes else None, q[2] if q else None])
        recent = [x for x in self.tape if x["ts"] / 1000 >= now - 30 and x["T"] == T]
        flow = dict(up=sum(x["size"] for x in recent if x["buy"]), down=sum(x["size"] for x in recent if not x["buy"]))
        bars = [[i, *self.flows[v][i]] for i in sorted(self.flows[v])] if v in self.flows else []
        return dict(now=now, T=T, view=v, depth=self.depth.get(T), flow=flow, rules=rules_status(), bars=bars,
                    ladder=align(ladder(self.books.get(T)), n.get("bid"), n.get("ask")), tape=[x for x in self.tape if x["T"] == T][-40:][::-1], ptb=w.get("ptb"), settled=settled,
                    twap=self.tw, spot=self.spot, binance=self.bn, replaying=self.replaying,
                    error=self.fatal or (self.error if now - self.error_at < 60 else None),
                    points=points, cur=n, edge=edge, hist=self.hist[-24:][::-1],
                    age={k: (now - x / 1000 if x else None) for k, x in self.last_rx.items()},
                    quote_age=(now - self.quote_ts[T] / 1000) if T in self.quote_ts else None,
                    quote_lag=self.lag.get(T) if self.lagging(T, now) else None)


def feed(eng, raw, tick_from):
    """One bad line must not stop the tail. Reseeding talks to the network, so it happens outside the lock."""
    with lock:
        try:
            eng.line(raw, tick_from)
        except Exception as e:
            eng.note(e)
        due = eng.need_seed and time.time() - eng.seed_try > 60
        if due:
            eng.seed_try = time.time()
    if due:
        try:
            ser = seed(int(time.time()))
        except Exception as e:                     # retried at most once a minute
            with lock:
                eng.note(e)
            return
        with lock:
            eng.ser, eng.need_seed = ser, False


def newest_plain():
    plain = sorted(glob.glob(str(ROOT / "data/live/*.jsonl")))
    return plain[-1] if plain else None


def previous_hour():
    """Last hour's file, compressed if finished, otherwise still plain (collector compressing or killed)."""
    key = D.datetime.fromtimestamp(time.time() - 3600, D.timezone.utc).strftime("%Y%m%d_%H")
    base = ROOT / f"data/live/{key}.jsonl"
    gz = str(base) + ".gz"
    return gz if glob.glob(gz) else str(base) if base.exists() else None


def run(eng):
    tick_from = int(time.time()) - REPLAY
    old = previous_hour()
    if old:
        try:
            with (gzip.open(old, "rt") if old.endswith(".gz") else open(old)) as h:
                for raw in h:
                    feed(eng, raw, tick_from)
        except (OSError, EOFError, zlib.error) as e:
            print("skipped", old, repr(e), flush=True)
    while True:
        cur = newest_plain()
        if not cur:
            eng.replaying = False                  # collector has written nothing; the page says so
            time.sleep(1)
            continue
        try:
            h = open(cur)
        except FileNotFoundError:                  # compressed between glob and open
            continue
        with h:
            buf = ""
            draining = False
            while True:
                chunk = h.readline()
                if chunk:
                    buf += chunk
                    if buf.endswith("\n"):
                        feed(eng, buf, tick_from)
                        buf = ""
                    continue
                if draining:                       # old file fully read after the hour rolled
                    break
                eng.replaying = False              # caught up with the live file
                if (newest_plain() or cur) != cur:
                    draining = True
                    continue
                time.sleep(0.05)


def tail_changes(eng):
    """Follow data/live_pc from now on; only the live book needs it, and each snapshot resets it anyway."""
    newest = lambda: max(glob.glob(str(ROOT / "data/live_pc/*.jsonl")), default=None)
    start = True
    while True:
        cur = newest()
        if not cur:
            time.sleep(1)
            continue
        try:
            h = open(cur)
        except FileNotFoundError:                  # compressed between glob and open
            continue
        with h:
            if start:
                h.seek(0, 2)
                start = False
            buf = ""
            while True:
                buf += h.readline()
                if buf.endswith("\n"):
                    try:
                        m = json.loads(buf)["m"]
                    except ValueError:             # started in the middle of a line
                        m = None
                    buf = ""
                    if m:
                        with lock:
                            try:
                                eng.change(m)
                            except Exception as e:
                                eng.note(e)
                    continue
                if newest() != cur:                # the collector closes an hour's file before opening the next
                    last = h.readline()
                    if not last:
                        break
                    buf += last
                    continue
                time.sleep(.05)


def guarded(eng):
    try:
        run(eng)
    except Exception as e:
        traceback.print_exc()
        eng.fatal = "读取线程已停止：" + repr(e)


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == "/state":
            t = parse_qs(url.query).get("t", [""])[0]
            with lock:
                body = json.dumps(eng.snapshot(int(t) if t.isdigit() else None)).encode()
            ctype = "application/json"
        elif url.path == "/":
            body = (ROOT / "src/dashboard.html").read_bytes()
            ctype = "text/html; charset=utf-8"
        else:
            self.send_error(404); return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    print("seeding Binance history...", flush=True)
    eng = Engine(seed(int(time.time())))
    threading.Thread(target=guarded, args=(eng,), daemon=True).start()
    threading.Thread(target=refresh_rules, daemon=True).start()
    threading.Thread(target=tail_changes, args=(eng,), daemon=True).start()
    print(f"http://127.0.0.1:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
