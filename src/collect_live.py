"""Live read-only collector (no auth, no orders).

Streams, each line = JSON with local receive time `rx` (ms):
  pm   : Polymarket CLOB market WS per BTC 5m window, T-120s to T+420s
         (best_bid_ask, book, last_trade_price, tick_size_change, market_resolved)
  pc   : price_change, compacted to the Up token, in data/live_pc/ (the flood is kept apart so readers of data/live stay fast)
  cl   : legacy RTDS crypto_prices_chainlink + crypto_prices_twap_sixty (btc/usd)
  bn   : Binance BTCUSDT bookTicker (only when bid/ask changes) + aggTrade
Files: data/live/YYYYMMDD_HH.jsonl (current hour), .jsonl.gz once the hour is done. Run: python src/collect_live.py
"""
import asyncio, atexit, fcntl, gzip, json, os, shutil, signal, sys, threading, time, datetime as D
from pathlib import Path
import requests, websockets

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "data/live"; DIR.mkdir(parents=True, exist_ok=True)
PC_DIR = ROOT / "data/live_pc"; PC_DIR.mkdir(parents=True, exist_ok=True)
LEAD, TAIL = 120, 420                                     # seconds before T / after T per window
KEEP = {"best_bid_ask", "book", "last_trade_price", "tick_size_change", "market_resolved"}


SILENCE = 20                                              # reconnect a socket quiet for this long


def compress(path):
    """gzip a finished hourly file. The .gz only appears complete; readers never see a partial one."""
    part = path.with_suffix(".gzpart")
    with open(path, "rb") as a, gzip.open(part, "wb") as b:
        shutil.copyfileobj(a, b, 1 << 20)
    os.replace(part, str(path) + ".gz")
    path.unlink()


def hour_key(now):
    return D.datetime.fromtimestamp(now, D.timezone.utc).strftime("%Y%m%d_%H")


def leftovers():
    """Hours left behind by a killed collector. Listed before any Sink opens, so a Sink can never reach them."""
    current = hour_key(time.time())
    return [p for d in (DIR, PC_DIR) for p in d.glob("*.jsonl") if p.stem < current]


def sweep(paths):
    for path in paths:
        compress(path)


class Sink:
    def __init__(self, folder, buffering):
        self.folder, self.buffering = folder, buffering
        self.key, self.f = None, None
        self.lock = threading.Lock()              # also written from the Gamma worker thread

    def write(self, src, msg):
        body = json.dumps(msg, separators=(",", ":"))
        with self.lock:                           # clock read under the lock so the hour can't step backwards
            now = time.time()
            key = hour_key(now)
            if key != self.key:
                if self.f:
                    self.f.close()
                    threading.Thread(target=compress, args=(Path(self.f.name),)).start()
                self.key, self.f = key, open(self.folder / f"{key}.jsonl", "a", buffering=self.buffering)
            self.f.write(f'{{"src":"{src}","rx":{int(now * 1000)},"m":{body}}}\n')


sink = Sink(DIR, 1)                                       # line buffered: the page tails this file
pc_sink = Sink(PC_DIR, 1 << 20)


def changes(T, up, x):
    """Keep what a book rebuild needs. Down is the mirror image of Up, so one side is enough."""
    c = [[e["price"], e["size"], e["side"]] for e in x.get("price_changes", []) if e.get("asset_id") == up]
    if c:
        pc_sink.write("pc", {"T": T, "ts": int(x["timestamp"]), "c": c})


async def keepalive(w, heard):
    """PING on a timer; drop a socket that goes quiet while PONGs may still arrive."""
    while True:
        await asyncio.sleep(8)
        if time.time() - heard[0] > SILENCE:
            await w.close()
            return
        await w.send("PING")


def tokens(t):
    r = requests.get(f"https://gamma-api.polymarket.com/markets?slug=btc-updown-5m-{t}", timeout=15).json()
    if not r:
        return []
    sink.write("meta", {"t": t, "conditionId": r[0]["conditionId"], "tokens": json.loads(r[0]["clobTokenIds"]),
                        "outcomes": r[0]["outcomes"], "feeSchedule": r[0].get("feeSchedule")})
    return json.loads(r[0]["clobTokenIds"])


async def window(T):
    """One connection per window, from T-LEAD to T+TAIL; connections overlap, so no gap at rollover."""
    toks = []
    while not toks and time.time() < T + TAIL:
        try:
            toks = await asyncio.to_thread(tokens, T)
        except Exception as e:
            sink.write("err", {"src": "gamma", "e": repr(e)})
        if not toks:
            await asyncio.sleep(3)
    while time.time() < T + TAIL:
        opened = time.time()
        try:
            # compression=None: inflating ~400 KB/s of price_change in Python made busy sockets lag (p99 6.5 s vs 1.3 s).
            async with websockets.connect("wss://ws-subscriptions-clob.polymarket.com/ws/market", compression=None,
                                          ping_interval=None, max_size=2 ** 24, close_timeout=1) as w:
                await w.send(json.dumps({"assets_ids": toks, "type": "market", "custom_feature_enabled": True}))
                heard = [time.time()]
                aux = asyncio.create_task(keepalive(w, heard))
                try:
                    async with asyncio.timeout_at(asyncio.get_running_loop().time() + T + TAIL - time.time()):
                        while True:
                            raw = await w.recv()
                            if raw == "PONG":
                                continue
                            heard[0] = time.time()
                            data = json.loads(raw)
                            for x in data if isinstance(data, list) else [data]:
                                e = x.get("event_type")
                                if e == "price_change":
                                    changes(T, toks[0], x)
                                elif e in KEEP:
                                    sink.write("pm", x)
                except TimeoutError:
                    return                                # window is over
                finally:
                    aux.cancel()
        except websockets.ConnectionClosedOK as e:
            sink.write("err", {"src": "pm", "t": T, "e": repr(e)})
            if "resolved" in repr(e):
                return                                    # market is over; nothing left to subscribe to
        except Exception as e:
            sink.write("err", {"src": "pm", "t": T, "e": repr(e)})
        if time.time() - opened < 5:                      # reconnect at once unless the server keeps refusing
            await asyncio.sleep(1)


async def polymarket():
    tasks = {}
    while True:
        T = (int(time.time()) + LEAD) // 300 * 300        # newest window within LEAD seconds of opening
        for t in (T - 300, T):
            if t + TAIL > time.time() and t not in tasks:
                tasks[t] = asyncio.create_task(window(t))
        for t in [k for k, v in tasks.items() if v.done() and k + TAIL < time.time()]:
            del tasks[t]
        await asyncio.sleep(1)


async def chainlink():
    sub = {"action": "subscribe", "subscriptions": [
        {"topic": "crypto_prices_chainlink", "type": "*", "filters": '{"symbol":"btc/usd"}'},
        {"topic": "crypto_prices_twap_sixty", "type": "*", "filters": '{"symbol":"btc/usd"}'}]}
    while True:
        try:
            async with websockets.connect("wss://ws-live-data.polymarket.com", ping_interval=None) as w:
                await w.send(json.dumps(sub)); last = heard = time.time()
                while True:
                    if time.time() - last > 5:
                        await w.send("PING"); last = time.time()
                    if time.time() - heard > SILENCE:     # half-open socket: PING succeeds, nothing arrives
                        raise TimeoutError(f"no message for {SILENCE}s")
                    try:
                        raw = await asyncio.wait_for(w.recv(), 2)
                    except asyncio.TimeoutError:
                        continue
                    if raw and raw[0] == "{":
                        heard = time.time()
                        sink.write("cl", json.loads(raw))
        except Exception as e:
            sink.write("err", {"src": "cl", "e": repr(e)}); await asyncio.sleep(2)


async def binance():
    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade"
    while True:
        try:
            async with websockets.connect(url) as w:
                prev = None
                async for raw in w:
                    x = json.loads(raw); d = x["data"]
                    if x["stream"].endswith("bookTicker"):
                        q = (d["b"], d["a"])
                        if q == prev:
                            continue
                        prev = q
                        sink.write("bn", {"k": "q", "b": d["b"], "B": d["B"], "a": d["a"], "A": d["A"], "u": d["u"]})
                    else:
                        sink.write("bn", {"k": "t", "p": d["p"], "q": d["q"], "T": d["T"], "m": d["m"]})
        except Exception as e:
            sink.write("err", {"src": "bn", "e": repr(e)}); await asyncio.sleep(2)


async def flush_pc():
    """The page rebuilds the live book from this file; flushing a few times a second costs little."""
    while True:
        await asyncio.sleep(.25)
        with pc_sink.lock:
            if pc_sink.f:
                pc_sink.f.flush()


async def main():
    threading.Thread(target=sweep, args=(leftovers(),), daemon=True).start()
    await asyncio.gather(polymarket(), chainlink(), binance(), flush_pc())


if __name__ == "__main__":
    # Two collectors would double every line and corrupt each other's compression.
    instance = open(DIR / ".collector.lock", "w")
    try:
        fcntl.flock(instance, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("collect_live.py is already running")
    atexit.register(lambda: pc_sink.f and pc_sink.f.close())   # up to 1 MiB of price_change sits in the buffer
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    asyncio.run(main())
