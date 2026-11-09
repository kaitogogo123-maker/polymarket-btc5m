"""Push the local page's state to the Cloudflare Worker every second, with a small tick the Worker forwards to open pages. Outbound only; carries no credentials except its own push token.
Run: python src/push_state.py   (token in .push_token, target overridable with PUSH_URL)
"""
import asyncio, json, os, requests, time, websockets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
URL = os.environ.get("PUSH_URL", "wss://polymarket-btc5m.kaitogogo123.workers.dev/push")
LOCAL = "http://127.0.0.1:8791/state"
EVERY = 1
WHOLE = 3     # seconds between whole states: /state is only the page's first load and its fallback; 17 KB a second would be 1.5 GB a day
LIVE = ("now", "T", "ptb", "twap", "spot", "binance", "replaying", "error", "cur", "edge", "age", "quote_age", "quote_lag", "flow", "depth", "ladder")


def log(*a):
    print(time.strftime("%m-%d %H:%M:%S"), *a, flush=True)


def fetch(t=None):
    return requests.get(LOCAL, params={"t": t} if t else None, timeout=5).json()


def make_tick(s, seen):
    """Only what changes each second: about 1 KB, against 17 KB for the whole state."""
    t = {k: s.get(k) for k in LIVE}
    t["points"], t["bars"] = s["points"][-6:], s["bars"][-2:]
    t["tape"] = [r for r in s["tape"] if r["ts"] >= seen.get("ts", 0)]
    if s["tape"]:
        seen["ts"] = s["tape"][0]["ts"]
    for k in ("hist", "rules"):
        text = json.dumps(s[k], sort_keys=True)
        if seen.get(k) != text:
            seen[k] = text
            t[k] = s[k]
    return t


async def main():
    token = (ROOT / ".push_token").read_text().strip()
    sent, wait = {}, 1
    while True:
        try:
            async with websockets.connect(URL, additional_headers={"Authorization": "Bearer " + token}) as w:
                log("push: connected")
                wait = 1
                seen, whole_at, whole_t = {}, 0, None
                while True:
                    began = time.monotonic()
                    try:
                        state = await asyncio.to_thread(fetch)
                    except (requests.RequestException, ValueError) as e:   # the local page restarting is no reason to drop the link
                        log("push: local page", type(e).__name__)
                        await asyncio.sleep(1)
                        continue
                    msg = {"kind": "cur", "tick": make_tick(state, seen)}
                    if began - whole_at >= WHOLE or state["T"] != whole_t:   # a new window at once: pages fetch it the moment the tick says so
                        msg["state"], whole_at, whole_t = state, began, state["T"]
                    await w.send(json.dumps(msg))
                    for h in state["hist"][:KEEP_PAST]:
                        mark = (h["up"], h["ptb_approx"], h["final_approx"])
                        if sent.get(h["T"]) != mark:
                            past = await asyncio.to_thread(fetch, h["T"])
                            if past["view"] == h["T"]:    # older windows are gone locally; keep the stored copy
                                await w.send(json.dumps({"kind": "past", "t": h["T"], "state": past}))
                            sent[h["T"]] = mark
                    await asyncio.sleep(max(0, EVERY - (time.monotonic() - began)))
        except Exception as e:
            log("push:", repr(e)[:200])
            await asyncio.sleep(wait)
            wait = min(wait * 2, 10)


KEEP_PAST = 24
if __name__ == "__main__":
    asyncio.run(main())
