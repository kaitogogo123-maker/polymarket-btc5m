"""Backfill official BTC 5m labels (read-only public Gamma API).

Output: data/raw/labels.parquet with columns
  t (window start unix), up (1/0), price_to_beat, final_price, config_id
Usage: python src/fetch_labels.py 2026-08-14 2026-11-04
"""
import json, sys, time, datetime as D, concurrent.futures as F
from pathlib import Path
import pandas as pd, requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/raw/labels.parquet"
S = requests.Session(); S.headers["User-Agent"] = "Mozilla/5.0 research"


def get(url):
    for i in range(6):
        try:
            r = S.get(url, timeout=30)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException:
            pass
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(url)


def parse(m):
    op = json.loads(m.get("outcomePrices") or "[]")
    if len(op) != 2 or op[0] not in ("0", "1") or op[1] not in ("0", "1"):
        return None
    ev = (m.get("events") or [{}])[0]
    meta = ev.get("eventMetadata") or {}
    t = int(m["slug"].rsplit("-", 1)[1])
    return dict(t=t, up=int(op[0] == "1"),
                price_to_beat=meta.get("priceToBeat"), final_price=meta.get("finalPrice"),
                config_id=m.get("cryptoMarketConfigId"), condition_id=m.get("conditionId"),
                token_up=json.loads(m.get("clobTokenIds") or "[null,null]")[0])


def batch(ts):
    q = "&".join(f"slug=btc-updown-5m-{t}" for t in ts)
    return [r for r in (parse(m) for m in get(f"https://gamma-api.polymarket.com/markets?{q}&closed=true&limit=50")) if r]


def main(a, b):
    utc = lambda s: int(D.datetime.fromisoformat(s).replace(tzinfo=D.timezone.utc).timestamp())
    old = pd.read_parquet(OUT) if OUT.exists() else pd.DataFrame(columns=["t"])
    have = set(old.t.astype(int))
    todo = [t for t in range(utc(a), min(utc(b), int(time.time()) - 600), 300) if t not in have]
    chunks = [todo[i:i + 25] for i in range(0, len(todo), 25)]
    rows = []
    with F.ThreadPoolExecutor(6) as ex:
        for i, r in enumerate(ex.map(batch, chunks)):
            rows += r
            if i % 100 == 0:
                print(f"{i}/{len(chunks)} rows={len(rows)}", flush=True)
    df = pd.concat([old, pd.DataFrame(rows)], ignore_index=True).drop_duplicates("t").sort_values("t")
    for c in ("price_to_beat", "final_price"):
        df[c] = pd.to_numeric(df[c])
    df.to_parquet(OUT, index=False)
    ok = df.dropna(subset=["price_to_beat", "final_price"])
    bad = ((ok.final_price >= ok.price_to_beat).astype(int) != ok.up).sum()
    print(f"saved {len(df)} labels; with prices {len(ok)}; rule mismatches {bad}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
