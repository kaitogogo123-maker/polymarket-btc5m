"""Download Binance BTCUSDT 1s klines (public data.binance.vision daily files).

Output: data/binance/YYYY-MM-DD.parquet with columns ts (unix s), close, vol, taker_buy_vol
Usage: python src/fetch_binance.py 2026-08-13 2026-11-03
"""
import io, sys, zipfile, datetime as D
from pathlib import Path
import pandas as pd, requests

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "data/binance"
URL = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-{d}.zip"


def one(d):
    out = DIR / f"{d}.parquet"
    if out.exists():
        return
    r = requests.get(URL.format(d=d), timeout=120)
    if r.status_code != 200:
        print("missing", d, r.status_code)
        return
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        raw = pd.read_csv(z.open(z.namelist()[0]), header=None)
    ts = raw[0].astype("int64")
    ts = ts // (1_000_000 if ts.iloc[0] > 1e14 else 1000)  # files switched to microseconds in 2025
    pd.DataFrame({"ts": ts, "close": raw[4].astype(float), "vol": raw[5].astype(float),
                  "taker_buy_vol": raw[9].astype(float)}).to_parquet(out, index=False)
    print("ok", d, len(raw))


if __name__ == "__main__":
    a, b = (D.date.fromisoformat(x) for x in sys.argv[1:3])
    DIR.mkdir(parents=True, exist_ok=True)
    while a <= b:
        one(a.isoformat()); a += D.timedelta(days=1)
