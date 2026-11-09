"""Compact earlier Data-API trade dumps into data/raw/trades.parquet (BTC 5m, post-TWAP only).

Columns: t (window start), ts, taker (1/0), side, outcome (0=Up,1=Down), price, size
Usage: python src/import_trades.py dump1.jsonl [dump2.jsonl ...]
"""
import json, sys
from pathlib import Path
import pandas as pd

OUT = Path(__file__).resolve().parents[1] / "data/raw/trades.parquet"
rows, seen = [], set()
for fn in sys.argv[1:]:
    for line in open(fn):
        d = json.loads(line)
        if d["w"] != "5m" or d["reg"] != "post" or d["t"] in seen or any(d["trunc"]):
            continue
        seen.add(d["t"])
        rows += [(d["t"], r[0], r[2], r[3], r[4], float(r[5]), float(r[6])) for r in d["rows"]]
df = pd.DataFrame(rows, columns=["t", "ts", "taker", "side", "outcome", "price", "size"])
df.to_parquet(OUT, index=False)
print(f"{len(seen)} windows, {len(df)} rows -> {OUT}")
