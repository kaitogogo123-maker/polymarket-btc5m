"""Copy finished windows and rule results to Postgres, so the research survives this machine.

One row per window: the per-second panel as a compressed npz. Raw hourly files are far too big for the 1 GB plan.
Writes in one short batch an hour, because a constantly awake compute would exhaust the monthly compute hours.
Run: python src/db_sync.py   (connection string in .neon_url)
"""
import io, json, sys
from pathlib import Path
import numpy as np, psycopg
from book_panel import load

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = """
create table if not exists windows(t bigint primary key, up smallint, panel bytea not null, synced_at timestamptz default now());
create table if not exists kv(k text primary key, v jsonb not null, at timestamptz default now());
"""


def connect():
    return psycopg.connect((ROOT / ".neon_url").read_text().strip())


def pack(P, n):
    buf = io.BytesIO()
    arrays = {k: np.round(v[n], 4).astype("f4") if v.dtype.kind == "f" else v[n] for k, v in P.items() if k not in ("T", "up")}
    np.savez_compressed(buf, **arrays)
    return buf.getvalue()


def unpack(blob):
    with np.load(io.BytesIO(blob)) as z:
        return {k: z[k] for k in z.files}


def sync():
    P = load()
    with connect() as db:
        db.execute(SCHEMA)
        have = {r[0] for r in db.execute("select t from windows")}
        new = [n for n, t in enumerate(P["T"].tolist()) if t not in have]
        with db.cursor() as c:
            for n in new:
                up = None if np.isnan(P["up"][n]) else int(P["up"][n])
                c.execute("insert into windows(t, up, panel) values (%s, %s, %s) on conflict do nothing", (int(P["T"][n]), up, pack(P, n)))
        rules = ROOT / "out/rules.json"
        if rules.exists():
            db.execute("insert into kv(k, v) values ('rules', %s) on conflict (k) do update set v = excluded.v, at = now()", (rules.read_text(),))
        shadow = ROOT / "out/shadow/summary.json"
        if shadow.exists():
            db.execute("insert into kv(k, v) values ('shadow', %s) on conflict (k) do update set v = excluded.v, at = now()", (shadow.read_text(),))
        total, size = db.execute("select count(*), pg_database_size(current_database()) from windows").fetchone()
    print(f"uploaded {len(new)}, total {total} windows, database {size / 1e6:.1f} MB", flush=True)


def load_db():
    """Same arrays as book_panel.load(), read back from the database."""
    with connect() as db:
        rows = db.execute("select t, up, panel from windows order by t").fetchall()
    parts = [unpack(r[2]) for r in rows]
    out = {k: np.stack([p[k] for p in parts]) for k in parts[0]} if parts else {}
    out["T"] = np.array([r[0] for r in rows], dtype=int)
    out["up"] = np.array([np.nan if r[1] is None else r[1] for r in rows], dtype=float)
    return out


if __name__ == "__main__":
    sync()
