"""
BSE build step: raw prices (R2: bse/raw/*) -> adjusted files the AmiBroker web engine reads, in the SAME layout as the NSE database:

    bse/<period>.parquet                 all stocks of one period (year by default; PERIOD_MODE=half -> 2026_H1.parquet), adjusted
    bse/h/<fingerprint>/bNNN.parquet     history buckets (~64 stocks each, one stock = one row group)
    bse/p/<fingerprint>/bNNN.pack        the same history in the compact pack format (one range request per stock)
    bse/manifest.json                    which stock is in which bucket, byte positions of the packs, symbol aliases
    bse/r/<token>.pack + bse/recent.json last 400 bars of every active stock (scans read just this file)
    bse/symbols.json                     symbol search list           bse/scrips.json   symbol -> BSE scrip code, company, ISIN

Differences to the NSE build:
  * input is raw parquet in R2, not .db files in git, so no data file ever has to fit under GitHub's 100 MB limit
  * a stock is identified by its BSE scrip code; every row carries the stock's LATEST symbol (renames join automatically)
  * a history bucket is rebuilt only when its own stocks (or the raw past data) changed; unchanged buckets are copied inside R2,
    so a new dividend of one company does not rewrite the whole history

Environment: FORCE_ALL=true (rebuild every period file and every bucket), HIST_BUCKET_SIZE (64), RECENT_BARS (400),
ADJUST_DIVIDENDS, PRICE_ROUND, PERIOD_MODE.
"""
import datetime
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import actions
import pio
from common import (DB_ID, PERIOD_RE, PERIOD_MODE, RAW_PREFIX, SCRIPS_KEY, period_sort_key, period_start, raw_key, period_of)
from packs import PACK_VERSION, pack_group
from storage import get_store

FORCE_ALL = os.getenv("FORCE_ALL", "false").strip().lower() == "true"
HIST_BUCKET_SIZE = int(os.getenv("HIST_BUCKET_SIZE", "64") or 64)
HIST_LAYOUT_VERSION = 2                         # same manifest format number as the NSE engine
RECENT_BARS = int(os.getenv("RECENT_BARS", "400") or 400)
HIST_SCHEMA = pa.schema([("symbol", pa.string()), ("date", pa.string()),
                         ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
                         ("volume", pa.int64()), ("delivery", pa.float64())])
OUT_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "volume", "delivery"]
RAW_NEEDED = ["scrip_code", "date", "open", "high", "low", "close", "volume"]

DONE, FAILED = [], []


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def list_periods(store):
    out = []
    for key, size, etag in store.list(RAW_PREFIX):
        m = re.match(r"^" + re.escape(RAW_PREFIX) + r"(\d{4}(?:_H[12])?)\.parquet$", key)
        if m:
            out.append((m.group(1), size, etag))
    return sorted(out, key=lambda t: period_sort_key(t[0]))


def resolve_symbols(scrips):
    """{scrip code: unique latest symbol}. Two codes with the same symbol: the newer one keeps it, the other becomes SYMBOL-CODE."""
    out, owner = {}, {}
    for code, s in sorted(scrips.items(), key=lambda kv: (kv[1].get("last", ""), kv[0]), reverse=True):
        sym = s["symbol"] or code
        if sym in owner:
            sym = f"{sym}-{code}"
        owner[sym] = code
        out[code] = sym
    return out


def to_out(df, sym_of, adj):
    """raw frame (scrip_code, date, o,h,l,c, volume) -> adjusted frame with the output columns."""
    df = df[(df["close"] > 0) & (df["open"] > 0)]
    df = adj.adjust(df) if adj else df
    df = df.assign(symbol=df["scrip_code"].map(sym_of))
    df = df[df["symbol"].notna()].copy()
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    df["delivery"] = np.float64("nan")
    df["delivery"] = df["delivery"].astype("float64")
    return df.sort_values(["symbol", "date"]).reset_index(drop=True)[OUT_COLUMNS]


# ----------------------------------------------------------------------------------------------------------------------
# period files
# ----------------------------------------------------------------------------------------------------------------------
def build_period_file(store, period, sym_of, adj):
    df = pio.read_bytes(store.get(raw_key(period)), columns=RAW_NEEDED)
    out = to_out(df, sym_of, adj)
    if out.empty:
        print(f"[PERIOD] {period}: no rows")
        return None, out
    key = f"{DB_ID}/{period}.parquet"
    fd, p = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        pio.write_path(out, p, row_group_size=5000)
        mb = os.path.getsize(p) / 1048576
        groups = pq.ParquetFile(p).num_row_groups
        store.upload_file(p, key)
    finally:
        os.remove(p)
    print(f"[PERIOD] uploaded {key}: {len(out)} rows, {mb:.1f} MB")
    DONE.append((f"{DB_ID}/{period}", len(out), round(mb, 1), groups))
    return key, out


# ----------------------------------------------------------------------------------------------------------------------
# history buckets + packs
# ----------------------------------------------------------------------------------------------------------------------
def read_manifest(store):
    body = store.get(f"{DB_ID}/manifest.json")
    try:
        return json.loads(body) if body else None
    except ValueError:
        return None


def plan_buckets(codes, sym_of, old):
    """Keeps every stock in the bucket it already has (so unchanged buckets can be reused); new stocks go to the last / new buckets."""
    codes = set(codes)
    buckets = []
    if old and old.get("bucketSize") == HIST_BUCKET_SIZE and old.get("bucketCodes"):
        for b in sorted(old["bucketCodes"]):
            buckets.append([c for c in old["bucketCodes"][b] if c in codes])
    placed = {c for b in buckets for c in b}
    new = sorted(codes - placed, key=lambda c: (sym_of[c], c))
    for c in new:
        if not buckets or len(buckets[-1]) >= HIST_BUCKET_SIZE:
            buckets.append([])
        buckets[-1].append(c)
    return buckets


def build_history(store, periods, scrips, sym_of, adj, aliases, old):
    cur = periods[-1][0]
    hist = [(p, s, e) for p, s, e in periods if p != cur]
    if not hist:
        print(f"[HISTORY] {DB_ID}: only one period so far, nothing to build.")
        return None
    cur_start = period_start(cur)
    codes = [c for c, s in scrips.items() if s.get("first", "9") < cur_start and c in sym_of]
    buckets = plan_buckets(codes, sym_of, old)
    raw_fp = hashlib.sha1("".join(f"{p}:{s}:{e};" for p, s, e in hist).encode()).hexdigest()

    names = [f"b{i:03d}" for i in range(len(buckets))]
    hashes = {}
    for bname, cl in zip(names, buckets):
        h = hashlib.sha1()
        h.update(f"{raw_fp}|layout{HIST_LAYOUT_VERSION}|pack{PACK_VERSION}|round{actions.ROUND_TO}|".encode())
        h.update("".join(f"{c}={sym_of[c]};" for c in sorted(cl)).encode())
        h.update(adj.signature_for(cl).encode() if adj else b"")
        hashes[bname] = h.hexdigest()
    fp = hashlib.sha1((";".join(f"{n}={hashes[n]}" for n in names) + f"|{cur}|{PERIOD_MODE}").encode()).hexdigest()
    fp10 = fp[:10]
    if (not FORCE_ALL and old and old.get("format") == HIST_LAYOUT_VERSION and old.get("fingerprint") == fp and old.get("currentPeriod") == cur):
        print(f"[HISTORY] {DB_ID}: unchanged (fingerprint {fp10}), nothing to rebuild.")
        return None

    old_b = (old or {}).get("buckets", {})
    old_p = ((old or {}).get("packs") or {}).get("buckets", {})
    old_h_path = (old or {}).get("histPath")
    old_p_path = ((old or {}).get("packs") or {}).get("path")
    reuse = {}
    if not FORCE_ALL and old_h_path and old_p_path:
        for n in names:
            if old_b.get(n, {}).get("hash") == hashes[n] and n in old_p:
                reuse[n] = True
    todo = [n for n in names if n not in reuse]
    print(f"[HISTORY] {DB_ID}: {len(names)} buckets: {len(reuse)} reused (copied inside R2), {len(todo)} rebuilt")

    tmp = tempfile.mkdtemp(prefix="hist_")
    try:
        # spill the rows of the rebuilt buckets to disk, one small file per (bucket, period)
        bucket_of = {c: n for n, cl in zip(names, buckets) if n in todo for c in cl}
        if bucket_of:
            for p, _, _ in hist:
                df = pio.read_bytes(store.get(raw_key(p)), columns=RAW_NEEDED)
                df = df[df["scrip_code"].isin(bucket_of)]
                if df.empty:
                    continue
                b = df["scrip_code"].map(bucket_of)
                for n, g in df.groupby(b):
                    os.makedirs(os.path.join(tmp, n), exist_ok=True)
                    pio.write_path(g, os.path.join(tmp, n, f"{p}.parquet"))
        manifest_buckets, pack_buckets, total_rows, total_bytes, pack_total, total_groups = {}, {}, 0, 0, 0, 0
        for n, cl in zip(names, buckets):
            if n in reuse:
                store.copy(f"{DB_ID}/{old_h_path}/{n}.parquet", f"{DB_ID}/h/{fp10}/{n}.parquet")
                store.copy(f"{DB_ID}/{old_p_path}/{n}.pack", f"{DB_ID}/p/{fp10}/{n}.pack")
                manifest_buckets[n] = {**old_b[n], "hash": hashes[n]}
                pack_buckets[n] = old_p[n]
                total_bytes += old_b[n].get("size", 0)
                pack_total += old_p[n].get("size", 0)
                total_rows += sum(v[2] for v in old_p[n]["idx"].values())
                total_groups += len(old_p[n]["idx"])
                continue
            part_dir = os.path.join(tmp, n)
            parts = [pio.read_path(os.path.join(part_dir, f)) for f in sorted(os.listdir(part_dir))] if os.path.isdir(part_dir) else []
            if not parts:
                continue
            df = to_out_with_code(pd.concat(parts, ignore_index=True), sym_of, adj)
            local = os.path.join(tmp, f"{n}.out.parquet")
            idx, blobs, off = {}, [], 0
            with pq.ParquetWriter(local, HIST_SCHEMA, compression="snappy") as w:
                for sym, g in df.groupby("symbol", sort=True):
                    w.write_table(pa.Table.from_pandas(g, schema=HIST_SCHEMA, preserve_index=False), row_group_size=len(g))
                    blob = pack_group(g)
                    idx[sym] = [off, len(blob), len(g)]
                    blobs.append(blob)
                    off += len(blob)
            groups = pq.ParquetFile(local).num_row_groups
            if groups != df["symbol"].nunique():
                raise RuntimeError(f"{n}: expected {df['symbol'].nunique()} row groups, got {groups}")
            pack = b"".join(blobs)
            store.put(f"{DB_ID}/p/{fp10}/{n}.pack", pack, "application/octet-stream")
            store.upload_file(local, f"{DB_ID}/h/{fp10}/{n}.parquet")
            size = os.path.getsize(local)
            os.remove(local)
            manifest_buckets[n] = {"size": size, "symbols": sorted(idx), "hash": hashes[n]}
            pack_buckets[n] = {"size": len(pack), "idx": idx}
            total_rows += len(df); total_bytes += size; pack_total += len(pack); total_groups += groups
        manifest = {
            "format": HIST_LAYOUT_VERSION, "db": DB_ID, "periodMode": PERIOD_MODE,
            "firstYear": int(hist[0][0][:4]), "histThrough": int(hist[-1][0][:4]), "currentYear": int(cur[:4]), "currentPeriod": cur,
            "fingerprint": fp, "histPath": f"h/{fp10}", "bucketSize": HIST_BUCKET_SIZE, "built": now_iso(), "rows": total_rows,
            "buckets": manifest_buckets, "bucketCodes": {n: cl for n, cl in zip(names, buckets) if n in manifest_buckets},
            "packs": {"version": PACK_VERSION, "path": f"p/{fp10}", "buckets": pack_buckets},
            "aliases": aliases, "indices": [],
        }
        store.put(f"{DB_ID}/manifest.json", json.dumps(manifest, separators=(",", ":")).encode(), "application/json", "no-cache")   # last: the app keeps the old complete history until now
        stale = [k for folder in ("h", "p") for k, _, _ in store.list(f"{DB_ID}/{folder}/") if not k.startswith(f"{DB_ID}/{folder}/{fp10}/")]
        store.delete(stale)
        print(f"[HISTORY] done: {sum(len(v['symbols']) for v in manifest_buckets.values())} stocks in {len(manifest_buckets)} buckets, "
              f"{total_rows} rows, {total_bytes / 1048576:.1f} MB parquet + {pack_total / 1048576:.1f} MB packs; removed {len(stale)} old files")
        return (f"{DB_ID}/history", total_rows, round(total_bytes / 1048576, 1), total_groups)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def to_out_with_code(df, sym_of, adj):
    return to_out(df, sym_of, adj)


# ----------------------------------------------------------------------------------------------------------------------
# recent pack
# ----------------------------------------------------------------------------------------------------------------------
def build_recent(store, periods, sym_of, adj):
    frames = [pio.read_bytes(store.get(raw_key(p)), columns=RAW_NEEDED) for p, _, _ in periods[-3:]]
    raw = pd.concat(frames, ignore_index=True)
    last = raw["date"].max()
    cut = (datetime.datetime.strptime(last, "%Y-%m-%d") - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    active = set(raw.loc[raw["date"] >= cut, "scrip_code"])
    df = to_out(raw[raw["scrip_code"].isin(active)], sym_of, adj)
    df = df.groupby("symbol", sort=True).tail(RECENT_BARS)
    idx, parts, off = {}, [], 0
    for sym, g in df.groupby("symbol", sort=True):
        blob = pack_group(g)
        idx[sym] = [off, len(blob), len(g)]
        parts.append(blob)
        off += len(blob)
    pack = b"".join(parts)
    token = hashlib.sha1(pack).hexdigest()[:12]
    old = None
    body = store.get(f"{DB_ID}/recent.json")
    if body:
        try:
            old = json.loads(body)
        except ValueError:
            pass
    if old and old.get("token") == token and not FORCE_ALL:
        print(f"[RECENT] unchanged ({len(idx)} stocks, token {token}).")
        return None
    store.put(f"{DB_ID}/r/{token}.pack", pack, "application/octet-stream")
    meta = {"format": 1, "db": DB_ID, "token": token, "file": f"r/{token}.pack", "size": len(pack), "bars": RECENT_BARS,
            "currentYear": int(periods[-1][0][:4]), "currentPeriod": periods[-1][0], "stocks": len(idx), "built": now_iso(), "idx": idx}
    store.put(f"{DB_ID}/recent.json", json.dumps(meta, separators=(",", ":")).encode(), "application/json", "no-cache")
    store.delete([k for k, _, _ in store.list(f"{DB_ID}/r/") if k != f"{DB_ID}/r/{token}.pack"])
    print(f"[RECENT] {len(idx)} stocks x last {RECENT_BARS} bars, {len(pack) / 1048576:.1f} MB (token {token})")
    return (f"{DB_ID}/recent", int(sum(v[2] for v in idx.values())), round(len(pack) / 1048576, 1), len(idx))


def build_aliases(scrips, sym_of):
    cur = set(sym_of.values())
    out = {}
    for code, s in scrips.items():
        sym = sym_of.get(code)
        if not sym:
            continue
        out[code] = sym                                           # typing the BSE scrip code (e.g. 500325) finds the stock
        for old in s.get("prev", []):
            if old and old not in cur and old != sym:
                out[old] = sym                                    # an older symbol finds the renamed stock
    return out


# ----------------------------------------------------------------------------------------------------------------------
def run(store=None):
    store = store or get_store()
    periods = list_periods(store)
    if not periods:
        print(f"No raw files under {RAW_PREFIX}. Run the BSE fetch first.")
        return
    cur = periods[-1][0]
    body = store.get(SCRIPS_KEY)
    scrips = json.loads(body) if body else {}
    sym_of = resolve_symbols(scrips)
    print(f"[BUILD] periods {periods[0][0]} .. {cur} ({len(periods)} raw files), {len(scrips)} scrips, mode={PERIOD_MODE}")

    factors, report = actions.compute_all(store, [p for p, _, _ in periods], known_codes=set(scrips), force=os.getenv("RECOMPUTE_FACTORS", "").lower() == "true")
    for line in report:
        print("[ADJUST]", line)
    adj = actions.Adjuster(factors)
    print(f"[ADJUST] {len(factors)} adjustment events over {len(adj.by_code)} stocks")

    existing = {k for k, _, _ in store.list(f"{DB_ID}/")}
    cur_out = None
    for p, _, _ in periods:
        if p == cur or FORCE_ALL or f"{DB_ID}/{p}.parquet" not in existing:
            try:
                _, out = build_period_file(store, p, sym_of, adj)
                if p == cur:
                    cur_out = out
            except Exception as e:
                print(f"[PERIOD] ERROR {p}: {e}")
                FAILED.append(f"{DB_ID}/{p}")

    aliases = build_aliases(scrips, sym_of)
    try:
        res = build_history(store, periods, scrips, sym_of, adj, aliases, read_manifest(store))
        if res:
            DONE.append(res)
    except Exception as e:
        print(f"[HISTORY] ERROR: {e}")
        FAILED.append(f"{DB_ID}/history")
    try:
        res = build_recent(store, periods, sym_of, adj)
        if res:
            DONE.append(res)
    except Exception as e:
        print(f"[RECENT] ERROR: {e}")
        FAILED.append(f"{DB_ID}/recent")

    if cur_out is not None and len(cur_out):
        names = sorted(set(cur_out["symbol"].astype(str)))
        store.put(f"{DB_ID}/symbols.json", json.dumps({"year": int(cur[:4]), "period": cur, "symbols": names, "indices": []}, separators=(",", ":")).encode(),
                  "application/json", "no-cache")
        print(f"[SYMBOLS] {DB_ID}/symbols.json ({len(names)} symbols)")
    info = {sym_of[c]: {"code": c, "name": s.get("name", ""), "isin": s.get("isin", ""), "first": s.get("first"), "last": s.get("last")}
            for c, s in scrips.items() if c in sym_of}
    store.put(f"{DB_ID}/scrips.json", json.dumps(info, separators=(",", ":")).encode(), "application/json", "no-cache")


if __name__ == "__main__":
    run()
    print("\n===== SUMMARY =====")
    for k, rows, mb, groups in DONE:
        print(f"OK     {k}: {rows} rows, {mb} MB, {groups} groups")
    for k in FAILED:
        print(f"FAILED {k}")
    if FAILED:
        sys.exit(1)
