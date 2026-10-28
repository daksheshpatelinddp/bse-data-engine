"""
Raw (unadjusted) BSE prices, stored in git as SMALL parquet files:

    data/raw/<year>/<yyyy-mm>.parquet        one file per past month (~2-4 MB)
    data/raw/<year>/<yyyy-mm-dd>.parquet     one file per day of the CURRENT month (~100 KB); merged into the month file on month change

So no file ever comes near GitHub's 100 MB limit, and a daily commit adds only one ~100 KB file.
Columns on disk: scrip_code (int32), date (text), open/high/low/close/prev_close as integer paise (int32), volume (int64).
On read they come back as scrip_code text and float rupees.
"""
import hashlib
import os
import re
import sys
import tempfile

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pio
from common import RAW_DIR, period_of, period_sort_key

PRICE_COLS = ["open", "high", "low", "close", "prev_close"]
RAW_COLUMNS = ["scrip_code", "date"] + PRICE_COLS + ["volume"]
_FILE = re.compile(r"^(\d{4}-\d{2})(-\d{2})?\.parquet$")


def _encode(df):
    out = pd.DataFrame({"scrip_code": pd.to_numeric(df["scrip_code"], errors="coerce").astype("int32"),
                        "date": df["date"].astype(str)})
    for c in PRICE_COLS:
        v = pd.to_numeric(df[c], errors="coerce")
        out[c] = np.rint(v.fillna(0).values * 100).astype("int32")
    out["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64").values
    return out


def _decode(df):
    out = pd.DataFrame({"scrip_code": df["scrip_code"].astype(str), "date": df["date"].astype(str)})
    for c in PRICE_COLS:
        out[c] = df[c].values.astype("float64") / 100.0
    out["volume"] = df["volume"].values.astype("int64")
    return out


def _write(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=".parquet", dir=os.path.dirname(path))
    os.close(fd)
    pio.write_path(_encode(df).sort_values(["scrip_code", "date"]), tmp, row_group_size=20000, compression="zstd")
    os.replace(tmp, path)


def write_day(df, date_str):
    _write(df[RAW_COLUMNS], os.path.join(RAW_DIR, date_str[:4], f"{date_str}.parquet"))


def _files():
    """[(path, 'YYYY-MM', 'YYYY-MM-DD' or None)] of every raw file."""
    out = []
    if not os.path.isdir(RAW_DIR):
        return out
    for y in sorted(os.listdir(RAW_DIR)):
        d = os.path.join(RAW_DIR, y)
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            m = _FILE.match(f)
            if m:
                out.append((os.path.join(d, f), m.group(1), f[:-8] if m.group(2) else None))
    return out


def periods():
    return sorted({period_of(month + "-01") for _, month, _ in _files()}, key=period_sort_key)


def _period_files(period):
    return [(p, m, day) for p, m, day in _files() if period_of(m + "-01") == period]


def read_period(period, columns=None):
    """All raw rows of one period (a day file wins over the month file when both hold the same day)."""
    files = sorted(_period_files(period), key=lambda t: (t[2] is not None, t[1], t[2] or ""))     # month files first, then day files
    frames = [_decode(pio.read_path(p)) for p, _, _ in files]
    if not frames:
        return pd.DataFrame(columns=RAW_COLUMNS)
    df = pd.concat(frames, ignore_index=True).drop_duplicates(["scrip_code", "date"], keep="last")
    return df[columns] if columns else df


def period_hash(period):
    h = hashlib.sha1()
    for p, m, day in _period_files(period):
        with open(p, "rb") as f:
            h.update(f"{m}|{day}|".encode() + hashlib.sha1(f.read()).digest())
    return h.hexdigest()


def consolidate(current_month):
    """Merges the day files of every month BEFORE current_month ('YYYY-MM') into one month file. Returns how many months were merged."""
    by_month = {}
    for p, month, day in _files():
        by_month.setdefault(month, []).append((p, day))
    merged = 0
    for month, items in sorted(by_month.items()):
        days = [p for p, d in items if d]
        if month >= current_month or not days:
            continue
        month_path = os.path.join(RAW_DIR, month[:4], f"{month}.parquet")
        frames = ([_decode(pio.read_path(month_path))] if os.path.exists(month_path) else []) + [_decode(pio.read_path(p)) for p in sorted(days)]
        df = pd.concat(frames, ignore_index=True).drop_duplicates(["scrip_code", "date"], keep="last")
        _write(df, month_path)
        for p in days:
            os.remove(p)
        merged += 1
    return merged
