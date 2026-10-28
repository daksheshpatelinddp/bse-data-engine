"""Shared settings: the database id, the file period (half-year by default) and the raw-data layout."""
import os
import re

DB_ID = "bse"                                    # folder in the bucket: bse/...
PERIOD_MODE = os.getenv("PERIOD_MODE", "year").strip().lower()    # "year" -> 2026.parquet (same as NSE)   "half" -> 2026_H1.parquet
RAW_PREFIX = f"{DB_ID}/raw/"                     # unadjusted prices, one file per period
SCRIPS_KEY = f"{DB_ID}/raw/scrips.json"          # scrip code -> names, first/last date
STATE_KEY = f"{DB_ID}/raw/state.json"            # days already tried (so holidays are not asked again)

PERIOD_RE = re.compile(r"^(\d{4})(?:_H([12]))?$")


def period_of(date_str):
    """'2026-03-05' -> '2026_H1' (or '2026' when PERIOD_MODE=year)."""
    y, m = date_str[:4], int(date_str[5:7])
    return y if PERIOD_MODE == "year" else f"{y}_H{1 if m <= 6 else 2}"


def period_start(period):
    m = PERIOD_RE.match(period)
    y, h = m.group(1), m.group(2)
    return f"{y}-07-01" if h == "2" else f"{y}-01-01"


def period_sort_key(period):
    m = PERIOD_RE.match(period)
    return (int(m.group(1)), int(m.group(2) or 0))


def raw_key(period):
    return f"{RAW_PREFIX}{period}.parquet"


def clean_symbol(s):
    """Symbol text without spaces (the app splits symbol lists at spaces)."""
    return re.sub(r"\s+", "", str(s or "")).upper()
