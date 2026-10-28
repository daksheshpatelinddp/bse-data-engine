"""Shared settings: database id, file period, and where everything lives.

GIT (this repo, data/):                              R2 (bucket, folder bse/):
  data/raw/<year>/<yyyy-mm>.parquet  raw prices        bse/<year>.parquet     adjusted period files
  data/scrips.json, state.json       small indexes     bse/h/.., bse/p/..     history buckets + packs
  data/factors_cache.csv             adjustment cache  bse/manifest.json, recent.json, symbols.json, scrips.json
  data/build_state.json              what R2 holds
Raw prices never go to R2: R2 only receives the final adjusted files (fewest possible write operations).
"""
import os
import re

DB_ID = "bse"                                    # folder in the bucket: bse/...
PERIOD_MODE = os.getenv("PERIOD_MODE", "year").strip().lower()    # "year" -> 2026.parquet (same as NSE)   "half" -> 2026_H1.parquet

DATA_DIR = os.getenv("DATA_DIR", "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
STATE_FILE = os.path.join(DATA_DIR, "state.json")              # stored days, days BSE had no file for
SCRIPS_FILE = os.path.join(DATA_DIR, "scrips.json")            # scrip code -> latest symbol, name, ISIN, first/last date, older symbols
BUILD_STATE_FILE = os.path.join(DATA_DIR, "build_state.json")  # hashes of what is already in R2, dividend cutoff
FACTORS_FILE = os.path.join(DATA_DIR, "factors_cache.csv")     # computed adjustment factors

PERIOD_RE = re.compile(r"^(\d{4})(?:_H([12]))?$")


def period_of(date_str):
    """'2026-03-05' -> '2026' (or '2026_H1' when PERIOD_MODE=half)."""
    y, m = date_str[:4], int(date_str[5:7])
    return y if PERIOD_MODE == "year" else f"{y}_H{1 if m <= 6 else 2}"


def period_start(period):
    m = PERIOD_RE.match(period)
    y, h = m.group(1), m.group(2)
    return f"{y}-07-01" if h == "2" else f"{y}-01-01"


def period_sort_key(period):
    m = PERIOD_RE.match(period)
    return (int(m.group(1)), int(m.group(2) or 0))


def clean_symbol(s):
    """Symbol text without spaces (the app splits symbol lists at spaces)."""
    return re.sub(r"\s+", "", str(s or "")).upper()
