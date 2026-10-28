"""
BSE daily fetcher.  Downloads BSE's equity bhavcopy for every trading day and stores the RAW (unadjusted) prices in GIT as small files
(data/raw/<year>/<yyyy-mm>.parquet, see rawstore.py).  Nothing is written to R2 here: R2 only receives the final adjusted files (build.py).

Also kept in git (small):
    data/scrips.json   scrip code -> latest symbol, company name, ISIN, first/last date, older symbols (a renamed stock stays ONE history)
    data/state.json    days already stored and days BSE had no file for (holidays are not asked again)

Environment (all optional):
  START_DATE / END_DATE   YYYY-MM-DD.  Default start: the day after the last stored day (else DEFAULT_START = 2007-01-01).
  BSE_SERIES              comma list of BSE groups kept (default below).  Counts of dropped groups are printed.
  PROBE=true              only report which BSE file types exist for sample dates (changes nothing)
  MAX_RUNTIME_MINUTES     stop downloading after this long, keep everything so far (GitHub stops jobs after 6 h)
  REQUEST_DELAY           pause after every request (default 0.3 s)
  SPECIAL_SESSIONS_FILE   weekend sessions to fetch (data/special_sessions.txt, one YYYY-MM-DD per line)
"""
import io
import json
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rawstore
from common import STATE_FILE, SCRIPS_FILE, clean_symbol

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
}

# BSE file types, tried in this order (the newer UDiFF layout first for recent dates, the old layout first for older dates).
# Confirmed by the probe run: UDiFF real since ~June 2024; the old zip files are real from 2007 until mid-2024. BSE answers HTTP 200 with a ~14 KB HTML page for missing files.
URL_UDIFF = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{yyyymmdd}_F_0000.CSV"
URL_OLD = "https://www.bseindia.com/download/BhavCopy/Equity/EQ{ddmmyy}_CSV.ZIP"
URL_OLD_ISIN = "https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{ddmmyy}.zip"
UDIFF_FROM = "2024-07-01"

DEFAULT_SERIES = "A,B,T,X,XT,XD,Z,ZP,M,MT,MS,P,R,SS,ST,IF,IT"
SERIES = {s.strip().upper() for s in os.getenv("BSE_SERIES", DEFAULT_SERIES).split(",") if s.strip()}


def _flag(name):
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes")


def _float_env(name, default):
    try:
        return float(os.getenv(name, "") or default)
    except ValueError:
        return default


REQUEST_DELAY = _float_env("REQUEST_DELAY", 0.3)
MAX_RUNTIME_MINUTES = _float_env("MAX_RUNTIME_MINUTES", 0)
SCRIPT_START = time.time()


# ----------------------------------------------------------------------------------------------------------------------
# reading one BSE file
# ----------------------------------------------------------------------------------------------------------------------
def _norm(c):
    return re.sub(r"[^A-Z0-9]", "", str(c).upper())


def _pick(df, *names):
    lookup = {}
    for c in df.columns:
        lookup.setdefault(_norm(c), c)
    for n in names:
        if _norm(n) in lookup:
            return df[lookup[_norm(n)]]
    return None


def _num(series):
    s = series.astype(str).str.strip().str.replace(",", "", regex=False)
    s = s.mask(s.isin(["-", "", "nan", "None", "NaN"]))
    return pd.to_numeric(s, errors="coerce")


def parse_bhavcopy(df, date_str, url, allowed=None):
    """Any BSE equity bhavcopy layout -> tidy table. Returns (table, error, dropped_groups)."""
    allowed = SERIES if allowed is None else allowed
    code = _pick(df, "FININSTRMID", "SC_CODE", "SCRIPCODE", "SECURITYCODE")
    symbol = _pick(df, "TCKRSYMB", "SC_NAME", "SYMBOL", "SECURITYID")
    series = _pick(df, "SCTYSRS", "SC_GROUP", "GROUP", "SERIES")
    if code is None or symbol is None:
        return None, f"no scrip code / symbol column in {url}, columns were: {list(df.columns)[:15]}", {}
    if series is None:
        return None, f"no group column (SctySrs / SC_GROUP) in {url}: refusing to store unfiltered rows, columns were: {list(df.columns)[:15]}", {}

    date_col = _pick(df, "TRADDT", "TRADING_DATE", "TRADEDATE", "DATE1")
    if date_col is not None:
        parsed = pd.to_datetime(date_col.astype(str).str.strip(), errors="coerce", dayfirst=False).dropna()
        if len(parsed):
            file_date = parsed.mode().iloc[0].strftime("%Y-%m-%d")
            if file_date != date_str:
                return None, f"file is dated {file_date} but {date_str} was requested ({url})", {}

    out = pd.DataFrame({
        "scrip_code": code.astype(str).str.strip().str.replace(r"\.0$", "", regex=True),
        "symbol": symbol.astype(str).str.strip(),
        "series": series.astype(str).str.strip().str.upper(),
    })
    for name, cands in {"open": ("OPNPRIC", "OPEN"), "high": ("HGHPRIC", "HIGH"), "low": ("LWPRIC", "LOW"),
                        "close": ("CLSPRIC", "CLOSE"), "prev_close": ("PRVSCLSGPRIC", "PREVCLOSE", "PREVCLOSEPRICE"),
                        "volume": ("TTLTRADGVOL", "NO_OF_SHRS", "NOOFSHRS", "TOTALTRADEDQTY")}.items():
        col = _pick(df, *cands)
        out[name] = _num(col) if col is not None else float("nan")
    isin = _pick(df, "ISIN", "ISIN_CODE", "ISINCODE")
    out["isin"] = isin.astype(str).str.strip() if isin is not None else ""
    name_col = _pick(df, "FININSTRMNM", "SC_NAME", "SECURITYNAME")
    out["name"] = name_col.astype(str).str.strip() if name_col is not None else out["symbol"]

    dropped = out.loc[~out["series"].isin(allowed), "series"].value_counts().to_dict()
    out = out[out["series"].isin(allowed)].copy()
    out = out[out["scrip_code"].str.fullmatch(r"\d{4,7}")]                       # real BSE scrip codes only
    out["date"] = date_str
    out["symbol"] = out["symbol"].map(clean_symbol)
    out = out.drop_duplicates("scrip_code", keep="last")

    if len(out) > 0 and (out["close"].fillna(0) > 0).sum() == 0:
        return None, f"parsed {len(out)} rows from {url} but every close is 0/null: unrecognised columns {list(df.columns)[:15]}", dropped
    return out, None, dropped


def _get(session, url):
    last = None
    for _ in range(2):
        try:
            r = session.get(url, timeout=30)
            time.sleep(REQUEST_DELAY)
            return r
        except Exception as e:
            last = e
            time.sleep(1.5)
    raise last


def _is_data_file(label, res):
    """BSE answers HTTP 200 with a ~14 KB HTML 'page not found' for days/types it has no file for. Real files: zip ('PK') or CSV text."""
    body = res.content
    if res.status_code != 200 or len(body) < 200:
        return False
    head = body[:64].lstrip(b"\xef\xbb\xbf \r\n\t").lower()
    if head.startswith(b"<"):                       # <!doctype html>, <html ...
        return False
    if label != "udiff":
        return head.startswith(b"pk")              # old types are zip files
    return True


def _read_payload(label, res):
    if label == "udiff":
        return pd.read_csv(io.StringIO(res.content.decode("utf-8-sig", errors="replace")), skipinitialspace=True, dtype=str)
    with zipfile.ZipFile(io.BytesIO(res.content)) as z:
        with z.open(z.namelist()[0]) as f:
            return pd.read_csv(f, skipinitialspace=True, dtype=str, encoding="latin-1")


def fetch_day(session, dt, date_str):
    """Returns (table, source, last_error, dropped_groups)."""
    ctx = {"yyyymmdd": dt.strftime("%Y%m%d"), "ddmmyy": dt.strftime("%d%m%y")}
    sources = [("udiff", URL_UDIFF.format(**ctx)), ("old", URL_OLD.format(**ctx)), ("oldisin", URL_OLD_ISIN.format(**ctx))]
    if date_str < UDIFF_FROM:
        sources = sources[1:] + sources[:1]
    last_error = None
    for label, url in sources:
        try:
            res = _get(session, url)
            if not _is_data_file(label, res):
                last_error = f"HTTP {res.status_code} / not a data file ({len(res.content)} bytes; BSE answers 200 with an HTML page when a file does not exist) from {url}"
                continue
            table, err, dropped = parse_bhavcopy(_read_payload(label, res), date_str, url)
            if err:
                last_error = err
                continue
            return table, label, None, dropped
        except Exception as e:
            last_error = f"{type(e).__name__}: {e} ({url})"
    return None, None, last_error, {}


# ----------------------------------------------------------------------------------------------------------------------
# small files in git
# ----------------------------------------------------------------------------------------------------------------------
def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, separators=(",", ":"), sort_keys=True)


def update_scrips(scrips, table, date_str):
    for r in table.itertuples(index=False):
        s = scrips.setdefault(r.scrip_code, {"symbol": r.symbol, "name": r.name, "isin": r.isin, "first": date_str, "last": date_str, "prev": []})
        if date_str < s["first"]:
            s["first"] = date_str
        if date_str >= s["last"]:
            if s["symbol"] != r.symbol and r.symbol not in s["prev"]:
                s["prev"].append(s["symbol"])
            s["symbol"], s["name"], s["last"] = r.symbol, r.name, date_str
            if r.isin and r.isin != "nan":
                s["isin"] = r.isin
        elif r.symbol != s["symbol"] and r.symbol not in s["prev"]:
            s["prev"].append(r.symbol)


def load_special_sessions(path):
    dates = set()
    if os.path.exists(path):
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                t = line.split("#", 1)[0].strip()
                if t:
                    try:
                        dates.add(datetime.strptime(t, "%Y-%m-%d").strftime("%Y-%m-%d"))
                    except ValueError:
                        print(f"[WARNING] special sessions: cannot read '{t}' (use YYYY-MM-DD)")
    return dates


# ----------------------------------------------------------------------------------------------------------------------
# main loop
# ----------------------------------------------------------------------------------------------------------------------
def run(session=None, fetch=fetch_day, today=None):
    today = today or datetime.now().date()
    state = load_json(STATE_FILE, {"days": [], "no_file": []})
    scrips = load_json(SCRIPS_FILE, {})
    days = set(state.get("days", []))
    no_file = set(state.get("no_file", []))

    start = os.getenv("START_DATE", "").strip().strip("'\"")
    if not start:
        last = max(days) if days else ""
        start = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if last else os.getenv("DEFAULT_START", "2007-01-01")
    end = os.getenv("END_DATE", "").strip().strip("'\"") or today.strftime("%Y-%m-%d")
    special = load_special_sessions(os.getenv("SPECIAL_SESSIONS_FILE", "data/special_sessions.txt"))
    weekend_all = os.getenv("WEEKEND_MODE", "").strip().lower() == "all"
    recent_weekend_days = int(os.getenv("RECENT_WEEKEND_DAYS", "10") or 10)

    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        try:
            session.get("https://www.bseindia.com", timeout=15)
        except Exception as e:
            print(f"[WARNING] session setup issue: {e}")

    d0, d1 = datetime.strptime(start, "%Y-%m-%d"), datetime.strptime(end, "%Y-%m-%d")
    print(f"[FETCH] BSE bhavcopy {start} .. {end}; groups kept: {sorted(SERIES)}")

    stats = {"stored": 0, "rows": 0, "no_file_weekday": 0, "skipped_known": 0, "weekends_skipped": 0}
    failed_weekdays, dropped_total, weekend_found = [], {}, []
    stopped_before = None

    def save_small():
        state["days"], state["no_file"] = sorted(days), sorted(no_file)
        save_json(STATE_FILE, state)
        save_json(SCRIPS_FILE, scrips)

    dt = d0
    while dt <= d1:
        date_str = dt.strftime("%Y-%m-%d")
        weekend = dt.weekday() >= 5
        if MAX_RUNTIME_MINUTES and time.time() - SCRIPT_START > MAX_RUNTIME_MINUTES * 60:
            stopped_before = date_str
            break
        if weekend and not (weekend_all or date_str in special or (today - dt.date()).days <= recent_weekend_days):
            stats["weekends_skipped"] += 1
            dt += timedelta(days=1)
            continue
        if date_str in days or (date_str in no_file and (today - dt.date()).days > 7):    # recent days without a file are re-asked (BSE can publish late)
            stats["skipped_known"] += date_str in no_file
            dt += timedelta(days=1)
            continue

        table, source, err, dropped = fetch(session, dt, date_str)
        if table is None or table.empty:
            if not weekend:
                stats["no_file_weekday"] += 1
                failed_weekdays.append((date_str, err))
            no_file.add(date_str)
            dt += timedelta(days=1)
            continue
        no_file.discard(date_str)
        for k, v in dropped.items():
            dropped_total[k] = dropped_total.get(k, 0) + v
        keep = table[table["close"].fillna(0) > 0]                         # only rows that really traded
        rawstore.write_day(keep, date_str)
        update_scrips(scrips, keep, date_str)
        days.add(date_str)
        stats["stored"] += 1
        stats["rows"] += len(keep)
        if weekend:
            weekend_found.append(date_str)
        print(f"[STORED] {date_str} ({len(keep)} rows, source={source})")
        if stats["stored"] % 100 == 0:
            save_small()
        dt += timedelta(days=1)

    merged = rawstore.consolidate(today.strftime("%Y-%m"))
    save_small()
    if stopped_before:
        print(f"[TIME LIMIT] stopped before {stopped_before}; everything earlier is saved. Run again to continue.")
    print(f"[FETCH COMPLETE] days stored={stats['stored']} rows={stats['rows']} weekday-without-file={stats['no_file_weekday']} "
          f"known-missing skipped={stats['skipped_known']} weekends skipped={stats['weekends_skipped']} months merged into one file={merged} "
          f"weekend sessions found={weekend_found[:20]}")
    if dropped_total:
        top = sorted(dropped_total.items(), key=lambda kv: -kv[1])[:15]
        print(f"[FETCH] rows dropped because their BSE group is not in BSE_SERIES: {top}  (add a group to BSE_SERIES if it is wanted)")
    if failed_weekdays:
        print(f"[FETCH] {len(failed_weekdays)} weekday(s) with no usable file (holidays are normal). First ones: {failed_weekdays[:8]}")
    return stats


def probe(session=None):
    """Reports which BSE file types answer for sample dates; changes nothing."""
    session = session or requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.bseindia.com", timeout=15)
    except Exception as e:
        print(f"[WARNING] session setup issue: {e}")
    for d in ["2026-10-23", "2025-03-18", "2024-07-09", "2024-06-18", "2022-03-15", "2019-03-19", "2015-03-17", "2010-03-16", "2007-03-14", "2003-03-18", "2000-03-14"]:
        dt = datetime.strptime(d, "%Y-%m-%d")
        ctx = {"yyyymmdd": dt.strftime("%Y%m%d"), "ddmmyy": dt.strftime("%d%m%y")}
        cells = []
        for label, tpl in (("udiff", URL_UDIFF), ("old", URL_OLD), ("old_isin", URL_OLD_ISIN)):
            try:
                r = _get(session, tpl.format(**ctx))
                cells.append(f"{label}={r.status_code}/{len(r.content)}B" + ("" if _is_data_file(label, r) else "(not a file)"))
            except Exception as e:
                cells.append(f"{label}=ERR({type(e).__name__})")
        print(f"[PROBE] {d} | " + " | ".join(cells))


if __name__ == "__main__":
    if _flag("PROBE"):
        probe()
    else:
        run()
