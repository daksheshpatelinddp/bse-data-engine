"""
Corporate events -> price adjustment factors for BSE, computed from the RAW prices at build time.

Because the stored prices are raw, an event can be (re)computed any number of times and is never applied twice.

Event sources (all keyed by BSE scrip code):
  * data/corporate_actions/*.csv      BSE corporate-action exports you drop in (columns: Security Code, Security Name, Ex Date, Purpose ...)
  * data/corporate_actions/bse_live.csv   written by `python backend/actions.py --live` (BSE's own corporate-action list, daily run)
  * data/manual_adjustments.json      {"500325": [{"ex_date": "2023-07-20", "factor": 0.9082, "description": "..."}]}

How each event type becomes a factor (a row dated BEFORE the ex-date is multiplied by it):
  SPLIT     parsed from the text "from Rs 10 to Rs 2"            -> 2/10
  BONUS     "Bonus 1:2" (1 free share per 2 held)                -> 2/(1+2)
  CONSOL    consolidation / reverse split "from Rs 1 to Rs 10" -> 10 (factor above 1: old prices go up)
  RIGHTS    BSE's own adjustment of the reference price: PREVCLOSE of the ex-date / last close before it
  DEMERGER  same (spin-off, capital reduction)                   -> PREVCLOSE of the ex-date / last close before it
  If BSE did NOT adjust its reference price (spin-offs, rights, splits/bonus without ratio) the factor is ESTIMATED from the real price
  drop on the ex-date, divided by the median move of all stocks that day (so a market fall is not mistaken for a spin-off);
  split/bonus estimates snap to the usual ratios. data/reports/ lists everything a human should check.
  data/manual_adjustments.json entries with "replace": true cancel every computed event of that code + ex-date (factor 1.0 = ignore).
  DIVIDEND  "Dividend - Rs. - 8.0000"                            -> 1 - amount / last raw close before the ex-date
Splits/bonus are cross-checked against BSE's reference price and a warning is printed when they disagree by more than 15 %.
Dividends bigger than half the price are skipped (DIVIDEND_MIN_FACTOR).  ADJUST_DIVIDENDS=false switches dividends off.
"""
import glob
import hashlib
import io
import json
import os
import re
import sys
from collections import OrderedDict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rawstore
from common import period_of, period_sort_key, DATA_DIR, FACTORS_FILE

CA_DIR = os.getenv("CA_DIR", os.path.join(DATA_DIR, "corporate_actions"))
MANUAL_FILE = os.getenv("MANUAL_FILE", os.path.join(DATA_DIR, "manual_adjustments.json"))
LIVE_CSV = os.path.join(CA_DIR, "bse_live.csv")
ROUND_TO = int(os.getenv("PRICE_ROUND", "4") or 4)                      # penny stocks need more than 2 decimals
MIN_FACTOR = float(os.getenv("DIVIDEND_MIN_FACTOR", "0.5") or 0.5)
REPORT_DIR = os.getenv("REPORT_DIR", os.path.join(DATA_DIR, "reports"))
MARKET_MIN = int(os.getenv("MARKET_MIN_STOCKS", "50") or 50)              # a day needs this many stocks before its median move counts as "the market"
DEMERGER_GAP_MAX = float(os.getenv("DEMERGER_GAP_MAX", "0.9999") or 0.9999)   # like the NSE engine: every event in the list is real, so any drop (< 1) is applied; lower these to ignore small drops
RIGHTS_GAP_MAX = float(os.getenv("RIGHTS_GAP_MAX", "0.9999") or 0.9999)
SPLIT_GAP_MAX = float(os.getenv("SPLIT_GAP_MAX", "0.94") or 0.94)


def _on(name, default="true"):
    return os.getenv(name, default).strip().lower() not in ("0", "false", "no", "off")


# ----------------------------------------------------------------------------------------------------------------------
# 1. reading the purpose text
# ----------------------------------------------------------------------------------------------------------------------
_FACE = re.compile(r"(?:face\s*value|f\.?\s*v\.?)\s*(?:of\s*)?(?:rs|re|inr)?\.?\s*[:\-–]*\s*\d+(?:\.\d+)?", re.I)
_RUPEES = re.compile(r"\b(?:rs|re|inr)(?![a-z])\.?[\s:\-–.]*?(\d+(?:\.\d+)?)", re.I)
_DIVWORD = r"\b(?:div\b|divi?d\w*|divd\w*)"
_NOT_DIVIDEND = re.compile(r"dividend\s+distribution\s+tax|ddt\b", re.I)


def parse_dividend(purpose):
    """Per-share amounts (rupees) in one purpose text: 'Dividend - Rs. - 8.0000' -> [8.0]. Percent-of-face-value dividends are not read."""
    text = re.sub(r"\s+", " ", str(purpose or "")).strip()
    if not re.search(_DIVWORD, text, re.I):
        return []
    out = []
    for seg in re.split(r"/(?!\s*-)", text):
        if not re.search(_DIVWORD, seg, re.I) or _NOT_DIVIDEND.search(seg):
            continue
        seg = _FACE.sub(" ", seg)
        found = [float(x) for x in _RUPEES.findall(seg)]
        words = len(re.findall(_DIVWORD, seg, re.I))
        if found:
            amt = sum(found) if (words > 1 and len(found) <= words) else found[0]
            if amt > 0:
                out.append(round(amt, 4))
    return out


def parse_split(text, allow_up=False):
    m = re.search(r"(?:from|of)\s*(?:rs|re|inr|\.)*\s*(\d+(?:\.\d+)?)\s*(?:/-)?.*?\bto\b\s*(?:rs|re|inr|\.)*\s*(\d+(?:\.\d+)?)", text)
    if m:
        v1, v2 = float(m.group(1)), float(m.group(2))
        if v1 > v2 > 0 or (allow_up and 0 < v1 < v2):
            return v2 / v1
    return None


def parse_purpose(purpose):
    """-> [(type, parsed_factor_or_None, amount_or_None)]"""
    text = re.sub(r"\s+", " ", str(purpose or "")).strip().lower()
    out = []
    if "preference" in text or "preferance" in text:          # preference-share dividends / rights do not change the ordinary share price
        return out
    if re.search(r"consolidat|reverse split", text):          # face value up (e.g. Rs 1 -> Rs 10): old prices go UP, factor > 1
        f = parse_split(text, allow_up=True)
        out.append(("CONSOL", f if (f and f > 1) else None, None))
    if any(k in text for k in ("split", "sub-division", "sub division", "subdivision", "consolidation")) or "face value" in text:
        f = parse_split(text)
        if f and 0 < f < 1:
            out.append(("SPLIT", f, None))
        elif re.search(r"split|sub[\s-]?division", text) and not re.search(r"consolidat|reverse", text):
            out.append(("SPLIT", None, None))                 # no ratio in the text (BSE's export often has none): BSE's reference price decides
    m = re.search(r"bonus[^0-9]{0,40}?(\d+)\s*:\s*(\d+)", text)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        if a > 0 and b > 0:
            out.append(("BONUS", b / (a + b), None))
    elif re.search(r"\bbonus\b", text):
        out.append(("BONUS", None, None))                     # no ratio in the text
    if re.search(r"\brights?\b", text):
        out.append(("RIGHTS", None, None))
    if re.search(r"de-?merger|spin[\s-]?off|capital reduction|reduction of (?:share )?capital", text):
        out.append(("DEMERGER", None, None))
    if _on("ADJUST_DIVIDENDS"):
        for a in parse_dividend(purpose):
            out.append(("DIVIDEND", None, a))
    return out


# ----------------------------------------------------------------------------------------------------------------------
# 2. event sources
# ----------------------------------------------------------------------------------------------------------------------
def _norm(c):
    return re.sub(r"[^A-Z0-9]", "", str(c).replace("\ufeff", "").upper())


def _col(df, *names):
    look = {_norm(c): c for c in df.columns}
    for n in names:
        if _norm(n) in look:
            return look[_norm(n)]
    return None


def _to_date(x):
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "-"):
        return None
    for fmt in ("%d %b %Y", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y%m%d", "%d %B %Y", "%d-%b-%y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        return pd.to_datetime(s, dayfirst=True).strftime("%Y-%m-%d")
    except Exception:
        return None


FILE_LABELS = [("subdiv", "Sub Division of Equity shares"), ("split", "Stock Split"), ("bonus", "Bonus issue"), ("spin", "Spin Off"),
               ("capitalred", "Capital Reduction"), ("reduction", "Capital Reduction"), ("consol", "Consolidation of shares"),
               ("demerger", "Demerger"), ("right", "Right Issue of Equity Shares")]


def label_from_filename(path):
    """BSE's export of one category often has '-' as the purpose of some rows (no text, no ratio). The file name tells the category."""
    name = re.sub(r"[^a-z]", "", os.path.basename(str(path)).lower())
    if "pref" in name or "dividend" in name:
        return None
    for key, label in FILE_LABELS:
        if key in name:
            return label
    return None


def frame_to_events(df, default_purpose=None):
    """A BSE corporate-action table (export or live list) -> DataFrame(scrip_code, ex_date, purpose).
    default_purpose replaces purposes that are only '-' (see label_from_filename)."""
    code = _col(df, "Security Code", "scrip_code", "ScripCode", "SC_CODE", "Code")
    ex = _col(df, "Ex Date", "Ex_date", "ExDate", "EX_DATE")
    pur = _col(df, "Purpose", "Subject", "Description")
    if code is None or ex is None or pur is None:
        return pd.DataFrame(columns=["scrip_code", "ex_date", "purpose"])
    nm = _col(df, "Security Name", "Company Name", "Name")
    out = pd.DataFrame({"scrip_code": df[code].astype(str).str.strip().str.replace(r"\.0$", "", regex=True),
                        "ex_date": df[ex].map(_to_date), "purpose": df[pur].astype(str).str.strip(),
                        "name": df[nm].astype(str).str.strip() if nm is not None else ""})
    if default_purpose:
        out["purpose"] = out["purpose"].mask(out["purpose"].isin(["-", "", "nan", "None"]), default_purpose)
    return out.dropna(subset=["ex_date"]).query("scrip_code != '' and purpose != ''")


def load_events():
    frames = []
    for f in sorted(glob.glob(os.path.join(CA_DIR, "*.csv"))):
        try:
            try:
                df = pd.read_csv(f, dtype=str, skipinitialspace=True, encoding="utf-8-sig")
            except UnicodeDecodeError:
                df = pd.read_csv(f, dtype=str, skipinitialspace=True, encoding="latin-1")
            ev = frame_to_events(df, label_from_filename(f))
            if len(ev):
                frames.append(ev)
            else:
                print(f"[ACTIONS] {f}: no 'Security Code / Ex Date / Purpose' columns found, file ignored")
        except Exception as e:
            print(f"[ACTIONS] could not read {f}: {e}")
    if not frames:
        return pd.DataFrame(columns=["scrip_code", "ex_date", "purpose"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(["scrip_code", "ex_date", "purpose"]).reset_index(drop=True)


def expand_events(ev):
    """Purpose text -> one row per (code, ex-date, type)."""
    rows = []
    for code, ex, purpose in ev[["scrip_code", "ex_date", "purpose"]].itertuples(index=False):
        for typ, parsed, amt in parse_purpose(purpose):
            rows.append((code, ex, typ, parsed, amt, purpose[:120]))
    df = pd.DataFrame(rows, columns=["scrip_code", "ex_date", "type", "parsed", "amount", "purpose"])
    if df.empty:
        return df
    df["_k"] = df["amount"].fillna(0).round(4).astype(str) + df["parsed"].fillna(0).round(6).astype(str)
    return df.drop_duplicates(["scrip_code", "ex_date", "type", "_k"]).drop(columns="_k").sort_values(["ex_date", "scrip_code"]).reset_index(drop=True)


def fetch_live(from_date=None, to_date=None):
    """BSE's corporate-action list for a date window -> appended to data/corporate_actions/bse_live.csv. Best effort, never fatal."""
    import requests
    today = datetime.now()
    to_date = to_date or today.strftime("%Y%m%d")
    from_date = from_date or (today - timedelta(days=int(os.getenv("LIVE_LOOKBACK_DAYS", "20") or 20))).strftime("%Y%m%d")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
               "Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com", "Accept": "application/json, text/plain, */*"}
    url = ("https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w?Fdate=%s&Purposecode=&TDate=%s&ddlcategorys=E&ddlindustrys=&scripcode=&segment=0&strSearch=S"
           % (from_date, to_date))
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"[LIVE-CA] BSE API answered HTTP {r.status_code}: live list skipped (the CSV files are unaffected)")
            return 0
        data = r.json()
        rows = data.get("Table", data) if isinstance(data, dict) else data
        if not isinstance(rows, list):
            print("[LIVE-CA] unexpected answer shape from BSE: live list skipped")
            return 0
        ev = frame_to_events(pd.DataFrame(rows))
    except Exception as e:
        print(f"[LIVE-CA] fetch failed ({type(e).__name__}: {e}): live list skipped")
        return 0
    old = pd.read_csv(LIVE_CSV, dtype=str) if os.path.exists(LIVE_CSV) else pd.DataFrame(columns=["scrip_code", "ex_date", "purpose"])
    allr = pd.concat([old, ev], ignore_index=True).drop_duplicates(["scrip_code", "ex_date", "purpose"]).sort_values(["ex_date", "scrip_code"])
    os.makedirs(CA_DIR, exist_ok=True)
    allr.to_csv(LIVE_CSV, index=False)
    print(f"[LIVE-CA] {len(ev)} events in {from_date}..{to_date}; {len(allr)} stored in {LIVE_CSV}")
    return len(ev)


def load_manual():
    if not os.path.exists(MANUAL_FILE):
        return pd.DataFrame(columns=["scrip_code", "ex_date", "type", "factor", "amount", "override"])
    rows = []
    for code, evs in json.load(open(MANUAL_FILE, encoding="utf-8")).items():
        for e in evs:
            f = float(e.get("factor", 1))
            if 0 < f < 500 and e.get("ex_date"):
                rows.append((str(code), e["ex_date"], "MANUAL", f, 0.0, bool(e.get("replace", False))))
    return pd.DataFrame(rows, columns=["scrip_code", "ex_date", "type", "factor", "amount", "override"])


# ----------------------------------------------------------------------------------------------------------------------
# 3. looking prices up in the raw files
# ----------------------------------------------------------------------------------------------------------------------
class RawLookup:
    def __init__(self, periods, keep=4):
        self.keep = keep
        self.periods = sorted(periods, key=period_sort_key)
        self.cache = OrderedDict()
        self.mkt = {}

    def _groups(self, p):
        if p in self.cache:
            self.cache.move_to_end(p)
            return self.cache[p]
        df = rawstore.read_period(p, columns=["scrip_code", "date", "open", "close", "prev_close"])
        df = df.sort_values(["scrip_code", "date"])
        ok = (df["open"] > 0) & (df["prev_close"] > 0)                       # the market's move of each day = median of open / PREVCLOSE over all stocks
        r = (df.loc[ok, "open"] / df.loc[ok, "prev_close"]).groupby(df.loc[ok, "date"].astype(str))
        med, cnt = r.median(), r.size()
        self.mkt[p] = {d: float(m) for d, m in med.items() if cnt[d] >= MARKET_MIN and 0.5 < m < 2}
        g = {c: (x["date"].values.astype("U10"), x["close"].values.astype("float64"), x["prev_close"].values.astype("float64"),
                 x["open"].values.astype("float64"))
             for c, x in df.groupby("scrip_code")}
        self.cache[p] = g
        while len(self.cache) > self.keep:
            old, _ = self.cache.popitem(last=False)
            self.mkt.pop(old, None)
        return g

    def around(self, code, ex):
        """(last positive close before ex, PREVCLOSE of the first row on/after ex) -- either may be None."""
        pk = period_sort_key(period_of(ex))
        cum = None
        for p in [p for p in self.periods if period_sort_key(p) <= pk][::-1][:4]:
            hit = self._groups(p).get(code)
            if hit is None:
                continue
            dates, close = hit[0], hit[1]
            i = int(np.searchsorted(dates, ex, side="left")) - 1
            while i >= 0 and not close[i] > 0:
                i -= 1
            if i >= 0:
                cum = float(close[i])
                break
        pc = None
        for p in [p for p in self.periods if period_sort_key(p) >= pk][:3]:
            hit = self._groups(p).get(code)
            if hit is None:
                continue
            dates, prev = hit[0], hit[2]
            i = int(np.searchsorted(dates, ex, side="left"))
            while i < len(dates) and not prev[i] > 0:
                i += 1
            if i < len(dates):
                pc = float(prev[i])
                break
        return cum, pc

    def gap(self, code, ex):
        """Market-neutral price gap at the ex-date -> (gap, first trading date) or (None, None).
        gap = open of the first trading day on/after ex / last close before ex / that day's median move of all stocks.
        A spin-off, rights issue, split or bonus shows up as a gap below 1 when BSE did not adjust its own reference price."""
        cum, _ = self.around(code, ex)
        if not cum:
            return None, None
        pk = period_sort_key(period_of(ex))
        limit = (datetime.strptime(ex, "%Y-%m-%d") + timedelta(days=10)).strftime("%Y-%m-%d")
        for p in [p for p in self.periods if period_sort_key(p) >= pk][:3]:
            hit = self._groups(p).get(code)
            if hit is None:
                continue
            dates, close, _, opn = hit
            i = int(np.searchsorted(dates, ex, side="left"))
            while i < len(dates) and not (opn[i] > 0 or close[i] > 0):
                i += 1
            if i < len(dates):
                if dates[i] > limit:
                    return None, None
                px = opn[i] if opn[i] > 0 else close[i]
                return float(px / cum / self.mkt.get(p, {}).get(str(dates[i]), 1.0)), str(dates[i])
        return None, None

    def window(self, code, ex, days=10):
        """[(date, market-neutral gap vs the previous trading day)] for the days around ex -- used to find where a price drop really happened."""
        d0 = datetime.strptime(ex, "%Y-%m-%d")
        lo, hi = (d0 - timedelta(days=days)).strftime("%Y-%m-%d"), (d0 + timedelta(days=days)).strftime("%Y-%m-%d")
        k0, k1 = period_sort_key(period_of((d0 - timedelta(days=days + 20)).strftime("%Y-%m-%d"))), period_sort_key(period_of(hi))
        out, last = [], None
        for p in [p for p in self.periods if k0 <= period_sort_key(p) <= k1]:
            hit = self._groups(p).get(code)
            if hit is None:
                continue
            dates, close, _, opn = hit
            mk = self.mkt.get(p, {})
            for i in range(len(dates)):
                px = opn[i] if opn[i] > 0 else close[i]
                if last and px > 0 and lo <= dates[i] <= hi:
                    out.append((str(dates[i]), float(px / last / mk.get(str(dates[i]), 1.0))))
                if close[i] > 0:
                    last = float(close[i])
        return out


_NICE = sorted({b / (a + b) for a in range(1, 11) for b in range(1, 11)} | {1.0 / k for k in range(2, 21)} | {0.4, 0.25, 0.3})


def snap_ratio(g):
    """A measured price gap -> the nearest usual split/bonus ratio (1/2, 1/5, 1/10, 1:1 bonus ...) if within 7 %, else the gap itself."""
    best = min(_NICE, key=lambda c: abs(g / c - 1))
    return best if abs(g / best - 1) <= 0.07 else g


def compute_event(look, code, ex, typ, parsed, amount):
    """-> (factor or None, note)"""
    cum, pc = look.around(code, ex)
    observed = (pc / cum) if (cum and pc and cum > 0) else None
    est_on = _on("ESTIMATE_FROM_GAP")
    if typ == "CONSOL":
        if parsed is not None and not pd.isna(parsed):
            return parsed, ("" if not observed or observed < 1.0001 or abs(observed / parsed - 1) <= 0.15 else f"WARNING text says {parsed:.4f}, BSE reference price implies {observed:.4f} (text used)")
        if observed and 1.0001 < observed < 500:
            return observed, "ratio taken from BSE's reference price"
        return None, "consolidation without ratio in the text and BSE reference price not adjusted"
    if typ in ("SPLIT", "BONUS") and (parsed is None or pd.isna(parsed)):
        if not cum:
            return None, "no price before the ex-date (nothing to adjust)"
        if observed is not None and 0.01 < observed < 0.9999:
            return observed, "ratio taken from BSE's reference price"
        g, gd = look.gap(code, ex) if est_on else (None, None)
        if g and 0.02 < g < SPLIT_GAP_MAX:
            return snap_ratio(g), f"ESTIMATE from the price drop on {gd} (market-adjusted gap {g:.4f})"
        return None, "no ratio in the text, BSE reference price not adjusted and no clear price drop on the ex-date" + (f" (gap {g:.4f})" if g else "")
    if typ in ("SPLIT", "BONUS"):
        if observed and 0 < observed < 0.9999 and abs(observed / parsed - 1) > 0.15:
            return parsed, f"WARNING text says {parsed:.4f}, BSE reference price implies {observed:.4f} (text used)"
        return parsed, ""
    if typ == "DIVIDEND":
        if not cum:
            return None, "no price before the ex-date"
        f = 1.0 - amount / cum
        if f < MIN_FACTOR:
            return None, f"dividend {amount:g} is {100 * amount / cum:.0f}% of price {cum:.2f}"
        return f, ""
    # RIGHTS / DEMERGER: BSE's own reference-price adjustment, else the measured price drop on the ex-date (like the NSE engine)
    if not cum:
        return None, "no price before the ex-date (nothing to adjust)"
    if observed is not None and 0.2 < observed < 0.9999:
        return observed, ""
    g, gd = look.gap(code, ex) if est_on else (None, None)
    top = DEMERGER_GAP_MAX if typ == "DEMERGER" else RIGHTS_GAP_MAX
    if g and 0.2 < g < top:
        return float(g), f"ESTIMATE from the price drop on {gd} (market-adjusted gap {g:.4f})"
    return None, "BSE reference price not adjusted and no price drop on the ex-date" + (f" (gap {g:.4f})" if g else " (no trade within 10 days)")


# ----------------------------------------------------------------------------------------------------------------------
# 3b. review files: what a human should look at (data/reports/)
# ----------------------------------------------------------------------------------------------------------------------
def _md(df, cols):
    if df.empty:
        return "_none_\n"
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in df[cols].itertuples(index=False):
        out.append("| " + " | ".join("" if (isinstance(x, float) and pd.isna(x)) else (f"{x:.4f}" if isinstance(x, float) else str(x)) for x in r) + " |")
    return "\n".join(out) + "\n"


def write_review(cache, events, look, names):
    """data/reports/review_no_ratio.csv, review_disagreements.csv, review_demerger_rights.csv and adjustment_review.md"""
    os.makedirs(REPORT_DIR, exist_ok=True)
    ev = events[["scrip_code", "ex_date", "type", "parsed", "purpose"]].copy()
    ev["key"] = (events["amount"].fillna(0).round(4).astype(str) + "|" + events["parsed"].fillna(0).round(6).astype(str))
    m = cache.merge(ev[["scrip_code", "ex_date", "type", "key", "parsed", "purpose"]], on=["scrip_code", "ex_date", "type", "key"], how="left")
    m.insert(1, "company", m["scrip_code"].map(names).fillna(""))

    # A. split / bonus rows whose purpose text carries no ratio
    nr = m[m["type"].isin(["SPLIT", "BONUS"]) & m["parsed"].isna() & ~m["note"].fillna("").str.startswith("no price before")].copy()   # events with no price before them have nothing to adjust
    gaps = [look.gap(c, e) for c, e in zip(nr["scrip_code"], nr["ex_date"])]
    nr["price_gap"] = [g[0] for g in gaps]
    nr["applied"] = nr["factor"]
    nr = nr[["scrip_code", "company", "ex_date", "type", "purpose", "status", "applied", "price_gap", "note"]]
    nr.to_csv(os.path.join(REPORT_DIR, "review_no_ratio.csv"), index=False)

    # B. split / bonus rows where the text and BSE's reference price disagree
    dg = m[m["note"].fillna("").str.startswith("WARNING")].drop_duplicates(["scrip_code", "ex_date"]).copy()
    same_day = m[m["type"].isin(["SPLIT", "BONUS"]) & (m["status"] == "ok")].groupby(["scrip_code", "ex_date"])["factor"].prod()   # a split + bonus on one day move the price together
    rows = []
    for r in dg.itertuples(index=False):
        t = float(same_day.get((r.scrip_code, r.ex_date), r.factor))
        g, gd = look.gap(r.scrip_code, r.ex_date)
        cum, pc = look.around(r.scrip_code, r.ex_date)
        bse = (pc / cum) if (cum and pc) else np.nan
        win = look.window(r.scrip_code, r.ex_date, 10)
        best = min(win, key=lambda x: abs(np.log(x[1] / t))) if win else (None, None)
        if g and abs(g / t - 1) <= 0.15:
            sug, hint = "KEEP: the price really dropped by this ratio on the ex-date (BSE's reference price was not adjusted)", ""
        elif not g:
            sug, hint = "KEEP text: the stock did not trade within 10 days of the ex-date, so the price cannot confirm it (look at it if the stock matters)", ""
        elif best[0] and best[0] != r.ex_date and abs(best[1] / t - 1) <= 0.15:
            sug = f"MOVE: the price drop ({best[1]:.2f}) is on {best[0]}; the ex-date in the file looks wrong"
            hint = json.dumps({r.scrip_code: [{"ex_date": r.ex_date, "factor": 1.0, "replace": True, "description": "wrong ex-date"},
                                               {"ex_date": best[0], "factor": round(t, 4), "description": "moved to the real drop"}]})
        elif g and g >= 0.93:
            sug = "IGNORE?: no price drop within 10 days of the ex-date; the event may never have taken effect"
            hint = json.dumps({r.scrip_code: [{"ex_date": r.ex_date, "factor": 1.0, "replace": True, "description": "no price drop seen"}]})
        else:
            sug, hint = f"CHECK: the price moved by {g:.2f}", ""
        rows.append((r.scrip_code, r.company, r.ex_date, r.type, round(t, 4), round(bse, 4) if bse == bse else np.nan,
                     round(g, 4) if g else np.nan, best[0] or "", round(best[1], 4) if best[1] else np.nan, sug, hint))
    dgf = pd.DataFrame(rows, columns=["scrip_code", "company", "ex_date", "type", "text_ratio", "bse_ratio", "price_gap", "biggest_drop_date", "drop_then", "suggestion", "manual_override"])
    dgf.to_csv(os.path.join(REPORT_DIR, "review_disagreements.csv"), index=False)

    # C. spin-offs / demergers / rights
    dr = m[m["type"].isin(["DEMERGER", "RIGHTS"])][["scrip_code", "company", "ex_date", "type", "status", "factor", "note"]]
    dr.to_csv(os.path.join(REPORT_DIR, "review_demerger_rights.csv"), index=False)

    est = int(m["note"].fillna("").str.startswith("ESTIMATE").sum())
    lines = ["# Adjustment review (written by every run, safe to read on the phone)\n",
             f"Events estimated from the price drop (BSE reference price was not adjusted): **{est}**\n",
             f"## 1. Split / bonus without a ratio in the text ({len(nr)})\n",
             "`applied` = factor used, empty = event left out.\n", _md(nr.assign(purpose=nr["purpose"].str[:30]), ["scrip_code", "company", "ex_date", "type", "applied", "price_gap", "note"]),
             f"\n## 2. Text and BSE reference price disagree ({len(dgf)})\n",
             "Details and ready-to-paste overrides: review_disagreements.csv\n", _md(dgf, ["scrip_code", "company", "ex_date", "type", "text_ratio", "bse_ratio", "price_gap", "suggestion"]),
             "\n## 3. Spin-off / demerger / rights\n"]
    for typ in ("DEMERGER", "RIGHTS"):
        x = dr[dr["type"] == typ]
        lines.append(f"{typ}: applied {int((x['status'] == 'ok').sum())} (of which estimated {int(x['note'].fillna('').str.startswith('ESTIMATE').sum())}), skipped {int((x['status'] == 'skip').sum())}. Full list: review_demerger_rights.csv\n")
    open(os.path.join(REPORT_DIR, "adjustment_review.md"), "w", encoding="utf-8").write("\n".join(lines))
    return f"review files written to {REPORT_DIR}/ (no-ratio {len(nr)}, disagreements {len(dgf)}, demerger/rights {len(dr)}, estimated from price drop {est})"


def load_factor_cache():
    if not os.path.exists(FACTORS_FILE):
        return pd.DataFrame(columns=["scrip_code", "ex_date", "type", "key", "factor", "amount", "status", "note"])
    return pd.read_csv(FACTORS_FILE, dtype={"scrip_code": str, "key": str, "note": str, "status": str})


def compute_all(periods, asof=None, force=False, known_codes=None, dividend_cutoff=None):
    """Incremental: only new events (and recent skipped ones) are looked up. Returns (factors, report_lines).
    factors = DataFrame(scrip_code, ex_date, factor) with one row per (code, ex-date), several events multiplied.
    Dividends with an ex-date AFTER dividend_cutoff are left out (they are applied by the weekly run, which moves the cutoff forward)."""
    ev_all = load_events()
    events = expand_events(ev_all)
    if known_codes is not None and len(events):
        events = events[events["scrip_code"].isin(known_codes)]
    cache = load_factor_cache()
    if force:
        cache = cache.iloc[0:0]
    asof = asof or datetime.now().strftime("%Y-%m-%d")
    recent_cut = (datetime.strptime(asof, "%Y-%m-%d") - timedelta(days=12)).strftime("%Y-%m-%d")
    report = []
    if len(events):
        events["key"] = (events["amount"].fillna(0).round(4).astype(str) + "|" + events["parsed"].fillna(0).round(6).astype(str))
        done = {}
        for r in cache.itertuples(index=False):
            if r.status == "ok" or r.ex_date < recent_cut:
                done[(r.scrip_code, r.ex_date, r.type, r.key)] = r
        look = RawLookup(periods)
        rows, new_ok, new_skip, future = [], 0, 0, 0
        for r in events.itertuples(index=False):
            k = (r.scrip_code, r.ex_date, r.type, r.key)
            if k in done:
                d = done[k]
                rows.append((r.scrip_code, r.ex_date, r.type, r.key, d.factor, d.amount, d.status, d.note))
                continue
            if r.ex_date > asof:
                future += 1
                continue
            f, note = compute_event(look, r.scrip_code, r.ex_date, r.type, r.parsed, r.amount)
            amt = 0.0 if pd.isna(r.amount) else float(r.amount)
            if f is None:
                new_skip += 1
                rows.append((r.scrip_code, r.ex_date, r.type, r.key, np.nan, amt, "skip", note))
            else:
                new_ok += 1
                rows.append((r.scrip_code, r.ex_date, r.type, r.key, float(f), amt, "ok", note))
        cache = pd.DataFrame(rows, columns=["scrip_code", "ex_date", "type", "key", "factor", "amount", "status", "note"])
        report.append(f"events with a price effect: {len(events)}; newly computed ok={new_ok} skipped={new_skip}; future ex-date={future}")
        if _on("REVIEW_FILES"):
            try:
                names = {} if "name" not in ev_all.columns else ev_all[ev_all["name"].fillna("") != ""].groupby("scrip_code")["name"].last().to_dict()
                report.append(write_review(cache, events, look, names))
            except Exception as e:                                             # the review files are a help, never a reason to fail the build
                report.append(f"review files not written ({type(e).__name__}: {e})")
    os.makedirs(os.path.dirname(FACTORS_FILE) or ".", exist_ok=True)
    cache.to_csv(FACTORS_FILE, index=False)
    ok = cache[cache["status"] == "ok"]
    if dividend_cutoff:
        late = (ok["type"] == "DIVIDEND") & (ok["ex_date"] > dividend_cutoff)
        if late.any():
            report.append(f"{int(late.sum())} dividend(s) with ex-date after {dividend_cutoff} wait for the weekly run")
        ok = ok[~late]
    man = load_manual()
    if len(man) and man["override"].any():                                     # "replace": true in manual_adjustments.json drops every computed event of that code + ex-date
        rep = set(zip(man.loc[man["override"], "scrip_code"], man.loc[man["override"], "ex_date"]))
        ok = ok[[(c, e) not in rep for c, e in zip(ok["scrip_code"], ok["ex_date"])]]
    fac = pd.concat([ok[["scrip_code", "ex_date", "type", "factor", "amount"]], man[["scrip_code", "ex_date", "type", "factor", "amount"]]], ignore_index=True)
    warn = cache[cache["note"].fillna("").str.startswith("WARNING")]
    skipped = cache[cache["status"] == "skip"]
    if len(warn):
        report.append(f"{len(warn)} split/bonus rows disagree with BSE's reference price (first 5): " + "; ".join(f"{r.scrip_code} {r.ex_date}" for r in warn.head(5).itertuples()))
    if len(skipped):
        by = skipped["type"].value_counts().to_dict()
        report.append(f"skipped events by type: {by}")
    if fac.empty:
        return pd.DataFrame(columns=["scrip_code", "ex_date", "factor"]), report or ["no events"]
    fac = fac.groupby(["scrip_code", "ex_date"], as_index=False).agg(factor=("factor", "prod"))
    return fac, report


# ----------------------------------------------------------------------------------------------------------------------
# 4. applying
# ----------------------------------------------------------------------------------------------------------------------
class Adjuster:
    """adjust(df) multiplies open/high/low/close of each row by the product of the factors of that scrip's events with ex-date AFTER the row's date."""

    def __init__(self, factors):
        self.by_code, self.sig = {}, {}
        if factors is not None and len(factors):
            for code, g in factors.sort_values("ex_date").groupby("scrip_code"):
                ex = np.array(g["ex_date"].values, dtype="datetime64[D]")
                f = g["factor"].values.astype("float64")
                self.by_code[code] = (ex, np.append(np.cumprod(f[::-1])[::-1], 1.0))
                self.sig[code] = hashlib.sha1("".join(f"{e}|{x:.8f};" for e, x in zip(g["ex_date"], f)).encode()).hexdigest()[:12]

    def __bool__(self):
        return bool(self.by_code)

    def signature_for(self, codes):
        return hashlib.sha1("".join(f"{c}:{self.sig[c]};" for c in sorted(codes) if c in self.sig).encode()).hexdigest()[:16]

    def adjust(self, df):
        """df needs scrip_code, date, open, high, low, close. Returns an adjusted copy."""
        if not self.by_code or df.empty:
            return df
        df = df.copy()
        mult = np.ones(len(df))
        codes = df["scrip_code"].values
        dates = df["date"].values
        for code in set(self.by_code) & set(np.unique(codes)):
            idx = np.nonzero(codes == code)[0]
            ex, suffix = self.by_code[code]
            mult[idx] = suffix[np.searchsorted(ex, np.asarray(dates[idx], dtype="datetime64[D]"), side="right")]
        hit = mult != 1.0
        if hit.any():
            for c in ("open", "high", "low", "close"):
                df.loc[hit, c] = np.round(df.loc[hit, c].values * mult[hit], ROUND_TO)
        return df


def print_report(limit=12):
    """What happened to the events: counts by type, why some were skipped, and the splits/bonus where text and BSE's reference price disagree."""
    c = load_factor_cache()
    if c.empty:
        print("[REPORT] no corporate events loaded (put BSE CSV exports into data/corporate_actions/)")
        return
    print("[REPORT] events by type and result:")
    for (typ, st), n in c.groupby(["type", "status"]).size().items():
        print(f"[REPORT]   {typ:9s} {st:5s} {n}")
    est = c[c["note"].fillna("").str.startswith("ESTIMATE")]
    if len(est):
        print(f"[REPORT] {len(est)} events estimated from the market-adjusted price drop on the ex-date: " + ", ".join(f"{k} {v}" for k, v in est["type"].value_counts().items()))
    sk = c[c["status"] == "skip"]
    if len(sk):
        why = sk["note"].fillna("").str.replace(r"[\d.]+", "#", regex=True).value_counts().head(limit)
        print("[REPORT] skipped because:")
        for note, n in why.items():
            print(f"[REPORT]   {n:5d}  {note}")
    w = c[c["note"].fillna("").str.startswith("WARNING")]
    if len(w):
        print(f"[REPORT] {len(w)} split/bonus rows where the text and BSE's reference price disagree (first {limit}):")
        for r in w.head(limit).itertuples():
            print(f"[REPORT]   {r.scrip_code} {r.ex_date} {r.type} {r.note}")


if __name__ == "__main__":
    if "--live" in sys.argv:
        fetch_live()
    if "--backfill" in sys.argv:             # python backend/actions.py --backfill 2007  (one call per quarter, slow, one-off)
        y0 = int(sys.argv[sys.argv.index("--backfill") + 1])
        for y in range(y0, datetime.now().year + 1):
            for a, b in (("0101", "0331"), ("0401", "0630"), ("0701", "0930"), ("1001", "1231")):
                fetch_live(f"{y}{a}", f"{y}{b}")
