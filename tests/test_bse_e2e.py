"""End-to-end test of the BSE engine without network: fake bhavcopy -> fetch -> raw files in "git" -> build -> adjusted files in a fake R2.
Run:  python tests/test_bse_e2e.py            (PERIOD_MODE=half python tests/test_bse_e2e.py   for half-year files)

Hand-calculated expectations (raw prices 'raw', everything adjusted to today's share structure):
  500001 AAA : raw 100 until the split 10->5 (ex 2025-02-03), 50 after; dividend Rs5 ex 2025-03-03 (raw price before = 50 -> factor 0.9)
               close 2024-12-02 = 100 * 0.5 * 0.9 = 45.00 ; 2025-02-10 = 50 * 0.9 = 45.00 ; 2025-03-31 = 50.00
  500002 BBB : renamed OLDNAME -> NEWNAME on 2025-01-15 (same scrip code): ONE history under NEWNAME
  500003 CCC : spin-off ex 2025-02-10, close before 200, BSE reference price (PREVCLOSE) on ex-date 150 -> factor 0.75 -> old close 150
  500004 DDD : "Bonus issue 1:1" ex 2025-02-17 -> factor 0.5: raw 80 -> 40
  500005 EEE : raw 10; dividend Re1 ex 2025-04-02 (announced later) -> waits for the weekly run, then 10 * 0.9 = 9.0
"""
import io, json, os, shutil, sys, tempfile
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))
import numpy, pandas as pd
try:
    import pyarrow, boto3
    STUB = False
except ImportError:
    sys.path.insert(0, os.path.join(ROOT, "tests", "stubs")); STUB = True

tmp = tempfile.mkdtemp(prefix="bse_e2e_")
DATA = os.path.join(tmp, "data")
os.environ.update(DATA_DIR=DATA, HIST_BUCKET_SIZE="2", START_DATE="2024-12-02", END_DATE="2025-03-31",
                  SPECIAL_SESSIONS_FILE=os.path.join(tmp, "none.txt"), REQUEST_DELAY="0")
CA = os.path.join(DATA, "corporate_actions"); os.makedirs(CA)
import actions, build, common, fetch, rawstore, pio
from packs import decode_pack_block
from storage import LocalStore
MODE = common.PERIOD_MODE
print("period mode:", MODE, "| stand-in parquet:", STUB)

# ---- synthetic BSE files -------------------------------------------------------------------------------------------
def raw_close(code, d):
    if code == "500001": return 100.0 if d < "2025-02-03" else 50.0
    if code == "500002": return 30.0
    if code == "500003": return 200.0 if d < "2025-02-10" else 150.0
    if code == "500004": return 80.0 if d < "2025-02-17" else 40.0
    return 10.0

def fake_fetch(session, dt, date_str):
    if dt.weekday() >= 5 or date_str == "2025-01-28":                     # weekend / holiday: BSE has no file
        return None, None, "HTTP 404", {}
    rows = []
    names = {"500001": "AAA", "500002": "NEWNAME", "500003": "CCC", "500004": "DDD", "500005": "EEE"}
    for code in names:
        c = raw_close(code, date_str)
        pc = c                                                                # normal day: PREVCLOSE == yesterday's close (same price level in this fake)
        if code == "500001" and date_str == "2025-02-03": pc = 50.0           # BSE adjusts the reference price for the split
        if code == "500003" and date_str == "2025-02-10": pc = 150.0          # ... and for the spin-off
        if code == "500004" and date_str == "2025-02-17": pc = 40.0           # ... and for the bonus
        sym = "OLDNAME" if (code == "500002" and date_str < "2025-01-15") else names[code]
        rows.append(dict(scrip_code=code, symbol=sym, series="A", open=c, high=c + 1, low=c - 1, close=c, prev_close=pc, volume=1000, isin="INE000A01010", name=sym))
    rows.append(dict(scrip_code="500006", symbol="DEBT", series="F", open=1, high=1, low=1, close=1, prev_close=1, volume=1, isin="INE000A07010", name="DEBT"))
    t = pd.DataFrame(rows)
    keep = t[t["series"].isin(fetch.SERIES)].copy(); keep["date"] = date_str
    return keep, "fake", None, {"F": 1}

# ---- corporate action export you would drop into data/corporate_actions/ ------------------------------------------------
pd.DataFrame([["500001", "AAA LTD", "03-Feb-2025", "Stock  Split From Rs.10/- To Rs.5/-"],
              ["500001", "AAA LTD", "03-Mar-2025", "Dividend - Rs. - 5.0000"],
              ["500003", "CCC", "10-Feb-2025", "Spin Off"],
              ["500004", "DDD", "17-Feb-2025", "Bonus issue 1:1"],
              ["599999", "UNKNOWN", "01-Feb-2025", "Bonus issue 1:1"]],
             columns=["Security Code", "Security Name", "Ex Date", "Purpose"]).to_csv(os.path.join(CA, "bse_export.csv"), index=False)

store = LocalStore(os.path.join(tmp, "r2"))          # the fake R2: ONLY final files may appear here

# ---- 1. fetch -> git files -----------------------------------------------------------------------------------------------
stats = fetch.run(session=object(), fetch=fake_fetch, today=date(2025, 3, 31))
files = sorted(os.path.relpath(os.path.join(d, f), DATA) for d, _, fs in os.walk(os.path.join(DATA, "raw")) for f in fs)
print("\n".join(files[:3]), "...", len(files), "raw files")
assert "raw/2024/2024-12.parquet" in files and "raw/2025/2025-02.parquet" in files, files          # past months: ONE file each
assert not [f for f in files if f.startswith("raw/2025/2025-02-")], "day files of a past month are merged"
assert "raw/2025/2025-03-31.parquet" in files and "raw/2025/2025-03.parquet" not in files, "current month: one small file per day"
assert max(os.path.getsize(os.path.join(DATA, f)) for f in files) < 5_000_000
assert rawstore.periods() == (["2024_H2", "2025_H1"] if MODE == "half" else ["2024", "2025"]), rawstore.periods()
scr = json.load(open(os.path.join(DATA, "scrips.json")))
assert "500006" not in scr and scr["500002"]["symbol"] == "NEWNAME" and "OLDNAME" in scr["500002"]["prev"], scr["500002"]
st = json.load(open(os.path.join(DATA, "state.json"))); assert "2025-03-31" in st["days"] and "2025-01-28" in st["no_file"]
raw25 = rawstore.read_period("2025" if MODE == "year" else "2025_H1")
assert raw25[(raw25.scrip_code == "500001") & (raw25.date == "2025-02-03")].iloc[0].to_dict()["close"] == 50.0
n1 = stats["stored"]
os.environ["START_DATE"] = ""                     # a second daily run continues after the last stored day: nothing new
assert fetch.run(session=object(), fetch=fake_fetch, today=date(2025, 3, 31))["stored"] == 0
print("fetch ok:", n1, "days stored")

# ---- 2. build -> fake R2 ----------------------------------------------------------------------------------------------------
build.run(store); assert not build.FAILED, build.FAILED
cur = "2025_H1" if MODE == "half" else "2025"; first = "2024_H2" if MODE == "half" else "2024"
allp = pd.concat([pio.read_bytes(store.get(f"bse/{first}.parquet")), pio.read_bytes(store.get(f"bse/{cur}.parquet"))])
a = allp[allp.symbol == "AAA"].set_index("date")["close"]
assert a["2024-12-02"] == 45.0 and a["2025-02-10"] == 45.0 and a["2025-03-31"] == 50.0 and a["2025-03-03"] == 50.0, a.loc[["2024-12-02", "2025-02-10", "2025-03-03", "2025-03-31"]].to_dict()
assert allp[allp.symbol == "AAA"].set_index("date").loc["2024-12-02", "high"] == 45.45, "high = 101*0.45"
assert set(allp.symbol) == {"AAA", "NEWNAME", "CCC", "DDD", "EEE"}, set(allp.symbol)
b = allp[allp.symbol == "NEWNAME"]; assert len(b) == allp[allp.symbol == "EEE"].shape[0] and (b.close == 30).all(), "rename joined into one history"
assert allp[allp.symbol == "CCC"].set_index("date")["close"]["2024-12-02"] == 150.0
d = allp[allp.symbol == "DDD"].set_index("date")["close"]; assert d["2024-12-02"] == 40.0 and d["2025-03-31"] == 40.0
assert list(pio.read_bytes(store.get(f"bse/{cur}.parquet")).columns) == ["symbol", "date", "open", "high", "low", "close", "volume", "delivery"]
assert not [k for k, _, _ in store.list("bse/") if "/raw/" in k or k.startswith("bse/raw")], "raw prices must never reach R2"
print("period files ok (split + dividend + spin-off + bonus + rename)")

man = json.loads(store.get("bse/manifest.json"))
assert man["format"] == 2 and man["currentPeriod"] == cur and len(man["buckets"]) == 3 and man["aliases"]["OLDNAME"] == "NEWNAME" and man["aliases"]["500001"] == "AAA", man["aliases"]
bname = [n for n, v in man["buckets"].items() if "AAA" in v["symbols"]][0]
off, ln, n = man["packs"]["buckets"][bname]["idx"]["AAA"]
blk = decode_pack_block(store.get(f"bse/{man['packs']['path']}/{bname}.pack")[off:off + ln])
assert len(blk["dates"]) == n and abs(blk["close"][0] - 45.0) < 1e-9, blk["close"][:3]
rec = json.loads(store.get("bse/recent.json")); assert "AAA" in rec["idx"] and "NEWNAME" in rec["idx"]
sy = json.loads(store.get("bse/symbols.json")); assert sorted(sy["symbols"]) == ["AAA", "CCC", "DDD", "EEE", "NEWNAME"], sy
sc = json.loads(store.get("bse/scrips.json")); assert sc["AAA"]["code"] == "500001"
print("manifest / packs / recent / symbols ok; first build write operations:", build.PUTS["n"])

# ---- 3. nothing changed -> ZERO R2 writes ----------------------------------------------------------------------------------------
fp1, h1 = man["fingerprint"], {n: v["hash"] for n, v in man["buckets"].items()}
build.run(store); assert build.PUTS["n"] == 0, f"unchanged data must not write to R2, wrote {build.PUTS['n']}"
assert json.loads(store.get("bse/manifest.json"))["fingerprint"] == fp1
print("unchanged run: 0 R2 writes ok")

# ---- 4. three new days + a dividend of EEE (ex 2025-04-02): the daily run does NOT rewrite history ---------------------------------
os.environ["END_DATE"] = "2025-04-03"
assert fetch.run(session=object(), fetch=fake_fetch, today=date(2025, 4, 3))["stored"] == 3
files = sorted(os.listdir(os.path.join(DATA, "raw", "2025")))
assert "2025-03.parquet" in files and not [f for f in files if f.startswith("2025-03-")], files      # March merged into one file when April started
assert "2025-04-03.parquet" in files, files
pd.DataFrame([["500005", "EEE", "02-Apr-2025", "Dividend - Rs. - 1.0000"]], columns=["Security Code", "Security Name", "Ex Date", "Purpose"]).to_csv(os.path.join(CA, "bse_live.csv"), index=False)
build.run(store)
m2 = json.loads(store.get("bse/manifest.json"))
assert m2["fingerprint"] == fp1, "a waiting dividend must not rebuild the history"
assert build.PUTS["n"] <= 3, f"daily run should write the period file + recent pack + recent.json only, wrote {build.PUTS['n']}"
e25 = pio.read_bytes(store.get(f"bse/{cur}.parquet")); e25 = e25[e25.symbol == "EEE"].set_index("date")["close"]
assert e25["2025-03-31"] == 10.0, "dividend not applied yet"
print("daily run with waiting dividend ok, writes:", build.PUTS["n"])

# ---- 5. weekly run applies the dividend: only EEE's bucket is rebuilt -------------------------------------------------------------
build.WEEKLY = True
build.run(store); assert not build.FAILED
m3 = json.loads(store.get("bse/manifest.json")); h3 = {n: v["hash"] for n, v in m3["buckets"].items()}
changed = [n for n in h1 if h1[n] != h3[n]]
assert m3["fingerprint"] != fp1 and len(changed) == 1 and "EEE" in m3["buckets"][changed[0]]["symbols"], (changed,)
assert not [k for k, _, _ in store.list("bse/h/") if fp1[:10] in k], "old history folder removed"
e = pd.concat([pio.read_bytes(store.get(f"bse/h/{m3['fingerprint'][:10]}/{n}.parquet")) for n in m3["buckets"]]); e = e[e.symbol == "EEE"].set_index("date")["close"]
assert e["2024-12-02"] == 9.0, e["2024-12-02"]
e24 = pio.read_bytes(store.get(f"bse/{first}.parquet")); assert e24[e24.symbol == "EEE"].set_index("date")["close"]["2024-12-02"] == 9.0, "weekly run refreshes older period files"
print("weekly run ok: only bucket", changed, "rebuilt; writes:", build.PUTS["n"])
print("\nALL OK")
shutil.rmtree(tmp, ignore_errors=True)
