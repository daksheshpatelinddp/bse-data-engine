# BSE data engine (for the AmiBroker web engine)

Same idea as the NSE engine: the app reads a **separate database** from R2, folder `bse/`, in the same layout as `nse/`
(period parquet files, history buckets, packs, manifest, recent pack), so a BSE symbol opens on a chart as fast as an NSE one.

**No data file goes into git.** NSE keeps yearly `.db` files in git; BSE has far more symbols, so here the raw prices live in R2 and
git only holds code and small CSV lists. The 100 MB GitHub limit can never be hit.

## What is stored in R2 (bucket `nse-stock-data`)

| key | what | who reads it |
|---|---|---|
| `bse/raw/<year>.parquet` | **raw** (unadjusted) daily prices incl. BSE scrip code, PREVCLOSE, trades, group, ISIN | the build step only |
| `bse/raw/scrips.json`, `state.json`, `factors.csv` | scrip code -> symbols, days already tried, adjustment-factor cache | the build step only |
| `bse/<year>.parquet` | all stocks of the year, **adjusted** (same columns as NSE: symbol, date, open, high, low, close, volume, delivery) | the app / DuckDB |
| `bse/h/<fp>/bNNN.parquet`, `bse/p/<fp>/bNNN.pack`, `bse/manifest.json` | history buckets + packs (one stock = one block) | the app (chart loading) |
| `bse/recent.json`, `bse/r/<token>.pack` | last 400 bars of every active stock | the app (scans) |
| `bse/symbols.json`, `bse/scrips.json` | symbol search list; symbol -> scrip code, company, ISIN | the app |

`PERIOD_MODE=year` (default) writes `bse/2026.parquet` exactly like NSE. `PERIOD_MODE=half` writes `bse/2026_H1.parquet` / `2026_H2.parquet`
(set it in both workflows **before the first run**; changing it later needs a full rebuild).

## How adjustment works (better than in-place)

Prices are stored raw and adjusted **at build time**, so an event can never be applied twice and can be recomputed at any time.

| event | factor for rows before the ex-date |
|---|---|
| split `from Rs 10 to Rs 2` | 2/10 |
| bonus `1:2` | 2/(1+2) |
| rights, demerger / spin-off / capital reduction | BSE's own reference price: PREVCLOSE of the ex-date / last close before it |
| cash dividend `Rs. - 8.0000` | 1 - amount / last raw close before the ex-date (skipped if > 50 % of the price; `ADJUST_DIVIDENDS=false` turns dividends off) |
| anything else | `data/manual_adjustments.json`, keyed by **scrip code** |

Split/bonus are cross-checked against BSE's reference price; a disagreement > 15 % is printed as a warning (text is used).
A renamed company is **one history**: rows are joined by BSE scrip code and carry the latest symbol; old names and the scrip code
(`500325`) work as aliases in `manifest.json`. A history bucket is rebuilt only when its own stocks changed; the others are copied inside R2.

## First run (in this order)

1. Add the three secrets (same as the NSE repo): `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT_URL`.
2. Actions -> **Daily BSE Fetcher** -> Run workflow with **probe** ticked. It prints which BSE file URLs answer for which years.
   (I could not reach bseindia.com from my sandbox, so the URL patterns in `backend/fetch.py` come from memory; this run proves them.)
3. Run it again with `ca_backfill_from` = `2007` (one-off corporate-action history), then with empty inputs (fetches 2007 -> today;
   it stops itself after 4.5 h and continues where it stopped the next time).
4. Add BSE corporate-action export CSVs to `data/corporate_actions/` if BSE's API history is incomplete (columns: Security Code, Security Name, Ex Date, Purpose).
5. After that the schedule runs Mon-Fri 19:15 IST; **BSE Weekly Full Rebuild** refreshes older period files every Sunday.

## Tests (no network)
`python tests/test_bse_e2e.py` and `PERIOD_MODE=half python tests/test_bse_e2e.py`
(without pyarrow/boto3 installed they use the small stand-ins in `tests/stubs`).

## Not included
BSE indices (SENSEX etc.) and delivery quantity: the `delivery` column exists (empty) so the file layout equals NSE's.
