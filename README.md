# BSE data engine (for the AmiBroker web engine)

Same idea as the NSE engine: the app reads a **separate database** from R2, folder `bse/`, in the same layout as `nse/`
(period parquet files, history buckets, packs, manifest, recent pack), so a BSE symbol opens on a chart as fast as an NSE one.

## Where things live

| where | what |
|---|---|
| **git** `data/raw/<year>/<yyyy-mm>.parquet` | **raw** (unadjusted) daily prices, one small file per month (~2-4 MB); the current month has one ~100 KB file per day until the month ends. No file ever gets near GitHub's 100 MB limit |
| **git** `data/scrips.json`, `state.json`, `factors_cache.csv`, `build_state.json`, `corporate_actions/` | small indexes: scrip code -> symbols, stored days, adjustment factors, what R2 holds, BSE corporate-action lists |
| **R2** `bse/<year>.parquet` | all stocks of the year, **adjusted** (same columns as NSE: symbol, date, open, high, low, close, volume, delivery) |
| **R2** `bse/h/<fp>/bNNN.parquet`, `bse/p/<fp>/bNNN.pack`, `bse/manifest.json` | history buckets + packs (one stock = one block) |
| **R2** `bse/recent.json`, `bse/r/<token>.pack`, `bse/symbols.json`, `bse/scrips.json` | last 400 bars, symbol list, symbol -> scrip code / company / ISIN |

Raw prices never go to R2. `PERIOD_MODE=year` (default) writes `bse/2026.parquet` exactly like NSE; `half` writes `2026_H1.parquet` / `2026_H2.parquet`
(set it in both workflows **before the first run**; changing it later needs `force_all`).

## R2 write budget (Class A operations)

Everything is built to write as little as possible:
* one `put` per file (no multipart), no list calls (what R2 holds is remembered in `data/build_state.json`), a file whose content did not change is **not uploaded**
* a normal day writes about **3 files**: the current-year parquet, the recent pack, `recent.json`
* history buckets are rewritten only when a stock's adjustment changed; unchanged buckets are copied inside R2; old files are removed with (free) delete calls
* **cash dividends wait for the weekly run** (tick `weekly_run` in *BSE Upload to R2* about once a week). Splits, bonus, rights and demergers apply the same day.
  So a dividend does not rewrite the history every day. Set `DIVIDEND_MODE=daily` in the build step if you prefer dividends at once (more writes).
* weekly run: older period files are refreshed too (about 20 files + the changed buckets). The log prints `[R2] write operations (Class A) in this run`.

## How adjustment works

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
(`500325`) work as aliases in `manifest.json`.

## Corporate actions: download the CSVs from the BSE website

BSE's corporate-action API refuses GitHub's servers (HTTP 403), so `--live` and `ca_backfill_from` download nothing. Use BSE's own export instead:
BSE website -> Corporate Actions -> choose the purpose (**Bonus Issue, Stock Split, Sub Division, Dividend, Right Issue (+ with Warrants), Spin Off, and also Capital Reduction and Consolidation if BSE lists them**)
and the date range -> Download CSV. Do one file per purpose (split long ranges into several files), give them any name and upload them to
`data/corporate_actions/`. The folder is read as a whole: every `.csv` with the columns `Security Code`, `Ex Date`, `Purpose` is used.
Not needed: preference-share dividends/rights (ignored on purpose), buy-back, amalgamation. Rows whose purpose is just `-` are labelled from the file name, so keep the category in the file name (`...Splits.csv`, `...Bonus.csv`).
A purpose text without a ratio is fine: the factor is then taken from BSE's own reference price (PREVCLOSE) on the ex-date.
For new events repeat the download (for example monthly, range = last 40 days to next 40 days); events already known are not counted twice.
After adding files, run **Daily BSE Fetcher** once with `recompute_factors` and `force_all` ticked to rebuild everything in R2.

## Two stages: fix first, upload later

| workflow | what it does | R2 |
|---|---|---|
| **Daily BSE Fetcher** (schedule Mon-Fri 19:15 IST, or by hand) | downloads BSE prices, reads the corporate-action CSVs, computes the factors, runs a **dry-run build** that prints an adjustment report, and commits everything to git | **never touched** (no R2 secrets needed) |
| **BSE Upload to R2** (by hand) | builds the adjusted files from git and uploads them; unchanged files are skipped | the only one that writes |

So you can change code or corporate-action files as often as you like and read the `[REPORT]` lines (events applied / skipped / warnings) without spending a single R2 operation.
Run **BSE Upload to R2** when the report looks right. For automatic uploads later, remove the `#` before the `workflow_run` lines in `bse_upload_r2.yml`;
tick `weekly_run` about once a week to apply the dividends (see the write budget above). After changing corporate-action files tick `force_all` once.

## First run (in this order)

1. Secrets (needed only by **BSE Upload to R2**): `R2_ACCESS_KEY_ID` (32 characters), `R2_SECRET_ACCESS_KEY` (64), `R2_ENDPOINT_URL`.
   Settings -> Actions -> General -> Workflow permissions: **Read and write**.
2. Upload the corporate-action CSVs to `data/corporate_actions/` (see above).
3. **Daily BSE Fetcher** with all inputs empty (it stops itself after 4.5 h and commits what it has; run it again until the log says it reached today).
   Tick `recompute_factors` once after adding CSVs, then read the `[REPORT]` lines.
4. When the report looks right: **BSE Upload to R2** with `force_all` ticked. After that, normal days need no ticks.
5. Size check: after the first full load look at the repository size (Settings -> General). I estimate a few hundred MB for 2007-today, growing about 20-30 MB a year.

## Tests (no network)
`python tests/test_bse_e2e.py` and `PERIOD_MODE=half python tests/test_bse_e2e.py`
(without pyarrow/boto3 installed they use the small stand-ins in `tests/stubs`).

## Not included
BSE indices (SENSEX etc.) and delivery quantity: the `delivery` column exists (empty) so the file layout equals NSE's.
