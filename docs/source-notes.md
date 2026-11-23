# Source notes (BAT-01)

What each batch source really looks like, measured on **2026-11-23** (not copied from docs). Owner: R4.
Update this page whenever a source surprises you.

Reproduce (files go to the git-ignored `data/raw/bat01/`):

```bash
# downloads: see the URLs below; total ≈ 31 MB, AWS Parquet is read footer-only
PYTHONPATH=batch/src python -m cg_batch.tools.source_spike --dir data/raw/bat01 --eth-date 2026-11-20 --out data/raw/bat01/summary.json
PYTHONPATH=batch/src python -m cg_batch.tools.pump_candidates --pump-csv data/raw/bat01/pump_telegram.csv
```

Parsers: `batch/src/cg_batch/sources/` (pure Python, tested in `batch/tests/test_sources.py`).
Schemas below were printed with pyarrow from the real files; the Spark `printSchema()` view is added in BAT-02.

## 1. data.binance.vision: spot daily trades

| | |
|---|---|
| URL | `https://data.binance.vision/data/spot/daily/trades/<SYMBOL>/<SYMBOL>-trades-<YYYY-MM-DD>.zip` + `.CHECKSUM` |
| Access | public HTTPS, no key. Missing day → HTTP 404 |
| Format | zip with one CSV, **no header**, 7 columns: `trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_match` |
| Checksum | `.CHECKSUM` = `<sha256>  <file name>`; matched for both files below |

Measured:

| File | Size | Rows | `time` unit | First / last trade (UTC) |
|---|---|---|---|---|
| `PEPEUSDT-trades-2026-11-20.zip` | 1.57 MB | 109,252 | **µs** (16 digits) | 00:00:07.605239 / 23:59:40.077001 |
| `GVTBTC-trades-2020-09-30.zip` (a pump day) | 15.8 KB | 1,175 | **ms** (13 digits) | 00:17:18.871 / 23:48:43.483 |
| `BTCUSDT-trades-2026-11-20.zip` (HEAD only) | 43.4 MB | | | |

Pitfalls:
- **ms before 2025, µs in 2026 files.** Both confirmed. `timeutil.epoch_to_us` (magnitude-based) handles both, so `trade_row_to_canonical` needs no change. The exact switch date was not checked (only the two ends).
- `is_buyer_maker=True` means the taker **sold**. Already mapped in `cryptoguard_common.binance_vision`.
- Several trades can share the same millisecond (old files): never dedup on time, only on `trade_id`.
- Big pairs are big: BTCUSDT is ~43 MB/day zipped, so it stays out of the local budget. For BTC→USD conversion use 1-minute klines (`spot/daily/klines/BTCUSDT/1m/`), not trades (BAT-05).

## 2. AWS Public Blockchain: Ethereum

| | |
|---|---|
| Location | `s3://aws-public-blockchain/v1.0/eth/<table>/date=<YYYY-MM-DD>/`, region `us-east-2`, anonymous (`--no-sign-request`) |
| Files | one Parquet (snappy) file per table per day, written by Spark 3.1.1 / parquet-mr 1.10.1 |

Daily partition sizes for `date=2026-11-20`:

| Table | Size |
|---|---|
| `token_transfers` | **351.8 MB**, 3,702,867 rows, 3 row groups |
| `transactions` | 829.5 MB |
| `logs` | 1,205.2 MB |
| `traces` | 3,202.6 MB |
| `blocks` | 5.5 MB |
| `contracts` | 15.5 MB |

`token_transfers` schema (real):

| Column | Arrow type | Parquet physical |
|---|---|---|
| `token_address` | string | BYTE_ARRAY |
| `from_address` | string | BYTE_ARRAY |
| `to_address` | string | BYTE_ARRAY |
| `value` | **double** | DOUBLE |
| `transaction_hash` | string | BYTE_ARRAY |
| `log_index` | int64 | INT64 |
| `block_number` | int64 | INT64 |
| `block_hash` | string | BYTE_ARRAY |
| `block_timestamp` | timestamp[ns] | **INT96** |
| `date` | string | BYTE_ARRAY |
| `last_modified` | timestamp[ns] | INT96 |

Pitfalls (all seen in the column statistics):
- **`from_address`/`to_address` are 66-character, 32-byte left-padded words** (`0x` + 24 zeros + 40 hex), in every row group. Joining them against OFAC/darklist/exchange wallets without normalising matches nothing. Use `cg_batch.sources.evm.normalize_address` (keeps the last 20 bytes). `token_address` is a normal 42-char address.
- `value` is a **double**, not a decimal string: raw amounts above 2^53 lose precision (fine for USD totals, not for exact accounting). `silver.onchain_transfers.amount_raw` can only hold the double's string form.
- Spam tokens carry absurd values (max seen 1.36e202). Always filter by `token_registry.csv` (`tracked=true`) **before** aggregating.
- `block_timestamp` is INT96: Spark reads it as TIMESTAMP. Convert to `block_ts_us` explicitly (ADR-0004).
- Local budget is 1 ETH day; downloading the whole `token_transfers` file costs 352 MB. Filtering to tracked tokens happens in BAT-07; the size of the filtered subset is not measured yet.

## 3. OFAC SDN list

| | |
|---|---|
| URL | `https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML` (server does not send Content-Length) |
| Measured | 29.6 MB, `Publish_Date` 11/20/2026, `Record_Count` 19,599 = 19,599 `sdnEntry` |
| Structure | namespace `https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML`; addresses in `sdnEntry/idList/id` with `idType` = `Digital Currency Address - <TAG>` and the address in `idNumber` |

Counts:
- 1,082 digital currency ids. By tag: XBT 534, TRX 273, ETH 124, USDT 94, LTC 14, XMR 11, BCH 7, DASH 5, ZEC 4, SOL 4, USDC 2, DOGE 2, and one each for BTG, ETC, BSV, XVG, XRP, ARB, BSC, BNB.
- **137 ids in EVM format (`0x` + 40 hex) → 127 unique addresses**, held by 51 SDN entries. By tag: ETH 124, USDT 8, USDC 2, ETC 1, ARB 1, BSC 1. 7 addresses appear in more than one entry.

Pitfalls:
- The tag is the asset, not the chain: `USDT` holds both TRON (`T…`) and Ethereum (`0x…`) addresses, so select by address format, not by tag.
- XML element is spelled `publshInformation` (sic).
- **No listing date per entry.** A label's `valid_from` must be the first archived snapshot that contains the address, and `valid_to` the first snapshot where it is gone. Archive `SDN.XML` daily in Bronze (`bronze/ofac/sdn/<date>.xml`) from now on. Addresses delisted before our first snapshot (e.g. Tornado Cash contracts, delisted in 2025) are absent from today's file: use OpenSanctions (P4) or a manual label if they are needed.

## 4. Darklist (MyEtherWallet ethereum-lists)

| | |
|---|---|
| URL | `https://raw.githubusercontent.com/MyEtherWallet/ethereum-lists/master/src/addresses/addresses-darklist.json` |
| Format | JSON array of `{"address", "comment", "date"}` |
| Measured | 98 KB, 715 items → **652 unique addresses** (63 duplicates, 0 invalid), 0 overlap with OFAC EVM addresses |

Pitfalls:
- Addresses are mixed-case (EIP-55 checksummed): lowercase before any join.
- `date` formats: `YYYY-MM-DD` (676), `M/D/YY` (16), empty (23). It is the report date, not when the wallet turned bad.
- **Stale:** the newest report is 2020-11-17. Good for historical seeds, useless for 2026 activity. Its confidence must stay below OFAC's.

## 5. Pump-and-dump dataset (SystemsLab-Sapienza)

| | |
|---|---|
| URL | `https://raw.githubusercontent.com/SystemsLab-Sapienza/pump-and-dump-dataset/master/pump_telegram.csv` (repo last pushed 2021-11-23) |
| Format | CSV with header `symbol,group,date,hour,exchange`. `hour` is `HH:MM` UTC. The README says every pump is on **SYM/BTC** |
| Measured | 36.8 KB, 1,110 rows, 0 rejected. Binance 520 rows, yobit 276, cryptopia 210, bittrex 102, kucoin 2 |

The same pump is often posted by several groups, sometimes a few minutes apart. Rows with the same coin and exchange within 60 minutes are merged (`pump_dataset.parse_pump_csv`): 1,110 rows → 887 events, **318 on Binance** (202 Binance rows merged away). Binance events per year: 2018 91, 2019 102, 2020 111, 2021 14 (the last one is 2021-01-18).

### Evaluable events → `contracts/reference/pump_events_candidates.csv`

HEAD requests only (2,510 of them) for the pump day and the 7 baseline days before it (`detector.baseline_days`):

- **314 / 318 events have the pump-day file, and all 314 have the full 7-day baseline.** The 4 missing are YOYOW-BTC (no YOYOWBTC files at all).
- 84 distinct coins. Pump-day file size: median 121 KB, max 1.03 MB. Pump day + 7 baseline days: median 400 KB, max 2.5 MB per event.
- 1 event is announced at 23:00 UTC or later and also needs the next day's file (see `notes`).

Proposed local shortlist (smallest 8-day footprint, 2 per year, distinct coins, 0.94 MB in total). Its pairs were added to `symbol_map.csv`. To be confirmed with R3/R5:

| pump_id | Announced (UTC) | Group(s) | 8-day KB |
|---|---|---|---|
| `OAX-BTC:binance:1534521600000000` | 2018-08-17 16:00 | BPG | 123 |
| `ICN-BTC:binance:1536679800000000` | 2018-09-11 15:30 | BPG | 167 |
| `EDO-BTC:binance:1567792800000000` | 2019-09-06 18:00 | CPI | 91 |
| `BNT-BTC:binance:1568059200000000` | 2019-09-09 20:00 | CPI, CPS, TCG | 107 |
| `DNT-BTC:binance:1582819200000000` | 2020-02-27 16:00 | BPF, CW | 93 |
| `GTO-BTC:binance:1604419200000000` | 2020-11-03 16:00 | CPI | 60 |
| `SNM-BTC:binance:1610139600000000` | 2021-01-08 21:00 | CP | 173 |
| `SNGLS-BTC:binance:1610398800000000` | 2021-01-11 21:00 | CP | 126 |

Pitfalls:
- `hour` is the **Telegram announcement** time, not the first abnormal trade. Detection delay must be measured from it, with the pump starting within a few minutes of it.
- File availability was checked, not content: a pair may have traded very little on the day (smallest pump-day file is 15.8 KB, 1,175 trades).

## 6. Risks and decisions to take with other roles

| # | Finding | Impact | Proposal | Who |
|---|---|---|---|---|
| 1 | **Every real pump is on a BTC-quoted pair.** `trade_row_to_canonical` sets `quote_qty_usd = None` for non-USD quotes, so `features.window_features` returns `volume_usd = None` and `volume_z = None` | The pump rule (`VOLUME_Z_HIGH`) can never fire when replaying the evaluation events. This blocks O6 | Choose one: (a) replay/normalizer fills `quote_qty_usd` with BTC-USD from `silver.minute_price` (BAT-05, BTCUSDT 1m klines); (b) features and baselines also work in quote currency (`volume_quote`) when USD is missing. Either way is a change to `binance_vision.py`/`features.py` → needs R2, R3, R4 and R5 to agree | R2, R3, R4, R5 |
| 2 | Evaluating one event needs pump day + 7 baseline days = 8 days, but local `data_budget.max_days_history` is 7 | The budget blocks a full-baseline replay of even one event | The budget exists to bound size; the 8-event shortlist is < 1 MB. Count the budget in MB or per event, or set `detector.baseline_days` lower locally | R1, R3, R4 |
| 3 | AWS addresses are 32-byte padded | Silent zero matches in graph joins | Normalise in BAT-07 (`evm.normalize_address`), add a test | R4 (BAT-07) |
| 4 | OFAC has no listing dates; darklist is stale (2020) | Historical labels (`valid_from`) are approximate | Archive SDN.XML daily from now on; document the limitation in O6 | R4 (BAT-06), R5 |
| 5 | Pump labels are on 2018–2021 BTC pairs, but the live stream runs on 2026 USDT pairs | The evaluation is offline-only; live alerts have no ground truth | Expected (brief §7). Say so explicitly in the O6 report | R5 |
