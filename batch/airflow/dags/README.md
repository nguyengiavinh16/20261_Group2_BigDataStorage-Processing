# DAGs (owner R4)

| DAG | Schedule | Task IDs | Produces |
|---|---|---|---|
| `binance_vision_daily` | daily | BAT-03, BAT-04 | bronze/binance_vision → silver.trades |
| `features_daily` (todo) | daily after trades | BAT-05 | silver.minute_price, gold.symbol_features_1m, gold.symbol_baseline, Redis baselines |
| `labels_refresh` (todo) | daily | BAT-06 | bronze/ofac, bronze/darklist, bronze/pump_dataset → silver.labels, gold.pump_cases |
| `aws_eth_transfers_daily` (todo) | daily | BAT-07 | bronze/aws_eth → silver.onchain_transfers |
| `wallet_graph_daily` (todo) | daily after transfers+labels | BAT-08, BAT-09 | gold.wallet_edges, gold.wallet_risk, Redis/Mongo export |
| `data_quality_daily` (todo) | daily | BAT-14 | gold.data_quality_daily |

Bronze layout (object storage, bucket `cg-lake`):

```
bronze/binance_vision/spot/trades/<SYMBOL>/<YYYY-MM-DD>.zip
bronze/aws_eth/token_transfers/date=<YYYY-MM-DD>/*.parquet      (tracked tokens only)
bronze/ofac/sdn/<YYYY-MM-DD>.xml
bronze/darklist/<YYYY-MM-DD>.json
bronze/pump_dataset/<git-sha>/...
bronze/kafka/<topic>/dt=<YYYY-MM-DD>/hour=<HH>/*.jsonl.gz        (written by ingest/archiver, R2)
```
