"""Spark jobs. Each module has `main()` and is launched by a SparkApplication (batch/k8s) triggered from Airflow.

| Module | Task | Reads | Writes |
|---|---|---|---|
| bronze_to_silver_trades | BAT-04 | bronze/binance_vision/trades, bronze/kafka/canonical.trades.v1 | silver.trades |
| minute_price | BAT-05 | silver.trades (or klines) | silver.minute_price |
| gold_symbol_features | BAT-05 | silver.trades | gold.symbol_features_1m, gold.symbol_baseline (+ Redis export) |
| labels | BAT-06 | bronze/ofac, bronze/darklist, bronze/pump_dataset, (bronze/elliptic) | silver.labels, gold.pump_cases |
| onchain_transfers | BAT-07 | bronze/aws_eth/token_transfers | silver.onchain_transfers |
| wallet_graph | BAT-08/09 | silver.onchain_transfers, silver.labels | gold.wallet_edges, gold.wallet_risk (+ Redis/Mongo export) |
| data_quality | BAT-14 | bronze/kafka/dlq.*, silver.* | gold.data_quality_daily |
"""
