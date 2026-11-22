# CryptoGuard I02 - Kafka Topic Design


| Topic | Purpose | Producer | Consumer (planned/current) |
|---|---|---|---|
| `raw.trades.binance` | Store untouched collector records from Binance | I02 Python producer / collector integration | I02 smoke consumer; later N2 normalizer |
| `raw.trades.kraken` | Raw feed placeholder for second exchange | Future second-exchange collector | Future normalizer |
| `dlq.trades` | Records rejected by validation/normalization with failure metadata | Future Flink normalizer | Debug/reprocessing |
| `canonical.trades` | Unified normalized trade schema | Future Flink normalizer | Detector N3 and downstream modules |


## Initial local configuration (development only)
- Partitions: 3 per topic for local demonstration.
- Replication factor: 1 because this is a single-broker local environment.
- Production replication factor should be greater than 1, subject to cluster size and policy.
- Keys for raw Binance messages: combined stream name (e.g. `btcusdt@trade`) in this smoke test.
- Later canonical topic key should be the normalized symbol (e.g. `BTC-USDT`) to group symbol events by partition.
- This topic layout is an initial project contract; confirm names and retention with N1 before team-wide integration.
