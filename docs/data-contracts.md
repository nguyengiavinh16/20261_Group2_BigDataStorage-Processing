# Data contracts

Machine-readable sources of truth:
- Kafka messages: [contracts/schemas/](../contracts/schemas/) + [contracts/kafka-topics.yaml](../contracts/kafka-topics.yaml)
- Iceberg tables: [batch/ddl/iceberg_tables.sql](../batch/ddl/iceberg_tables.sql)
- Mongo collections: [serving/src/cg_serving/mongo_schema.py](../serving/src/cg_serving/mongo_schema.py)
- Redis keys: §4 below
- Feature definitions: [libs/common/src/cryptoguard_common/features.py](../libs/common/src/cryptoguard_common/features.py)

This page explains the *why* and the cross-cutting conventions. If this page and a schema file disagree, the schema file wins. Fix this page.

## 1. Conventions

| Topic | Rule |
|---|---|
| Time | integer epoch **microseconds** UTC, suffix `_ts_us` (ADR-0004). Iceberg tables add a derived `TIMESTAMP` for partitioning |
| Symbols | canonical `BASE-QUOTE` uppercase (`BTC-USDT`); exchange-native kept in `native_symbol` |
| Addresses / hashes | lowercase `0x…` hex |
| Versions | `schema_version` `"1.x"` in every message; topic suffix `.v<major>` |
| Unknown fields | rejected (`additionalProperties: false`) |
| Money | `quote_qty_usd` / `amount_usd` / `volume_usd` in USD; USD stablecoins treated as 1.0 in MVP |
| Ids | `event_id` deterministic (dedup), `alert_id` UUIDv4, `case_id` `<symbol>:<exchange>:<first_window_start_ts_us>` |
| Serialization | UTF-8 JSON on Kafka (no Schema Registry in MVP, same choice as W1); key = UTF-8 string |

## 2. Message flow and ownership

| Contract | Producer (role) | Consumers (role) |
|---|---|---|
| RawEnvelope v1 | collectors (R2) | normalizer (R2), archiver (R2) |
| CanonicalTrade v1 | normalizer, replay (R2) | detector (R3), silver.trades (R4) |
| OnchainTransfer v1 | normalizer (R2) / silver job (R4) | detector (R3), graph (R4) |
| SymbolFeatures1m v1 | detector (R3) / gold job (R4) | sink + API (R5), training (R5) |
| Alert v1 | detector (R3) | sink + API (R5), evaluation (R5) |
| WalletRisk v1 | graph job (R4) | detector via Redis (R3), API via Mongo (R5) |
| Label v1 | labels job (R4) | graph (R4), training/eval (R5) |
| DlqRecord v1 | normalizer (R2), detector (R3) | archiver (R2), data quality (R4), dashboard (R5) |

## 3. Alert reason codes

| Code | Emitted by | Meaning | value / threshold |
|---|---|---|---|
| `VOLUME_Z_HIGH` | pump-v1 | window USD volume far above baseline | volume_z / volume_z_min |
| `PRICE_SPIKE` | pump-v1 | price change in window | % / price_change_pct_min |
| `TAKER_BUY_DOMINANT` | pump-v1 | aggressive buying | taker_buy_ratio / min |
| `INCOMPLETE_WINDOW` | any | gap/late data in the window: treat with care | true |
| `PRICE_DROP_AFTER_PUMP` | dump-v1 | sharp drop while a pump case is open | % / price_change_pct_max |
| `CROSS_VENUE_DIVERGENCE` | P4 | volume/price diverges from other exchanges | ratio |
| `NO_NEWS` | P4 | no GDELT coverage of the asset in the last N hours | true |
| `RISKY_INFLOW_TO_EXCHANGE` | P3 | risky-wallet USD flowed into this exchange's wallets in the last N minutes (exchange-level signal, not coin-level) | USD |
| `MODEL_SCORE_HIGH` | P4 | ML model probability | score / threshold |

Add a row here in the same PR that introduces a new code.

## 4. Redis keys (written by R4 batch exports, read by R3 detector)

| Key | Value | TTL |
|---|---|---|
| `baseline:<symbol>:<exchange>:<hour_of_day>` | JSON `{mean_volume_usd, std_volume_usd, n_windows, as_of_date}` | 3 days |
| `wallet_risk:<address>` | WalletRisk v1 JSON (only risk_score ≥ 0.1) | 3 days |
| `exchange_wallet:<address>` | exchange name | none |

## 5. Mongo collections (R5)

`alerts`, `alerts_replay`, `cases`, `symbol_features_recent` (TTL 7 days), `wallet_risk`. Indexes and the access
pattern each index serves are in `mongo_schema.py`.
