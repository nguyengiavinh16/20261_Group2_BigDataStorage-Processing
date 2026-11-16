# Architecture

Background and motivation: [brief/CryptoGuard_Project_Brief.html](brief/CryptoGuard_Project_Brief.html) (Vietnamese).

## 1. Paths

| Path | Latency | Components | Purpose |
|---|---|---|---|
| **Hot** | seconds | collectors → Kafka → normalizer (Flink) → detector (Flink) → alerts → sink → MongoDB → API/UI | Detect pump/dump/wash trading while it happens |
| **Cold** | hours/days | Airflow → Bronze (object storage) → Spark → Silver/Gold (Iceberg) → graph → wallet_risk | History, labels, baselines, graph, training data |
| **Feedback** | daily | Gold → Redis (baselines, wallet risk) → detector; Gold → MLflow model → detector | Batch knowledge enriches the hot path |
| **Replay** | on demand | Bronze/binance.vision → replay.* topics → detector (replay mode) → replay.alerts → evaluation | Evaluate on real historical pumps; load tests |

## 2. Component map

```
                ┌──────────────────────────── cg-ingest ────────────────────────────┐
 Binance WS ───▶│ collector-binance ┐                                               │
 OKX WS ───────▶│ collector-okx     ├──▶ raw.trades.<ex>.v1 ─┐                      │
 (P4) CB/Kraken▶│ collector-…       ┘                        │                      │
 ETH RPC (P3) ─▶│ collector-eth ───────▶ raw.eth.transfers.v1┤                      │
                └────────────────────────────────────────────┼──────────────────────┘
                                                             ▼                cg-kafka (Strimzi)
                ┌───────────────────────── cg-stream (Flink) ─────────────────────────┐
                │ normalizer: adapters, dedup(event_id), watermark, DLQ               │
                │   └─▶ canonical.trades.v1, canonical.onchain_transfers.v1, dlq.*    │
                │ detector: windows 1m/5m → features → rules/model → cases            │
                │   ◀── Redis: baseline:*, wallet_risk:*                              │
                │   └─▶ features.symbol_1m.v1, alerts.v1                              │
                └─────────────────────────────────────────────────────────────────────┘
                                │ archiver (all topics → Bronze)      │ sink
                                ▼                                     ▼
 cg-storage: MinIO/GCS  bronze/ … warehouse/ (Iceberg)      cg-serving: MongoDB, Redis, API, dashboard
             Postgres (Iceberg catalog, MLflow, Airflow)
 cg-batch:   Airflow DAGs → SparkApplications (Spark Operator) → silver.*, gold.* → exports to Redis/Mongo
             MLflow (training, registry)
 cg-monitoring: Prometheus, Grafana (system O8, data quality O7)
```

## 3. Design principles

1. **Contracts first**: components only share `contracts/` + `batch/ddl/`; everything else is private.
2. **Pure logic, thin engines**: adapters, features, rules, taint, metrics are plain Python with unit tests (ADR-0003).
3. **Same features online and offline**: `cryptoguard_common.features` is called by Flink and Spark.
4. **Immutable Bronze + replay**: every input is archived as-is, so any layer can be rebuilt.
5. **Live and replay never mix**: separate `replay.*` topics, Mongo collections and Flink deployments.
6. **Local first, cloud ready**: same manifests and images; only `config/cloud.yaml` + overlays change (ADR-0001).
7. **Everything observable**: each component exposes `cg_<component>_*` Prometheus metrics; DLQ for every failure.

## 4. Non-goals

Trading, investment advice, legal conclusions about wallets, personal/KYC data.
