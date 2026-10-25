# Interfaces and schemas

## Dataset assumption

The common PaySim CSV variant often contains `step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`. Verify the actual header before coding; variants may differ.

## Transaction field mapping proposal

| Contract field | Source mapping / rule |
|---|---|
| `schema_version` | Constant `"1.0"` |
| `transaction_id` | No universal transaction ID in the common schema. Generate deterministically from source file identity/version plus stable row index/row locator. Row index depends on unchanged file ordering; hashing fields alone may collide for identical duplicate rows. |
| `event_time` | `step` + configured simulation anchor; not an actual wall-clock timestamp |
| `event_time_step` | `step`, preserving original relative time |
| `transaction_type` | `type`; profile actual values before defining a fixed enum |
| `amount` | `amount`; currency/unit remains unspecified unless source documentation defines it |
| `origin_account_id` | `nameOrig` |
| `destination_account_id` | `nameDest` |
| `origin_balance_before` | `oldbalanceOrg` |
| `origin_balance_after` | `newbalanceOrig` |
| `destination_balance_before` | `oldbalanceDest` |
| `destination_balance_after` | `newbalanceDest` |
| `source_dataset` | Config/provenance, e.g. `paysim` |
| `source_row_id` | Stable source row locator used to reproduce ID |
| `isFraud` | Label only; deliberately excluded from scoring schema |
| `isFlaggedFraud` | Potential existing rule outcome; exclude from scoring features unless justified |

## Prediction/alert schema

Required fields: `schema_version`, `transaction_id`, `event_time`, `scored_at`, `fraud_score`, `risk_level`, `alert_triggered`, `model_version`, `threshold`. `source_dataset` is included for provenance. The score semantics must match the selected model. Do not call it a calibrated probability unless calibration is evaluated.

## Interfaces

- Dataset inspector → ETL/replay: actual source header, types, quality report.
- ETL → Parquet/HDFS: typed processed data and quality summary.
- Training → streaming: versioned model plus feature order, preprocessing and threshold metadata.
- Replay producer → Kafka: JSON matching `schemas/transaction-event.json`, message key `transaction_id`.
- Kafka → Structured Streaming: UTF-8 JSON on `transaction-events`.
- Streaming → NoSQL: JSON matching `schemas/prediction-event.json`.
- NoSQL → API: query methods with filters and pagination.
- API → dashboard: documented JSON responses and error codes.

Use snake_case; parse RFC 3339 timestamps at the Spark boundary; update schemas, samples, producer, consumer, tests, and docs together for schema changes.
