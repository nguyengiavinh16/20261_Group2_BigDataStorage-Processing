# Event schema draft — v1 (DRAFT W1)

| Item | Value |
|---|---|
| Owner | Streaming & Serving Lead. Designed together with the Lead, Batch & ML Lead and Data & ML Evaluation (plan sections 5.1 / 6) |
| Machine-readable files | `schemas/transaction_event.v1.schema.json`, `schemas/prediction_event.v1.schema.json` (JSON Schema 2020-12) |
| Samples | `schemas/samples/*.json`, `*.jsonl` |
| Status | W1 draft; the prediction schema will be finalized in W3 (streaming-input-contract) |

## 1. Source dataset: PaySim

Source: Kaggle `mtalaltariq/paysim-data`, a copy of the PaySim synthetic dataset. According to the public description, the file has 11 columns and each row is one transaction. Row counts, label counts and other statistics are **pending confirmation by the Batch & ML Lead's profiling**.

| PaySim column | Meaning | Used in the event? |
|---|---|---|
| `step` | Time unit, 1 step = 1 hour, 744 steps in total (30 days) | Yes, kept as-is and used to derive `event_time` |
| `type` | CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER | Yes → `transaction_type` |
| `amount` | Amount | Yes → `amount` |
| `nameOrig` | Source account | Yes → `source_account` (Kafka key) |
| `oldbalanceOrg` | Source balance before the transaction | Optional → `old_balance_orig` ⚠️ |
| `newbalanceOrig` | Source balance after the transaction | **No** (post-transaction state) |
| `nameDest` | Destination account (`M…` = merchant) | Yes → `destination_account` |
| `oldbalanceDest` | Destination balance before the transaction | Optional → `old_balance_dest` ⚠️ |
| `newbalanceDest` | Destination balance after the transaction | **No** (post-transaction state) |
| `isFraud` | **Ground-truth label** | **NEVER**, used only in the evaluation path |
| `isFlaggedFraud` | Simulator rule flag (TRANSFER > 200,000), not a label | **No**. If needed, recompute it as a feature from `amount` + `type` |

⚠️ **Leakage warning from the dataset description:** transactions detected as fraud are cancelled, so the balance columns can carry label information. Therefore:
- `new_balance_*` is **excluded from the event** because it is a post-transaction outcome that a real system would not know at scoring time.
- `old_balance_*` is kept as **optional/nullable**. The event carries these fields so the schema does not have to change later. **Whether the model uses them as features is decided by Data & ML Evaluation (feature inventory) and the Batch & ML Lead.** Streaming only transports them and does not decide.

## 2. Two field-generation rules (applied in the batch ETL, reused by streaming)

**R1 — `transaction_id`.** PaySim has no transaction ID.
`transaction_id = "ps_" + zero_pad(row_number, 8)`, where `row_number` is the line number (1-based, excluding the header) in the original CSV file whose checksum has been recorded.
- Deterministic: no matter how many times data is replayed, the same transaction keeps the same ID. This is the **idempotency key** at the sink.
- Depends on the source file not changing, so the Batch & ML Lead must record its checksum (SHA-256) in the data source note.

**R2 — `event_time`.** PaySim only has `step` (hourly resolution).
`event_time = SIM_START + (step − 1) hours + floor(rank_in_step × 3600 / n_in_step) seconds`
- `SIM_START = 2026-01-01T00:00:00Z` (configurable). `rank_in_step` is 0-based, following the original row order within the same step. `n_in_step` is the number of rows in that step.
- Purpose: spread transactions evenly within each hour so windows and watermarks are meaningful. The formula is deterministic, so batch and streaming produce the same value.
- **This is simulated time, not real time.** The dashboard must state this clearly. Late events are created deliberately in W6 by reordering during replay.

## 3. TransactionEvent v1 (input, topic `fraud.transactions.v1`)

| Field | Type | Required | Constraint | Source |
|---|---|---|---|---|
| `schema_version` | string | ✔ | `^1\.[0-9]+$` | constant `"1.0"` |
| `transaction_id` | string | ✔ | `^ps_[0-9]{8}$` | R1 |
| `event_time` | string (ISO-8601 UTC, `Z`) | ✔ | date-time | R2 |
| `step` | integer | ✔ | 1–744 | `step` |
| `transaction_type` | string | ✔ | enum of 5 values | `type` |
| `amount` | number | ✔ | ≥ 0 | `amount` |
| `source_account` | string | ✔ | `^[CM][0-9]+$` | `nameOrig` |
| `destination_account` | string | ✔ | `^[CM][0-9]+$` | `nameDest` |
| `old_balance_orig` | number \| null | – | ≥ 0 | `oldbalanceOrg` ⚠️ |
| `old_balance_dest` | number \| null | – | ≥ 0 | `oldbalanceDest` ⚠️ |
| `source` | string | ✔ | `"paysim_replay"` | constant |
| `produced_at` | string date-time | – | | set by the producer on send |
| `replay_id` | string | – | ≤ 64 characters | producer |

`additionalProperties: false`, meaning **any unknown field is rejected**, including `isFraud` and `isFlaggedFraud`. The requirement "scoring must not depend on the ground-truth label" is therefore enforced by machine checks, not just by convention.

Sample (row 3 of PaySim. In the source data this row is fraud, but the event does **not** carry the label):

```json
{
  "schema_version": "1.0",
  "transaction_id": "ps_00000003",
  "event_time": "2026-01-01T00:00:02Z",
  "step": 1,
  "transaction_type": "TRANSFER",
  "amount": 181.0,
  "source_account": "C1305486145",
  "destination_account": "C553264065",
  "old_balance_orig": 181.0,
  "old_balance_dest": 0.0,
  "source": "paysim_replay",
  "produced_at": "2026-10-20T08:15:30.125Z",
  "replay_id": "replay_20261020_01"
}
```

> Sample values are taken from the first rows of the public PaySim file. The seconds in `event_time` only illustrate R2 and will be recomputed when the batch ETL runs.

## 4. PredictionEvent v1 (output, written to MongoDB, optionally also to `fraud.predictions.v1`)

| Field | Type | Required | Notes |
|---|---|---|---|
| `schema_version` | string | ✔ | `"1.0"` |
| `transaction_id` | string | ✔ | idempotency key, the `_id` in MongoDB |
| `event_time` | date-time | ✔ | copied from the input |
| `scored_at` | date-time | ✔ | wall-clock time when scoring finished |
| `fraud_score` | number [0,1] | ✔ | model output, **not** to be treated as a calibrated probability |
| `risk_level` | LOW / MEDIUM / HIGH | ✔ | HIGH ≥ `t_high`, MEDIUM ≥ `t_medium`, otherwise LOW |
| `alert_triggered` | boolean | ✔ | `fraud_score ≥ alert_threshold` |
| `alert_threshold` | number | – | threshold used, stored for reproducibility |
| `model_version` | string | ✔ | e.g. `baseline_v1` |
| `transaction_type`, `amount`, `source_account`, `destination_account` | | – | denormalized so the dashboard can filter without joins |
| `latency_ms` | integer \| null | – | `scored_at − produced_at` |

Provisional thresholds: `t_medium = alert_threshold = 0.5`, `t_high = 0.8`. Every transaction at MEDIUM or above raises an alert. HIGH alerts are urgent and go to the top of the queue. **Data & ML Evaluation finalizes the thresholds in W3** based on the PR curve on the validation set.

## 5. Test samples (handed to Data Quality & Documentation as W2 fixtures)

`schemas/samples/transaction_events.valid.jsonl`: 6 valid events, all must PASS.

`schemas/samples/transaction_events.invalid.jsonl`: each line has exactly one error, all must FAIL:

| Line | Error | Expected | DLQ `error_type` |
|---|---|---|---|
| 1 | Missing `transaction_id` | FAIL – required | SCHEMA_VIOLATION |
| 2 | `amount` is the string `"1864.28"` | FAIL – type | SCHEMA_VIOLATION |
| 3 | `transaction_type = "WIRE"` | FAIL – enum | SCHEMA_VIOLATION |
| 4 | Contains an `isFraud` field | FAIL – additionalProperties (**label leak**) | FORBIDDEN_FIELD |
| 5 | `event_time = "01/01/2026 00:00"` | FAIL – format/pattern | SCHEMA_VIOLATION |
| 6 | `amount = -50.0` | FAIL – minimum | SCHEMA_VIOLATION |
| 7 | Contains an `isFlaggedFraud` field | FAIL – additionalProperties | FORBIDDEN_FIELD |
| 8 | Truncated JSON | FAIL – parse | PARSE_ERROR |

## 6. Implementation notes for later weeks

- **Spark `from_json`** in PERMISSIVE mode does not raise an error for missing fields; it returns `null`. The streaming app (W4) must therefore **check explicitly** that required fields are non-null and that enum values and ranges are valid, then route invalid records to the DLQ. It must also keep the raw `value` column to detect unknown fields (`isFraud`).
- Keep `event_time` as an ISO string in JSON. When writing to MongoDB, convert it to **BSON Date** so time-range queries can use indexes.
- **Schema evolution:** adding an optional field → `1.x`; update `docs/decisions.md` before editing the schema file. Renaming or removing a field → a new `.v2` topic.
