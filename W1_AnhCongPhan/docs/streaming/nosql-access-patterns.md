# NoSQL access-pattern note — v1 (DRAFT W1)

| Item | Value |
|---|---|
| Owner | Streaming & Serving Lead |
| Decision for the Lead | Database choice: **MongoDB proposed** |
| Implementation | W5 (the schema/indexes in `configs/mongodb/init-indexes.js` are a draft) |

Principle: with NoSQL, **design starts from the queries, not from the data**. So the access patterns are listed first, and only then are the database, document model and indexes chosen.

## 1. Access patterns

| ID | Used by | Query | Frequency | Requirement |
|---|---|---|---|---|
| AP1 | API / analyst | Get a prediction/alert by `transaction_id` | High | Point lookup < 10 ms |
| AP2 | Dashboard | Latest alerts in a time range, sorted newest first | Continuous (refresh every 5–10 s) | Range + sort + limit |
| AP3 | Dashboard | Filter alerts by `risk_level` + time range | High | Equality + range |
| AP4 | Analyst | Alert queue by status (`NEW`, `ACKNOWLEDGED`, …) + time | Medium | Equality + range |
| AP5 | Dashboard | Count transactions/alerts per minute; by `transaction_type`; `fraud_score` distribution | Continuous | Aggregation by time bucket |
| AP6 | Analyst | Alert history of one `source_account` | Low | Equality + range |
| W1 | Streaming sink | Write predictions **idempotently** by `transaction_id` (retries or replays create no duplicates) | Very high (follows the replay rate) | Upsert by key |
| W2 | Analyst / API | Update alert status without being overwritten by streaming | Low | Partial update |

**Which time?** There are two kinds of time:
- `scored_at` is the real time at which the system scored the transaction. Used for the "live" dashboard and for latency.
- `event_time` is the simulated time derived from `step`. Used for analysis by day/hour of the data.

The real-time dashboard filters on `scored_at` by default. The "transaction time" filter uses `event_time`.

## 2. Data model

### Collection `predictions` (every scored transaction)

```js
{
  _id: "ps_00000003",                 // = transaction_id -> idempotency key (AP1, W1)
  event_time: ISODate("2026-01-01T00:00:02Z"),
  scored_at:  ISODate("2026-10-20T08:15:30.912Z"),
  fraud_score: 0.94,
  risk_level: "HIGH",
  alert_triggered: true,
  alert_threshold: 0.5,
  model_version: "baseline_v1",
  transaction_type: "TRANSFER",
  amount: 181.0,
  source_account: "C1305486145",
  destination_account: "C553264065",
  latency_ms: 787,
  ingested_at: ISODate("2026-10-20T08:15:31.020Z")   // most recent write
}
```

### Collection `alerts` (only when `alert_triggered = true`, with a lifecycle)

```js
{
  _id: "ps_00000003",
  ...same fields as predictions...,
  status: "NEW",                       // NEW -> ACKNOWLEDGED -> CONFIRMED_FRAUD | FALSE_POSITIVE -> CLOSED
  created_at: ISODate(...),            // set only on first insert
  updated_at: ISODate(...),
  status_history: [ { status: "NEW", at: ISODate(...), by: "system" } ]
}
```

Why two collections:
- `alerts` is far smaller than `predictions` (the PaySim fraud rate is about 0.1%), so alert-queue queries are fast and the indexes stay compact.
- `alerts` carries a handling status changed by users (W2), whereas `predictions` is purely machine output.

### Indexes (each index maps to one access pattern)

| Collection | Index | Serves |
|---|---|---|
| predictions | `_id` | AP1, W1 |
| predictions | `{scored_at: -1}` | AP5 (counts over the latest minutes) |
| predictions | `{event_time: -1}` | AP5 by transaction time |
| predictions | `{transaction_type: 1, scored_at: -1}` | AP5 by type |
| alerts | `{scored_at: -1}` | AP2 |
| alerts | `{risk_level: 1, scored_at: -1}` | AP3 |
| alerts | `{status: 1, scored_at: -1}` | AP4 |
| alerts | `{source_account: 1, event_time: -1}` | AP6 |

Field order in compound indexes follows the **ESR** rule (Equality → Sort → Range): exact-match fields first, time fields after.

## 3. Idempotent writes (idempotency key = `transaction_id`)

```js
// predictions: last-write-wins for the score fields
db.predictions.updateOne(
  { _id: p.transaction_id },
  { $set: { ...p, ingested_at: now } },
  { upsert: true }
)

// alerts: must NOT reset the analyst's status on retry/replay
db.alerts.updateOne(
  { _id: p.transaction_id },
  {
    $set:         { fraud_score: p.fraud_score, risk_level: p.risk_level, model_version: p.model_version,
                    scored_at: p.scored_at, updated_at: now /* ...other machine-generated fields */ },
    $setOnInsert: { status: "NEW", created_at: now,
                    status_history: [{ status: "NEW", at: now, by: "system" }] }
  },
  { upsert: true }
)
```

- Spark restarts and reprocesses a micro-batch (at-least-once): the upsert overwrites the same `_id`, so **no** duplicate documents are created.
- Replaying the whole file: same result, because `transaction_id` is deterministic (rule R1).
- An analyst has moved an alert to `CONFIRMED_FRAUD` and streaming writes it again: `$setOnInsert` does not run again, so the status is preserved.
- **Do not use `$inc`** for dashboard counters, because retries would double-count. The dashboard counts with aggregations over `predictions`. If a precomputed summary table is needed, write window results with `$set` and `_id = window_start`.
- Limitation: when re-scoring with a new model, `predictions` keeps the result of the **latest scoring run**. To keep history across models, change the key to `(transaction_id, model_version)`. This is deferred until after the MVP.

## 4. Sample queries (to be wrapped by the W5 API)

```js
// AP1
db.alerts.findOne({ _id: "ps_00000003" })
// AP3: HIGH alerts in the last 15 minutes
db.alerts.find({ risk_level: "HIGH", scored_at: { $gte: new Date(Date.now() - 15*60*1000) } })
         .sort({ scored_at: -1 }).limit(50)
// AP4
db.alerts.find({ status: "NEW" }).sort({ scored_at: -1 }).limit(100)
// AP5: transactions and alerts per minute, last 60 minutes
db.predictions.aggregate([
  { $match: { scored_at: { $gte: new Date(Date.now() - 60*60*1000) } } },
  { $group: { _id: { $dateTrunc: { date: "$scored_at", unit: "minute" } },
              total: { $sum: 1 }, alerts: { $sum: { $cond: ["$alert_triggered", 1, 0] } } } },
  { $sort: { _id: 1 } }
])
```

## 5. Estimated storage

A `predictions` document is about 350–450 bytes. Replaying all of PaySim (~6.36 million rows per the public description) yields roughly 2.5–3 GB of data plus indexes. Demos usually replay only a subset (`REPLAY_MAX_EVENTS`). If needed, add a **TTL index** on `ingested_at` (e.g. 7 days) for `predictions`. No TTL on `alerts`.

## 6. Evaluation path (separate from scoring)

The `isFraud` label is **never** written to MongoDB through the streaming path. Data & ML Evaluation (W5) evaluates offline by joining `predictions` (exported) with the labeled Parquet on `transaction_id`. The real-time dashboard does **not** show precision/recall.
