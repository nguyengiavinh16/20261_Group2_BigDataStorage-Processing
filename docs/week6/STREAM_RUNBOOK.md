# P4-W6A/B — Streaming start / resume / fresh / restart and release

## Preflight

- Freeze revision, model_version, feature_version, run_id, topic/partition layout, source checksum, UTC anchor, cutoff and URI.
- Kafka broker reachable from executors; 3-partition mode must preserve **per-destination nondecreasing step** after key routing. Do **not** claim arbitrary out-of-order correctness.
- MongoDB unique index `(run_id, transaction_id)` created and confirmed; archive and scoring have distinct checkpoint paths on persistent HDFS; no one else is writing to the same checkpoint.
- Model path points to immutable complete artifact, **never** `latest`; input Kafka has no `isFraud` or post-transaction balances.
- Load Kafka connector in Spark driver/executors; ensure Java/Python/Hadoop/Spark versions from the actual release.

## Fresh vs resume (MUST NOT confuse)

**Fresh run**: new `run_id`, new archive/scoring checkpoint directories; replay historical steps to populate state. For test 379..743, warm-up includes 355..378; score only the configured scored period. Do not start state with empty history and call that a correct 24h feature.

**Resume**: same `run_id`, pinned model, feature, topic, partition layout and identical checkpoint URI. Restart processes and validate offsets/state/Mongo ID set. Changing checkpoint or model/version is a **new** run/migration and needs separate evidence. Do not delete checkpoints, Kafka data, Mongo records or Docker volumes as a shortcut.

Configuration checker (static only):

```powershell
python -m scripts.week6.cli stream-guard --config configs/week6/stream-release.example.json --output ..\stream-config-check.json
# for resume, add --prior path\to\pinned-run-checkpoint-manifest.json
```

### Ordered startup sequence

1. Confirm HDFS and Spark and Iceberg, then Kafka and Mongo endpoints. Check Mongo unique index; confirm topic partitions and replication factor.
2. Stage immutable model, verify manifest; configure `FRAUD_*` secrets privately, outside Git.
3. Run archive query first to capture all accepted events; launch scoring query using independent checkpoint path.
4. Replay source with deterministic IDs and ordering; record source manifest and Kafka offsets.
5. Check sink ACK count, quarantine reasons, per-partition Kafka backlog, API run/cutoff/model_version; drain until finalized cutoff.
6. Pause/restart one service on staging, *without* deleting volumes/checkpoints; resume with same pins and reconcile expected IDs and predictions.
7. Inspect Gold/Silver/Mongo by run_id, unacknowledged outbox/quarantine, late events; validation `PASS/FAIL/INCONCLUSIVE` must reflect evidence.

## Failure matrix

| Symptom | Response | Evidence |
|---|---|---|
| `MODEL_VERSION_MISMATCH` | Stop; reconcile model manifest, feature signature, scorer. Never auto-promote | manifest + start logs |
| Missing HDFS checkpoint | Check correct run/path/permissions; if lost, open **new fresh** run and warm up | checkpoint audit + incident |
| Kafka partition backlog | Measure next-offset lag, throughput and downstream ACK; avoid assuming loss | offset/lag time series |
| Mongo ACK failures | Pause/retry with same ID upsert; do not claim durable until ACK | Mongo upsert + reconciler |
| Duplicate ID | Dedup in archive/scoring with stable key; collision payload must fail/quarantine | ID reconciliation report |
| Late new step | Quarantine with reason; don't retroactively alter finalized prediction | late report, expected IDs |
| API 503 / stale metrics | Distinguish dashboard availability from correctness; check Mongo and collector | API response + collector timestamp |

## Release regression & handover

- Run golden fixture and one restart with frozen config; compare features and score within tolerance from W3/W4; retain known negative tests.
- Demo one alert and locate its `transaction_id` in source locator, published event, Mongo record and API sample. For every latency/AP number show real run/manifest or label as *unavailable*.
- Known limitations: one host, Kafka broker RF1 (if single broker), one NameNode; no atomic transaction across all sinks; state/dedup scope and growth; ordered replay assumptions. **Do not claim arbitrary-disorder support, site HA or global exactly-once.**
- Backup operator must practice pause/restart and read a FAIL report. Sign-off goes in actual evidence only after observed run.
