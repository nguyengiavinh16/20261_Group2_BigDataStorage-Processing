# Fraud Analytics — Week 1 code overlay

This deliverable covers **all five owners / ten tasks of W1** from the uploaded
6-week, 60-task plan. It has deliberately NOT started W2 or later.

**Important:** The original repository source files, Compose file, runtime
and full PaySim dataset were not uploaded—only a directory screenshot and the
HTML plan. This is therefore an **additive patch**, not a merged branch or
verified deployed application. It adds new files and does not replace the
existing `src/ingestion/producer.py`, `src/processing/etl.py`,
`configs/model.yaml`, root Compose, or v1 JSON schemas.

| Area | Implemented in W1 overlay |
|---|---|
| Contracts | stable source ID/time/cents, event & prediction v2 schemas, ADR |
| Batch | immutable source locator, multipart JSONL stage, checksums and manifest, optional explicit HDFS CLI upload |
| Features | deterministic 24-step point-in-time reference and golden fixture |
| Evaluation | train/validation/test boundaries, AP, threshold policy, train-only preprocessing reference, full CSV audit CLI |
| Replay | label-free source mapping, offline label sidecar, ordered dry-run, Kafka ACK adapter, manifest |
| Validation/API | mock fixtures, ID-set reconciliation, API/metric/report contracts |
| Platform | runtime inventory and real Spark/HDFS/Iceberg/Kafka smoke scripts, config candidate and restart instructions |

Start with **`docs/week1/INTEGRATION.md`** for step-by-step PowerShell commands,
then read **`docs/week1/STATUS.md`** for pending acceptance gates and sign-off.
Use `requirements-w1.txt` for local tests only; real runtime dependencies have
to be established using your repository's Docker environment.

Unit/contract/fixture checks ran in the assistant environment. They are NOT
equivalent to the G0 distributed runtime sign-off. To continue to W2, the user
must explicitly authorize the next week.
