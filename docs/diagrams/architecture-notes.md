# Fraud detection system architecture

Companion to [system-architecture.drawio](system-architecture.drawio).
Base: `main` commit `3ddb62a45c849a4c13414214bb200312f79929d7`, plus uncommitted
Docker/ETL support files imported from `dat/week2-spark-etl` into the working tree.
Latest verification: 20/20 Docker tests, 13/13 Conda core tests, all 3 notebooks,
and a 2-message local Kafka replay smoke passed. See the [check report](../../reports/project-check-2026-10-25.md).

## 01 Target architecture

The first diagram page shows the intended academic prototype:

- Batch: historical PaySim CSV → Spark ETL → Parquet → model training → versioned model and metadata.
- Replay: historical CSV → validating producer → Kafka → Spark Structured Streaming → predictions/alerts → NoSQL → FastAPI → dashboard.
- The streaming job loads the model for inference. FastAPI serves stored results.
- Ground-truth labels stay offline; evaluation joins labels and exported predictions by `transaction_id`.
- Logs, quality checks, model metrics and processing measurements support the pipeline.

Replay simulates near-real-time ingestion; it is not a live bank feed.
Target components and arrows express documented intent, not verified deployment.
See [architecture](../week1/architecture.md), [interfaces](../week1/interfaces.md),
[evaluation plan](../week1/evaluation-plan.md) and [project charter](../week1/project_charter.md).

## 02 Current main

The second page distinguishes the local working tree from planned components.

| Component | Repository evidence | Status |
| --- | --- | --- |
| Spark batch ETL | [etl.py](../../src/processing/etl.py), [schema.py](../../src/processing/schema.py), [etl.json](../../configs/etl.json) | Code and support files present; fixture ETL verified |
| Docker Linux Spark | [Compose](../../docker-compose.yml), [Dockerfile](../../Dockerfile.jupyter) | Configured Spark 4.2.0 / Java 21; `local[2]` |
| Local Parquet outputs | ETL writes processed, quarantine, replay and labels datasets | Fixture Parquet write/readback verified |
| CSV replay producer | [producer.py](../../src/ingestion/producer.py), [validation.py](../../src/ingestion/validation.py) | Sample replay verified; input edge cases remain |
| Kafka smoke consumer | [consumer_test.py](../../src/ingestion/consumer_test.py) | Code present; prints/counts events, without scoring |
| Kafka broker | [configuration](../../configs/kafka.yaml), [runbook](../week2/kafka-design.md) | Local broker replay smoke verified on a temporary topic |
| Training and model artifact | [model configuration](../../configs/model.yaml) | Planned; no tracked training implementation |
| Structured Streaming scoring | [architecture](../week1/architecture.md) | Planned; no tracked scoring job |
| NoSQL, FastAPI and dashboard | [architecture](../week1/architecture.md) | Planned; no tracked serving implementation |

## Runtime boundaries and integration gaps

- **Docker boundary:** service `spark` runs in a Linux container; the project
  directory is bind-mounted at `/workspace`, sharing input, code and outputs.
  The Dockerfile installs PySpark and kafka-python. Host Conda `bigdata` is the
  local notebook/check environment; the configured batch ETL runs in Docker.
  Host [requirements](../../requirements.txt) alone do not define that runtime.
- **Current replay input:** the producer reads raw PaySim CSV directly. It does
  not consume the ETL replay Parquet output; that handoff remains unimplemented.
- **ID compatibility:** ETL hashes the file checksum plus physical source line.
  The producer builds `paysim-{checksum_prefix}-row-{row_number}` instead.
  These policies must align before an evaluation join can be trusted.
- **Field compatibility:** ETL replay uses `transaction_type`, `source_account`
  and `destination_account`; the producer expects raw CSV `type`, `nameOrig`
  and `nameDest`, then emits `origin_account_id`/`destination_account_id`.
  ETL retains `step`; the producer generates simulated `event_time`.
- **Kafka topic:** current code, config and W2 runbook use `transactions`.
  W1 architecture/interfaces still name `transaction-events`; reconcile these
  documents before integration. Message keys are `transaction_id`.
- **Storage:** ETL implements local filesystem Parquet. HDFS paths and durable
  checkpoints in [spark.yaml](../../configs/spark.yaml) are targets; HDFS and
  distributed execution were not verified by this review.
- **NoSQL choice:** MongoDB remains proposed. [Decisions](../week1/decisions.md)
  suggests one `predictions` collection, while [access patterns](../week1/nosql-access-patterns.md)
  proposes separate `predictions` and lifecycle-managed `alerts` collections.
  The final collection layout remains open.

Historical [ETL results](../../reports/etl-test-report.md) describe saved executions,
not a new test of these uncommitted imports. No end-to-end exactly-once or
production-readiness claim is made.
