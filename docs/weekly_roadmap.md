# Eight-Week Roadmap

| Week | Focus | Deliverable / acceptance gate |
|---|---|---|
| 1 | Scope, dataset, architecture, repository | Schema inspector, docs, config, sample fixture, basic tests pass |
| 2 | HDFS, Spark ETL, Kafka replay | Raw/processed storage, validation, topic and producer integration tested |
| 3 | Batch ML | Leakage-reviewed features, baseline/candidate model, metrics, saved metadata |
| 4 | Structured Streaming | Kafka consumer, scoring, event-time policy, checkpoint, duplicate handling |
| 5 | MongoDB, API, dashboard | End-to-end MVP with stored results and queryable dashboard |
| 6 | Optimization and reliability | Measured baseline, throughput/latency tests, recovery and failure evidence |
| 7 | Containers and deployment | Docker Compose and staged Kubernetes/cloud configuration verified if claimed |
| 8 | Final evaluation and report | Reproducible demo, final report, tests, measured results, limitations |

Do not move to a later gate merely because its configuration files exist. Each
week should extend the same repository and preserve prior interfaces.
