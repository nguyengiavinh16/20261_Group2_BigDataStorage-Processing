# Performance Evaluation Plan

No performance results are claimed in advance.

Record the hardware, operating system, Python/Java/Spark/Kafka versions, dataset
size, record count, Spark settings, and service configuration.

Measure:
- batch ETL wall-clock duration and records processed;
- streaming throughput (events/second);
- event-to-output latency with a clearly defined measurement method;
- number of predictions and alerts;
- model precision, recall, F1, PR-AUC, ROC-AUC where useful, and confusion matrix;
- storage size and restart/recovery behavior where practical.

Repeat runs where feasible and report the method and variability. A single-machine
test does not establish production-scale distributed performance.
