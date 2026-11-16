# ADR-0003: PyFlink for streaming; business logic in pure Python

- Status: Accepted
- Date: 2026-11-16

## Context
Detection needs event-time windows, keyed state (dedup, cases, baselines), watermarks and side outputs. W1 planned
Spark Structured Streaming. The team writes Python.

## Decision
- Streaming engine: **Apache Flink (PyFlink DataStream API, Flink 1.20)** run by the Flink Kubernetes Operator.
- Parsing, features, rules, case logic live in **pure Python modules** (`normalizer.adapters/normalize`,
  `cryptoguard_common.features`, `detector.rules/cases`) with unit tests. Flink code only wires sources, state, windows, sinks.
- Escape hatch: if PyFlink blocks the team by the end of P2, the same pure functions are wired in Spark Structured
  Streaming (foreachBatch / applyInPandasWithState). Contracts do not change.

## Consequences
- + True per-event stateful processing with low latency; good story for the report.
- + Logic is testable without a cluster and shared with Spark batch (no train/serve skew).
- − PyFlink setup (Java, connector jars, Python ≤ 3.11 in the image) costs time in P1; R2/R3 must spike it early.
