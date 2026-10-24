# Project Charter

## Problem
Build an academic prototype that analyzes historical transaction records and
simulates near-real-time fraud detection using a replayed event stream.

## Objectives
1. Inspect and validate a public or synthetic dataset.
2. Demonstrate distributed-storage and Spark batch processing.
3. Train and evaluate a leakage-aware fraud classifier.
4. Replay transactions through Kafka and score them with Structured Streaming.
5. Store predictions and alerts in NoSQL and expose them through an API/dashboard.
6. Measure model quality, throughput, latency, and recovery behavior.

## Scope boundaries
This is not a production banking service. Replayed historical records are not
genuinely live transactions. No benchmark results are assumed in advance.

## Success criteria
Success is measured by reproducible execution, consistent schemas/features,
automated tests, traceable model metadata, working end-to-end flow, and measured
rather than invented evaluation results.

## Assumptions
PaySim is the initial dataset. The actual downloaded schema and local hardware
must be inspected before implementation details are frozen.
