# Architecture

## Lambda Architecture
- **Batch layer:** historical data ingestion, Spark cleaning and feature
  engineering, Parquet outputs, aggregations, model training and evaluation.
- **Speed layer:** a producer replays historical records to Kafka; Spark
  Structured Streaming parses and validates events, scores them, and creates alerts.
- **Serving layer:** MongoDB stores queryable predictions and alerts; FastAPI
  exposes queries; Streamlit visualizes results. Batch aggregates and streaming
  results will be exposed separately unless a verified combined query is implemented.

## Data flow
PaySim CSV -> raw storage -> Spark ETL -> processed Parquet -> training/model
artifact. In parallel, selected records -> replay producer -> Kafka ->
Structured Streaming -> model inference -> MongoDB -> FastAPI -> Streamlit.

## Consistency contracts
- A documented transaction event schema is shared by producer and consumer.
- Model metadata records feature order, preprocessing, model version, and threshold.
- Deterministic event/prediction IDs support duplicate-aware writes.
- Event time, ingestion time, and processing time are distinct concepts.
- Checkpoints support Spark recovery but do not independently guarantee exactly-once
  effects in an external database.

## Local-first strategy
Validate each service and the end-to-end local flow before Kubernetes. A single
computer is a development environment, not proof of distributed production scale.
