# Architecture

- **Batch path:** PaySim CSV -> validation/ETL -> Parquet -> model training -> versioned model artifact + evaluation metrics.
- **Speed path:** CSV replay producer -> Kafka topic `transactions` -> Spark Structured Streaming JSON parse + model scoring -> MongoDB upsert sink.
- **Serving path:** FastAPI loads the same model artifact and scores a single transaction; Streamlit calls the API.
- **Operational path:** logs, health endpoint, simple latency/error probe, unit tests, Docker Compose and starter Kubernetes manifests.

This is an academic prototype. The current Compose file does not deploy a Spark cluster, HDFS, or Kubernetes. Spark streaming is a separate process and requires a matching Spark Kafka connector. HDFS integration is intentionally environment-dependent; local Spark ETL can be redirected to an HDFS URI when Hadoop is configured.
