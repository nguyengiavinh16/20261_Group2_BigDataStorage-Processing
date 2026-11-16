"""PyFlink job: raw.trades.*.v1 → canonical.trades.v1 + dlq.normalizer.v1 (ING-06). NOT IMPLEMENTED YET.

Required behaviour (acceptance criteria in team/02-ingestion-normalization.md):
1. Source: KafkaSource on topic pattern ^raw\\.trades\\..*\\.v1$, group id 'cg-normalizer', start from committed
   offsets (earliest on first run).
2. Map: normalize_envelope(value, source_topic=..., partition=..., offset=...) → split into main output and a
   side output (DLQ).
3. Event time: timestamp = event_ts_us/1000; WatermarkStrategy.for_bounded_out_of_orderness(watermark_seconds).
4. Dedup: key_by(event_id) + ValueState[bool] with TTL 10 min; drop duplicates and count them in a metric
   (cg_normalizer_duplicates_total). Duplicates are expected after collector reconnects.
5. Sinks: KafkaSink canonical.trades.v1 (key = symbol), KafkaSink dlq.normalizer.v1 (key = source_topic),
   delivery guarantee AT_LEAST_ONCE (consumers dedup on event_id), checkpoint every config flink.checkpoint_interval.
6. Must run both locally (`python -m normalizer.job` with a local mini-cluster, Kafka at localhost:32100) and as
   a FlinkDeployment (streaming/k8s/normalizer.flinkdeployment.yaml).

Needs flink-sql-connector-kafka jar on the classpath (see streaming/README.md).
If PyFlink blocks the team past the P2 deadline, ADR-0003 allows Spark Structured Streaming with the SAME
normalize_envelope function.
"""


def main() -> None:
    raise NotImplementedError("ING-06")


if __name__ == "__main__":
    main()
