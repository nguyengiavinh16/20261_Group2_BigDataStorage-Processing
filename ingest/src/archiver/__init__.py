"""Kafka → Bronze archiver (ING-08, owner R2).

Consumes raw.*, canonical.*, features.*, alerts.*, dlq.* and writes newline-delimited JSON (gzip) or Parquet to
  s3://<bucket>/bronze/kafka/<topic>/dt=YYYY-MM-DD/hour=HH/<partition>-<first_offset>.jsonl.gz
Commit Kafka offsets only after the object is written (at-least-once; Silver dedups on event_id).
Option B: Kafka Connect + S3 sink connector via Strimzi KafkaConnect (decide in ING-08, record in an ADR).
"""
