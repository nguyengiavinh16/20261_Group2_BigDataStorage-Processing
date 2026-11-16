# ADR-0002: S3-compatible object storage, MinIO locally, GCS in cloud

- Status: Accepted (cloud access method to be confirmed in INF-23)
- Date: 2026-11-16

## Context
The data lake (Bronze files + Iceberg warehouse) must be cheap at scale and identical in code across environments.
Persistent disks on VMs cost ~5× object storage per GB (and more with replicated volumes).

## Decision
- All components access storage through the **S3 API** configured by `config.storage.*` (endpoint, bucket, keys).
- Local: MinIO in `cg-storage`. Cloud: GCS bucket.
- In cloud, first try GCS **S3 interoperability** (HMAC keys) so no code changes. If Iceberg S3FileIO has issues with it,
  switch the Iceberg catalog's `io-impl` to `GCSFileIO` (only `cg_batch.spark_session` changes) and keep boto3/S3 for Bronze.
- HDFS is not used unless the lecturer requires it (then: a small HDFS for a demo dataset only).

## Consequences
- + VMs become disposable; data survives cluster rebuilds.
- − MinIO community distribution changed in 2025: the manifest pins an existing image tag; fallback = another
  S3-compatible server (SeaweedFS, Garage). Code is unaffected.
