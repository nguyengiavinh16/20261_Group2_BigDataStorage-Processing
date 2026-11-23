# batch/: lakehouse, labels, graph (owner: R4)

```
batch/
├── ddl/iceberg_tables.sql         # Silver/Gold table definitions = batch contract
├── airflow/                       # DAGs (+ Dockerfile baking them into cg-airflow image)
├── src/cg_batch/
│   ├── spark_session.py           # Iceberg JDBC catalog + S3 config from config/*.yaml   ✅
│   ├── jobs/                      # one module per Spark job (see jobs/__init__.py table)  ⏳
│   ├── sources/                   # pure parsers: binance.vision, OFAC, darklist, pump dataset, EVM ✅
│   ├── tools/                     # host CLI tools (BAT-01 spike, pump candidates)          ✅
│   └── graph/taint.py             # reference taint/hops implementation = spec for Spark   ✅
├── k8s/sparkapplication.template.yaml
├── Dockerfile                     # cg-batch (Spark 3.5)
└── tests/
```

## Lakehouse layout

- **Bronze**: raw files exactly as downloaded / archived, `s3://cg-lake/bronze/...` (see airflow/dags/README.md). Immutable.
- **Silver**: cleaned, typed, deduplicated Iceberg tables `cg.silver.*`.
- **Gold**: features, baselines, graph, risk, evaluation tables `cg.gold.*`.

## Wallet risk formula (v1, to refine in BAT-09; document every change here)

```
label_score = max(confidence of direct bad labels valid at as_of)            (0 if none)
risk_score  = max(label_score, taint_score)                                  # v1
is_exchange_wallet → risk_score capped at 0.3 (they receive everything)
```

## Sources and spike tools (BAT-01)

What every source really contains (schemas, sizes, pitfalls) is in [docs/source-notes.md](../docs/source-notes.md).
The tools run on the host, from the repo root, with the dev venv:

```bash
export PYTHONPATH=batch/src
# measure local copies in data/raw/bat01/ (+ AWS token_transfers footer, no full download)
python -m cg_batch.tools.source_spike --dir data/raw/bat01 --eth-date 2026-11-20 --out data/raw/bat01/summary.json
# HEAD-check binance.vision for every Binance pump event → contracts/reference/pump_events_candidates.csv
python -m cg_batch.tools.pump_candidates --pump-csv data/raw/bat01/pump_telegram.csv
```

## Local run

R4 only needs MinIO + Postgres (from `make up`) and `make up-batch` (Spark Operator, Airflow). Credentials:
copy `.env.example` to `.env` (host runs), and create the in-cluster secret once from the same values:

```bash
kubectl -n cg-batch create secret generic cg-batch-env \
  --from-literal=CG_STORAGE__ACCESS_KEY=... --from-literal=CG_STORAGE__SECRET_KEY=... \
  --from-literal=CG_POSTGRES__PASSWORD=...
```

### Iceberg tables (BAT-02)

`cg_batch.jobs.apply_ddl` creates every namespace/table of `ddl/iceberg_tables.sql` (`IF NOT EXISTS`, safe to
re-run); `--smoke` also writes, reads and drops a scratch table to prove the S3FileIO + JDBC catalog write path.

```bash
# host, Spark local mode (JDK 17 + `pip install pyspark==3.5.3` in the venv), MinIO/Postgres via NodePorts
PYTHONPATH=batch/src python -m cg_batch.jobs.apply_ddl --smoke
# cluster
make image-batch
kubectl -n cg-batch delete sparkapplication cg-apply-ddl --ignore-not-found
kubectl apply -f batch/k8s/apply-ddl.sparkapplication.yaml
kubectl -n cg-batch logs cg-apply-ddl-driver | grep '"ddl applied"'
```

Verified together (2026-11-23): Spark 3.5.3 / Scala 2.12, Iceberg 1.6.1 (`iceberg-spark-runtime-3.5_2.12` +
`iceberg-aws-bundle`), `hadoop-aws` 3.3.4 + `aws-java-sdk-bundle` 1.12.262, `postgresql` 42.7.3, spark-operator
chart 2.6.0 (service account `spark-operator-spark`). The image bakes these jars (`batch/Dockerfile`, kept in
sync with `spark_session.PACKAGES` by a test); host runs resolve them with Ivy.

Windows host without WSL: Spark needs `winutils.exe` to `chmod` files added with `spark.jars.packages`. Without
it, resolve the jars once (the first run downloads them into `~/.ivy2/jars` before failing), then put them on
the driver classpath instead, in `.env`:

```
CG_SPARK__JARS_PACKAGES=false
CG_SPARK__DRIVER_EXTRA_CLASSPATH=C:/Users/<you>/.ivy2/jars/*
```

### Bronze ingestion DAG (BAT-03)

`airflow/dags/binance_vision_daily.py` wires `cg_batch.bronze`: plan (watchlist × run date, or explicit
symbols/dates, or pump events = pump day + `detector.baseline_days` before it) → download each day file,
verify `.CHECKSUM`, upload to `bronze/binance_vision/spot/trades/<SYMBOL>/<date>.zip` with `sha256` metadata →
summarize. Same sha256 already in Bronze → `skipped` (incremental, idempotent); a different sha256 →
`conflict` (never overwritten, fails the run); no file on binance.vision → `missing`. `data_budget` is enforced
unless the run sets `allow_over_budget`.

```bash
make image-airflow
helm upgrade --install airflow apache-airflow/airflow --version 1.16.0 -n cg-batch \
  -f infra/helm-values/airflow.local.yaml -f batch/airflow/airflow.cg-env.local.yaml --timeout 15m --wait
kubectl -n cg-batch exec airflow-scheduler-0 -c scheduler -- airflow dags unpause binance_vision_daily
# evaluation shortlist (8 events × 8 days, 0.94 MB) — over the 7-day budget on purpose:
kubectl -n cg-batch exec airflow-scheduler-0 -c scheduler -- airflow dags trigger binance_vision_daily \
  --conf '{"pump_ids": ["GTO-BTC:binance:1604419200000000"], "allow_over_budget": true}'
```
