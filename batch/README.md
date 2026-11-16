# batch/: lakehouse, labels, graph (owner: R4)

```
batch/
├── ddl/iceberg_tables.sql         # Silver/Gold table definitions = batch contract
├── airflow/                       # DAGs (+ Dockerfile baking them into cg-airflow image)
├── src/cg_batch/
│   ├── spark_session.py           # Iceberg JDBC catalog + S3 config from config/*.yaml   ✅
│   ├── jobs/                      # one module per Spark job (see jobs/__init__.py table)  ⏳
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

## Local run

```bash
make up && make up-batch && make image-batch
# DDL (once): run batch/ddl/iceberg_tables.sql through a Spark SQL session (BAT-02 provides a helper job)
kubectl apply -f batch/k8s/sparkapplication.template.yaml
kubectl -n cg-batch get sparkapplications
```
