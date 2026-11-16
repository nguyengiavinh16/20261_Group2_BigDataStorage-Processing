# infra/: platform (owner: R1)

```
infra/
├── local/kind-config.yaml        # single-node kind cluster + NodePort → localhost mappings
├── k8s/
│   ├── base/namespaces.yaml      # cg-ingest, cg-kafka, cg-stream, cg-batch, cg-storage, cg-serving, cg-monitoring
│   ├── kafka/kafka.local.yaml    # Strimzi Kafka (KRaft, 1 node)
│   ├── storage/                  # MinIO (S3) + bucket init, Postgres (iceberg_catalog, mlflow, airflow DBs)
│   ├── serving/                  # MongoDB, Redis
│   └── generated/                # KafkaTopic CRs from contracts/kafka-topics.yaml (make gen-topics). Do not edit
├── helm-values/                  # values for Airflow, kube-prometheus-stack, ...
├── scripts/
│   ├── up-local.sh               # make up / up-stream / up-batch / up-serving / up-monitoring
│   ├── build-image.sh            # make image-<component> → docker build + kind load
│   ├── gen_topics.py             # make gen-topics
│   └── seed_samples.py           # make seed-samples
└── cloud/                        # P4: GCP (VMs, kubeadm, GCS). Empty until INF-20
```

## Local endpoints (from WSL2/host)

| Service | Address | Notes |
|---|---|---|
| Kafka bootstrap | `localhost:32100` | in-cluster: `cg-kafka-kafka-bootstrap.cg-kafka:9092` |
| MinIO S3 / console | `http://localhost:30900` / `http://localhost:30901` | `cgadmin` / `cgadmin-change-me` (local only) |
| Postgres | `localhost:30432` | DBs: `iceberg_catalog`, `mlflow`, `airflow` |
| MongoDB | `mongodb://localhost:30017` | |
| Redis | `redis://localhost:30379` | |
| Grafana | `http://localhost:30300` | `admin` / `cg-admin` |
| Airflow | `http://localhost:30808` | after `make image-airflow up-batch` |
| MLflow | `http://localhost:30500` | INF-11 |

## Resource budget (laptop, 16 GB)

| Profile | Approx. RAM | Who needs it |
|---|---|---|
| core (Kafka, MinIO, Postgres) | ~2.5 GB | everyone |
| stream (Flink operator + 1 JobManager + 1 TaskManager per job) | +2–3 GB | R2, R3 |
| batch (Spark operator + 1 driver + 1 executor, Airflow) | +3–4 GB | R4, R5 |
| serving (MongoDB, Redis) | +0.7 GB | R3, R5 |
| monitoring | +1.5 GB | R1 (others optional) |

Do not run everything at once on 16 GB. Stop what you don't use: `kubectl scale deploy/<x> --replicas=0`.

## Cloud (P4)

See [docs/cloud-migration.md](../docs/cloud-migration.md). The same manifests are reused with cloud overlays (more replicas, GCS instead of MinIO, node pools on-demand vs spot).
