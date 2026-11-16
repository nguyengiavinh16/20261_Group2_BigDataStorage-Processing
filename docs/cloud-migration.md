# Cloud migration plan (phase P4, owner R1, all roles verify their part)

Decision (team discussion 16/11/2026): **Google Cloud**, self-managed Kubernetes (kubeadm) on Compute Engine VMs,
data lake on **GCS**. Reasons: $300 trial credits, cheaper e2 VMs, Spot VMs for batch. Confirm with the lecturer
first whether the school provides credits on AWS/GCP (free credits beat any price difference).

## 1. Prerequisites (start early, quotas take time)

- Billing account upgraded from free trial (trial limits to 8 concurrent vCPUs; credits are kept after upgrade).
- Budget alerts at 50 / 80 / 100 % of the credit amount; label every resource `project=cryptoguard`.
- CPU quota ≥ 32 vCPU in the chosen region.
- Region in Asia (e.g. `asia-southeast1`): **Binance blocks US IPs**. Test a WS connection from a tiny VM first.

## 2. Target layout

| VM | Type (suggestion) | Disk | Pricing | Runs |
|---|---|---|---|---|
| cp-1 | e2-standard-4 | 50 GB | on-demand | control plane, Airflow, Prometheus |
| worker-1, worker-2 | e2-standard-8 | 150 GB pd-balanced | on-demand | Kafka (3 brokers spread), Flink, MongoDB, Redis, Postgres |
| worker-spot-1 | e2-standard-8 | 100 GB | **Spot** | Spark executors, training jobs (`nodeSelector: cg/pool=spot`) |

All in one zone (cross-zone traffic is billed; Kafka replication is chatty).

## 3. What changes vs local

| Area | Local | Cloud |
|---|---|---|
| Cluster | kind | kubeadm + Calico + MetalLB, Longhorn only for Kafka/Mongo/Postgres PVCs |
| Kafka | 1 node, RF 1 | 3 nodes, RF 3, min ISR 2 (`topics.cloud.yaml`) |
| Object storage | MinIO | GCS bucket (Iceberg via S3 interoperability or GCSFileIO: ADR-0002) |
| Config | `CG_PROFILE=local` | `CG_PROFILE=cloud` + K8s Secrets for keys |
| Images | `kind load` | Artifact Registry |
| Exchanges | Binance + OKX | + Coinbase + Kraken |
| Data volume | days, ≤10 symbols | 3–6 months, 50+ symbols, 3 months ETH transfers |

## 4. Cost discipline

- Instance schedules: stop VMs at night; start for work sessions. Disks persist, data lives in GCS.
- `cluster-up.sh` / `cluster-down.sh` in `infra/cloud/scripts/`.
- Batch on Spot; checkpoint long jobs.
- Weekly cost check in the team sync (R1 reports).

## 5. Migration checklist

1. INF-20 project, billing, budget, quotas · 2. INF-21 VPC, firewall (SSH via IAP only), VMs · 3. INF-22 kubeadm cluster ·
4. INF-23 GCS bucket + HMAC/service account, storage switch verified by BAT jobs · 5. Re-deploy all profiles with cloud overlays ·
6. Each role re-runs its acceptance tests on cloud · 7. Scale-up data backfill (BAT-11) · 8. Load + chaos tests (INF-25, DET-13).
