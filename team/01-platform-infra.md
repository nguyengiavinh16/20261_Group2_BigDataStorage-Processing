# R1: Platform & Infra

> Dành cho: thành viên R1 và AI agent của họ. **Đọc `team/00-shared-context.md` trước.**
> Người dự phòng (backup): R2.

## 1. Sứ mệnh

Cung cấp một nền tảng Kubernetes **dựng lại được bằng một lệnh**: trên laptop (kind) từ P1, trên GCP VM (kubeadm) ở P4. Bốn thành viên còn lại chỉ việc `make up-*` rồi deploy code của họ. R1 cũng chịu trách nhiệm chứng minh hệ thống **chịu tải, tự phục hồi và tốn ít chi phí** (output O8).

## 2. Phạm vi

**Sở hữu (được sửa tự do):** `infra/**`, `.github/**`, `Makefile`, `docs/local-setup.md`, `docs/cloud-migration.md`, `config/cloud.yaml` (phần hạ tầng).

**Phối hợp (sửa qua PR có người liên quan review):**
- `config/base.yaml`, `config/local.yaml` (hostname, port)
- Manifest `k8s/` trong thư mục của R2–R5 (R1 review resource và security, họ sở hữu nội dung)

**Ngoài phạm vi:**
- Logic nghiệp vụ (collector, Flink job, Spark job, API)
- Nội dung contract
- Viết DAG

R1 không viết job thay người khác, nhưng **phải giúp họ chạy được job trên cluster**.

## 3. Môi trường theo giai đoạn

| Phase | R1 làm gì |
|---|---|
| P0–P3 | kind 1 node trên laptop. Mọi thành viên dùng cùng script `infra/scripts/up-local.sh`. **Không** tạo tài nguyên cloud, trừ 1 VM e2-micro để thử kết nối Binance từ region châu Á (tùy chọn, xin nhóm đồng ý) |
| P4 | GCP: 4 VM (1 control-plane, 2 worker on-demand, 1 worker Spot), kubeadm, GCS. Kế hoạch ở `docs/cloud-migration.md` |

## 4. Đầu vào (Inputs)

| Input | Từ ai | Dạng | Nếu chưa có |
|---|---|---|---|
| Danh sách topic | `contracts/kafka-topics.yaml` (chung) | YAML → `make gen-topics` | Đã có bản v1 |
| Nhu cầu tài nguyên của từng job (RAM, CPU, số replica) | R2, R3, R4, R5 | Ghi trong README hoặc manifest của họ | Dùng mặc định trong `infra/README.md` §Resource budget |
| Port metrics của từng service | R2, R3, R5 | Port + path `/metrics` | Collector: 9108, Flink: 9249 (prom reporter) |
| Yêu cầu secret (MinIO key, Postgres password) | R2, R4, R5 | Tên biến `CG_*` | Theo `.env.example` |

## 5. Đầu ra (Outputs) và tiêu chí chấp nhận

| Output | Consumer | Tiêu chí chấp nhận |
|---|---|---|
| `make up` (core) | mọi người | Chạy được trên **5/5 laptop** (16 GB), < 15 phút lần đầu, idempotent khi chạy lại; Kafka Ready, 14 topic Ready, MinIO bucket `cg-lake` tồn tại, Postgres có 3 DB |
| `make up-stream`, `up-batch`, `up-serving`, `up-monitoring` | R2–R5 | Mỗi profile có lệnh smoke test trong `docs/local-setup.md` và đều pass |
| Secrets cho job | R2, R4, R5 | K8s Secret `cg-storage-credentials`, `cg-postgres-credentials` trong các namespace cần dùng; không còn password trong manifest của job |
| `make image-*` | mọi người | Build + `kind load` cho 5 image; tag `dev` local, tag theo git SHA trên cloud |
| Grafana dashboards (O8) | cả nhóm, giảng viên | Ít nhất: Kafka (msg/s theo topic, consumer lag theo group), collectors (msg/s, reconnects), Flink (records/s, checkpoint, backpressure), pods (CPU/RAM), latency end-to-end của detector |
| Cluster GCP (P4) | mọi người | Cùng các profile chạy với `CG_PROFILE=cloud`; Kafka 3 broker RF=3; dữ liệu ở GCS; script up/down; cảnh báo ngân sách |
| Demo chịu lỗi (P4) | O8 | Kill 1 Kafka broker và 1 Flink TaskManager khi đang chạy: không mất alert, job phục hồi từ checkpoint, có ảnh chụp Grafana |
| Báo cáo chi phí (P4) | cả nhóm | Bảng chi phí theo tuần, so với ngân sách |

## 6. Task board

Trạng thái: `[ ]` chưa làm · `[~]` đang làm · `[x]` xong (kèm link PR).

### P0: Khởi động
- [ ] **INF-01** Tạo GitHub repo, đẩy codebase này lên, bật branch protection cho `main` (PR + 1 approve + CI), điền `.github/CODEOWNERS` bằng username thật, kiểm tra CI chạy xanh.
  - *Done khi:* một PR thử bị chặn merge nếu CI đỏ.
- [ ] **INF-02** Kiểm chứng `docs/local-setup.md` trên cả 5 laptop (WSL2, Docker RAM ≥ 8 GB, kind, kubectl, helm, Python 3.11). Ghi lại vấn đề gặp phải vào mục Troubleshooting.
  - *Done khi:* 5/5 người chạy `make dev-install test` xanh.

### P1: Nền tảng local
- [ ] **INF-03** Chốt version (kind node image, Strimzi, Flink operator, Spark operator, chart) trong `up-local.sh` (biến ở đầu file). Thay `STRIMZI_INSTALL_URL=latest` bằng version cố định.
- [ ] **INF-04** Kafka Strimzi KRaft 1 node + listener NodePort (`localhost:32100`) + topic sinh từ contract. Kiểm tra producer/consumer từ host và từ pod.
- [ ] **INF-05** MinIO + bucket init + Postgres (3 DB) + K8s Secret cho `cg-ingest`, `cg-stream`, `cg-batch`, `cg-serving`. **Kiểm chứng image MinIO pull được** (xem ADR-0002); nếu không được thì chuyển sang server S3-compatible khác.
- [ ] **INF-06** Flink operator + Spark operator: chạy thử một FlinkDeployment mẫu và SparkPi. Quyết định checkpoint của Flink (S3 MinIO qua plugin `flink-s3-fs-presto`/`hadoop`, hoặc PVC). Cập nhật `streaming/k8s/` template.
- [ ] **INF-07** MongoDB + Redis (`make up-serving`).
- [ ] **INF-08** kube-prometheus-stack; quy ước `PodMonitor`/`ServiceMonitor` (label `app.kubernetes.io/part-of: cryptoguard`); bật metrics cho Kafka (Strimzi `metricsConfig` + Kafka Exporter).
- [ ] **INF-09** Chuẩn build image: `make image-*`, quy ước tag, (tùy chọn) CI build image để bắt Dockerfile lỗi.

### P2: Dịch vụ nền cho MVP
- [ ] **INF-10** Airflow (Helm, LocalExecutor, DB ngoài trên Postgres, image `cg-airflow` có DAG bên trong). Kiểm chứng: DAG `binance_vision_daily` hiển thị trên UI `:30808`. Kiểm tra mặc định chart dùng Airflow 2 hay 3.
- [ ] **INF-11** MLflow server (backend Postgres DB `mlflow`, artifact lưu ở MinIO `s3://cg-lake/mlflow`), NodePort 30500. (Tùy chọn) Trino với Iceberg JDBC catalog cho Superset.
- [ ] **INF-12** Grafana dashboards O8 (xem §5), lưu JSON trong `infra/grafana/` để dựng lại được.

### P4: Cloud
- [ ] **INF-20** GCP project, nâng cấp billing, budget alert 50/80/100%, quota CPU ≥ 32 vCPU ở region châu Á. Thêm 4 thành viên vào IAM.
- [ ] **INF-21** VPC, firewall (SSH chỉ qua IAP; cổng K8s chỉ nội bộ), 4 VM (bảng trong `docs/cloud-migration.md`), cùng 1 zone. Viết bằng Terraform hoặc script `gcloud` trong `infra/cloud/`.
- [ ] **INF-22** kubeadm + Calico + MetalLB + Longhorn (chỉ cho PVC của Kafka/Mongo/Postgres), label node `cg/pool=general|spot`.
- [ ] **INF-23** GCS bucket + HMAC/service account; thử Iceberg qua S3 interoperability cùng R4 (ADR-0002); overlay manifest cho cloud (Kafka 3 node, `topics.cloud.yaml`).
- [ ] **INF-24** Tối ưu chi phí: instance schedule, `cluster-up.sh`/`cluster-down.sh`, node Spot cho Spark/training, báo cáo chi phí theo tuần.
- [ ] **INF-25** Demo chịu lỗi + PodDisruptionBudget + test tải cùng R2/R3 (replay x100).
- [ ] **INF-26** Runbook cloud và tài liệu hạ tầng cuối cùng cho báo cáo.

## 7. Quy tắc kỹ thuật riêng

- **Không** đưa secret thật vào git. Secret local chỉ là giá trị mẫu, có ghi chú `local only`. Trên cloud dùng K8s Secret tạo bằng script đọc từ biến môi trường hoặc Secret Manager.
- Mọi pod có `resources.requests/limits`. Mọi tài nguyên có label `app.kubernetes.io/part-of: cryptoguard`.
- Script phải **idempotent** (`helm upgrade --install`, `kubectl apply`) và chạy được trong WSL2 lẫn Linux.
- Thay đổi version operator: ghi vào PR description và báo cả nhóm, vì có thể làm hỏng job của họ.
- `infra/k8s/generated/` chỉ sinh bằng `make gen-topics`, không sửa tay.

## 8. Kiểm thử bắt buộc

- `make down && make up` trên máy sạch, ghi lại thời gian.
- Smoke test của từng profile (`docs/local-setup.md` §3).
- CI: lint + test + kiểm tra file generated luôn xanh.
- P4: chạy lại smoke test trên cloud; demo chịu lỗi có ảnh chụp và log.

## 9. Cần tìm hiểu

Kubernetes (Pod, Deployment, StatefulSet, Service/NodePort, PVC, Secret, RBAC); kind; Helm; Strimzi (Kafka, KafkaNodePool, KafkaTopic, listener); Flink Kubernetes Operator; Kubeflow Spark Operator; kube-prometheus-stack; kubeadm, Calico, MetalLB, Longhorn; GCP Compute Engine, IAP, Spot VM, budget, GCS HMAC.

## 10. Giao tiếp với các vai trò khác

| Khi nào | Với ai | Nội dung |
|---|---|---|
| Kết thúc INF-04/05 | Tất cả | Thông báo endpoint, cách tạo secret, lệnh smoke test |
| INF-06 | R2, R3 | Template FlinkDeployment chạy được, cách xem log và Flink UI |
| INF-06, INF-10 | R4, R5 | Template SparkApplication, Airflow chạy DAG, service account |
| INF-23 | R4 | Cùng kiểm chứng Iceberg trên GCS |
| Hằng tuần (P4) | Tất cả | Báo cáo chi phí, lịch bật/tắt cụm |

## 11. Rủi ro và câu hỏi mở

- Laptop 16 GB không đủ chạy song song stream + batch. Giải pháp: profile riêng; ai có máy 32 GB thì làm integration test.
- Image MinIO hoặc chart Airflow thay đổi theo version. Giải pháp: pin version ở INF-03/05/10.
- **Hỏi giảng viên:** có bắt buộc HDFS không? Có hỗ trợ credit cloud không?

## 12. Prompt mẫu cho AI agent

> Tôi là R1 (Platform & Infra) của dự án CryptoGuard. Đọc `AGENTS.md`, `team/00-shared-context.md`, `team/01-platform-infra.md`, `infra/README.md`. Làm task **INF-05**: hoàn thiện MinIO + Postgres + Secrets cho các namespace. Chỉ sửa trong `infra/`, `Makefile`, `docs/local-setup.md`. Giữ script idempotent, không đưa secret thật vào git. Kết thúc bằng danh sách lệnh smoke test tôi cần chạy và kết quả mong đợi.
