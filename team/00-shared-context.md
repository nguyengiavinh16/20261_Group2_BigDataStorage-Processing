# 00: Context chung của dự án CryptoGuard

> **Đọc file này trước tiên.** Dành cho cả 5 thành viên và mọi AI agent làm việc trong repo.
> Sau đó đọc file vai trò của bạn (`team/0X-*.md`). Nếu file này mâu thuẫn với `contracts/` thì `contracts/` đúng.

---

## 1. Dự án là gì (tóm tắt 30 giây)

CryptoGuard là hệ thống Big Data **phát hiện thao túng thị trường crypto và rửa tiền on-chain**:

- **Off-chain:** thu giao dịch real-time từ nhiều sàn (Binance, OKX; sau thêm Coinbase, Kraken), phát hiện **pump-and-dump**, sau đó là wash trading, lệch giữa các sàn và spoofing.
- **On-chain:** phân tích dòng tiền ERC-20 (USDT/USDC/WETH) trên Ethereum, tính **điểm rủi ro ví** dựa trên khoảng cách và mức "nhiễm bẩn" (taint) tới ví bị trừng phạt hoặc lừa đảo (OFAC, darklist).
- **Nối hai phía:** tiền từ ví rủi ro chảy vào sàn, kết hợp với bất thường giao dịch trên sàn.
- **Đánh giá trung thực:** phát lại (replay) dữ liệu của các vụ pump **có thật** trong lịch sử để đo recall, precision và độ trễ phát hiện.

Đây là bản kế thừa đề tài Fraud Detection (PaySim) ở W1. Code W1 đã được lưu trữ ở `_backup/` ngoài repo. Một số quy ước của W1 được giữ lại: tên topic `.v<major>`, JSON + JSON Schema, DLQ envelope.

Bối cảnh đầy đủ, giải thích nguồn dữ liệu và output nằm trong [docs/brief/CryptoGuard_Project_Brief.html](../docs/brief/CryptoGuard_Project_Brief.html).

## 2. Ràng buộc bắt buộc (mọi task phải tuân thủ)

| # | Ràng buộc | Hệ quả cho code |
|---|---|---|
| C1 | **Chạy trên Kubernetes**, không chạy service bằng docker-compose | Mọi service có manifest/Helm trong repo. Docker chỉ dùng để build image và làm runtime cho kind |
| C2 | **Local trước, cloud sau.** P0–P3 chạy trên **kind** ở laptop. P4 mới chuyển lên **GCP VM (kubeadm)** | Không hard-code endpoint. Mọi thứ đi qua `config/*.yaml` (`CG_PROFILE=local\|cloud`). Không tạo tài nguyên cloud trước P4 |
| C3 | **Tiết kiệm tài nguyên local** | Tuân `data_budget` trong `config/local.yaml` (≤ 7 ngày lịch sử, ≤ 10 symbol, 1 ngày ETH). Chỉ bật profile cần dùng |
| C4 | **Dữ liệu công khai, lấy được ngay**, không dùng dữ liệu cá nhân | Không scrape trang cần đăng nhập. Không lưu secret trong repo |
| C5 | **Dữ liệu lớn, bẩn, nhiều nguồn**: phải *chứng minh* được đã xử lý | Mọi lỗi đi vào DLQ hoặc quarantine kèm lý do. Có metric và báo cáo data quality (O7) |
| C6 | **Contract là luật** | Giao tiếp giữa các thành phần chỉ qua `contracts/` và `batch/ddl/`. Đổi contract phải theo `CONTRIBUTING.md` §4 |

## 3. Kiến trúc tóm tắt

```
HOT PATH (giây)
  collectors (R2) ─▶ Kafka raw.*.v1 ─▶ normalizer/Flink (R2) ─▶ canonical.*.v1
      ─▶ detector/Flink (R3) ◀─ Redis (baseline, wallet_risk từ R4)
      ─▶ features.symbol_1m.v1 + alerts.v1 ─▶ sink (R5) ─▶ MongoDB ─▶ API + dashboard (R5)

COLD PATH (giờ/ngày)
  Airflow (R4) ─▶ Bronze (MinIO/GCS) ─▶ Spark (R4) ─▶ Silver/Gold Iceberg ─▶ graph & wallet_risk (R4)
      ─▶ Redis / MongoDB exports ─▶ training & evaluation (R5, MLflow)

REPLAY (đánh giá)
  binance.vision ─▶ replay tool (R2) ─▶ replay.canonical.trades.v1 ─▶ detector replay mode (R3)
      ─▶ replay.alerts.v1 ─▶ evaluation (R5) so với gold.pump_cases (R4)

PLATFORM (R1): kind/kubeadm, Strimzi Kafka, Flink Operator, Spark Operator, Airflow, MinIO/GCS, Postgres,
               MongoDB, Redis, MLflow, Prometheus/Grafana
```

Chi tiết: [docs/architecture.md](../docs/architecture.md).

## 4. Năm vai trò

| Role | Tên | Sở hữu thư mục | Output chính |
|---|---|---|---|
| **R1** | Platform & Infra | `infra/`, `.github/`, `Makefile` | Cluster local và cloud, operator, monitoring (O8), chi phí |
| **R2** | Ingestion & Stream Normalization | `ingest/`, `streaming/normalizer/` | `raw.*`, `canonical.*`, replay tool, Bronze archive, DLQ |
| **R3** | Real-time Detection | `streaming/detector/`, `libs/common/.../features.py` (đồng sở hữu với R4) | `features.symbol_1m.v1`, `alerts.v1` có lý do, case |
| **R4** | Batch, Lakehouse & Graph | `batch/`, `contracts/reference/` | Silver/Gold Iceberg, labels, baselines, `wallet_risk`, data quality |
| **R5** | ML, Serving & Evaluation | `ml/`, `serving/` | API, dashboard (O1–O5), model, báo cáo đánh giá (O6) |

Chung: `contracts/`, `libs/common/`, `config/`, `docs/`, `team/`.

### Ma trận phụ thuộc (ai cần gì từ ai)

| Người nhận ↓ / Người cấp → | R1 | R2 | R3 | R4 | R5 |
|---|---|---|---|---|---|
| **R1** | | metric endpoints | metric endpoints | job resource needs | API deploy needs |
| **R2** | Kafka, MinIO, Flink operator | | | `symbol_map.csv` | |
| **R3** | Flink operator, Redis | `canonical.trades.v1`, replay | | baselines, `wallet_risk` trong Redis | model (P4) |
| **R4** | Spark operator, Airflow, MinIO, Postgres | Bronze archive (`bronze/kafka/...`) | định nghĩa feature | | protocol chọn event |
| **R5** | MongoDB, MLflow, Grafana | | `alerts.v1`, `features.symbol_1m.v1` | Gold tables, `pump_cases`, `wallet_risk` | |

**Không ai phải ngồi chờ:** dùng `contracts/samples/` + `make seed-samples` + dữ liệu binance.vision để phát triển trước khi phía cấp dữ liệu làm xong.

## 5. Contracts (tóm tắt, chi tiết ở `docs/data-contracts.md`)

| Contract | Topic / bảng | Producer → Consumer |
|---|---|---|
| `raw_envelope.v1` | `raw.trades.<exchange>.v1`, `raw.eth.transfers.v1` | R2 → R2 |
| `canonical_trade.v1` | `canonical.trades.v1`, `replay.canonical.trades.v1`, `silver.trades` | R2 → R3, R4 |
| `onchain_transfer.v1` | `canonical.onchain_transfers.v1`, `silver.onchain_transfers` | R2/R4 → R3, R4 |
| `symbol_features_1m.v1` | `features.symbol_1m.v1`, `gold.symbol_features_1m` | R3/R4 → R5 |
| `alert.v1` | `alerts.v1`, `replay.alerts.v1`, Mongo `alerts` | R3 → R5 |
| `wallet_risk.v1` | `gold.wallet_risk`, Redis `wallet_risk:<addr>`, Mongo `wallet_risk` | R4 → R3, R5 |
| `label.v1` | `silver.labels` | R4 → R4, R5 |
| `dlq_record.v1` | `dlq.normalizer.v1`, `dlq.detector.v1` | R2, R3 → R2, R4, R5 |

Quy ước bất biến:
- Thời gian là **số nguyên micro giây UTC**, hậu tố `_ts_us` (ADR-0004).
- Symbol có dạng `BASE-QUOTE`. Địa chỉ ví viết thường.
- Mọi message có `schema_version`. Không cho phép field lạ.

## 6. Lộ trình

| Phase | Môi trường | Mục tiêu | Demo khi kết thúc |
|---|---|---|---|
| P0 | laptop | Hiểu bài, spike dữ liệu, chốt contract v1 | `make test` xanh trên 5 máy; danh sách vụ pump đánh giá được |
| P1 | kind | Nền tảng và dữ liệu đầu tiên | `make up` chạy được; Binance + OKX vào Kafka; Bronze có dữ liệu |
| P2 | kind | **MVP end-to-end** | Replay 1 vụ pump thật thì alert hiện trên dashboard |
| P3 | kind | On-chain và graph | API trả về đường đi từ ví tới ví OFAC/darklist |
| P4 | **GCP** | Cloud, scale, nâng cao, đánh giá | Chạy trên GCP với 3–6 tháng dữ liệu; báo cáo O6; demo chịu lỗi |
| P5 | GCP | Hoàn thiện | Báo cáo, slide, video demo dự phòng |

Chi tiết: [docs/roadmap.md](../docs/roadmap.md).

## 7. MVP (bắt buộc có) và nâng cao

**MVP:**
- Cụm K8s với Kafka, MinIO, Postgres, Flink, Spark, Airflow, MongoDB, Redis và monitoring.
- 2 sàn (Binance + OKX) với 10–20 cặp.
- Chuẩn hóa + dedup + DLQ.
- Rule pump-and-dump real-time.
- Lịch sử Binance + 1–3 tháng ETH ở Bronze/Silver/Gold (local dùng ít ngày hơn).
- Taint/wallet risk từ OFAC + darklist.
- Replay các vụ pump thật để đo recall, precision và độ trễ.
- API + dashboard alert, báo cáo data quality, dashboard hệ thống.

**Nâng cao (P4):**
- 4 sàn, wash trading, cross-venue, spoofing.
- Model ML (IsolationForest/XGBoost) qua MLflow.
- GDELT news, Neo4j graph view, chaos demo.

**Ngoài phạm vi:** trading, khuyến nghị đầu tư, kết luận pháp lý về ví, dữ liệu cá nhân/KYC.

## 8. Output cuối cùng (giảng viên sẽ chấm)

| Mã | Output | Người chính |
|---|---|---|
| O1 | Feed cảnh báo real-time (risk + lý do) | R3 → R5 |
| O2 | Trang case điều tra | R5 |
| O3 | API điểm rủi ro ví | R4 → R5 |
| O4 | Graph điều tra (nâng cao) | R4, R5 |
| O5 | Dashboard thị trường | R5 |
| O6 | Báo cáo đánh giá trên vụ pump thật | R5 (+R3, R4) |
| O7 | Báo cáo data quality | R4 (+R2), hiển thị R5 |
| O8 | Dashboard hệ thống, scale, chịu lỗi | R1 |

## 9. Quy tắc làm việc chung

1. **Pure logic, thin engine:** logic nghiệp vụ là hàm Python thuần, có unit test. Flink/Spark/Airflow chỉ nối I/O (ADR-0003).
2. **Feature dùng chung:** `cryptoguard_common.features` được gọi bởi cả Flink (R3) và Spark (R4). Đổi định nghĩa cần R3, R4 và R5 cùng duyệt.
3. **Live và replay không bao giờ trộn:** dùng topic `replay.*`, collection `alerts_replay`, và FlinkDeployment riêng.
4. **Idempotent:** chạy lại một job hoặc ngày dữ liệu không được sinh bản ghi trùng (dedup theo `event_id`, MERGE theo khóa).
5. **Observable:** mỗi service expose metric `cg_<component>_*` và log JSON.
6. **Không bịa về API bên ngoài:** chỗ nào chưa chắc thì để `# VERIFY:` kèm điều cần kiểm tra, rồi xác minh bằng tài liệu chính thức hoặc dữ liệu thật.
7. **Git:** branch `feat/<TASK-ID>-...`, Conventional Commits có task ID, PR cần 1 approve và CI xanh (`CONTRIBUTING.md`).
8. **Definition of Done:** code + test merge vào main; chạy được trên profile local; output validate theo contract; README thành phần được cập nhật; Task board được cập nhật.

## 10. Lệnh hay dùng

```bash
make dev-install && make test     # môi trường Python + test
make up                           # kind + Kafka + MinIO + Postgres
make up-stream | up-batch | up-serving | up-monitoring
make seed-samples                 # đẩy message mẫu vào Kafka
make image-<ingest|streaming|batch|airflow|serving>
make gen-topics                   # sau khi sửa contracts/kafka-topics.yaml
make status | make down
```

Endpoint local: Kafka `localhost:32100`, MinIO `:30900`/`:30901`, Postgres `:30432`, Mongo `:30017`, Redis `:30379`, Grafana `:30300`, Airflow `:30808`, MLflow `:30500`.

## 11. Thuật ngữ nhanh

`maker/taker` (bên đặt lệnh chờ / bên khớp vào) · `taker_side` (chiều của bên chủ động khớp) · `pump-and-dump` · `wash trading` · `spoofing` · `taint` (mức nhiễm bẩn lan truyền) · `OFAC SDN` (danh sách trừng phạt của Mỹ) · `event-time` / `watermark` · `DLQ` · `Bronze/Silver/Gold` · `replay` · `reorg` (chuỗi block bị thay thế) · `decimals` (số chữ số thập phân của token).
