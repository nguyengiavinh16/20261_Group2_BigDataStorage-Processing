# R4: Batch, Lakehouse & Graph

> Dành cho: thành viên R4 và AI agent của họ. **Đọc `team/00-shared-context.md` trước.**
> Người dự phòng (backup): R5.

## 1. Sứ mệnh

Xây **lakehouse** (Bronze → Silver → Gold trên Iceberg) chứa phần "Volume" lớn nhất của dự án: lịch sử giao dịch Binance và giao dịch token trên Ethereum. Tạo **nhãn** (OFAC, darklist, vụ pump), **baseline** cho detector, **graph dòng tiền** và **điểm rủi ro ví**, cùng **báo cáo chất lượng dữ liệu** (O7). R4 là người cấp "tri thức lịch sử" cho hot path (qua Redis) và dữ liệu train/đánh giá cho R5.

## 2. Phạm vi

**Sở hữu:**
- `batch/**`: DAG, Spark job, DDL, graph
- `contracts/reference/*.csv`: symbol_map, token_registry, exchange_wallets
- `docs/source-notes.md` (tạo ở BAT-01)
- Đồng sở hữu `libs/common/.../features.py` (với R3) và `binance_vision.py`

**Phối hợp:**
- Contract `onchain_transfer.v1`, `label.v1`, `wallet_risk.v1` và các bảng trong `batch/ddl/`: đổi cần consumer (R3, R5) duyệt.
- Khóa Redis do R4 ghi (data-contracts §4).
- Collection Mongo `wallet_risk` do R4 export, R5 sở hữu index.

**Ngoài phạm vi:**
- Luồng real-time (R2, R3)
- Train model và dashboard (R5)
- Cài Airflow/Spark operator (R1). R4 viết DAG và job.

## 3. Môi trường theo giai đoạn

| Phase | Phạm vi dữ liệu (theo `data_budget`) |
|---|---|
| P0–P3 (kind) | binance.vision: ≤ 10 symbol × ≤ 7 ngày (quanh các vụ pump + vài ngày bình thường); AWS ETH `token_transfers`: **1 ngày**, chỉ token tracked; OFAC/darklist đầy đủ (nhỏ); Spark 1 executor 2 GB |
| P4 (GCP) | 50+ symbol × 3–6 tháng; ETH 3 tháng; executor trên node Spot; đo thời gian xử lý theo số executor (2/4/6) cho báo cáo scale |

Bronze trên MinIO (local) hoặc GCS (cloud), luôn truy cập qua S3 API (`cg_batch.spark_session`).

## 4. Đầu vào (Inputs)

| Input | Nguồn | Ghi chú / bẫy |
|---|---|---|
| binance.vision `spot/daily/trades` (+ klines nếu cần) | `https://data.binance.vision` | Zip CSV **không header**, có `.CHECKSUM`; timestamp ms (cũ) hoặc **µs (spot 2025+)**; có ngày thiếu file |
| AWS Public Blockchain ETH | `s3://aws-public-blockchain/v1.0/eth/{token_transfers,transactions,...}/date=YYYY-MM-DD/` (`--no-sign-request`, region us-east-2) | Đọc **schema thật** trước khi dùng; `value` chưa chia decimals; token giả mạo tên |
| OFAC SDN | sanctionslistservice.ofac.treas.gov (XML/CSV) | Địa chỉ ví nằm trong feature "Digital Currency Address – <coin>"; danh sách đổi theo thời gian |
| Darklist | github.com/MyEtherWallet/ethereum-lists | Độ tin cậy thấp hơn OFAC, có trùng |
| Pump-and-dump dataset | github.com/SystemsLab-Sapienza/pump-and-dump-dataset | Ground truth quan trọng nhất. Cần kiểm tra coin còn dữ liệu trên Binance |
| Elliptic (P3) | Kaggle (tài khoản free) | Benchmark thuật toán graph (Bitcoin) |
| OpenSanctions, GDELT (P4, tùy chọn) | opensanctions.org, data.gdeltproject.org | |
| Bronze Kafka archive | R2 (ING-08) `bronze/kafka/<topic>/dt=.../` | canonical trades live, alerts, DLQ |
| Hạ tầng | R1: MinIO, Postgres, Spark operator, Airflow | Trước khi có: chạy Spark local mode trên host |

## 5. Đầu ra (Outputs) và tiêu chí chấp nhận

| Output | Contract | Consumer | Tiêu chí chấp nhận |
|---|---|---|---|
| Bronze | layout trong `batch/airflow/dags/README.md` | mọi người | File gốc nguyên vẹn; DAG **incremental** (không tải lại file trùng checksum), **idempotent**; tôn trọng `data_budget` |
| `cg.silver.trades` | DDL + `canonical_trade.v1` | R3 (parity), R5 | Không trùng `event_id`; chạy lại một ngày thì số dòng không đổi; timestamp đúng cả file ms lẫn µs (có test) |
| `cg.silver.minute_price`, `cg.silver.onchain_transfers` | DDL + `onchain_transfer.v1` | R4 graph, R3 | `amount` đã chia decimals; `amount_usd` tính bằng **as-of join** (giá phút gần nhất ≤ thời điểm block); token không có trong registry thì `amount=null` và được đếm |
| `cg.silver.labels` | `label.v1` | graph, R5 | Mỗi nguồn có `confidence`, `valid_from/valid_to`; `label_id` xác định; OFAC parse ra đủ địa chỉ ETH (đếm và ghi số lượng vào source-notes) |
| `cg.gold.pump_cases` | DDL | R5 (O6), R3 | Mỗi vụ có `has_trade_data` đã kiểm tra thật trên binance.vision; **≥ 5 vụ dùng được** cho đánh giá (mục tiêu, báo sớm nếu không đạt) |
| `cg.gold.symbol_features_1m` + `cg.gold.symbol_baseline` | `symbol_features_1m.v1`, DDL | R5 (train), R3 (baseline) | Dùng **chính hàm** `cryptoguard_common.features.window_features` (applyInPandas); parity test với R3; baseline export lên Redis đúng khóa `baseline:<symbol>:<exchange>:<hour>` mỗi ngày |
| `cg.gold.wallet_edges`, `cg.gold.wallet_risk` | `wallet_risk.v1`, DDL | R3 (Redis), R5 (Mongo/API) | Spark taint cho kết quả **khớp `cg_batch.graph.taint.propagate_taint`** trên graph test; ví OFAC có risk 1.0; ví sàn không lan truyền taint; export Redis (risk ≥ 0.1) và Mongo |
| `cg.gold.data_quality_daily` (O7) | DDL | R5 (hiển thị), báo cáo | Mỗi nguồn mỗi ngày: records in/valid, duplicates, schema errors, unknown symbols, gaps, late, DLQ, p95 ingest lag |
| `docs/source-notes.md` | n/a | cả nhóm | Mỗi nguồn: URL, cách tải, schema thật (in từ Spark), kích thước đo được, bẫy đã gặp |
| Bảng scale (P4) | n/a | báo cáo | Thời gian xử lý 1 tháng ETH với 2/4/6 executor, dung lượng từng tầng |

## 6. Task board

### P0: Khởi động
- [x] **BAT-01** Spike dữ liệu:
  - Tải 1 ngày trades của 1 symbol (kiểm tra ms/µs).
  - In schema `token_transfers` của 1 ngày từ AWS.
  - Parse OFAC lấy địa chỉ ETH.
  - Đọc pump dataset; cùng R3 và R5 lập danh sách vụ pump có dữ liệu Binance, lưu `contracts/reference/pump_events_candidates.csv` (nhỏ).
  - Viết `docs/source-notes.md`.
  - *Done khi:* có danh sách ≥ 5 vụ hoặc báo rủi ro cho cả nhóm.
  - *Kết quả (2026-11-23, branch `thai`):* 314/318 vụ Binance có dữ liệu ngày pump và đủ 7 ngày baseline; shortlist local gồm 8 vụ (< 1 MB) đã thêm vào `symbol_map.csv`, **chờ R3/R5 xác nhận**. File 2020 dùng ms, file 2026 dùng µs. OFAC có 127 địa chỉ EVM. **Rủi ro cần cả nhóm quyết:** mọi vụ pump đều trên cặp BTC nên `volume_z` hiện luôn là `None` khi replay (xem `docs/source-notes.md` §6).

### P1: Lakehouse cơ bản
- [ ] **BAT-02** Spark + Iceberg (JDBC catalog) chạy được local mode trên host và bằng SparkApplication; job `apply_ddl` tạo toàn bộ bảng trong `batch/ddl/iceberg_tables.sql`. Kiểm chứng version jar (`spark_session.PACKAGES`).
- [ ] **BAT-03** DAG `binance_vision_daily`: plan theo watchlist và ngày, tải lên Bronze, kiểm checksum, incremental.

### P2: Silver, Gold, nhãn
- [ ] **BAT-04** Job `bronze_to_silver_trades` (binance.vision + Bronze Kafka canonical), MERGE theo `event_id`, đếm chất lượng.
- [ ] **BAT-05** `minute_price`, `gold.symbol_features_1m` (applyInPandas với hàm chung), `gold.symbol_baseline` + export Redis. Parity test với R3.
- [ ] **BAT-06** Labels: OFAC, darklist, pump dataset → `silver.labels`; `gold.pump_cases` (kèm kiểm tra `has_trade_data`).

### P3: On-chain và graph
- [ ] **BAT-07** DAG + job AWS ETH `token_transfers` (lọc token tracked) → `silver.onchain_transfers` (decimals, USD as-of).
- [ ] **BAT-08** `gold.wallet_edges` (gộp theo src, dst, token).
- [ ] **BAT-09** Taint (Pregel hoặc join lặp, khớp bản reference), hops, PageRank (GraphFrames), fan-in/out → `gold.wallet_risk`; export Redis và Mongo. Ghi công thức risk vào `batch/README.md`.
- [ ] **BAT-10** Kiểm chứng thuật toán graph trên Elliptic (so với baseline đơn giản, báo AUC/precision).
- [ ] **BAT-13** Lập `exchange_wallets.csv` (mỗi dòng có nguồn công khai `source_url`) + export Redis `exchange_wallet:*`.
- [ ] **BAT-14** `gold.data_quality_daily` từ Bronze DLQ + Silver counts (+ metric stream từ ING-09) → O7.

### P4: Scale (cloud)
- [ ] **BAT-11** Backfill trên GCP (3–6 tháng, 50+ symbol, 3 tháng ETH); tối ưu partition, skew (ví sàn là node siêu lớn), compaction Iceberg; đo bảng scale.
- [ ] **BAT-12** (Tùy chọn) GDELT 15 phút → tín hiệu "có tin tức" theo asset cho reason `NO_NEWS`.

## 7. Quy tắc kỹ thuật riêng

- **Không đụng file trong Bronze sau khi đã ghi** (bất biến). Sửa lỗi bằng cách chạy lại Silver/Gold.
- Mọi job nhận tham số ngày, **idempotent** (MERGE hoặc overwrite partition), ghi log số dòng in/out/rejected.
- Công việc nặng chạy trong Spark, không chạy trong Airflow worker.
- Feature batch **phải** gọi hàm chung. Không viết lại công thức bằng Spark SQL.
- Nhãn có thời hạn: khi đánh giá ở thời điểm T, chỉ dùng nhãn có `valid_from ≤ T < valid_to`, tránh rò rỉ tương lai.
- Mỗi dòng `exchange_wallets.csv` phải có nguồn công khai. Không đoán địa chỉ.
- Local tuân `data_budget`; download mặc định không được vượt budget.

## 8. Metrics và log

Mỗi job log JSON: `job`, `date`, `rows_in`, `rows_out`, `rows_rejected`, `duration_s`. Các con số này được tổng hợp vào `gold.data_quality_daily`. Airflow SLA/alert khi DAG fail (P4).

## 9. Kiểm thử bắt buộc

- Unit: taint/hops reference (đã có), parser binance.vision (ms/µs), parser OFAC (fixture XML nhỏ tự tạo), decode decimals, as-of join (pandas nhỏ).
- Parity: `window_features` batch so với stream (với R3).
- Idempotency: chạy job 2 lần trên cùng ngày cho số dòng bằng nhau (integration).

## 10. Cần tìm hiểu

Spark DataFrame, partitioning, skew, applyInPandas/mapInPandas; Iceberg (MERGE, partition transforms, time travel, compaction); Airflow (TaskFlow, dynamic task mapping, backfill, KubernetesPodOperator/SparkKubernetesOperator); GraphFrames (PageRank, Pregel/aggregateMessages); ERC-20 (Transfer event, decimals); taint analysis/AML; S3 API.

## 11. Giao tiếp với các vai trò khác

| Khi nào | Với ai | Nội dung |
|---|---|---|
| BAT-01 | R3, R5 | Danh sách vụ pump đánh giá được, kích thước dữ liệu thực tế |
| BAT-02 | R5 | Cách đọc Gold (Spark hoặc Trino), tên bảng |
| BAT-05 | R3 | Baseline trong Redis sẵn sàng, ngày cập nhật |
| BAT-06 | R5 | `gold.pump_cases` cho evaluation |
| BAT-09 | R3, R5 | `wallet_risk` trong Redis và Mongo, công thức risk |
| BAT-14 | R2, R5 | Định nghĩa từng cột data quality |
| Symbol mới | R2 | Cập nhật `symbol_map.csv` |

## 12. Rủi ro và câu hỏi mở

- Ít vụ pump còn dữ liệu Binance: báo ở P0; bổ sung bằng nhãn bán tự động (rule mạnh + kiểm tra tay), ghi rõ hạn chế.
- Graph quá lớn: giới hạn token, khoảng thời gian, k bước, cắt ở ví sàn.
- Iceberg trên GCS (P4): thử sớm cùng R1 (ADR-0002).

## 13. Prompt mẫu cho AI agent

> Tôi là R4 (Batch, Lakehouse & Graph) của CryptoGuard. Đọc `AGENTS.md`, `team/00-shared-context.md`, `team/04-batch-lakehouse-graph.md`, `batch/ddl/iceberg_tables.sql`, `batch/src/cg_batch/spark_session.py` và `libs/common/src/cryptoguard_common/binance_vision.py`. Làm task **BAT-04**: job `bronze_to_silver_trades` theo docstring, dùng `trade_row_to_canonical` qua mapInPandas, MERGE vào `cg.silver.trades` theo `event_id`, tham số `--date`, log số dòng. Chỉ sửa trong `batch/`. Viết unit test cho phần thuần Python và hướng dẫn chạy bằng Spark local mode trên host trỏ vào MinIO `localhost:30900`.
