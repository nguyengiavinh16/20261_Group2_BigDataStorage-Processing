# P2-W6A/B — Data dictionary, trace, lakehouse/runbook

**Dữ liệu mặc định:** PaySim simulator (`step` 1..743, xấp xỉ 6,362,620 dòng nếu dùng bản gốc đã ETL trước đây); repo có thể thêm dataset khác **chỉ khi có adapter/schema/checksum/split tương ứng**, không giả định identity hay time anchor PaySim áp dụng cho dataset khác.

## Data dictionary (PaySim CSV)

| CSV | Định nghĩa / đơn vị | Online scoring event? |
|---|---|---|
| `step` | Chỉ số thời gian mô phỏng, **1 step = 1 giờ**, 1..743 | `step`, `event_time` tính từ UTC anchor được cấu hình |
| `type` | Loại giao dịch (CASH_IN/CASH_OUT/DEBIT/PAYMENT/TRANSFER) | `transaction_type` |
| `amount` | Đơn vị tiền PaySim, 2 chữ số thập phân | `amount_cents` integer, không float trong event |
| `nameOrig` | Tài khoản nguồn | `origin_account_id` |
| `nameDest` | Tài khoản đích | `destination_account_id` |
| `oldbalanceOrg`, `newbalanceOrig` | Số dư trước/sau của tài khoản nguồn | **KHÔNG** đưa vào event/Feature A/H |
| `oldbalanceDest`, `newbalanceDest` | Số dư trước/sau của tài khoản đích | **KHÔNG** đưa vào event/Feature A/H |
| `isFraud` | Nhãn fraud 0/1, chỉ để học/đánh giá offline | **KHÔNG** vào Kafka/scoring |
| `isFlaggedFraud` | Cờ rule-based của simulator | **KHÔNG** vào Kafka/scoring |

## Identity và lineage

- SHA-256 **CSV gốc nguyên bytes**: `source_checksum`. Dòng dữ liệu vật lý đầu tiên sau header là `source_line=1`.
- `transaction_id = sha256(f"{source_checksum}:{source_line}")`. Khi split CSV sang Parquet/HDFS hoặc replay, không đổi locator. Nếu đổi CSV (kể cả thứ tự dòng), checksum và ID namespace khác; phải tạo snapshot mới.
- UTC anchor cố định cho cùng run, không dùng clock lúc replay; `event_time(step)=anchor+(step-1) hours`. `published_at` là thời gian publish, khác event_time.
- Bronze giữ occurrence và provenance; Silver dedup theo transaction_id (collision payload khác phải fail); label sidecar tách; Gold `destination_account_id × step` từ Silver snapshot pin, predicate cutoff. Không đếm duplicate hai lần.
- `dest_prior24_count`, `dest_prior24_amount_cents` từ `[step-24,step-1]`, loại cùng step; cập nhật sau khi tính feature; window theo mọi type, amount integer cents.
- Grain: Bronze = occurrence, Silver = unique canonical transaction, Gold = destination × step × run/cutoff/snapshot. Gold **không** thay thế training labels.

## Trace một ví dụ

1. CSV checksum + source_line → `stable_id` (W1 contract).
2. Silver unique ID mang `destination_account_id`, `step`, `amount_cents` và `feature_version`.
3. Gold group `(destination_account_id, step)` có `transaction_count`, `amount_sum_cents` theo Silver snapshot được pin.
4. `src/release_w6/lineage_w6.py` so triple này bằng reference local; cluster phải lưu **Bronze/Silver/Gold manifest + checksum** và SQL query cụ thể.

## Các bước live (chưa xác nhận trên cluster của bạn)

Xem chính xác CLI của `docs/week2/INTEGRATION.md` và `docs/week3/INTEGRATION.md` để chạy snapshot → Bronze → Silver → Gold. Sau đó kiểm `COUNT`, `COUNT(DISTINCT transaction_id)`, `SUM(amount_cents)`, số nhóm Gold, `SUM(transaction_count)`, `SUM(amount_sum_cents)`; so với snapshot ID chứ không đọc `latest`. Lưu SQL/physical plan, CLI exit, input SHA, run_id/cutoff, version, URI, timestamp trong evidence. Chạy overlap để thấy Bronze nhiều occurrence, Silver một ID; Gold rerun replace không cộng dồn.

## Compaction và retention

Trước/sau compaction dùng code W4/W5: snapshot ID, logical IDs, count, sum, row hashes và query plan. Không gọi compaction là nhanh hơn nếu thiếu 3 phép đo có correctness PASS. **Giữ pinned snapshots của G3/G4/G5**, không expire snapshot khi còn được tham chiếu, không xóa volume/warehouse để chữa lỗi. HDFS RF2 giúp lưu trữ replicate blocks nhưng single NameNode vẫn là SPOF; không gọi đây là HA.

## Các file không commit

`data/raw/*.csv`, `data/processed/`, Spark warehouse/parquet, Gold snapshots, label dumps, checkpoint, Mongo dump, connector JAR, model artifact, API secrets. Trong Git chỉ có contract, ví dụ cấu hình, checksum và report nhỏ có provenance.
