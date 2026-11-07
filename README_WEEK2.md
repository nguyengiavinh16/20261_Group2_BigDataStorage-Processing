# Fraud Analytics — TUẦN 2 / 6 (overlay mã nguồn, tất cả 5 vai trò)

**Chỉ triển khai W2** theo file HTML kế hoạch ngày 26/10/2026; chưa triển khai W3–W6. Bản ZIP này phải được **ghép lên repo đã tích hợp W1**, không phải repository đầy đủ và không chứa dữ liệu PaySim.

> **Ranh giới nghiệm thu:** Các unit test local có thể đạt, nhưng **G1 CHƯA PASS** nếu chưa có bằng chứng thực tế Spark cluster, HDFS RF2, Iceberg snapshot, Kafka và LR Spark save/load trên snapshot Silver. Tôi chưa được cung cấp code nguồn thực tế của repo (chỉ ảnh cây thư mục), không có Docker, Kafka, Iceberg hay PySpark trong môi trường dựng. File Compose là **profile ứng viên**, không khẳng định tương thích với Docker Compose/JAR/WSL của bạn.

## Phạm vi / 10 task

| ID | Chủ đề | File mới chủ đạo | Tình trạng |
|---|---|---|---|
| P1-W2A | Spark 1 master/2 workers + HDFS NameNode/2 DataNodes, volumes, readiness, broker probe | `deploy/compose.week2.example.yaml`, `scripts/dev/cluster_audit_w2.py`, `scripts/dev/executor_network_probe_w2.py` | Template + probe code; external verification required |
| P1-W2B | Runner pin snapshot/cutoff, retry, metadata/exit | `src/platform/runner_w2.py`, `src/platform/storage_w2.py`, `schemas/run-attempt-w2.json` | Local tests pass; Spark integration pending |
| P2-W2A | Append Bronze Iceberg, source provenance, separate reject + label sidecar | `src/processing/iceberg_w2.py` | Spark implementation; Iceberg integration pending |
| P2-W2B | Silver canonical normalization + dedup MERGE, conflict fail-closed | `src/processing/iceberg_w2.py` | Spark implementation; overlap/restart integration pending |
| P3-W2A | Spark ML LR Feature A, train-only transforms, threshold validation, model manifest, save/load | `src/ml/lr_w2.py`, `scripts/dev/model_reload_check_w2.py` | Spark implementation; real-model evidence pending |
| P3-W2B | Offline Silver → history 24 steps point-in-time, every transaction type | `src/features/offline_w2.py` | Spark implementation; parity with real Silver pending |
| P4-W2A | Independent archive Kafka → Bronze with checkpoint | `src/streaming/queries_w2.py` | Spark streaming implementation; Kafka integration pending |
| P4-W2B | Independent stateless Feature-A scorer, real LR mandatory | `src/streaming/queries_w2.py` | Scorer code; one-partition Kafka/model parity pending |
| P5-W2A | Bounded fixture repository + read-only API, stable cursor, 503 timeout | `src/api/repository_w2.py`, `src/api/service_w2.py` | API fixture tests pass; Mongo handoff W3 |
| P5-W2B | 1-page UI + fixture validation (PASS/FAIL/INCONCLUSIVE) | `ui/index_w2.html`, `validation/validate_w2.py` | CLI tests/static UI checks; manual browser evidence pending |

## Chú ý thiết kế

- **Feature A trong W2** là `transaction_type + log_amount` (stateless). LR dùng inverse class-frequency weights chỉ từ TRAIN, không áp dụng oversampling xuyên thời gian. Hai feature lịch sử 24 bước được tính **offline** và dành cho phần mở rộng có state; không lấy giá trị bằng 0 rồi tuyên bố là history thật.
- **Bronze** giữ mọi lần xuất hiện, kể cả replay trùng ID. **Silver** loại duplicate theo ID trước Iceberg MERGE, từ chối ID có payload mâu thuẫn. Count `accepted/rejected` là ở manifest Bronze; Silver report ghi rõ input snapshot/count.
- Label `is_fraud` chỉ có trong bảng sidecar `silver_labels`, **không** có trong payload Kafka, Bronze event, Silver feature hoặc prediction schema.
- Model được publish bằng thư mục `.incomplete` → rename trong **cùng filesystem local/HDFS**, tránh nạp model chưa ghi xong. Mỗi run pin snapshot; offline training label Iceberg đòi `@snapshot_id`.
- Archive/scoring có **hai checkpoint HDFS khác nhau**. W2 scoring chỉ dedup trong từng microbatch; **chưa bảo đảm exactly-once liên microbatch**. W3 cần Mongo sink idempotent, W4 mới có lịch sử/state/recovery phức tạp.
- API có chế độ `fixture` **ghi rõ**, không coi dữ liệu giả là fraud metrics. Không tính accuracy/PR-AUC online khi chưa có nhãn.
- PaySim `step` 1..743, stable ID checksum+source line là **adapter PaySim**. Khi nhận dataset khác, tạo adapter/contract version riêng, không tự ánh xạ ngày giờ hoặc ID.

**Đọc tiếp:** `docs/week2/INTEGRATION.md` (runbook) và `docs/week2/STATUS.md` (tiêu chí nghiệm thu/mục còn thiếu).
