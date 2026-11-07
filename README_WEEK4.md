# Fraud Analytics — TUẦN 4/6 (10 task / 5 thành viên)

**Đã triển khai source code và test local của tuần 4**, tiếp nối các bản patch tuần 1–3. **Không thay repo gốc từ ảnh**, không ghi đè `docker-compose.yml`, ETL/producer cũ. **Tuần 5 chưa triển khai.**

> **Trạng thái thực tế: G3 = INCONCLUSIVE**, chưa có môi trường Spark/Kafka/Iceberg/Mongo/HDFS chạy end-to-end để kiểm chứng recovery/parity 3 partition. Unit test có PASS không đồng nghĩa G3 PASS.

## 10 task đã được triển khai mã

| Task | Nhóm | Module chính | Ghi chú |
|---|---|---|---|
| P1-W4A | Lead | `src/platform/guards_w4.py` | Local resource lock, candidate verification SHA256, pin/atomic pointer/rollback; chỉ local FS |
| P1-W4B | Lead | `src/platform/workflow_gate_w4.py`, `validation/gate_w4.py` | Gate 8 check MUST, pin run/cutoff/snapshot/version, cấm tự promote |
| P2-W4A | Data | `fixtures/week4`, `tests/week4`, `docs/week4/DATA_DICTIONARY.md` | Cửa sổ [s-24,s-1], same-step, cold-start, duplicate, late |
| P2-W4B | Data | `src/processing/maintenance_w4.py` | Iceberg compaction with logical-row equality, snapshot retention dry-run + pin check |
| P3-W4A | ML | `src/ml/lr_w4.py`, `scripts/week4/compare_models.py` | Spark LR A+H v2 candidate, train-only fit, validation AP, signatures, HDFS artifact staging |
| P3-W4B | ML | `src/evaluation/parity_w4.py` | Full ID + exact history, tolerant score, exact alert/decision comparator |
| P4-W4A | Streaming | `src/streaming/stateful_spark_w4.py`, `scoring_w4.py`, `replay_w4.py` | Candidate Spark stateful history + deterministic local 1/3-partition matrix |
| P4-W4B | Streaming | `src/streaming/state_w4.py`, `checkpoint_w4.py` | Transactional local SQLite state/outbox, HDFS checkpoint identity guard, warm-up gating |
| P5-W4A | Validation | `validation/compare_w4.py`, `src/evaluation/parity_w4.py` | ID partition reconciliation, restart/duplicate check, fail-closed reporter |
| P5-W4B | Monitoring | `monitoring/exporter_w4.py`, Grafana/Prometheus config, Compose example | Throughput, p95 wall-clock ack, lag, query errors, freshness/absent distinction |

**Đọc trước khi chạy:** `docs/week4/INTEGRATION.md`, `docs/week4/G3_CHECKLIST.md` (30 ca nghiệm thu lấy từ kế hoạch HTML), `docs/week4/STATUS.md` (những gì chưa nghiệm thu), `docs/week4/ARCHITECTURE.md`, `docs/week4/DATA_DICTIONARY.md`.

## Quick start (tại repo ghép W1–W3)

```powershell
$env:PYTHONPATH = "."
python -m pytest -q tests/week4
python -m pytest -q tests
python -m scripts.week4.cli local-parity --events fixtures/week4/boundary-events.json --output reports/evidence/w4/local-parity
```

**Kết quả trong môi trường xây dựng:** `161 passed, 3 skipped` của W1–W4 sau kiểm thử cuối. Spark integration skip do môi trường chưa có runtime. Local parity so với golden oracle offline độc lập, không chỉ so hai output cùng logic.

## Ghi chú bắt buộc

- Không “đạt G3” chỉ nhờ local parity/fixture. Gate đòi báo cáo tích hợp `PASS` trên cutoff finalized và bằng chứng thật cho 8 check.
- Kafka producer phải cung cấp thứ tự hợp đồng theo `destination_account_id`; Spark groupBy không tạo ra order guarantee. Offset guard sẽ dừng khi thấy nguồn lệch.
- `state_w4` SQLite outbox trên một writer và Spark state store là hai cơ chế **khác nhau**. Mongo ACK idempotency ≠ exactly-once across stores.
- Xóa Iceberg snapshots tự động bị vô hiệu hóa có chủ đích để bảo vệ training pin; testing `rewrite_data_files` chỉ chạy với allowlist và lock.
- Candidate A+H vẫn ở staging cho tới khi parity/restart/health gate thực sự đạt; baseline A tiếp tục phục vụ nếu candidate fail.
- P1 promotion lock là single-host; phải thay bằng distributed lease/registry nếu có nhiều launcher, và implement HDFS artifact registry đúng storage.

**Chỉ thực hiện tuần 5 nếu người dùng cho phép.**
