# Fraud Analytics — TUẦN 3/6 (10 task / 5 thành viên)

**Bản cập nhật mã nguồn W3**, không phải repo gốc. Cần ghép sau W1 và W2. Bản này không chứa PaySim dataset, JAR Spark/Iceberg, Docker volumes hay Mongo credentials; không ghi đè `docker-compose.yml`, `src/ingestion/producer.py`, `src/processing/etl.py` cũ. **Chỉ triển khai tuần 3; tuần 4 chưa thực hiện.**

> **Trạng thái:** Đã viết mã, test local và fixtures. **G2 = INCONCLUSIVE** vì chưa có Spark/Iceberg/Kafka/Mongo thật, chưa chạy end-to-end, chưa có LR model thật. Không suy luận G0/G1 đã được nghiệm thu chỉ vì unit test đạt.

## Đầu việc đã viết (theo HTML 26/10/2026)

| Task | Vai trò | Các file trọng tâm | Kết quả kiểm thử |
|---|---|---|---|
| P1-W3A | Platform / launcher | `src/platform/lifecycle_w3.py` | Start/stop/status, atomic lock query + checkpoint trên một launcher host, resume không xóa checkpoint, log file 0600 |
| P1-W3B | Scheduler | `src/platform/workflow_w3.py`, `configs/week3/workflow.example.json` | DAG Bronze → Silver → validation → Gold/features; manifest snapshot chain, retry, lock, block downstream, không chạy stream |
| P2-W3A | Gold + SQL | `src/processing/gold_w3.py`, `docs/week3/GOLD_SQL.md` | Gold grain destination/step, exact cents, cutoff pinned, predicate overwrite rerun |
| P2-W3B | Batch/replay overlap | `src/processing/archive_bridge_w3.py`, `validation/validate_silver_w3.py` | Gộp manifest Bronze/Silver, Silver uniqueness, 6+4 overlap→8 unique reference |
| P3-W3A | LR baseline evidence | `src/ml/baseline_w3.py` | Load W2 LR artifact, validation AP/TP/FP/FN/TN/threshold provenance, không dùng test tune |
| P3-W3B | Golden + parity | `src/evaluation/golden_w3.py`, `golden_score_w3.py`, `parity_w3.py` | Golden offline independent + model scores thật khi có LR, comparator full-outer ID and exact cents / tolerant scores |
| P4-W3A | Mongo durable sink | `src/streaming/mongo_sink_w3.py`, `queries_w3.py` | Majority-acknowledged two-stage PENDING/ACKED, unique run+transaction, retries idempotent |
| P4-W3B | Stateful history prototype | `src/streaming/state_w3.py`, `stateful_spark_w3.py` | Pending same-step, dedup-before-late, prior [step−24, step−1], SQLite restart; Spark applyInPandasWithState prototype (untested runtime) |
| P5-W3A | Mongo API + validator | `src/api/mongo_repository_w3.py`, `service_w3.py`, `validation/run.py` | Real repository port, 503 not zero, read ACKED, JSONL ID-set parity with PASS/FAIL/INCONCLUSIVE |
| P5-W3B | Metrics | `monitoring/collector_w3.py` | Progress offset/backlog per query/partition, unknown vs zero, wallclock ACK latency, freshness, bounded labels |

## Hướng dẫn rất nhanh

1. Đọc **`docs/week3/INTEGRATION.md`** để ghép patch và chạy PowerShell + Spark Linux. Mẫu cấu hình nằm ở `configs/week3/`.
2. Trên repo đã ghép W1 + W2 + W3:

   ```powershell
   python -m pip install -r requirements-w3.txt
   $env:PYTHONPATH = "."
   python -m pytest -q tests/week3
   python -m pytest -q tests
   python -m scripts.week3.demo_local --output reports/evidence/w3-local-demo
   ```

3. Với runtime thực, chạy workflow batch, start archive/scoring tách checkpoint và test API Mongo/validator theo runbook.
4. Đọc **`docs/week3/STATUS.md`** trước khi tự tuyên bố G2 PASS.

## Các ranh giới không được nhầm lẫn

- `P4-W3B` SQLite là prototype có checkpoint trên **một process**, không phải Spark state store; Spark `applyInPandasWithState` có mã nhưng chưa chứng minh chạy được trong cluster. Stateful history chưa tham gia baseline LR Feature A trong W3.
- `src/streaming/queries_w3.py` **sử dụng Logistic Regression thật** từ W2, không mock; lỗi nếu model không load được. Mongo API fixture là tùy chọn độc lập và luôn ghi `mode=fixture`.
- Kafka archive append Bronze theo *at-least-once*; Silver dedup và Mongo idempotency không đồng nghĩa toàn hệ thống exactly-once. Mongo hai giai đoạn không phải transaction Kafka↔Mongo.
- Gold không có label hoặc transaction-level spine. Không dùng Gold như full labelled training table.
- `golden_history.json` chứa golden count/cents **chưa có score**. Để sinh golden score thật, cần W2 COMPLETE LR artifact qua `src/evaluation/golden_score_w3.py`.
- `reports/evidence/` được tạo trên máy người dùng khi chạy, không mang chứng cứ external ngụy tạo trong ZIP. Unit test và demo local chỉ chứng minh logic, không chứng minh throughput/recovery/distributed readiness.

**Tiếp tục tuần 4 chỉ khi người dùng cho phép.**
