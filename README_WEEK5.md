# FRAUD ANALYTICS — TUẦN 5 / 6

**Đã viết mã cho 10 task của cả 5 thành viên theo file kế hoạch HTML.** Mục tiêu: Benchmark tái lập có correctness, phân tích ablation/final test bị khóa, fault/recovery có ID reconciliation, monitoring và gate G4. **Không tự triển khai tuần 6.**

> Trạng thái bàn giao: **mã đã viết & test local; G4 = INCONCLUSIVE**. Không có Spark/HDFS/Iceberg/Kafka/Mongo thật để lấy metrics thực. Các mẫu synthetic không được gọi là kết quả benchmark PaySim. G3 (tuần 4) cũng đang INCONCLUSIVE nếu chưa có nghiệm thu riêng trên cluster.

## 10 task

| P | Task A | Task B |
|--|--|--|
| P1 — Lead | `P1-W5A`: resource matrix, fair compare, three samples, median/MAD | `P1-W5B`: worker/DataNode process recovery, runbook/fault evidence |
| P2 — Data Engineering | `P2-W5A`: Iceberg pruning/compaction before–after audit, live query runner | `P2-W5B`: ETL 1/2 workers, task/shuffle evidence & ID correctness |
| P3 — ML | `P3-W5A`: locked test 379–743, offline labels, AP, full coverage | `P3-W5B`: A vs A+H, optional time features & B diagnostic, FP/FN/amount |
| P4 — Streaming | `P4-W5A`: load tiers, next-offset lag, publish→durable ack p50/p95 | `P4-W5B`: duplicate/late/invalid/Mongo fault and restart reconciliation |
| P5 — Monitoring | `P5-W5A`: runner, artifact hash manifests, G4 fail-closed gate | `P5-W5B`: capped API/quality read-port, baseline-derived alerts & runbook |

## Quick start

```powershell
$env:PYTHONPATH = "."
python -m pip install -r requirements-w5.txt
python -m pytest -q tests/week5
python -m pytest -q tests
python -m scripts.week5.demo_fixture --out reports/evidence/w5-synthetic-demo
```

Đầu ra synthetic chỉ chứng minh code evaluator hoạt động. **G4 không PASS khi chỉ có fixture.**

## Tài liệu

- `docs/week5/INTEGRATION.md`: cách ghép patch W1–W4 và chạy trên Windows/Docker/Spark, mẫu CLI.
- `docs/week5/STATUS.md`: trạng thái từng task và các phần chờ nghiệm thu.
- `docs/week5/G4_CHECKLIST.md`: 30 ca trong HTML của bạn, mặc định chưa chạy.
- `docs/week5/RUNBOOK.md`: giải quyết worker/DataNode/replay/Mongo/metrics stale.
- `configs/week5/`: matrix và config runner/monitoring, các giá trị ví dụ **chỉ để lập kế hoạch**.
- `fixtures/week5/`: synthetic test cho evaluator; không có dữ liệu ngân hàng.

## Cấu trúc code mới

```text
src/benchmark/  protocol_w5.py | lakehouse_w5.py | stream_w5.py | fault_w5.py
src/ml/         evaluate_w5.py | ablation_w5.py
src/api/        reporting_w5.py
monitoring/     baseline_w5.py
scripts/week5/  collect.py | evaluate.py | demo_fixture.py | spark_snapshot_benchmark.py | controlled_fault.py
validation/     gate_w5.py
tests/week5/    three pytest suites
```

Không ghi đè code repo gốc. **Không đoán thông số PaySim/cluster và không tạo file kết quả benchmark giả.**
