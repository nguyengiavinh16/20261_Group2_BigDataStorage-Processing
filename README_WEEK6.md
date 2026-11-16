# FRAUD ANALYTICS — TUẦN 6 / 6 (BẢN BÀN GIAO)

**Đã triển khai mã bổ sung cho 10 task P1–P5 tuần 6 theo HTML.** Phạm vi: freeze và audit release; tái lập cách ly; lineage dữ liệu; model card + independent Spark reload; kiểm tra vận hành streaming; nguồn chứng cứ, gate G5; runbook + diễn tập demo.

> **TRẠNG THÁI:** Mã/test local đã hoàn thành, **G5 = INCONCLUSIVE** vì G3/G4 còn chờ bằng chứng integration; chưa có clean Git checkout do người khác trực tiếp thực hiện, không có Spark/Kafka/Iceberg/HDFS/Mongo thật. Không tự động ký xác nhận 5 người. Không tuyên bố hệ thống production-ready/HA/exactly-once.

## Nội dung 10 task

| Task | Phần triển khai |
|---|---|
| `P1-W6A` | `src/release_w6/reproduction_w6.py`: clean source copy / optional `git clone` + subprocess isolated, ghi log; quickstart (`docs/week6/INTEGRATION.md`) |
| `P1-W6B` | `src/release_w6/package_w6.py`: staged release manifest SHA256, scan secrets/large artifacts; `architecture_w6.py`, `docs/week6/DEMO.md` |
| `P2-W6A` | `lineage_w6.py`: source checksum + physical source line → Silver ID → Gold group; `DATA_RUNBOOK.md` data dictionary, retention |
| `P2-W6B` | `DATA_RUNBOOK.md`: demo Bronze occurrences → Silver dedup → Gold cutoff → compaction; evidence/limitations |
| `P3-W6A` | `model_card_w6.py`: kiểm tra feature order, threshold policy, artifact provenance; sinh model card, `MODEL_RUNBOOK.md` |
| `P3-W6B` | `scripts/week6/independent_model_reload.py`: load một model Spark frozen và đối chiếu golden IDs/score/decision; guard version |
| `P4-W6A` | `stream_release_w6.py`: validated startup/resume/fresh, checkpoint, warm-up, cấu hình bắt buộc; `STREAM_RUNBOOK.md` |
| `P4-W6B` | `STREAM_RUNBOOK.md`, `DEMO.md`: release regression, restart/known limitations, version handshake |
| `P5-W6A` | `reproduction_w6.py` + `package_w6.py` + test âm tính; fixture mode chỉ mang nhãn local |
| `P5-W6B` | `claims_w6.py` kiểm số đo trỏ đúng run/report/hash + `validation/gate_w6.py` yêu cầu G4 live PASS, 30 ca và reviewer; `G5_CHECKLIST.md` |

## Chạy nhanh trên Windows (sau khi tích hợp patch W1→W6)

```powershell
$env:PYTHONPATH = "."
python -m pip install -r requirements-w6.txt
python -m pytest -q tests/week6
python -m pytest -q tests

# CHẠY BẰNG OUTPUT TUYỆT ĐỐI NẰM NGOÀI REPO
python -m scripts.week6.cli fixture-reproduce --root . --output ..\fraud-w6-fixture --operator local-dev
# Local copy không tương đương clean Git checkout; báo cáo có fixture_only=true.

python -m scripts.week6.cli stream-guard --config configs/week6/stream-release.example.json --output ..\fraud-w6-stream-guard.json
python -m validation.gate_w6 --context configs/week6/release-context.example.json --tasks configs/week6/task-reports.example.json --g4 configs/week6/g4-live-result.example.json --reproduction configs/week6/reproduction.example.json --output ..\fraud-w6-g5.json
# G5 trả 2 / INCONCLUSIVE nếu thiếu bằng chứng, không phải lỗi CLI.
```

**Trên môi trường tạo patch:** bộ ghép W1–W6 được chạy với `287 passed, 3 skipped`; ba nhóm skipped cần Spark/runtime ngoài. Bộ fixture-isolated 3/3 PASS nhưng KHÔNG phải benchmark hoặc hệ thống phân tán. Các báo cáo gốc nằm trong evidence local của quá trình tạo patch, không đại diện cluster của nhóm.

## Nguyên tắc release

- G5 **không được PASS** chỉ vì có code, fixture hoặc chữ ký điền tay trong JSON. Người thực hiện độc lập cần có biên bản, output từ checkout sạch, G4 PASS thật và ký xác nhận ngoài công cụ.
- Model/card/số liệu demo cần cùng commit, run_id, cutoff, snapshot, model_version, feature_version, hash config; test metric không chọn lại threshold.
- Dữ liệu gốc, dữ liệu đã xử lý, JAR, ML artifact, Mongo dump, Kafka logs, SQLite state, HDFS checkpoint, `.env` thật và secrets **không vào Git**. Cấu hình `.example` không chứa mật khẩu.
- Chỉ sửa lỗi core sau freeze; sửa xong chạy lại test/gate bị ảnh hưởng và cập nhật evidence. Giữ tất cả evidence FAIL/INCONCLUSIVE.
- Patch bổ sung, không ghi đè file repo gốc chưa được xem. `docker-compose.yml` thực tế của bạn vẫn phải được đối chiếu riêng.

## Điểm bắt buộc trước buổi bảo vệ

Đọc `docs/week6/INTEGRATION.md` → `DATA_RUNBOOK.md` → `MODEL_RUNBOOK.md` → `STREAM_RUNBOOK.md` → `DEMO.md` → `G5_CHECKLIST.md` → `STATUS.md`.

**Đây là tuần cuối. Không có module tuần 7 hay tác vụ nền được khởi tạo.**
