# Tuần 6 — Quickstart và nghiệm thu độc lập (Windows / PowerShell)

## 0. Phạm vi và điều kiện tiền đề

Bắt đầu từ **repo thật** đã merge và test W1→W5, không phải chỉ từ screenshot. Tài liệu này không giả định Docker Compose có container tên cố định. Windows: Docker Desktop/WSL phải RUNNING; Spark version/Scala/Iceberg connector phải khớp với W2, Kafka/Mongo DNS phải dùng hostname trong container network. **G3/G4 vẫn chưa PASS trong môi trường tạo patch** — cần làm thật, ghi evidence, không thay bằng fixture.

```powershell
# ở repo root
# git switch -c release/w6-candidate
python --version
git status --short
git rev-parse HEAD
python -m pip install -r requirements-w6.txt
$env:PYTHONPATH = "."
python -m pytest -q tests/week6
python -m pytest -q tests
```

Đối chiếu file collision nếu repo thật có file `scripts/week6` và chỉ merge qua Git/PR. Các patch W1→W5 đã có `docs/weekX/INTEGRATION.md` mô tả lệnh cluster, không tự đoán input URI.

## 1. Kiểm tra môi trường, config và checksum

```powershell
# KHÔNG đặt secret trong Git. .env.example chỉ là mẫu.
docker compose config
docker compose ps
# Trong Spark/HDFS namespace: ghi report của workers và DataNodes/FSCK từ W2.
# Tạo bản revision freeze ở Git sau khi sửa blocker:
git status --short
git rev-parse HEAD
```

Chỉ freeze khi staging clean, report test qua và source CSV có checksum SHA-256. Dữ liệu PaySim nằm ngoài Git (`data/raw/...` hoặc mounted volume), không tải về bằng lệnh ngầm hay commit CSV lớn:

```powershell
Get-FileHash .\data\raw\paysim_dataset.csv -Algorithm SHA256
```

Đối chiếu với manifest W1. Nếu sai checksum, **dừng**. Không xóa warehouse/HDFS để chữa checksum; tìm đúng source hoặc tạo snapshot mới có revision/version mới.

## 2. Chạy lại fixture độc lập trong isolated workspace

```powershell
# RUN tại repo root, output ngoài repo (tránh mang cache sang checkout)
python -m scripts.week6.cli fixture-reproduce --root . --output ..\fraud-w6-fixture --operator REVIEWER_ID
# nếu repo có .git sạch & commit chứa TẤT CẢ patch, dùng --git:
python -m scripts.week6.cli fixture-reproduce --root . --output ..\fraud-w6-git-fixture --operator REVIEWER_ID --git
```

Đầu ra: `reproduction-fixture.json`, ba log, W3 state+Gold, W4 partition/batch parity, W5 evaluator synthetic. Kể cả `--git` mode, kết quả **fixture_only=true**; không đủ G5. Log ghi có thực thi và exit; nếu thiếu dependency, báo FAIL để reviewer ghi blocker. Không xóa checkpoint thật.

Chạy riêng API fixture đã có ở W2:

```powershell
$env:FRAUD_W2_FIXTURE = "fixtures/week2/predictions-fixture.json"
python -m uvicorn src.api.service_w2:app --host 127.0.0.1 --port 8000
# Mở http://127.0.0.1:8000/ ; GET /api/v1/summary?run_id=w2-fixture-run&cutoff=379
```

Ở production run, chỉ dùng Mongo-backed W3 API. Fixture UI không được trình bày là durable Mongo hoặc metrics load test.

## 3. Tái lập tích hợp thật có giới hạn

Người không phải tác giả tạo **checkout sạch khác workspace**, revision cố định, download/source checksum đúng, đi theo lệnh đã xác minh trong W2/W3/W4/W5:

1. Khởi động Docker Compose core từ config đã xác nhận; kiểm tra Spark driver/executors, HDFS RF2, catalog Iceberg, Kafka broker/topic, Mongo index và API.
2. W1 snapshot từ CSV PaySim, stage parts đến HDFS. W2 tạo Bronze, chốt Bronze snapshot, Silver dedup, chốt Silver snapshot; W3 Gold từ **Silver snapshot pin**, kiểm SQL counts/amount/IDs. Ghi `run_id`, SHA256, ID manifest và cutoff.
3. W2 feature A và/or W4 A+H train candidate trên train, threshold từ validation. **Model artifact lưu HDFS**; load trong process riêng với `scripts/week6/independent_model_reload.py` (Spark submit, cần `--features`, `--expected`, `--model-kind`). Kiểm toàn bộ IDs, score và decision.
4. W3/W4 archive + scoring là hai queries có checkpoint khác nhau, input Kafka replay có published_at, không có label. W3 Mongo sink có unique key `(run_id, transaction_id)`; API đọc run thật.
5. Chạy warm-up 355–378 trước scored period 379–743 nếu dùng test split. Thử pause/restart trong staging, **không xóa checkpoint**. Finalize cutoff và chạy G3 parity/ID reconciliation cùng các fault scenarios G4 trước khi G5.
6. Điền report từ từng lần chạy (không trộn model/run), chốt `G4 PASS` nếu và chỉ nếu các test live đã PASS. Ghi operator, revision, commands, environment và blocker sửa rồi thử lại.

**Chưa chạy các lệnh cluster nêu ở mục này trong môi trường tạo patch.** Đường dẫn HDFS, jar, container names, `spark-submit` được lấy từ cấu hình thật — không coi các tên ví dụ là lệnh đã xác minh.

## 4. Kiểm tra config streaming và model

```powershell
python -m scripts.week6.cli stream-guard --config configs/week6/stream-release.example.json --output ..\w6-stream-plan.json
# config mẫu trả PASS nếu kiểm định dạng nhưng không xác nhận kết nối hay model thật.
# Điều chỉnh các field thực tế, không sửa code để gỡ guard.

python -m scripts.week6.cli model-card --manifest path\to\actual-model-manifest.json --output ..\model-card.md --audit-output ..\model-audit.json
# Lệnh đọc từ report thật; thiếu metadata phải INCONCLUSIVE, không tự điền AP/recall.
```

Nếu W4 model manifest có field tên khác, đưa qua adapter có kiểm chứng thay vì điền nhầm. `artifact_sha256` phải được lấy từ **artifact immutable/package thực**, không giả SHA HDFS directory.

## 5. Rà staged files và release archive

```powershell
# Package source only, không chạy audit lên repo có data/raw/.git: thư mục staging cần sạch.
python -m scripts.week6.cli package-audit --root path\to\staging-source --output ..\w6-package-audit.json
python -m scripts.week6.cli build-release --root path\to\staging-source --zip ..\fraud-source-release.zip --manifest ..\w6-source-manifest.json
# Luôn kiểm Git staged diff riêng:
git diff --cached --name-only
```

Lệnh audit FAIL nếu chứa secret pattern, checkpoint, `.env` thật, model/Parquet, symlink hoặc file >10 MiB. Không đưa data folder/checkpoint vào staging. SHA256 manifest so lại trước demo.

## 6. G5 — nguồn chứng cứ trước rồi mới gate

Cần **30 ca acceptance** trong `G5_CHECKLIST.md`, G4 PASS integration thật, `reproduction` độc lập đầy đủ, operator khác tác giả, hash files, `signoffs` của cả năm người. Mỗi task `P*-W6*` có 3 checks PASS và người review chéo. Bộ gate là validator JSON nội bộ — **không thay chữ ký con người và không thể chứng minh tính xác thực của nguồn bằng một JSON tự khai**.

```powershell
python -m validation.gate_w6 --context configs/week6/release-context.example.json --tasks configs/week6/task-reports.example.json --g4 configs/week6/g4-live-result.example.json --reproduction configs/week6/reproduction.example.json --root ..\g5-evidence --output ..\g5-final.json
# exit 0=PASS, 1=FAIL, 2=INCONCLUSIVE
```

Report mẫu cố ý INCONCLUSIVE. Không điền PASS cho những thứ chưa chạy. `G5 PASS` chỉ có ý nghĩa nếu reviewer mở và xác nhận evidence thật.

## 7. Test bất thường và cleanup an toàn

- Thiếu HDFS URI, model, Mongo endpoint → lỗi sớm; không fallback vào cache của tác giả.
- Restart: dùng **resume** với cùng run/version/topic/checkpoint; fresh run yêu cầu checkpoint mới + warm-up đầy đủ.
- ID mismatch/score tolerance => FAIL, không coi skipped test thành PASS.
- Có blocker sau freeze => cập nhật issue, commit mới, checksum và **rerun những phần bị ảnh hưởng**; không xóa log lỗi cũ.
- Không `docker compose down -v`, không `hdfs dfs -rm -r /fraud`, không xóa checkpoint để khắc phục scoring sai.
