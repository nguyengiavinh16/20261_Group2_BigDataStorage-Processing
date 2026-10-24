# Hướng dẫn triển khai 8 tuần (tiếng Việt)

## Trước khi bắt đầu
- Giữ `data/raw/paysim_dataset.csv` ở đúng vị trí; không commit dataset lớn hoặc thông tin nhạy cảm.
- Dùng Python 3.10 trong `.venv`.
- Mở PowerShell tại thư mục gốc repository.
- Các kết quả số liệu phụ thuộc dataset và thuật toán; không dùng số mẫu 100.000 dòng ở bước inspect làm kết quả toàn bộ dữ liệu.

## Tuần 1 — nền tảng
```powershell
python scripts/inspect_data.py --input data/raw/paysim_dataset.csv --sample-rows 100000 --output reports/paysim_inspection.json
python -m pytest -q
```
Inspect chỉ đọc mẫu theo giới hạn. Kiểm tra phân bố `isFraud`, schema và tỷ lệ missing trước khi train.

## Tuần 2 — ETL, Parquet, Spark, Kafka
Cài extras:
```powershell
pip install -e ".[batch,streaming]"
```
Chạy ETL local (cần `pyarrow` để ghi Parquet; nếu thiếu hãy cài `pip install pyarrow`):
```powershell
python scripts/run_etl.py --input data/raw/paysim_dataset.csv --output data/processed/paysim.parquet
```
Chạy Spark ETL khi Java/Spark môi trường đã sẵn sàng:
```powershell
python scripts/run_spark_etl.py --input data/raw/paysim_dataset.csv --output data/processed/paysim_parquet
```
Kafka cần broker đang chạy. Dùng `docker compose up -d kafka` rồi:
```powershell
pip install -e ".[streaming]"
python -m fraud_detection.ingestion.kafka_producer --input data/raw/paysim_dataset.csv --limit 10000 --delay-ms 10
```
Nếu module không được tìm thấy, cài dự án bằng `pip install -e .` trong `.venv`.

## Tuần 3 — train và đánh giá
```powershell
python -m fraud_detection.modeling.train --input data/raw/paysim_dataset.csv --algorithm logistic
```
Hoặc thử Random Forest:
```powershell
python -m fraud_detection.modeling.train --input data/raw/paysim_dataset.csv --algorithm forest
```
Lệnh tạo `artifacts/fraud_model.joblib` và `reports/model_metrics.json`. Với lần chạy thử, có thể thêm `--sample-rows 200000`, nhưng mẫu phải có cả hai lớp. Mặc định split phân tầng ngẫu nhiên là baseline; cần đánh giá thêm split theo `step` trước khi kết luận hiệu năng.

## Tuần 4 — streaming scoring
Đảm bảo model đã được train và Kafka đang chạy. Chạy producer từ terminal thứ nhất, Spark streaming từ terminal thứ hai:
```powershell
python -m fraud_detection.ingestion.kafka_producer --input data/raw/paysim_dataset.csv --limit 10000 --delay-ms 20
python -m fraud_detection.streaming.spark_stream
```
Spark cần Java tương thích và connector Kafka đúng phiên bản Spark. Chạy streaming trên môi trường Docker/Linux thường ổn định hơn Windows. Bản demo dùng `foreachBatch` để ghi MongoDB; cần giám sát checkpoint và idempotency khi restart.

## Tuần 5 — API và dashboard
Model phải tồn tại trước khi khởi động API:
```powershell
uvicorn fraud_detection.serving.api:app --reload --app-dir src
```
Mở `http://127.0.0.1:8000/docs`. Dashboard:
```powershell
pip install -e ".[dashboard]"
streamlit run app/dashboard.py
```
Mở `http://localhost:8501`. Endpoint `POST /predict` nhận các trường giao dịch không cần nhãn `isFraud`.

## Tuần 6 — reliability/monitoring
```powershell
python scripts/benchmark_api.py --url http://localhost:8000/health --requests 100
python -m pytest -q
```
Ghi lại latency p50/p95, error rate, throughput và chất lượng model. Đây là benchmark đơn giản, không thay thế load test phân tán.

## Tuần 7 — Docker Compose/Kubernetes
Trước tiên train model trên máy host để tạo artifact:
```powershell
python -m fraud_detection.modeling.train --input data/raw/paysim_dataset.csv
```
Sau đó:
```powershell
docker compose up --build -d
```
Kiểm tra `http://localhost:8000/health`, `http://localhost:8501`. Nếu build API báo thiếu model, xác minh `artifacts/fraud_model.joblib` tồn tại. Kafka Compose chỉ phục vụ broker; Spark streaming được chạy riêng bằng môi trường Spark có connector Kafka.

Kubernetes manifests là điểm khởi đầu, chưa phải production-ready: cần image registry, cách mount model, Secret/config, MongoDB/Kafka service, resource limits, autoscaling và security trước khi triển khai thật.

## Tuần 8 — nghiệm thu
- Chạy unit tests và integration tests trên hạ tầng đã khởi động.
- Lưu metrics, confusion matrix, PR curve, cấu hình threshold và phiên bản model.
- Đo end-to-end latency: publish Kafka → Spark score → MongoDB → API/dashboard.
- Báo cáo rõ phần nào chạy local, Docker, Spark cluster; không ghi kết quả benchmark chưa đo.

## Giới hạn bản prototype
- Không phải hệ thống ra quyết định tài chính production.
- API persistence là best-effort; bản production cần queue/retry, schema versioning, auth, secrets và audit.
- Model baseline dùng feature numeric + `type`; không đưa `nameOrig`/`nameDest` vào model để tránh cardinality cao.
- Streaming UDF là bản minh họa; với throughput cao nên dùng model serving riêng hoặc vectorized inference và kiểm tra khả năng tương thích model.
