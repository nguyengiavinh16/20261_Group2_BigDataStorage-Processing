# Phát hiện gian lận giao dịch theo thời gian thực với kiến trúc Lambda

> Nguyên mẫu end-to-end kết hợp xử lý **batch** (huấn luyện mô hình trên dữ liệu lịch sử) và **streaming** (chấm điểm giao dịch gần thời gian thực).

## 1. Giới thiệu

- **Bài toán:** Phát hiện giao dịch gian lận trong hệ thống thanh toán điện tử với độ trễ thấp.
- **Hướng tiếp cận:** Kiến trúc Lambda – batch layer huấn luyện mô hình, speed layer chấm điểm luồng giao dịch mới, serving layer lưu và hiển thị cảnh báo.
- **Thời gian thực hiện:** 10 tuần.
- **Nhóm thực hiện:** Nhóm 2 – Lưu trữ và xử lý dữ liệu lớn (20261)

## 2. Kiến trúc tổng quan

```
Nguồn dữ liệu ──► Kafka ──► Spark Structured Streaming ──► NoSQL (Cassandra/MongoDB) ──► API / Dashboard
      │                              ▲
      ▼                              │ nạp mô hình
   HDFS (Parquet) ──► Spark MLlib (Batch ML) ──► Model registry
```

_(Thay bằng sơ đồ chi tiết ở `docs/architecture.png` khi hoàn thành thiết kế – Tuần 2.)_

## 3. Công nghệ sử dụng

| Thành phần | Công nghệ | Phiên bản |
|---|---|---|
| Ngôn ngữ | Python | _3.x_ |
| Message broker | Apache Kafka | _x.y_ |
| Lưu trữ | HDFS, Parquet | _x.y_ |
| Xử lý & ML | Spark, Spark MLlib, Structured Streaming | _x.y_ |
| NoSQL | Cassandra / MongoDB | _x.y_ |
| Triển khai | Docker, Kubernetes | _x.y_ |
| Monitoring | _Prometheus / Grafana / ..._ | _x.y_ |

## 4. Cấu trúc thư mục

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── configs/               # etl.yaml, train.yaml, benchmark.yaml, model-metadata-v1.json
├── data/
│   ├── raw/               # fraudTrain.csv, fraudTest.csv (KHÔNG commit)
│   └── processed/         # Parquet sau khi làm sạch (KHÔNG commit)
├── deploy/                # docker-compose, manifest Kubernetes
├── docs/                  # data-source, data-dictionary, model-interface, model-card, ...
├── notebooks/             # 01_data_profiling.ipynb
├── reports/               # data-quality-report, model-evaluation, benchmark, ...
├── schemas/               # transaction.schema.json (JSON Schema cho thông điệp Kafka)
├── scripts/               # hello_spark.py và script tiện ích
├── src/
│   ├── processing/        # schemas.py, etl.py, features.py
│   ├── ml/                # features.py, train.py, evaluate.py, inference.py
│   ├── ingestion/         # validation.py, README.md; Kafka producer, nạp HDFS (Tuần 3)
│   └── streaming/         # Spark Structured Streaming (Tuần 5)
└── tests/
    ├── fixtures/
    ├── test_validation.py
    ├── test_etl.py
    └── test_inference_contract.py
```

## 5. Nguồn dữ liệu

- **Tên dataset:** Credit Card Transactions Fraud Detection (Sparkov, dữ liệu mô phỏng)
- **Nguồn:** Kaggle, sinh bằng công cụ Sparkov Data Generation; giai đoạn 01/01/2019 – 31/12/2020, 1.000 khách hàng, 800 merchant
- **Giấy phép:** _<ghi lại từ trang Kaggle>_
- **Kích thước, số bản ghi, tỉ lệ gian lận:** ~1,85 triệu giao dịch, tỉ lệ gian lận ~0,5% _(xác nhận lại sau khi khảo sát)_
- **Schema & định nghĩa nhãn:** xem `docs/data-source.md`, `docs/data-dictionary.md` và `src/processing/schemas.py` (nhãn `is_fraud`)

## 6. Hướng dẫn cài đặt môi trường

### Yêu cầu
- Python _3.x_, Git, Docker & Docker Compose, Java _(phiên bản tương thích Spark)_

### Các bước
```bash
# 1. Clone repo
git clone <repo-url>
cd <repo-name>

# 2. Tạo môi trường ảo
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Cài thư viện
pip install -r requirements.txt

# 4. Khởi động hạ tầng (sau khi có docker-compose)
docker compose -f deploy/docker-compose.yml up -d
```

### Kiểm tra môi trường
```bash
python --version
git --version
docker --version
```

## 7. Cách chạy

| Bước | Lệnh | Mô tả |
|---|---|---|
| Ingestion | _<điền>_ | Đẩy dữ liệu vào Kafka / HDFS |
| Huấn luyện | _<điền>_ | Train & lưu mô hình |
| Streaming | _<điền>_ | Chấm điểm giao dịch thời gian thực |
| Dashboard | _<điền>_ | Mở giao diện cảnh báo |

## 8. Kết quả

| Mô hình | Precision | Recall | PR-AUC | Ngưỡng |
|---|---|---|---|---|
| Logistic Regression | | | | |
| Random Forest | | | | |

_Kết quả benchmark (độ trễ, thông lượng) bổ sung ở Tuần 9._

## 9. Kế hoạch 10 tuần

| Tuần | Nội dung | Trạng thái |
|---|---|---|
| 1 | Setup môi trường, ôn Python/Git/JSON, tìm nguồn data | ⬜ |
| 2 | Khảo sát dữ liệu và thiết kế | ⬜ |
| 3 | Ingestion – lưu trữ | ⬜ |
| 4 | Batch ML | ⬜ |
| 5 | Streaming MVP | ⬜ |
| 6 | NoSQL và giao diện | ⬜ |
| 7 | Nâng cao Spark – streaming | ⬜ |
| 8 | Triển khai – monitoring | ⬜ |
| 9 | Benchmark – hoàn thiện báo cáo | ⬜ |
| 10 | Dự bị | ⬜ |

## 10. Quy ước làm việc nhóm

- **Nhánh:** `main` (ổn định), `dev` (tích hợp), `feature/<tên>-<mô tả>`.
- **Commit message:** `<loại>: <mô tả ngắn>` (ví dụ `feat: thêm Kafka producer`, `fix: sửa schema`, `docs: cập nhật README`).
- **Pull request:** có ít nhất 1 người review trước khi merge vào `dev`.
- **Không commit:** dữ liệu lớn, file `.env`, khóa/mật khẩu.

## 11. Thành viên & phân công

| Thành viên | Vai trò | Phụ trách |
|---|---|---|
| | | |

## 12. Tài liệu tham khảo

- _<liệt kê bài báo, tài liệu Spark/Kafka, dataset...>_

## 13. Giấy phép

_<MIT / Apache-2.0 / khác>_
