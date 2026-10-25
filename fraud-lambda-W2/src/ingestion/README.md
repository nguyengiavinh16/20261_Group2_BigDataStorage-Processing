# Ingestion

Phần nạp dữ liệu giao dịch vào hệ thống: định nghĩa định dạng thông điệp JSON, kiểm tra hợp lệ (validation) trước khi gửi vào Kafka, và quy ước xử lý thông điệp lỗi.

## 1. Luồng dữ liệu

```
fraudTrain.csv ──► producer (đọc theo thời gian giao dịch) ──► validate ──┬─► Kafka topic `transactions`
                                                                           └─► Kafka topic `transactions.invalid` (kèm lý do lỗi)
```

> Producer sẽ được viết ở Tuần 3. Tuần 2 chốt schema, validator và dữ liệu test.

## 2. Định dạng thông điệp

- Schema: [`schemas/transaction.schema.json`](../../schemas/transaction.schema.json) (JSON Schema 2020-12).
- Tên trường giữ nguyên theo dataset Sparkov, xem [`docs/data-source.md`](../../docs/data-source.md).
- **Bắt buộc:** `trans_num`, `cc_num`, `trans_date_trans_time`, `merchant`, `category`, `amt`, `lat`, `long`, `merch_lat`, `merch_long`.
- **Tùy chọn:** `unix_time`, `gender`, `city`, `state`, `zip`, `city_pop`, `job`, `is_fraud`.
- `additionalProperties: false`: trường lạ bị từ chối để phát hiện sớm lỗi đổi tên cột.
- `is_fraud` chỉ dùng khi replay để đánh giá; không đưa vào feature của mô hình.
- Không gửi `first`, `last`, `street`, `dob` (định danh cá nhân giả, không có ích cho mô hình).

Ví dụ hợp lệ: [`tests/fixtures/valid_transaction.json`](../../tests/fixtures/valid_transaction.json).

## 3. Quy tắc validation

| Loại kiểm tra | Ví dụ |
|---|---|
| Trường bắt buộc | thiếu `amt` |
| Kiểu dữ liệu | `amt` là chuỗi thay vì số |
| Miền giá trị | `amt <= 0`, `lat` ngoài [-90, 90], `long` ngoài [-180, 180] |
| Định dạng thời gian | không đúng `YYYY-MM-DD HH:MM:SS`, hoặc ngày không tồn tại (30/02) |
| Trường thừa | trường không có trong schema |

Quy ước: thông điệp lỗi **không bị bỏ**; nó được ghi vào topic `transactions.invalid` cùng danh sách lý do để phân tích sau (chốt khi viết producer ở Tuần 3).

## 4. Dữ liệu test

| File trong `tests/fixtures/` | Kỳ vọng |
|---|---|
| `valid_transaction.json` | hợp lệ |
| `invalid_missing_field.json` | thiếu `amt` |
| `invalid_wrong_type.json` | `amt` sai kiểu |
| `invalid_out_of_range.json` | `amt` âm, `lat` ngoài miền |
| `invalid_bad_timestamp.json` | sai định dạng thời gian |
| `invalid_extra_field.json` | có trường thừa `debug_flag` |

## 5. Cách chạy

```bash
pip install -r requirements.txt

# Validate từng file
python -m src.ingestion.validation tests/fixtures/valid_transaction.json tests/fixtures/invalid_wrong_type.json

# Chạy test
pytest tests/test_validation.py -v
```

## 6. Việc tiếp theo (Tuần 3)

- Producer Kafka: đọc CSV, chuyển mỗi dòng thành JSON, validate, gửi vào `transactions` hoặc `transactions.invalid`.
- Tốc độ replay có thể cấu hình (nhanh gấp N lần thời gian thật).
- Nạp song song dữ liệu lịch sử vào HDFS (Parquet).
