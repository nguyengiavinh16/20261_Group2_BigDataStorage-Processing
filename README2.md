# PaySim – Tổng hợp đầu việc W1 và W2

## Tổng quan

| Tuần | Đầu việc | Gói tương ứng | Trạng thái |
|---|---|---|---|
| W1 | Data quality report | `PaySim_W1_DataQuality_Schema_TestData.zip` | Hoàn thành: báo cáo trên dữ liệu gốc, Verdict `PASS (with warnings)` |
| W1 | Tài liệu schema | như trên, `schema/paysim_schema.md` | Hoàn thành, cần cập nhật nếu các cảnh báo WARN cho thấy quy tắc chưa khớp |
| W1 | Test data | như trên, `test_data/` | Hoàn thành (dữ liệu tổng hợp) |
| W2 | Test JSON | `PaySim_W2_Validation_TestJSON_Ingestion.zip`, `test_data/` | Hoàn thành với dữ liệu tổng hợp. Chưa tạo mẫu JSON từ dữ liệu thật |
| W2 | Validation | `scripts/validate_paysim.py`, `tests/` | Hoàn thành: 5 unit test pass |
| W2 | README ingestion | `README.md` | Hoàn thành |
| W2 | Ingestion dữ liệu gốc | `data/curated/paysim.parquet` | Validate đạt `PASS (with warnings)`. Cần xác nhận file parquet đã được tạo |

## Cấu trúc sản phẩm

```
W1_paysim/
  README.md
  download_paysim.py
  data/raw/README.md              (đặt file PaySim gốc vào đây)
  schema/paysim_schema.md
  reports/data_quality_report_synthetic_sample.md
  reports/dq_paysim_full.md       (báo cáo trên dữ liệu gốc, tạo trên máy bạn)
  test_data/                      (CSV hợp lệ, ca lỗi, file đối chiếu)
  scripts/validate_paysim.py
  scripts/make_test_data.py

W2_paysim/
  README.md
  download_paysim.py
  data/raw/README.md
  schema/paysim.record.schema.json
  scripts/validate_paysim.py
  tests/test_validate_paysim.py
  test_data/                      (JSON + CSV + ca lỗi)
```

## Lệnh chạy

```bash
# W1: báo cáo chất lượng dữ liệu gốc
python3 scripts/validate_paysim.py data/raw/paysim.csv --report reports/dq_paysim_full.md

# W2: unit test
python3 -m unittest discover -s tests -v

# W2: ingestion (chỉ nạp khi validate exit 0)
python3 scripts/validate_paysim.py data/raw/paysim.csv --report reports/validation_ingest.md \
  && python3 -c "import pandas as pd; pd.read_csv('data/raw/paysim.csv').to_parquet('data/curated/paysim.parquet', index=False)"
```

## Các lỗi đã sửa trong quá trình làm

1. Tên loại giao dịch dùng dấu gạch ngang (`CASH-IN`). PaySim dùng dấu gạch dưới (`CASH_IN`). Đã sửa toàn bộ code, schema và test data.
2. Quy tắc `amount > 0` quá chặt. Đã tách: số âm là ERROR, số 0 là WARN.
3. Tên file có khoảng trắng gây lỗi khi truyền đường dẫn vào shell. Cần đặt trong dấu nháy kép hoặc đổi tên.

## Việc còn lại

- [ ] Xác nhận file `data/curated/paysim.parquet` đã được tạo (`ls -lh`).
- [ ] Xem bảng checks trong `reports/validation_ingest.md`, ghi nhận số dòng của các cảnh báo ORIG_BALANCE_MATCH, DEST_BALANCE_MATCH, AMOUNT_ZERO, MERCHANT_*, FRAUD_TYPE_PROFILE.
- [ ] Đối chiếu các quy tắc WARN với mô tả chính thức của Kaggle `ealaxi/paysim1`. Cập nhật `schema/paysim_schema.md` nếu cần.
- [ ] (Tùy chọn) Tạo mẫu test JSON từ dữ liệu thật bằng `pd.read_csv(..., nrows=1000).to_json(...)`.

## Hạn chế cần ghi nhớ

- Test data và báo cáo mẫu trong gói là dữ liệu tổng hợp, không phải PaySim gốc.
- Một số quy tắc cân bằng số dư và quy tắc cờ gian lận được suy ra từ mô tả công khai, chưa được đối chiếu đầy đủ với dữ liệu gốc. Vì vậy chúng ở mức WARN.
- Validator nạp toàn bộ file vào RAM (khoảng 470 MB CSV), cần vài GB bộ nhớ trống.
