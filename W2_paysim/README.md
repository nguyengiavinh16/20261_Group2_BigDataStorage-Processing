# PaySim – Data Quality, Schema, Test Data & Ingestion

Bộ sản phẩm cho PaySim dataset (giao dịch tiền di động mô phỏng).

## Nội dung

| Đường dẫn | Mô tả | Tuần |
|---|---|---|
| `schema/paysim_schema.md` | Tài liệu schema, ràng buộc, quy tắc nghiệp vụ | W1 |
| `schema/paysim.record.schema.json` | JSON Schema cho một bản ghi | W2 |
| `scripts/validate_paysim.py` | Validator + sinh data quality report | W2 |
| `scripts/make_test_data.py` | Sinh dữ liệu test tổng hợp | W1 |
| `reports/data_quality_report_synthetic_sample.md` | Báo cáo DQ mẫu (dữ liệu tổng hợp) | W1 |
| `test_data/` | Dataset test: CSV, JSON, ca lỗi | W1, W2 |
| `tests/test_validate_paysim.py` | Unit test cho validator | W2 |

## Cài đặt

```bash
pip install pandas numpy
```

Không cần `jsonschema`. Validator không phụ thuộc vào thư viện này.

## Ingestion: quy trình

1. **Đặt file** PaySim gốc (CSV) vào thư mục `data/raw/`.
2. **Validate** trước khi nạp:
   ```bash
   python scripts/validate_paysim.py data/raw/paysim.csv --report reports/dq_paysim.md
   ```
3. **Kiểm tra exit code** trong pipeline:
   - `0`: không có lỗi ERROR, có thể nạp. WARN vẫn cần xem trong báo cáo.
   - `1`: có lỗi ERROR hoặc thiếu cột. Dừng pipeline.
   - `2`: không đọc được file.
4. **Nạp** dữ liệu sau khi validate pass (ví dụ):
   ```python
   import pandas as pd
   df = pd.read_csv("data/raw/paysim.csv")
   df.to_parquet("data/curated/paysim.parquet", index=False)
   ```

Định dạng JSON được hỗ trợ qua `--format json|ndjson`, hoặc tự nhận theo đuôi file.

## Chạy test

```bash
python scripts/make_test_data.py            # sinh lại dữ liệu test (tùy chọn)
python -m unittest discover -s tests -v
```

## Hạn chế cần biết
- Bộ test và báo cáo mẫu được sinh từ **dữ liệu tổng hợp** theo đúng cấu trúc PaySim, không phải dữ liệu thật.
  Số liệu trong báo cáo mẫu không đại diện cho PaySim gốc.
- Các quy tắc cross-field (cân bằng số dư, quy tắc cờ) được suy ra từ mô tả công khai của bộ dữ liệu.
  Khi chạy trên file gốc, cần xem tỷ lệ vi phạm WARN để xác nhận quy tắc có khớp hay không.
- Khi chạy trên file rất lớn (hàng triệu dòng, khoảng 470 MB CSV), validator nạp toàn bộ vào RAM. Cần khoảng vài GB bộ nhớ trống.
