# Tài liệu schema – PaySim Dataset

PaySim là bộ dữ liệu mô phỏng giao dịch tiền di động (mobile money), mỗi dòng là một giao dịch.
Tài liệu này mô tả cấu trúc, ràng buộc và quy tắc nghiệp vụ. Phần quy tắc nghiệp vụ được
hiện thực trong `scripts/validate_paysim.py`.

> Lưu ý: các ràng buộc dưới đây dựa trên mô tả công khai của bộ dữ liệu và kiểm tra trên dữ liệu
> tổng hợp. Trước khi dùng cho production, hãy đối chiếu với file PaySim gốc đã tải về.

## 1. Cấu trúc bảng

| # | Cột | Kiểu | Null | Mô tả | Ràng buộc |
|---|---|---|---|---|---|
| 1 | `step` | integer | Không | Chỉ số giờ trong mô phỏng (1 step = 1 giờ) | 1 ≤ step ≤ 744 |
| 2 | `type` | string | Không | Loại giao dịch | Một trong: `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, `TRANSFER` |
| 3 | `amount` | number | Không | Số tiền giao dịch | amount ≥ 0 (amount = 0 là WARN) |
| 4 | `nameOrig` | string | Không | Mã tài khoản người gửi | Khớp `C[0-9]+` |
| 5 | `oldbalanceOrg` | number | Không | Số dư người gửi trước giao dịch | ≥ 0 |
| 6 | `newbalanceOrig` | number | Không | Số dư người gửi sau giao dịch | ≥ 0 |
| 7 | `nameDest` | string | Không | Mã tài khoản người nhận | Khớp `[CM][0-9]+` (C = khách hàng, M = merchant) |
| 8 | `oldbalanceDest` | number | Không | Số dư người nhận trước giao dịch | ≥ 0 |
| 9 | `newbalanceDest` | number | Không | Số dư người nhận sau giao dịch | ≥ 0 |
| 10 | `isFraud` | integer | Không | Nhãn gian lận (ground truth) | 0 hoặc 1 |
| 11 | `isFlaggedFraud` | integer | Không | Cờ do hệ thống nghiệp vụ gán | 0 hoặc 1 |

Thứ tự cột phải đúng như bảng trên. Validator báo lỗi nếu thiếu hoặc thừa cột.

## 2. Quy tắc nghiệp vụ

### Lỗi (ERROR) – làm dataset không hợp lệ

| Mã | Quy tắc |
|---|---|
| `NULL_VALUES` | Không có giá trị thiếu ở 11 cột |
| `TYPE_DOMAIN` | `type` thuộc 5 giá trị cho phép |
| `STEP_RANGE` | `step` là số nguyên trong [1, 744] |
| `AMOUNT_NONNEGATIVE` | `amount` không âm |
| `BALANCE_NONNEGATIVE` | Tất cả số dư ≥ 0 |
| `FRAUD_LABEL_DOMAIN` | `isFraud` ∈ {0, 1} |
| `FLAG_LABEL_DOMAIN` | `isFlaggedFraud` ∈ {0, 1} |
| `ORIG_ID_FORMAT` | `nameOrig` bắt đầu bằng `C` (người gửi không thể là merchant) |
| `DEST_ID_FORMAT` | `nameDest` bắt đầu bằng `C` hoặc `M` |
| `FLAG_RULE` | `isFlaggedFraud = 1` chỉ khi `type = TRANSFER` và `amount > 200,000` |

### Cảnh báo (WARN) – đáng kiểm tra nhưng không làm fail dataset

| Mã | Quy tắc |
|---|---|
| `ORIG_BALANCE_MATCH` | `newbalanceOrig` = `oldbalanceOrg` + `amount` (CASH_IN) hoặc − `amount` (các loại còn lại), sai số 0.01 |
| `DEST_BALANCE_MATCH` | TRANSFER tới khách hàng: `newbalanceDest` = `oldbalanceDest` + `amount` |
| `MERCHANT_DEST_NO_BALANCE` | Merchant (M*) có số dư đích bằng 0 |
| `MERCHANT_DEST_TYPE` | Merchant chỉ xuất hiện như đích của `PAYMENT` |
| `FRAUD_TYPE_PROFILE` | Gian lận chỉ xuất hiện ở `TRANSFER` và `CASH_OUT` |
| `AMOUNT_ZERO` | `amount` bằng 0 (có trong dữ liệu gốc, cần xem lại trước khi dùng) |
| `DUPLICATE_ROWS` | Dòng trùng lặp hoàn toàn |

Lưu ý về ORIG_BALANCE_MATCH và DEST_BALANCE_MATCH: với dữ liệu PaySim gốc, một số dòng có thể không
khớp công thức này. Đây là lý do chúng là WARN chứ không phải ERROR. Cần kiểm tra tỷ lệ vi phạm trên
file thật trước khi quyết định mức độ nghiêm trọng.

## 3. Định dạng file hỗ trợ

| Định dạng | Ví dụ | Ghi chú |
|---|---|---|
| CSV | `paysim_valid_sample.csv` | Header là tên cột, cột `nameOrig`/`nameDest` đọc dạng chuỗi |
| JSON (mảng) | `paysim_valid_sample.json` | Mảng các object có đủ 11 key |
| JSON (object) | `{"records": [...]}` | Validator tự nhận diện |
| NDJSON | `.ndjson`, `.jsonl` | Một object mỗi dòng |

`schema/paysim.record.schema.json` là contract (JSON Schema draft 2020-12) cho một bản ghi, dùng cho
phía tiêu thụ dữ liệu. Nó không chứa quy tắc chéo giữa các trường; các quy tắc đó nằm trong validator.

## 4. Tập test
- `test_data/paysim_valid_sample.*`: 300 dòng tổng hợp, thỏa mọi quy tắc.
- `test_data/paysim_invalid_cases.csv`: 14 dòng, mỗi dòng vi phạm đúng một quy tắc.
  `invalid_cases_expected.json` ghi rõ quy tắc mong đợi cho từng dòng.
