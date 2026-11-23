# Feature Test Plan — v1 (W2)

**Owner:** Thái · **Phối hợp:** Đức Anh (validator, dữ liệu lỗi), Đạt (batch), Công Anh (streaming)
**Trạng thái:** lớp L1 đã chạy (`178 passed in 0.32s`); L2–L4 chờ pipeline của Đạt và Công Anh.
**Nguồn sự thật:** `tests/fixtures/feature_cases.json`. Mọi bảng ở mục 4 được sinh từ file này nên không thể lệch với test.

## 1. Mục tiêu và phạm vi

Kế hoạch W2 yêu cầu "test cases cho boundary values, unknown category, missing fields và threshold". Tài liệu này mở rộng thành 108 ca trong 8 nhóm, mỗi ca có **đầu vào cụ thể và giá trị kỳ vọng tính độc lập với mã đặc tả**, trong đó 13 ca dùng **dòng thật của PaySim** (ghi số dòng ở cột Mục tiêu).

| Nhóm | Mã | Số ca | Kiểm tra gì |
|---|---|---|---|
| Giá trị biên | BV | 29 | `amount`, số dư, `step` ở biên: 0, nhỏ nhất, trần 10M, lớn nhất, âm, NaN, sai kiểu; `drains_origin` ở dung sai |
| Ánh xạ thời gian | EV | 14 | `event_time` → `step` → `hour_of_day` ở biên giờ và ngày, múi giờ, thiếu múi giờ, mâu thuẫn với `step` |
| Category lạ | UC | 11 | 5 loại hợp lệ, loại lạ, khác hoa/thường, khoảng trắng, rỗng, sai kiểu |
| Thiếu trường | MF | 9 | trường bắt buộc thiếu/null; số dư thiếu ở set B (không điền 0); trường thừa vô hại |
| Chốt chặn leakage | LG | 8 | event chứa nhãn hoặc số dư sau giao dịch bị từ chối, kể cả biến thể tên |
| Cửa sổ lịch sử | HW | 10 | biên [s−24, s−1], cùng step, thứ tự đến, trùng `transaction_id`, đầu dữ liệu |
| Alert theo threshold | TH-01–15 | 15 | `fraud_score >= threshold` ở biên, giá trị không hợp lệ |
| Chọn threshold | TH-20–31 | 12 | quy tắc "precision cao nhất với recall ≥ ngưỡng", hòa điểm, nhánh F2, đầu vào lỗi |

## 2. Các lớp kiểm thử

| Lớp | Đối tượng | Ai chạy | Khi nào | Trạng thái |
|---|---|---|---|---|
| **L1** | `src/evaluation/feature_reference.py` với fixture | Thái | W2 | Đã chạy: `178 passed in 0.32s` |
| **L2** | Spark batch (ETL, feature) của Đạt, cho chạy đúng các event trong fixture | Đạt | W3 | Chờ |
| **L3** | Structured Streaming của Công Anh, cùng event | Công Anh | W4 | Chờ |
| **L4** | Nhất quán batch ↔ streaming trên cùng dữ liệu (mục 7) | Thái | W4 | Chờ |

**Cách dùng fixture ở L2/L3:** với mỗi ca `stateless`, đưa `event` vào pipeline. Kỳ vọng hợp lệ: vector feature (đúng thứ tự cột ở `feature-list.md` mục 5.4) bằng `expect.features` với sai số tương đối ≤ 1e-9. Kỳ vọng từ chối: event vào quarantine với đúng `code`, **không** được chấm điểm, và bộ đếm theo mã lỗi tăng đúng 1. Ca `history` cần state theo tài khoản nhận; ca `threshold_*` áp dụng cho khâu tạo `alert_triggered` và chọn threshold.

## 3. Cách chạy L1

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m pytest tests/test_feature_reference.py -v
```

Không cần dữ liệu PaySim. Ngoài các ca fixture, test còn kiểm tra: kết quả không phụ thuộc thứ tự đến (feature lịch sử, chọn threshold); `select_threshold` khớp bản brute-force độc lập trên 50 bộ dữ liệu ngẫu nhiên có nhiều điểm trùng; `hour_of_day` bằng giờ UTC khi T0 ở 00:00:00Z; cờ cấm bắt được các biến thể tên (`is_fraud`, `IS_FRAUD`, `newbalance_dest`...).

**Chất lượng của chính bộ test:** `python tests/mutation_check.py` cố ý làm hỏng đặc tả theo 12 cách (đổi `>=` thành `>`, lệch cửa sổ lịch sử 1 step, nới dung sai rút sạch, chấp nhận boolean làm số, bỏ yêu cầu số dư ở set B, bỏ kiểm tra trường cấm...) trên một bản sao tạm rồi xác nhận test bắt được từng lỗi (kết quả khi chạy notebook: 12/12 lỗi cố ý bị test bắt được).

## 4. Danh mục ca kiểm thử

Quy ước đầu vào: `type` = `transaction_type`, `old_src` = `source_balance_before`, `old_dest` = `destination_balance_before`, `time` = `event_time`. Cột Set là feature set cần tính (`A`, `A+T`, `B`). T0 dùng trong test là `2026-10-11T00:00:00Z`, **chỉ để chạy test**; T0 chính thức do Lead chốt.

### 4.1 Giá trị biên (BV)

| ID | Mục tiêu | Set | Đầu vào | Kết quả kỳ vọng |
|---|---|---|---|---|
| **BV-01** | amount = 0 (PaySim có 16 dòng như vậy, đều là CASH_OUT gian lận): hợp lệ, gắn cờ AMOUNT_ZERO (dòng thật #2736447) | A | type="CASH_OUT", amount=0.0 | hợp lệ: type=CASH_OUT, log_amount=0; cờ AMOUNT_ZERO |
| **BV-02** | amount = 0 và số dư = 0 (set B): ratio = 0, không phải rút sạch (dòng thật #2736447) | B | type="CASH_OUT", amount=0.0, old_src=0.0, old_dest=0.0 | hợp lệ: type=CASH_OUT, oldbalanceOrg=0, oldbalanceDest=0, amount_to_balance_ratio=0, drains_origin=0, dest_balance_zero=1, log_amount=0; cờ AMOUNT_ZERO |
| **BV-03** | amount dương nhỏ nhất của dataset (0,01) (dòng thật #4736180) | A | type="CASH_OUT", amount=0.01 | hợp lệ: type=CASH_OUT, log_amount=0.009950331 |
| **BV-04** | amount = trần 10.000.000 của gian lận, số dư người gửi lớn hơn → drains_origin = 0 (dòng thật #4440) | B | type="TRANSFER", amount=10000000.0, old_src=12930418.44, old_dest=0.0 | hợp lệ: type=TRANSFER, oldbalanceOrg=1.293042e+07, oldbalanceDest=0, amount_to_balance_ratio=0.7733701, drains_origin=0, dest_balance_zero=1, log_amount=16.1181 |
| **BV-05** | amount lớn nhất dataset (92.445.516,64): không tràn, không bị cắt (dòng thật #3686583) | A | type="TRANSFER", amount=92445516.64 | hợp lệ: type=TRANSFER, log_amount=18.34213 |
| **BV-06** | amount là số nguyên JSON (150) cho kết quả như 150.0 | A | type="PAYMENT", amount=150 | hợp lệ: type=PAYMENT, log_amount=5.01728 |
| **BV-07** | amount âm → từ chối | A | type="PAYMENT", amount=-0.01 | từ chối `NEGATIVE_VALUE` (amount) |
| **BV-08** | amount = NaN → từ chối | A | type="PAYMENT", amount=NaN | từ chối `NON_FINITE` (amount) |
| **BV-09** | amount = +Infinity → từ chối | A | type="PAYMENT", amount=+Infinity | từ chối `NON_FINITE` (amount) |
| **BV-10** | amount là chuỗi "100.5" → từ chối, không tự ép kiểu | A | type="PAYMENT", amount="100.5" | từ chối `BAD_TYPE` (amount) |
| **BV-11** | amount là boolean true → từ chối (bool là con của int trong Python) | A | type="PAYMENT", amount=true | từ chối `BAD_TYPE` (amount) |
| **BV-12** | set B: số dư người gửi = 0, amount > 0 (giao dịch hợp lệ thật): ratio = amount (dòng thật #79) | B | type="TRANSFER", amount=77957.68, old_src=0.0, old_dest=94900.0 | hợp lệ: type=TRANSFER, oldbalanceOrg=0, oldbalanceDest=94900, amount_to_balance_ratio=77957.68, drains_origin=0, dest_balance_zero=0, log_amount=11.26393 |
| **BV-13** | set B: rút sạch số dư, tài khoản nhận có số dư 0 (TRANSFER gian lận thật) (dòng thật #2) | B | type="TRANSFER", amount=181.0, old_src=181.0, old_dest=0.0 | hợp lệ: type=TRANSFER, oldbalanceOrg=181, oldbalanceDest=0, amount_to_balance_ratio=0.9945055, drains_origin=1, dest_balance_zero=1, log_amount=5.204007 |
| **BV-14** | set B: rút sạch số dư, tài khoản nhận có số dư > 0 (CASH_OUT gian lận thật) (dòng thật #3) | B | type="CASH_OUT", amount=181.0, old_src=181.0, old_dest=21182.0 | hợp lệ: type=CASH_OUT, oldbalanceOrg=181, oldbalanceDest=21182, amount_to_balance_ratio=0.9945055, drains_origin=1, dest_balance_zero=0, log_amount=5.204007 |
| **BV-15** | set B: lệch 0,004 so với số dư (< nửa cent) vẫn là rút sạch | B | type="TRANSFER", amount=181.0, old_src=181.004, old_dest=0.0 | hợp lệ: type=TRANSFER, oldbalanceOrg=181.004, oldbalanceDest=0, amount_to_balance_ratio=0.9944836, drains_origin=1, dest_balance_zero=1, log_amount=5.204007 |
| **BV-16** | set B: lệch 0,006 (> nửa cent) không phải rút sạch | B | type="TRANSFER", amount=181.0, old_src=181.006, old_dest=0.0 | hợp lệ: type=TRANSFER, oldbalanceOrg=181.006, oldbalanceDest=0, amount_to_balance_ratio=0.9944727, drains_origin=0, dest_balance_zero=1, log_amount=5.204007 |
| **BV-17** | set B: amount lớn hơn số dư (CASH_OUT hợp lệ thật): ratio > 1, không rút sạch (dòng thật #15) | B | type="CASH_OUT", amount=229133.94, old_src=15325.0, old_dest=5083.0 | hợp lệ: type=CASH_OUT, oldbalanceOrg=15325, oldbalanceDest=5083, amount_to_balance_ratio=14.95067, drains_origin=0, dest_balance_zero=0, log_amount=12.34207 |
| **BV-18** | set B: số dư người nhận = 0,01 → dest_balance_zero = 0 | B | type="TRANSFER", amount=100.0, old_src=500.0, old_dest=0.01 | hợp lệ: type=TRANSFER, oldbalanceOrg=500, oldbalanceDest=0.01, amount_to_balance_ratio=0.1996008, drains_origin=0, dest_balance_zero=0, log_amount=4.615121 |
| **BV-19** | set B: số dư người gửi âm → từ chối | B | type="TRANSFER", amount=100.0, old_src=-5.0, old_dest=0.0 | từ chối `NEGATIVE_VALUE` (source_balance_before) |
| **BV-20** | step = 1 → hour_of_day = 0 (dòng đầu dataset) (dòng thật #0) | A+T | type="PAYMENT", amount=9839.64, step=1 | hợp lệ: type=PAYMENT, hour_of_day=0, log_amount=9.194276 |
| **BV-21** | step = 24 → hour_of_day = 23 (dòng thật #571039) | A+T | type="PAYMENT", amount=27003.49, step=24 | hợp lệ: type=PAYMENT, hour_of_day=23, log_amount=10.20376 |
| **BV-22** | step = 25 → hour_of_day = 0 (sang ngày thứ hai) (dòng thật #574255) | A+T | type="PAYMENT", amount=19161.18, step=25 | hợp lệ: type=PAYMENT, hour_of_day=0, log_amount=9.860694 |
| **BV-23** | step = 743 (dòng cuối dataset) → hour_of_day = 22 (dòng thật #6362619) | A+T | type="CASH_OUT", amount=850002.52, step=743 | hợp lệ: type=CASH_OUT, hour_of_day=22, log_amount=13.653 |
| **BV-24** | step = 744 (ngoài dataset nhưng hợp lệ trong stream) → hour_of_day = 23 | A+T | type="CASH_OUT", amount=100.0, step=744 | hợp lệ: type=CASH_OUT, hour_of_day=23, log_amount=4.615121 |
| **BV-25** | step = 0 → từ chối (step bắt đầu từ 1) | A+T | type="CASH_OUT", amount=100.0, step=0 | từ chối `OUT_OF_RANGE` (step) |
| **BV-26** | step âm → từ chối | A+T | type="CASH_OUT", amount=100.0, step=-5 | từ chối `OUT_OF_RANGE` (step) |
| **BV-27** | step = 1.5 → từ chối (cần số nguyên) | A+T | type="CASH_OUT", amount=100.0, step=1.5 | từ chối `BAD_TYPE` (step) |
| **BV-28** | step là chuỗi "5" → từ chối | A+T | type="CASH_OUT", amount=100.0, step="5" | từ chối `BAD_TYPE` (step) |
| **BV-29** | step là boolean true → từ chối | A+T | type="CASH_OUT", amount=100.0, step=true | từ chối `BAD_TYPE` (step) |

### 4.2 Ánh xạ `event_time` → `step` (EV)

| ID | Mục tiêu | Set | Đầu vào | Kết quả kỳ vọng |
|---|---|---|---|---|
| **EV-01** | event_time = T0 → step 1, hour 0 | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T00:00:00Z" | hợp lệ: type=TRANSFER, hour_of_day=0, log_amount=4.615121 |
| **EV-02** | T0 + 59 phút 59 giây vẫn thuộc step 1 | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T00:59:59Z" | hợp lệ: type=TRANSFER, hour_of_day=0, log_amount=4.615121 |
| **EV-03** | T0 + 1 giờ → step 2, hour 1 | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T01:00:00Z" | hợp lệ: type=TRANSFER, hour_of_day=1, log_amount=4.615121 |
| **EV-04** | T0 + 23:59:59 → step 24, hour 23 | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T23:59:59Z" | hợp lệ: type=TRANSFER, hour_of_day=23, log_amount=4.615121 |
| **EV-05** | T0 + 24 giờ → step 25, hour 0 | A+T | type="TRANSFER", amount=100.0, time="2026-10-12T00:00:00Z" | hợp lệ: type=TRANSFER, hour_of_day=0, log_amount=4.615121 |
| **EV-06** | cùng thời điểm với T0 nhưng viết theo UTC+7 → vẫn step 1, hour 0 | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T07:00:00+07:00" | hợp lệ: type=TRANSFER, hour_of_day=0, log_amount=4.615121 |
| **EV-07** | ví dụ trong project plan (10:05Z) → step 11, hour 10 | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T10:05:00Z" | hợp lệ: type=TRANSFER, hour_of_day=10, log_amount=4.615121 |
| **EV-08** | 10:05 giờ UTC+7 = 03:05Z → hour 3, không phải 10 (không dùng giờ địa phương) | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T10:05:00+07:00" | hợp lệ: type=TRANSFER, hour_of_day=3, log_amount=4.615121 |
| **EV-09** | event_time trước T0 → từ chối | A+T | type="TRANSFER", amount=100.0, time="2026-10-10T23:59:59Z" | từ chối `BAD_TIMESTAMP` (event_time) |
| **EV-10** | event_time không có múi giờ → từ chối | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T10:05:00" | từ chối `BAD_TIMESTAMP` (event_time) |
| **EV-11** | event_time không phải ISO-8601 → từ chối | A+T | type="TRANSFER", amount=100.0, time="not-a-date" | từ chối `BAD_TIMESTAMP` (event_time) |
| **EV-12** | event_time là số (epoch) → từ chối | A+T | type="TRANSFER", amount=100.0, time=1760177100 | từ chối `BAD_TYPE` (event_time) |
| **EV-13** | step và event_time cùng có mặt và khớp nhau → hợp lệ | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T10:05:00Z", step=11 | hợp lệ: type=TRANSFER, hour_of_day=10, log_amount=4.615121 |
| **EV-14** | step và event_time mâu thuẫn → từ chối | A+T | type="TRANSFER", amount=100.0, time="2026-10-11T10:05:00Z", step=12 | từ chối `INCONSISTENT_TIME` (step, event_time) |

### 4.3 Category lạ (UC)

| ID | Mục tiêu | Set | Đầu vào | Kết quả kỳ vọng |
|---|---|---|---|---|
| **UC-01** | transaction_type = CASH_IN → đúng một cột one-hot bằng 1 | A | type="CASH_IN", amount=100.0 | hợp lệ: type=CASH_IN, log_amount=4.615121 |
| **UC-02** | transaction_type = CASH_OUT → đúng một cột one-hot bằng 1 | A | type="CASH_OUT", amount=100.0 | hợp lệ: type=CASH_OUT, log_amount=4.615121 |
| **UC-03** | transaction_type = DEBIT → đúng một cột one-hot bằng 1 | A | type="DEBIT", amount=100.0 | hợp lệ: type=DEBIT, log_amount=4.615121 |
| **UC-04** | transaction_type = PAYMENT → đúng một cột one-hot bằng 1 | A | type="PAYMENT", amount=100.0 | hợp lệ: type=PAYMENT, log_amount=4.615121 |
| **UC-05** | transaction_type = TRANSFER → đúng một cột one-hot bằng 1 | A | type="TRANSFER", amount=100.0 | hợp lệ: type=TRANSFER, log_amount=4.615121 |
| **UC-06** | transaction_type = REFUND (ngoài 5 loại của PaySim) → từ chối | A | type="REFUND", amount=100.0 | từ chối `UNKNOWN_CATEGORY` (transaction_type) |
| **UC-07** | viết thường "transfer" → từ chối (so khớp chính xác, không tự chuẩn hóa) | A | type="transfer", amount=100.0 | từ chối `UNKNOWN_CATEGORY` (transaction_type) |
| **UC-08** | có khoảng trắng " TRANSFER" → từ chối | A | type=" TRANSFER", amount=100.0 | từ chối `UNKNOWN_CATEGORY` (transaction_type) |
| **UC-09** | chuỗi rỗng → từ chối | A | type="", amount=100.0 | từ chối `UNKNOWN_CATEGORY` (transaction_type) |
| **UC-10** | transaction_type là số 3 → từ chối (sai kiểu) | A | type=3, amount=100.0 | từ chối `BAD_TYPE` (transaction_type) |
| **UC-11** | destination_account có tiền tố lạ (X999) không làm đổi feature không trạng thái | A | type="TRANSFER", amount=100.0, dest="X999" | hợp lệ: type=TRANSFER, log_amount=4.615121 |

### 4.4 Thiếu trường (MF)

| ID | Mục tiêu | Set | Đầu vào | Kết quả kỳ vọng |
|---|---|---|---|---|
| **MF-01** | event rỗng → báo thiếu cả hai trường bắt buộc | A | {} (rỗng) | từ chối `MISSING_FIELD` (transaction_type, amount) |
| **MF-02** | thiếu amount | A | type="TRANSFER" | từ chối `MISSING_FIELD` (amount) |
| **MF-03** | thiếu transaction_type | A | amount=100.0 | từ chối `MISSING_FIELD` (transaction_type) |
| **MF-04** | amount = null được coi là thiếu | A | type="TRANSFER", amount=null | từ chối `MISSING_FIELD` (amount) |
| **MF-05** | set B thiếu source_balance_before → từ chối, tuyệt đối không điền 0 | B | type="TRANSFER", amount=100.0, old_dest=0.0 | từ chối `MISSING_FIELD` (source_balance_before) |
| **MF-06** | set B thiếu cả hai số dư | B | type="TRANSFER", amount=100.0 | từ chối `MISSING_FIELD` (source_balance_before, destination_balance_before) |
| **MF-07** | cùng event ở MF-05 nhưng dùng set A → hợp lệ (số dư không được dùng) | A | type="TRANSFER", amount=100.0, old_dest=0.0 | hợp lệ: type=TRANSFER, log_amount=4.615121 |
| **MF-08** | set A+T thiếu cả step lẫn event_time | A+T | type="TRANSFER", amount=100.0 | từ chối `MISSING_FIELD` (step, event_time) |
| **MF-09** | trường thừa vô hại (source) bị bỏ qua | A | type="TRANSFER", amount=100.0, source="paysim_replay" | hợp lệ: type=TRANSFER, log_amount=4.615121 |

### 4.5 Chốt chặn leakage (LG)

| ID | Mục tiêu | Set | Đầu vào | Kết quả kỳ vọng |
|---|---|---|---|---|
| **LG-01** | event chứa isFraud (kể cả giá trị 0) → từ chối | A | type="TRANSFER", amount=100.0, isFraud=0 | từ chối `FORBIDDEN_FIELD` (isFraud) |
| **LG-02** | event chứa isFlaggedFraud → từ chối | A | type="TRANSFER", amount=100.0, isFlaggedFraud=0 | từ chối `FORBIDDEN_FIELD` (isFlaggedFraud) |
| **LG-03** | event chứa newbalanceOrig (số dư sau giao dịch) → từ chối | A | type="TRANSFER", amount=100.0, newbalanceOrig=0.0 | từ chối `FORBIDDEN_FIELD` (newbalanceOrig) |
| **LG-04** | event chứa newbalanceDest → từ chối | A | type="TRANSFER", amount=100.0, newbalanceDest=100.0 | từ chối `FORBIDDEN_FIELD` (newbalanceDest) |
| **LG-05** | biến thể snake_case is_fraud → từ chối | A | type="TRANSFER", amount=100.0, is_fraud=1 | từ chối `FORBIDDEN_FIELD` (is_fraud) |
| **LG-06** | trường tên label → từ chối | A | type="TRANSFER", amount=100.0, label=1 | từ chối `FORBIDDEN_FIELD` (label) |
| **LG-07** | nhiều trường cấm cùng lúc → liệt kê đủ, sắp xếp theo tên | A | type="TRANSFER", amount=100.0, isFraud=1, isFlaggedFraud=0, newbalanceDest=100.0 | từ chối `FORBIDDEN_FIELD` (isFlaggedFraud, isFraud, newbalanceDest) |
| **LG-08** | số dư sau giao dịch đặt tên theo contract (destination_balance_after) → từ chối | A | type="TRANSFER", amount=100.0, destination_balance_after=100.0 | từ chối `FORBIDDEN_FIELD` (destination_balance_after) |

### 4.6 Cửa sổ lịch sử (HW)

`s{n}` là step n. Kết quả là bộ ba (`dest_txn_count_past_24h`, `dest_amount_sum_past_24h`, `dest_seen_before`).

| ID | Mục tiêu | Đầu vào: id@thời điểm (amount); sN = step N | Kết quả kỳ vọng (count, sum, seen_before) |
|---|---|---|---|
| **HW-01** | biên cửa sổ [s-24, s-1]: step 6 (cận dưới) tính, step 5 không; cùng step và tương lai không tính; thứ tự đến đảo lộn | p2@s29 (100.0); t@s30 (1000.0); p4@s31 (9.0); p0@s5 (1.0); p3@s30 (7.0); p1@s6 (10.0) | p0: (0, 0, 0); p1: (1, 1, 1); p2: (2, 11, 1); t: (2, 110, 1); p3: (2, 110, 1); p4: (3, 1107, 1) |
| **HW-02** | tài khoản nhận khác nhau không ảnh hưởng nhau | t@s30 →C100 (1000.0); x@s29 →C200 (5000.0) | t: (0, 0, 0); x: (0, 0, 0) |
| **HW-03** | transaction_id trùng (gửi lại cùng event) chỉ tính một lần | p2@s29 (100.0); p2@s29 (100.0); t@s30 (1000.0) | p2: (0, 0, 0); t: (1, 100, 1) |
| **HW-04** | dest_seen_before = 1 dù giao dịch trước nằm ngoài cửa sổ 24 giờ | p@s1 (50.0); t@s100 (10.0) | p: (0, 0, 0); t: (0, 0, 1) |
| **HW-05** | đầu dữ liệu: cận dưới cửa sổ s-24 < 1 không gây lỗi | a@s1 (3.0); b@s2 (4.0); t@s3 (10.0) | a: (0, 0, 0); b: (1, 3, 1); t: (2, 7, 1) |
| **HW-06** | tính từ event_time (không có step) cho cùng kết quả: 00:30 → step 1, 23:59:59 → step 24, ngày sau 01:00 → step 26 | p@10-11 00:30:00Z (10.0); t@10-11 23:59:59Z (20.0); t2@10-12 01:00:00Z (30.0) | p: (0, 0, 0); t: (1, 10, 1); t2: (1, 20, 1) |
| **HW-07** | thiếu destination_account → từ chối | t@s3 (1.0) | từ chối `MISSING_FIELD` (destination_account) |
| **HW-08** | thiếu transaction_id → từ chối | ?@s3 (1.0) | từ chối `MISSING_FIELD` (transaction_id) |
| **HW-09** | event lịch sử chứa trường cấm → từ chối | t@s3 (1.0) [isFraud] | từ chối `FORBIDDEN_FIELD` (isFraud) |
| **HW-10** | amount âm trong lịch sử → từ chối | t@s3 (-1.0) | từ chối `NEGATIVE_VALUE` (amount) |

### 4.7 Threshold: tạo alert (TH-01–15)

| ID | Mục tiêu | fraud_score | threshold | Kết quả kỳ vọng |
|---|---|---|---|---|
| **TH-01** | score = threshold → alert (>=) | 0.5 | 0.5 | alert |
| **TH-02** | score ngay dưới threshold → không alert | 0.4999999 | 0.5 | không alert |
| **TH-03** | score ngay trên threshold → alert | 0.5000001 | 0.5 | alert |
| **TH-04** | score = 1,0 → alert | 1.0 | 0.5 | alert |
| **TH-05** | score = 0,0, threshold 0,5 → không alert | 0.0 | 0.5 | không alert |
| **TH-06** | threshold = 0 → mọi score đều alert (suy biến) | 0.0 | 0.0 | alert |
| **TH-07** | threshold = 1 và score = 1 → alert | 1.0 | 1.0 | alert |
| **TH-08** | score thiếu (null) → từ chối | null | 0.5 | từ chối `MISSING_FIELD` (fraud_score) |
| **TH-09** | score NaN → từ chối | NaN | 0.5 | từ chối `NON_FINITE` (fraud_score) |
| **TH-10** | score = +Infinity → từ chối | +Infinity | 0.5 | từ chối `NON_FINITE` (fraud_score) |
| **TH-11** | score < 0 → từ chối | -0.1 | 0.5 | từ chối `OUT_OF_RANGE` (fraud_score) |
| **TH-12** | score > 1 → từ chối | 1.0000001 | 0.5 | từ chối `OUT_OF_RANGE` (fraud_score) |
| **TH-13** | score là chuỗi → từ chối | "0.9" | 0.5 | từ chối `BAD_TYPE` (fraud_score) |
| **TH-14** | score là boolean → từ chối | true | 0.5 | từ chối `BAD_TYPE` (fraud_score) |
| **TH-15** | threshold ngoài [0, 1] → từ chối | 0.5 | 1.5 | từ chối `OUT_OF_RANGE` (threshold) |

### 4.8 Threshold: chọn threshold trên validation (TH-20–31)

| ID | Mục tiêu | Đầu vào | Kết quả kỳ vọng |
|---|---|---|---|
| **TH-20** | tách hoàn toàn: chọn ngưỡng thấp nhất vẫn bắt đủ positive | y=[1, 1, 0, 0], scores=[0.9, 0.8, 0.2, 0.1]; min_recall=0.8 | threshold=0.8, precision=1, recall=1, alerts=2, nhánh `max_precision_at_min_recall` |
| **TH-21** | ràng buộc recall ≥ 0,8 buộc phải hạ ngưỡng xuống 0,3 | y=[1, 0, 1, 0, 1], scores=[0.9, 0.8, 0.7, 0.6, 0.3]; min_recall=0.8 | threshold=0.3, precision=0.6, recall=1, alerts=5, nhánh `max_precision_at_min_recall` |
| **TH-22** | mọi score bằng nhau: chỉ có một ứng viên, alert tất cả | y=[1, 0, 0, 1], scores=[0.5, 0.5, 0.5, 0.5]; min_recall=0.8 | threshold=0.5, precision=0.5, recall=1, alerts=4, nhánh `max_precision_at_min_recall` |
| **TH-23** | min_recall = 0,5: dừng ở ngưỡng cao nhất vẫn đạt recall | y=[1, 1, 0, 1, 0, 0, 0, 0], scores=[0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2]; min_recall=0.5 | threshold=0.8, precision=1, recall=0.6666667, alerts=2, nhánh `max_precision_at_min_recall` |
| **TH-24** | hòa precision (cùng bằng 1): chọn ngưỡng có recall cao hơn | y=[1, 1, 1, 0], scores=[0.9, 0.8, 0.7, 0.1]; min_recall=0.5 | threshold=0.7, precision=1, recall=1, alerts=3, nhánh `max_precision_at_min_recall` |
| **TH-25** | lỗ hổng của quy tắc W1: không đặt min_precision thì chọn ngưỡng thấp nhất (alert gần như tất cả, precision 9%) | 5 positive điểm cao (0.95–0.99), 100 negative điểm 0.504–0.9, 5 positive điểm thấp (0.05–0.4); min_recall=0.8 | threshold=0.05, precision=0.09090909, recall=1, alerts=110, nhánh `max_precision_at_min_recall` |
| **TH-26** | đặt min_precision = 0,5 mà không đạt → dự phòng chọn F2 cao nhất (recall 0,5, precision 1) | 5 positive điểm cao (0.95–0.99), 100 negative điểm 0.504–0.9, 5 positive điểm thấp (0.05–0.4); min_recall=0.8, min_precision=0.5 | threshold=0.95, precision=1, recall=0.5, alerts=5, nhánh `max_f2_fallback` |
| **TH-27** | không có mẫu dương → từ chối | y=[0, 0, 0], scores=[0.1, 0.2, 0.3]; min_recall=0.8 | từ chối (`ValueError`) |
| **TH-28** | score NaN → từ chối | y=[0, 1, 1], scores=[0.1, NaN, 0.3]; min_recall=0.8 | từ chối `NON_FINITE` (scores) |
| **TH-29** | nhãn ngoài {0,1} → từ chối | y=[0, 1, 2], scores=[0.1, 0.2, 0.3]; min_recall=0.8 | từ chối (`ValueError`) |
| **TH-30** | độ dài y và scores khác nhau → từ chối | y=[0, 1], scores=[0.1, 0.2, 0.3]; min_recall=0.8 | từ chối (`ValueError`) |
| **TH-31** | min_recall = 0 → từ chối | y=[0, 1], scores=[0.1, 0.2]; min_recall=0.0 | từ chối (`ValueError`) |

Hai ca TH-25 và TH-26 minh họa **lỗ hổng của quy tắc ở `evaluation-plan.md` v1**: không đặt `min_precision` thì luôn có ứng viên thỏa recall ≥ 0,80 (threshold nhỏ nhất, alert tất cả), nên nhánh F2 không bao giờ chạy. Trên điểm số thật của notebook (tập val, TC), quy tắc cũ cho set A chọn threshold 0,0009: alert 34,0% giao dịch TC để đạt recall 80,3%, precision 0,31%.

## 5. Mã lỗi

| Mã lỗi | Khi nào | Ca kiểm thử |
|---|---|---|
| `MISSING_FIELD` | Trường bắt buộc thiếu hoặc bằng null; báo đủ danh sách trường thiếu | MF-01, MF-02, MF-03, MF-04, MF-05, MF-06, MF-08, HW-07, HW-08, TH-08 |
| `BAD_TYPE` | Sai kiểu: chuỗi hoặc boolean ở chỗ cần số, số thực ở chỗ cần số nguyên (step), event_time không phải chuỗi | BV-10, BV-11, BV-27, BV-28, BV-29, EV-12, UC-10, TH-13, TH-14 |
| `NEGATIVE_VALUE` | `amount` hoặc số dư âm | BV-07, BV-19, HW-10 |
| `NON_FINITE` | NaN hoặc ±Infinity | BV-08, BV-09, TH-09, TH-10, TH-28 |
| `UNKNOWN_CATEGORY` | `transaction_type` ngoài 5 loại; so khớp chính xác, phân biệt hoa/thường, không cắt khoảng trắng, không tự chuẩn hóa | UC-06, UC-07, UC-08, UC-09 |
| `FORBIDDEN_FIELD` | Event chứa nhãn hoặc số dư sau giao dịch (`isFraud`, `isFlaggedFraud`, `newbalance*`, `label` và biến thể) | LG-01, LG-02, LG-03, LG-04, LG-05, LG-06, LG-07, LG-08, HW-09 |
| `BAD_TIMESTAMP` | `event_time` sai định dạng ISO-8601, thiếu múi giờ hoặc trước T0 | EV-09, EV-10, EV-11 |
| `INCONSISTENT_TIME` | `step` và `event_time` cùng có mặt nhưng cho hai giá trị step khác nhau | EV-14 |
| `OUT_OF_RANGE` | `step` < 1; `fraud_score` hoặc `threshold` ngoài [0, 1] | BV-25, BV-26, TH-11, TH-12, TH-15 |

## 6. Quyết định cần nhóm xác nhận (các ca phụ thuộc vào)

| Quyết định đề xuất | Người chốt | Ca phụ thuộc |
|---|---|---|
| So khớp category chính xác, không tự chuẩn hóa chữ hoa/khoảng trắng | Đức Anh, Công Anh | UC-07, UC-08, UC-09 |
| Số dư thiếu ở set B là lỗi, không điền 0 | Công Anh, Đạt | MF-05, MF-06 |
| `amount = 0` hợp lệ nhưng gắn cờ | Đức Anh (validator) | BV-01, BV-02 |
| T0 ở 00:00:00Z; `step` tính bằng số học UTC | Lead | EV-01–EV-14 |
| `drains_origin` dùng dung sai < 0,005 | Đạt | BV-13–BV-16 |
| Cửa sổ lịch sử theo step, cùng step không tính, trùng `transaction_id` tính một lần | Công Anh | HW-01, HW-03 |
| `alert_triggered = fraud_score >= threshold` | Lead, Đạt, Công Anh | TH-01–TH-07 |
| Thêm `min_precision` (hoặc ngân sách alert) vào quy tắc chọn threshold | Cả nhóm | TH-25, TH-26 |
| Từ chối event có trường cấm thay vì lặng lẽ bỏ qua trường | Đức Anh | LG-01–LG-08 |

Nếu nhóm quyết định khác, sửa đặc tả và fixture **cùng lúc** (một PR) để test luôn phản ánh quyết định hiện hành.

## 7. Kiểm thử cho L2–L4 (chưa tự động hóa)

| ID | Kịch bản | Cách kiểm tra | Kỳ vọng |
|---|---|---|---|
| X-01 | Nhất quán feature không trạng thái batch ↔ streaming | Cùng 12 dòng thật (và ca biên) đi qua cả hai đường; so từng cột | Sai khác 0 (tối đa 1e-9 tương đối) |
| X-02 | Nhất quán feature lịch sử khi có warm-up | Replay step 355–378 không chấm điểm, rồi giai đoạn test; so với batch tính trên toàn bộ lịch sử | `dest_txn_count_past_24h`, `dest_amount_sum_past_24h` giống hệt |
| X-03 | Khởi động nguội (không warm-up) | Chạy test như X-02 nhưng không replay trước giai đoạn test | Ghi lại số sai khác; tham chiếu đã đo: 38.589/184.520 dòng TC của 24 step đầu |
| X-04 | `dest_seen_before` khi không nạp state từ batch | Như X-03, đo ở cả giai đoạn test | Tham chiếu: sai ở 48,7% dòng TC; nếu không chấp nhận được thì loại feature khỏi mô hình triển khai |
| X-05 | Event đến sai thứ tự và trễ | Trộn thứ tự giao dịch trong cùng 24 step; thêm event trễ hơn watermark | Feature lịch sử giống batch với event trong watermark; event trễ hơn ghi log và không làm hỏng state |
| X-06 | Trùng `transaction_id` | Gửi hai lần cùng event, kể cả sau restart | Feature và đếm không đổi (khớp HW-03) |
| X-07 | Restart giữa chừng | Dừng/khởi động lại streaming app giữa giai đoạn replay | Có log phát hiện state; không lặng lẽ dùng 0 (mục 5.3) |
| X-08 | Event lỗi không làm dừng stream | Trộn các ca từ chối ở mục 4 vào stream hợp lệ | Quarantine có đúng mã lỗi, stream tiếp tục, số event hợp lệ không đổi |
| X-09 | Múi giờ phiên Spark | Đổi `spark.sql.session.timeZone` rồi chạy EV-06, EV-08 | `hour_of_day` không đổi |
| X-10 | `alert_triggered` nhất quán | Với N event đã chấm điểm, so `alert_triggered` với `fraud_score >= threshold` của `model_version` | 0 sai khác |

## 8. Tiêu chí nghiệm thu W2

- [x] Mỗi nhóm trong kế hoạch (boundary, unknown category, missing fields, threshold) có ca kiểm thử với đầu vào và kỳ vọng cụ thể (108 ca).
- [x] Giá trị kỳ vọng được kiểm chứng bằng mã chạy được (`178 passed in 0.32s`) và bộ test bắt được lỗi cố ý.
- [x] Các ca dùng dòng thật của PaySim có số dòng để truy ngược (12 dòng: 0, 2, 3, 15, 79, 4440, 571039, 574255, 2736447, 3686583, 4736180, 6362619).
- [ ] Đạt và Công Anh xác nhận định nghĩa feature và hợp đồng streaming (`feature-list.md` mục 7).
- [ ] L2 và L3 chạy lại các ca fixture trên pipeline thật (W3–W4).
