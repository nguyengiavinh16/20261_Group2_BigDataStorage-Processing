# Feature List — v1 (W2)

**Owner:** Thái · **Trạng thái:** bản nháp, **chờ review** (Đạt: feature list và split; Công Anh: streaming contract) · **Phiên bản đặc tả:** `w2-v1`
**Bằng chứng:** `notebooks/w2_feature_draft_test_plan.ipynb` (Bước 1–3), `reports/feature_analysis_summary.json`
**Đặc tả chạy được:** `src/evaluation/feature_reference.py` · **Ca kiểm thử:** `docs/feature-test-plan.md` · **Báo cáo EDA:** `reports/eda-report-v1.md`

Tài liệu này là **định nghĩa để triển khai**. `docs/feature-inventory.md` (W1) giữ vai trò nhật ký rà soát; nếu hai tài liệu khác nhau thì tài liệu này thắng.

## 0. Thay đổi so với W1 (đọc trước)

1. **`dest_is_merchant` bị loại khỏi set A.** 2.151.495 dòng có tài khoản nhận là merchant đều là PAYMENT và có 0 dòng trong TRANSFER/CASH_OUT, nên feature đồng nhất với `type = PAYMENT` và không thêm thông tin nào. Set A chỉ còn `type` và `log_amount`.
2. **`amount_is_round` → Loại.** Trong 3.271 dòng TC có amount chia hết cho 1.000, 3.207 dòng bằng đúng 10.000.000. Đó là trần của simulator, không phải hành vi.
3. **Feature lịch sử theo tài khoản nhận có tín hiệu thật** (87,4% giao dịch gian lận tới tài khoản chưa có giao dịch nào trong 24 giờ trước, so với 44,1% giao dịch hợp lệ) → thêm set A+H, trạng thái **Cần xác minh**. Lịch sử theo tài khoản gửi và vai trò chéo bị loại vì không có tín hiệu.
4. **Mức tín hiệu của từng set (kiểm tra sơ bộ, không phải kết quả W3):** set A có AP 0,163 (val) / 0,270 (test), nhưng bỏ vùng amount sát trần 10M thì còn 0,078 / 0,117; set B đạt 1,000 / 1,000 do artifact số dư. W3 phải báo cáo đồng thời cả năm set.
5. **Hai phát hiện ảnh hưởng thiết kế streaming:** (a) event contract hiện **chưa có số dư**, nên set B cần trường mới; (b) streaming khởi động nguội làm lệch feature lịch sử so với batch, cần **warm-up** (mục 5.3).
6. **Quy tắc chọn threshold ở W1 có lỗ hổng:** hạ threshold xuống bằng score nhỏ nhất luôn cho recall = 1 ≥ 0,80, nên nhánh dự phòng F2 không bao giờ chạy. Đề xuất thêm `min_precision` (xem `docs/evaluation-plan.md` v1.1).

Tóm tắt trạng thái (21 mục): **Dùng** 3 · **Dùng + ablation** 6 · **Cần xác minh** 3 · **Loại** 9.

## 1. Quy ước

| Cột | Ý nghĩa |
|---|---|
| Trường event → biết lúc | Trường của transaction event cần có và thời điểm giá trị có sẵn lúc chấm điểm (trước khi biết kết quả giao dịch). |
| Null policy | Cách xử lý khi thiếu, sai kiểu hoặc giá trị lạ. Mã lỗi ở mục 5.1. |
| Leakage risk | Thấp / Trung bình / Cao. "Artifact" = tín hiệu mạnh do cách simulator sinh dữ liệu, khó tổng quát hóa sang dữ liệu thật. |
| Trạng thái | **Dùng** · **Dùng + ablation** (đưa vào mô hình nhưng luôn báo cáo kèm kết quả không có nó) · **Cần xác minh** (chưa đủ bằng chứng, báo cáo ablation ở W3) · **Loại**. |
| Set | Feature thuộc feature set nào ở mục 4. |

## 2. Ánh xạ trường: PaySim → transaction event → feature

| PaySim | Transaction event (project plan 7.1) | Ghi chú |
|---|---|---|
| `type` | `transaction_type` | Một trong CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| `amount` | `amount` | Số thực không âm, 2 chữ số thập phân |
| `nameOrig` | `source_account` | Không dùng làm feature |
| `nameDest` | `destination_account` | Khóa của feature lịch sử |
| `step` | `event_time` | `step = floor((event_time − T0) / 1 giờ) + 1`; T0 do Lead chốt |
| `oldbalanceOrg` | `source_balance_before` | **Trường mới, tùy chọn** (đề xuất), chỉ cần cho set B |
| `oldbalanceDest` | `destination_balance_before` | **Trường mới, tùy chọn** (đề xuất), chỉ cần cho set B |
| (không có) | `transaction_id` | `txn_` + số thứ tự dòng 7 chữ số (xem `label-definition.md` mục 4) |
| `newbalanceOrig`, `newbalanceDest`, `isFraud`, `isFlaggedFraud` | **Không được có trong event** | Gặp là từ chối (`FORBIDDEN_FIELD`) |

## 3. Danh sách feature

### 3.1 Transaction

| ID | Feature | Định nghĩa / công thức | Trường event → biết lúc | Null policy | Leakage risk | Trạng thái | Bằng chứng |
|---|---|---|---|---|---|---|---|
| **TX1** | `type_*` | 5 cột one-hot của transaction_type (type_CASH_IN … type_TRANSFER); thứ tự cố định CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER (Spark: StringIndexer stringOrderType="alphabetAsc") | transaction_type → Khi event đến | Thiếu/null → từ chối (MISSING_FIELD); giá trị lạ, khác hoa/thường hoặc có khoảng trắng → từ chối (UNKNOWN_CATEGORY); sai kiểu → BAD_TYPE | Thấp | **Dùng** | Gian lận chỉ ở TRANSFER (4.097; 0,77%) và CASH_OUT (4.116; 0,18%); 3 loại còn lại 0 |
| **TX2** | `log_amount` | ln(1 + amount) | amount → Khi event đến | Thiếu → từ chối; âm → NEGATIVE_VALUE; NaN/∞ → NON_FINITE; sai kiểu (chuỗi, boolean) → BAD_TYPE; = 0 hợp lệ và gắn cờ AMOUNT_ZERO | Thấp (có artifact trần 10M) | **Dùng** | 2.443 giao dịch hợp lệ > 10M, 0 gian lận (trần simulator); amount = 0 ở 16 dòng, đều gian lận |
| **TX3** | `is_tc` | transaction_type ∈ {TRANSFER, CASH_OUT}; dùng làm quy tắc phạm vi (loại khác: score = 0, không alert), không đưa vào model nếu huấn luyện riêng trên TC | transaction_type → Khi event đến | Theo TX1 | Thấp (dataset mô phỏng: ngoài TC không có gian lận) | **Dùng** | 100% gian lận nằm trong TC; TC có 2.770.409 dòng (43,5%) |
| **TX4** | `amount_is_round` | amount chia hết cho 1.000 | amount → Khi event đến | — | Cao (artifact trần 10M) | **Loại** | 3.271 dòng TC thỏa, trong đó 3.207 dòng đúng bằng 10.000.000 |

### 3.2 Account (gồm số dư)

| ID | Feature | Định nghĩa / công thức | Trường event → biết lúc | Null policy | Leakage risk | Trạng thái | Bằng chứng |
|---|---|---|---|---|---|---|---|
| **AC1** | `oldbalanceOrg` | số dư tài khoản gửi TRƯỚC giao dịch | source_balance_before (trường mới, tùy chọn) → Khi event đến, nếu event mang trường này | Thiếu → từ chối ở set B; tuyệt đối không điền 0 (0 mang nghĩa mạnh: gần như chắc chắn hợp lệ trong dữ liệu này); âm → NEGATIVE_VALUE | Cao (artifact) | **Dùng + ablation** | oldbalanceOrg = 0: 47,4% giao dịch hợp lệ vs 0,30% gian lận; AP đơn biến 98× prevalence |
| **AC2** | `oldbalanceDest` | số dư tài khoản nhận TRƯỚC giao dịch | destination_balance_before (trường mới, tùy chọn) → Khi event đến, nếu event mang trường này (cần hệ thống cùng biết số dư tài khoản nhận) | Như AC1 | Cao (artifact) | **Dùng + ablation** | oldbalanceDest = 0: 65,2% gian lận vs 13,9% hợp lệ |
| **AC3** | `amount_to_balance_ratio` | amount / (oldbalanceOrg + 1) | amount, source_balance_before → Khi event đến | Như AC1; mẫu số cộng 1 nên không chia cho 0 | Cao (artifact) | **Dùng + ablation** | trung vị = 1,00 (gian lận) vs 605,54 (hợp lệ) |
| **AC4** | `drains_origin` | amount > 0 và \|amount − oldbalanceOrg\| < 0,005 (rút sạch số dư) | amount, source_balance_before → Khi event đến | Như AC1; amount = 0 → 0 | Cao (artifact) | **Dùng + ablation** | 8.018 dòng, 100% gian lận (97,8% số ca); AP 0,976 |
| **AC5** | `dest_balance_zero` | oldbalanceDest = 0 | destination_balance_before → Khi event đến | Như AC2 | Cao (artifact) | **Dùng + ablation** | 65,2% gian lận vs 13,9% hợp lệ |
| **AC6** | `dest_is_merchant` | destination_account bắt đầu bằng M | destination_account → Khi event đến | — | Thấp | **Loại** | 2.151.495 dòng merchant đều là PAYMENT; 0 dòng trong TC → đồng nhất với type = PAYMENT |

### 3.3 Historical (theo `destination_account`)

| ID | Feature | Định nghĩa / công thức | Trường event → biết lúc | Null policy | Leakage risk | Trạng thái | Bằng chứng |
|---|---|---|---|---|---|---|---|
| **HI1** | `dest_txn_count_past_24h` | số giao dịch (mọi loại) tới cùng destination_account có step trong [s−24, s−1]; cùng step không tính | destination_account, event_time → Khi event đến; cần state theo tài khoản nhận | Chưa có lịch sử → 0 (giá trị hợp lệ, không phải null); state mất sau restart là lỗi vận hành, không được âm thầm dùng 0; transaction_id trùng chỉ tính một lần | Trung bình (chỉ dùng dữ liệu trước thời điểm dự đoán) | **Cần xác minh** | 87,4% gian lận vs 44,1% hợp lệ có 0 giao dịch; state tối đa 362.226 tài khoản; khởi động nguội lệch 20,9% dòng TC của 24 step đầu |
| **HI2** | `dest_amount_sum_past_24h` | tổng amount của các giao dịch trong HI1 | destination_account, amount, event_time → Như HI1 | Như HI1 (chưa có lịch sử → 0,0) | Trung bình | **Cần xác minh** | Cùng nhóm tín hiệu với HI1; tính bằng cent (số nguyên) để batch và streaming khớp chính xác |
| **HI3** | `dest_seen_before` | 1 nếu từng có giao dịch tới destination_account ở step < s (toàn lịch sử), ngược lại 0 | destination_account, event_time → Như HI1; state vĩnh viễn | Như HI1 | Trung bình (phụ thuộc điểm bắt đầu của lịch sử) | **Cần xác minh** | chưa từng thấy: 63,3% gian lận vs 15,1% hợp lệ; state 2.722.362 tài khoản; khởi động nguội sai 48,7% dòng TC của giai đoạn test |
| **HI4** | `orig_txn_count_past` | lịch sử theo tài khoản gửi | source_account → Cần state | — | Thấp | **Loại** | chỉ 9.298 tài khoản gửi xuất hiện > 1 lần (0,29% số dòng) |
| **HI5** | `dest_has_sent_before` | tài khoản nhận từng là tài khoản gửi ở step trước | source_account, destination_account → Cần state | — | Thấp | **Loại** | 0,12% gian lận vs 0,16% hợp lệ: không có tín hiệu |
| **HI6** | `matching_transfer_same_step` | CASH_OUT có TRANSFER cùng amount trong cùng step | amount, event_time → Cần state, phụ thuộc thứ tự trong step | — | Cao (artifact) | **Loại** | W1: 4.078/4.097 TRANSFER gian lận có cặp như vậy; kết quả phụ thuộc thứ tự trong cùng step nên batch và streaming không nhất quán |

### 3.4 Time

| ID | Feature | Định nghĩa / công thức | Trường event → biết lúc | Null policy | Leakage risk | Trạng thái | Bằng chứng |
|---|---|---|---|---|---|---|---|
| **TM1** | `hour_of_day` | (step − 1) mod 24; step = floor((event_time − T0) / 1 giờ) + 1 tính bằng số học UTC | event_time (hoặc step ở batch) → Khi event đến | Thiếu cả step lẫn event_time → từ chối; event_time phải có múi giờ, trước T0 hoặc không phải ISO-8601 → BAD_TIMESTAMP; step và event_time mâu thuẫn → INCONSISTENT_TIME | Trung bình (artifact) | **Dùng + ablation** | gian lận phẳng 3,3%–4,6% mỗi giờ; tỷ lệ 0,11%–57,9% do giao dịch hợp lệ ban đêm co lại |
| **TM2** | `day_index, step` | ngày thứ mấy; chỉ số giờ tuyệt đối (step thô) | event_time → Khi event đến | — | Cao (không tổng quát cho tương lai) | **Loại** | Phân phối giao dịch hợp lệ đổi mạnh theo ngày (xem Bước 3); chỉ dùng step để tạo event_time và split |

### 3.5 Loại tuyệt đối (không bao giờ đưa vào feature hoặc event)

| ID | Feature | Định nghĩa / công thức | Trường event → biết lúc | Null policy | Leakage risk | Trạng thái | Bằng chứng |
|---|---|---|---|---|---|---|---|
| **EX1** | `newbalanceOrig, newbalanceDest, error_balance_*` | số dư SAU giao dịch và các feature suy ra | (không được có trong event) → Chỉ biết sau khi giao dịch xử lý xong | Event có trường này → từ chối (FORBIDDEN_FIELD) | Cao | **Loại** | W1: 99,5% gian lận khớp old − amount = new so với 9,5% hợp lệ |
| **EX2** | `isFlaggedFraud` | cờ của rule trong simulator | (không được có trong event) → Là đầu ra của một rule | Event có trường này → từ chối | Cao | **Loại** | W1: 16 dòng, 100% là gian lận, recall 0,19% |
| **EX3** | `isFraud, is_fraud, label` | nhãn ground truth và các biến thể tên | (không được có trong event) → Chỉ có ở đường đánh giá | Event có trường này → từ chối | — | **Loại** | Tách khỏi scoring input theo label-definition.md mục 4 |

## 4. Feature set cho W3 (ma trận ablation)

| Set | Thành phần | Cần gì ở event / streaming | AP val | AP test | Lệch giữa 2 cấu hình |
|---|---|---|---|---|---|
| **A** (core) | 5 cột `type_*`, `log_amount` | `transaction_type`, `amount` | 0,163 | 0,270 | 0,001 |
| **A+T** | A + `hour_of_day` | + `event_time` (T0 do Lead chốt) | 0,326 | 0,417 | 0,009 |
| **A+H** | A + `dest_txn_count_past_24h`, `dest_amount_sum_past_24h`, `dest_seen_before` | + `destination_account`, `event_time`; state theo tài khoản nhận (tối đa 362.226 tài khoản cho cửa sổ 24 step, 2.722.362 cho `dest_seen_before`); warm-up | 0,182 | 0,434 | 0,002 |
| **A+H+T** | A+H + `hour_of_day` | Như A+H | 0,357 | 0,492 | 0,031 |
| **B** (số dư trước giao dịch) | A + `oldbalanceOrg`, `oldbalanceDest`, `amount_to_balance_ratio`, `drains_origin`, `dest_balance_zero` | + `source_balance_before`, `destination_balance_before` (**trường mới**, chưa có trong event contract) | 1,000 | 1,000 | 0,000 |

AP ở bảng trên là **kiểm tra sơ bộ** của Thái (HistGradientBoosting có regularization, huấn luyện trên TC của step 1–323; val = step 324–378, test = step 379–743; prevalence TC của val 0,13%, test 1,03%). Cột cuối là chênh lệch AP lớn nhất giữa cấu hình chính và một cấu hình phụ (nông hơn, learning rate nhỏ hơn). **Đây không phải kết quả W3**: chưa tune, chưa dùng MLlib, chưa chọn threshold. Con số nhạy với cấu hình: với early stopping mặc định, AP val của set A chỉ 0,008, còn cấu hình không regularization làm xác suất bão hòa (1,5% dòng val cùng mang score tối đa nên bị hòa điểm) và set B chỉ đạt 0,006.

## 5. Quy tắc chung

### 5.1 Đầu vào không hợp lệ và null policy

Mọi lỗi dưới đây dẫn tới **từ chối event** (đưa vào quarantine kèm mã lỗi, có thống kê theo mã), không bao giờ điền giá trị mặc định thay cho giá trị thiếu. Đặc biệt không điền 0 cho số dư: `oldbalanceOrg = 0` gần như chắc chắn là giao dịch hợp lệ trong dữ liệu này (47,4% giao dịch hợp lệ so với 0,30% gian lận), nên điền 0 sẽ âm thầm đẩy score xuống.

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

Ngoại lệ có chủ đích: `amount = 0` **hợp lệ** nhưng gắn cờ `AMOUNT_ZERO` (16 dòng trong PaySim, đều là CASH_OUT gian lận ngay sau một TRANSFER bị flag).

Quy tắc kiểm tra theo thứ tự: trường cấm → trường thiếu → kiểu → giá trị. Giá trị phân loại so khớp chính xác (không `upper()`, không `strip()`): producer replay luôn gửi đúng 5 giá trị viết hoa, nên mọi sai khác là lỗi cần nhìn thấy chứ không phải lỗi cần che.

### 5.2 Ánh xạ thời gian

- `step = floor((event_time − T0) / 1 giờ) + 1`, tính bằng **số học UTC** (không dùng `hour()` của Spark theo múi giờ phiên, không dùng giờ địa phương). `event_time` bắt buộc có múi giờ và không được trước T0.
- `hour_of_day = (step − 1) mod 24`. Nó bằng giờ UTC của `event_time` khi và chỉ khi T0 nằm ở 00:00:00Z. Đề xuất Lead chọn T0 như vậy.
- Dữ liệu chỉ có độ phân giải theo giờ. Nếu replay gán phút/giây mô phỏng trong giờ thì phần đó **không** dùng cho feature (mọi giao dịch cùng giờ cùng một `step`).
- Batch (có cột `step`) và streaming (có `event_time`) phải cho cùng `step`; khi event mang cả hai mà mâu thuẫn thì từ chối (`INCONSISTENT_TIME`).

### 5.3 Feature lịch sử

- **Cửa sổ:** giao dịch tới cùng `destination_account` có step trong [s−24, s−1]. Giao dịch cùng step **không** tính cho nhau để kết quả không phụ thuộc thứ tự đến.
- **Phạm vi:** tính trên mọi loại giao dịch (state ở streaming cũng chứa mọi event); `transaction_id` trùng chỉ tính một lần; tổng tiền cộng bằng cent (số nguyên) để batch và streaming khớp chính xác.
- **Chưa có lịch sử** → 0 (giá trị hợp lệ, không phải null). **State mất sau restart** là lỗi vận hành: không được âm thầm dùng 0, phải ghi log và coi kết quả là không đáng tin cho tới khi state được nạp lại.
- **Warm-up bắt buộc.** Nếu replay giai đoạn test bắt đầu tại step 379 với state rỗng thì `dest_txn_count_past_24h` sai ở 38.589/184.520 dòng TC của 24 step đầu (20,9%; 23/298 dòng gian lận). Cách xử lý: replay step 355–378 để nạp state mà không chấm điểm.
- **`dest_seen_before` không chữa được bằng warm-up 24 step:** state rỗng làm nó sai ở 48,7% dòng TC của cả giai đoạn test. Nạp state từ batch layer (kiến trúc Lambda) hoặc bỏ feature này khỏi mô hình triển khai.
- **Dung lượng state:** cửa sổ 24 step cần tối đa 362.226 tài khoản nhận (tại step 417; trung vị 55.730); `dest_seen_before` cần giữ toàn bộ 2.722.362 tài khoản.

### 5.4 Mã hóa và thứ tự feature

- `transaction_type` → 5 cột one-hot theo thứ tự cố định `CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER` (đủ 5 cột, không bỏ cột nào ở mức đặc tả). Spark `StringIndexer` mặc định sắp theo tần suất nên phải đặt `stringOrderType="alphabetAsc"`.
- Thứ tự cột của vector feature là một phần của đặc tả (`feature_reference.CORE_FEATURES`, `TIME_FEATURES`, `BALANCE_FEATURES`, `HISTORY_FEATURES`); test kiểm tra thứ tự này.
- `fraud_score` nằm trong [0, 1]; `alert_triggered = fraud_score >= threshold`; không coi `fraud_score` là xác suất đã hiệu chuẩn nếu chưa kiểm tra.

### 5.5 Quy tắc chống leakage

1. Feature chỉ dùng thông tin có tại hoặc trước thời điểm giao dịch đến.
2. Nhãn và các cột số dư sau giao dịch không bao giờ nằm trong event vào scoring (`FORBIDDEN_FIELD`).
3. Với feature lịch sử, chỉ dùng giao dịch ở step nhỏ hơn step hiện tại.
4. Mọi kết quả dùng nhóm số dư (set B) hoặc `hour_of_day` phải kèm kết quả set A tương ứng.

## 6. Split và quần thể đánh giá

Giữ **split theo thời gian** của W1 làm protocol chính: train step 1–323, val 324–378, test 379–743. Phương án phụ: stratified random (seed 42), **không dùng với feature lịch sử** vì cho phép dữ liệu tương lai rò vào feature.

| Split | Step | Số dòng (P1) | Gian lận | Prevalence P1 | Số dòng TC (P2) | Prevalence P2 |
|---|---|---|---|---|---|---|
| train | 1–323 | 4.463.587 | 3.643 | 0,082% | 1.951.895 | 0,187% |
| val | 324–378 | 980.416 | 564 | 0,058% | 427.991 | 0,132% |
| test | 379–743 | 918.617 | 4.006 | 0,436% | 390.523 | 1,026% |

- Prevalence đổi mạnh theo thời gian: giao dịch hợp lệ khoảng 349.478–573.984 mỗi ngày ở ngày 1–2 và 6–17 (ngày 3–5 gần như không có), còn 8.298–57.613 mỗi ngày ở ngày 18–30 và 0 ở ngày 31; gian lận đều 216–320 ca mỗi ngày. Mọi precision đọc kèm prevalence của đúng split.
- **Quần thể đánh giá:** báo cáo chính trên **P1** (toàn bộ giao dịch, như hệ thống chấm điểm), báo cáo phụ trên **P2** (chỉ TC). Nếu giao dịch ngoài TC được gán score thấp hơn mọi score của TC thì PR-AUC của P1 và P2 bằng nhau; chỉ prevalence dùng làm mốc so sánh khác nhau (P1 thấp hơn 2,3 lần). Mọi bảng số phải ghi rõ quần thể.
- Streaming replay giai đoạn test chia thành 16 cửa sổ 24 step; cửa sổ cuối (chỉ 5 step) không có giao dịch hợp lệ nào nên precision theo cửa sổ vô nghĩa ở đó.

## 7. Trạng thái review và câu hỏi mở

Buổi review với Đạt (feature list) và Công Anh (streaming contract) **chưa diễn ra**. Các câu hỏi dưới đây gom những điểm cần họ trả lời; Thái cập nhật cột trạng thái và các quyết định vào `decisions.md` sau buổi review.

| # | Người trả lời | Câu hỏi | Căn cứ | Trạng thái |
|---|---|---|---|---|
| 1 | Đạt | Dùng 5 feature set A, A+T, A+H, A+H+T, B làm ma trận ablation ở W3? Đặt `stringOrderType="alphabetAsc"` cho `transaction_type` để batch và streaming cùng thứ tự one-hot? | Mục 4; `feature_reference.TRANSACTION_TYPES` | Chờ phản hồi |
| 2 | Đạt | Huấn luyện chỉ trên TRANSFER + CASH_OUT (loại khác gán score thấp nhất, không alert) hay trên mọi loại giao dịch? | 100% gian lận nằm trong TC; PR-AUC không đổi nếu ngoài TC có score thấp hơn mọi score của TC (mục 6) | Chờ phản hồi |
| 3 | Đạt | Tính feature lịch sử một lần trên toàn bộ dòng đã sắp theo thời gian rồi mới chia split; Parquet processed giữ kèm `transaction_id` và `step`? | Feature chỉ dùng dữ liệu trước thời điểm dự đoán nên không leak; split ngẫu nhiên không dùng được với feature lịch sử | Chờ phản hồi |
| 4 | Đạt | Khi huấn luyện ở W3 đặt rõ số vòng và regularization, không dựa vào early stopping mặc định? | HistGradientBoosting mặc định dừng sau 11 vòng: AP val của A chỉ 0,008 so với 0,163 khi huấn luyện đủ | Chờ phản hồi |
| 5 | Đạt, Lead | Chốt `alert_triggered = fraud_score >= threshold` và bổ sung `min_precision` (hoặc ngân sách alert) cho quy tắc chọn threshold? | Nhánh F2 của quy tắc W1 không bao giờ chạy (TH-25, TH-26; `evaluation-plan.md` v1.1) | Chờ phản hồi |
| 6 | Công Anh, Lead | Thêm hai trường tùy chọn `source_balance_before`, `destination_balance_before` vào transaction event? Nếu không, mô hình triển khai chỉ có thể là A, A+T hoặc A+H. | Event contract ở project plan mục 7.1 không có số dư | Chờ phản hồi |
| 7 | Công Anh | State Structured Streaming có đủ cho tối đa 362.226 tài khoản nhận (cửa sổ 24 step) và 2.722.362 tài khoản (`dest_seen_before`)? | Notebook mục 1.3, hình `fig6_history_features.png` | Chờ phản hồi |
| 8 | Công Anh | Warm-up: replay step 355–378 (không chấm điểm) trước giai đoạn test để nạp state; riêng `dest_seen_before` cần nạp state từ batch layer hoặc bỏ khỏi mô hình triển khai? | State rỗng làm `dest_txn_count_past_24h` sai ở 38.589/184.520 dòng TC của 24 step đầu (20,9%); `dest_seen_before` sai ở 48,7% dòng TC của cả giai đoạn test | Chờ phản hồi |
| 9 | Công Anh, Lead | `hour_of_day` tính bằng số học UTC từ `event_time` (không dùng `hour()` theo múi giờ phiên Spark); T0 = 00:00:00Z để `hour_of_day` trùng giờ UTC? | EV-06, EV-08 | Chờ phản hồi |
| 10 | Công Anh | Cửa sổ lịch sử theo step (giờ): giao dịch cùng step không tính cho nhau; `transaction_id` trùng chỉ tính một lần? | HW-01, HW-03 | Chờ phản hồi |
| 11 | Công Anh | Gán `risk_level` theo `fraud_score` thế nào? Đề xuất: `HIGH` khi `alert_triggered`, đơn điệu theo score. | Project plan mục 7.2 chưa định nghĩa các mức | Chờ phản hồi |
| 12 | Đức Anh | Validator dùng chung bộ mã lỗi ở mục 5.1 và chạy `tests/fixtures/feature_cases.json` làm test dữ liệu lỗi? | `docs/feature-test-plan.md` | Chờ phản hồi |

## 8. Giới hạn

- Dữ liệu là mô phỏng: nhiều tín hiệu (329× prevalence cho `drains_origin`, mô hình theo giờ, trần 10M) là đặc tính của simulator và không đại diện cho giao dịch ngân hàng thật.
- AP ở mục 4 chỉ là kiểm tra sơ bộ, nhạy với cấu hình và không thay thế đánh giá ở W3.
- `src/evaluation/feature_reference.py` là đặc tả để đối chiếu, không phải pipeline production và chưa tối ưu hiệu năng.
- Danh sách trường event mới (`source_balance_before`, `destination_balance_before`), T0 và mã lỗi là **đề xuất**, cần Lead chốt trong `interfaces.md` và `decisions.md`.
