# Feature Inventory & Leakage/Availability Audit — v1 (W1)

> **Cập nhật W2:** định nghĩa để triển khai (công thức, null policy, feature set, trạng thái) nằm ở [`feature-list.md`](feature-list.md); nếu khác bản này thì `feature-list.md` thắng. Bản này giữ làm nhật ký rà soát W1. Đã đổi sau khi kiểm chứng trên dữ liệu thật: `dest_is_merchant` → **Loại**, `amount_is_round` → **Loại**, feature lịch sử theo tài khoản nhận đã được đo (HI1–HI3), set A chỉ còn `type` + `log_amount`.

**Owner:** Thái · **Đầu vào cho:** `feature-list.md` (W2), streaming input contract (Công Anh, W3), batch pipeline (Đạt, W3)
**Bằng chứng:** `notebooks/w1_eda_label_evaluation.ipynb` mục 4–8

Quy ước:
- **Có lúc scoring?** — giá trị có sẵn tại thời điểm giao dịch đến, trước khi biết kết quả giao dịch.
- **Leakage risk** — Thấp / Trung bình / Cao. "Artifact" = tín hiệu mạnh do cách simulator sinh dữ liệu, khó tổng quát hóa.
- **Đề xuất** — `Dùng` · `Dùng + ablation` (đưa vào feature set B, luôn báo cáo kèm set A không có nó) · `Loại` · `Chỉ để join/đánh giá`.

## 1. Cột gốc

| Cột | Ý nghĩa | Có lúc scoring? | Leakage risk | Đề xuất | Bằng chứng / ghi chú |
|---|---|---|---|---|---|
| `step` | Giờ mô phỏng (1–743) | Có | Trung bình | Loại (dạng thô) | Chỉ số thời gian tuyệt đối, không tổng quát cho tương lai; dùng để tạo `event_time` và split. |
| `type` | CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER | Có | Thấp | Dùng | Gian lận chỉ ở TRANSFER/CASH_OUT. |
| `amount` | Số tiền giao dịch | Có | Thấp | Dùng | Lệch phải mạnh; fraud bị chặn trên ở 10M (287 ca). |
| `nameOrig` | ID tài khoản gửi | Có | Thấp | Chỉ để join / tính lịch sử | 6.353.307 giá trị cho 6.362.620 dòng → không dùng trực tiếp; lịch sử theo tài khoản gửi gần như rỗng. |
| `nameDest` | ID tài khoản nhận (`C…` khách, `M…` merchant) | Có | Thấp | Chỉ để join / tính lịch sử | 2,72 triệu giá trị, tối đa 113 lần/tài khoản. |
| `oldbalanceOrg` | Số dư người gửi **trước** giao dịch | Có | **Cao (artifact)** | Dùng + ablation | `amount = oldbalanceOrg` đúng với 97,6% fraud và 0% legit (TRANSFER/CASH_OUT). Mô tả dataset khuyến cáo không dùng. |
| `newbalanceOrig` | Số dư người gửi **sau** giao dịch | **Không** | **Cao** | **Loại** | Post-transaction; 99,5% fraud khớp `old − amount` so với 9,5% legit. |
| `oldbalanceDest` | Số dư người nhận **trước** giao dịch | Có (nếu cùng hệ thống) | **Cao (artifact)** | Dùng + ablation | `old = new = 0` ở 49,6% fraud vs 0,06% legit; merchant luôn = 0. |
| `newbalanceDest` | Số dư người nhận **sau** giao dịch | **Không** | **Cao** | **Loại** | Post-transaction. |
| `isFraud` | Nhãn ground truth | Không | — | Chỉ là label | Xem `label-definition.md`. |
| `isFlaggedFraud` | Cờ rule của simulator | Không (output của rule) | **Cao** | **Loại** (chỉ làm baseline) | 16 dòng, 100% là fraud; không tái tạo được từ rule mô tả. |

## 2. Feature dẫn xuất ứng viên

| Nhóm | Feature | Cách tính | Có lúc scoring? | Leakage risk | Đề xuất |
|---|---|---|---|---|---|
| Transaction | `type_onehot` | One-hot / StringIndexer của `type` | Có | Thấp | Dùng |
| Transaction | `log_amount` | `log1p(amount)` | Có | Thấp | Dùng |
| Transaction | `is_tc` | `type ∈ {TRANSFER, CASH_OUT}` | Có | Thấp | Dùng (hoặc lọc tập huấn luyện) |
| Transaction | `amount_is_round` | amount chia hết cho 1.000 / = 10M | Có | Cao (artifact trần 10M) | **Loại** (W2: 3.207/3.271 dòng TC tròn nghìn là đúng 10.000.000) |
| Account | `dest_is_merchant` | `nameDest` bắt đầu bằng `M` | Có | Thấp | **Loại** (W2: ≡ `type = PAYMENT`, 0 dòng merchant trong TRANSFER/CASH_OUT) |
| Balance (set B) | `amount_to_balance_ratio` | `amount / (oldbalanceOrg + 1)` | Có | **Cao (artifact)** | Dùng + ablation |
| Balance (set B) | `drains_origin` | `amount == oldbalanceOrg` | Có | **Cao (artifact)** | Dùng + ablation |
| Balance (set B) | `dest_balance_zero` | `oldbalanceDest == 0` | Có | **Cao (artifact)** | Dùng + ablation |
| Balance | `error_balance_orig/dest` | `old − amount − new` (feature phổ biến trên Kaggle) | **Không** (dùng `new…`) | **Cao** | **Loại** |
| Time | `hour_of_day` | `(step − 1) mod 24` | Có | Trung bình (artifact) | Dùng + ablation — tỷ lệ fraud 22% lúc 3–4 giờ do giao dịch hợp lệ ban đêm rất ít. |
| Time | `day_index` | `(step − 1) div 24 + 1` | Có | Cao | Loại (không tổng quát cho tương lai) |
| Historical | `dest_txn_count_past_24h` | Số giao dịch tới `nameDest` trong 24 step trước | Có (cần state) | Trung bình | Cần xác minh (W2: có tín hiệu, xem `feature-list.md` HI1; báo cáo ablation A+H ở W3) |
| Historical | `dest_amount_sum_past_24h` | Tổng amount tới `nameDest` trong 24 step trước | Có (cần state) | Trung bình | Cần xác minh (W2: HI2) |
| Historical | `orig_txn_count_past` | Lịch sử tài khoản gửi | Có | Thấp | Loại — gần như mọi `nameOrig` chỉ xuất hiện 1 lần |
| Historical | `matching_transfer_same_step` | CASH_OUT có TRANSFER cùng amount trong cùng step | Có (cần state, phụ thuộc thứ tự) | **Cao (artifact)** | Không dùng ở MVP — 4.078/4.097 TRANSFER gian lận có cặp như vậy, chuỗi không nối được qua ID |

## 3. Feature set đề xuất cho W3

| Set | Thành phần | Mục đích |
|---|---|---|
| **A — core** | `type`, `log_amount` (+ `hour_of_day` ở biến thể A+T; W2 bỏ `dest_is_merchant`; thêm A+H với 3 feature lịch sử) | Mô hình "sạch", ít phụ thuộc artifact — dùng để báo cáo chính về khả năng tổng quát. |
| **B — pre-transaction balance** | A + `oldbalanceOrg`, `oldbalanceDest`, `amount_to_balance_ratio`, `drains_origin`, `dest_balance_zero` | Cho thấy tác động của artifact số dư; nếu B ≫ A thì phải ghi rõ trong báo cáo. |
| **Loại tuyệt đối** | `newbalanceOrig`, `newbalanceDest`, `error_balance_*`, `isFlaggedFraud`, `isFraud`, `step`/`day_index` thô, ID thô | Post-transaction, nhãn hoặc không tổng quát. |

## 4. Câu hỏi mở (chốt ở W2)

> W2: các câu hỏi dưới đây đã được đo trên dữ liệu thật và chuyển thành câu hỏi review có bằng chứng ở [`feature-list.md`](feature-list.md) mục 7 (đang chờ Đạt và Công Anh trả lời).

1. Streaming có cung cấp `oldbalanceOrg`/`oldbalanceDest` trong event không? (Ảnh hưởng tới việc chọn set A hay B cho model triển khai.)
2. Có huấn luyện riêng trên TRANSFER + CASH_OUT, các loại khác gán score = 0 bằng rule không?
3. State store của Structured Streaming có đủ cho feature lịch sử theo `nameDest` không (Công Anh)?
