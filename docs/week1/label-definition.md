# Label Definition — PaySim Fraud Detection

**Owner:** Thái (Data & ML Evaluation) · **Phiên bản:** v1 (W1) · **Trạng thái:** đề xuất, chờ nhóm chốt cuối W1
**Bằng chứng:** `notebooks/w1_eda_label_evaluation.ipynb` mục 2–3, `reports/eda_summary.json`

## 1. Nguồn dữ liệu

| Mục | Giá trị |
|---|---|
| Dataset | PaySim — *Synthetic Financial Datasets For Fraud Detection* (mô phỏng mobile money, 1/4 kích thước dữ liệu gốc trong bài báo PaySim) |
| Link nhóm dùng | https://www.kaggle.com/datasets/mtalaltariq/paysim-data |
| File phân tích | `PS_20174392719_1491204439457_log.csv` — 493.534.783 byte, 6.362.620 dòng, 11 cột |
| SHA-256 | `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b` |
| Ghi chú | Môi trường phân tích của Thái không truy cập được Kaggle; file được tải từ bản công khai cùng tên trên Google Cloud Storage (`gs://spls/gsp774/archive.zip`). **Đạt cần đối chiếu kích thước + SHA-256 với file tải từ link Kaggle của nhóm** và ghi vào data source note. |
| Bản quyền | Bản gốc trên Kaggle (`ealaxi/paysim1`) ghi license CC BY-SA 4.0 — cần kiểm tra lại trang `mtalaltariq/paysim-data`. |

## 2. Nhãn chính thức

| | Định nghĩa |
|---|---|
| **Cột nhãn** | `isFraud` (int, 0/1) |
| **Positive (1)** | Giao dịch do agent gian lận trong simulator thực hiện: chiếm quyền tài khoản khách hàng, chuyển tiền (TRANSFER) sang tài khoản khác rồi rút ra (CASH_OUT). |
| **Negative (0)** | Mọi giao dịch còn lại. |
| **Đơn vị dự đoán** | Một giao dịch (một dòng của file). |
| **Thời điểm dự đoán** | Ngay khi giao dịch đến, *trước* khi biết kết quả xử lý của giao dịch (không dùng thông tin sau giao dịch). |
| **Tỷ lệ positive** | 8.213 / 6.362.620 = **0,129%** (≈ 1 : 774) |

## 3. `isFlaggedFraud` không phải nhãn

Mô tả dataset: `isFlaggedFraud` là cờ của một **rule nghiệp vụ** trong simulator ("chặn chuyển khoản > 200.000 trong một giao dịch").

Kiểm tra bằng dữ liệu:

| Kiểm tra | Kết quả |
|---|---|
| Số dòng `isFlaggedFraud = 1` | 16 (đều là TRANSFER, đều `isFraud = 1`) |
| Số dòng thỏa rule mô tả (TRANSFER và amount > 200.000) | 409.110 — chỉ 16 dòng được flag |
| Precision / recall nếu dùng cờ làm dự đoán | 100% / **0,19%** |
| Giao dịch ngay sau mỗi TRANSFER bị flag | CASH_OUT gian lận với `amount = 0` (16/16 dòng amount = 0 của toàn dataset) |

**Quyết định đề xuất**
1. Không dùng `isFlaggedFraud` làm nhãn.
2. Không dùng `isFlaggedFraud` làm feature: cờ chỉ bật trên giao dịch gian lận và không tái tạo được từ rule mô tả, nên đưa vào model là leakage.
3. Giữ `isFlaggedFraud` làm **baseline rule** khi so sánh model (mục 9 notebook) và có thể hiển thị trên dashboard như "rule cũ".
4. 16 dòng `amount = 0` giữ nguyên (không tự xóa), ghi vào data-quality report và để Đạt/Đức Anh quyết định quy tắc validation.

## 4. Quy tắc sử dụng nhãn trong pipeline (chống leakage)

- **Batch training:** `isFraud` chỉ dùng làm cột label; mọi feature phải tính được từ thông tin có *tại hoặc trước* thời điểm giao dịch.
- **Transaction event (Kafka, input scoring):** **không chứa** `isFraud`, `isFlaggedFraud`, `newbalanceOrig`, `newbalanceDest`.
- **Evaluation path (streaming):** nhãn lưu ở bảng/file riêng `labels(transaction_id, isFraud)`, chỉ join với bảng prediction *sau khi* đã chấm điểm.
- **transaction_id:** PaySim không có ID giao dịch → đề xuất `transaction_id = "txn_" + số thứ tự dòng trong file gốc (0-based, 7 chữ số)`, ví dụ dòng đầu tiên → `txn_0000000`. ETL (Đạt) sinh cột này một lần và giữ nguyên ở mọi bước để join nhãn.
- **event_time:** `step` = 1 giờ mô phỏng (1–743, ≈ 31 ngày); đề xuất `event_time = T0 + (step − 1) giờ`, T0 do Lead chốt trong `decisions.md`. Dataset không có thời gian chi tiết hơn giờ — nếu cần, phân bổ đều trong giờ theo thứ tự dòng và ghi rõ là mô phỏng.

## 5. Đặc điểm nhãn cần biết khi đánh giá

- Gian lận **chỉ** xuất hiện ở `TRANSFER` (4.097) và `CASH_OUT` (4.116).
- Số ca gian lận gần như đều theo thời gian (~11 ca/giờ), trong khi giao dịch hợp lệ dao động theo ngày và gần như biến mất sau step ~400 → prevalence thay đổi mạnh theo thời gian (0,08% ở step 1–323, 0,44% ở step 379–743).
- Amount gian lận bị chặn trên ở 10.000.000 (287 ca).
- Dữ liệu là **mô phỏng**: kết quả model không đại diện trực tiếp cho hiệu quả trên giao dịch ngân hàng thật.

## 6. Việc cần nhóm xác nhận

- [ ] Lead: chốt `transaction_id`, `event_time`/T0 trong `decisions.md` và `interfaces.md`.
- [ ] Đạt: đối chiếu checksum file Kaggle; sinh `transaction_id` trong ETL.
- [ ] Công Anh: transaction event schema không chứa các cột ở mục 4.
- [ ] Đức Anh: thêm rule validation cho `amount = 0` và các cột bị cấm trong message.
