# Nguồn dữ liệu

## 1. Dataset chính: Credit Card Transactions Fraud Detection (Sparkov)

| Mục | Nội dung |
|---|---|
| Tên | Credit Card Transactions Fraud Detection Dataset |
| Nền tảng | Kaggle (tác giả đăng: kartik2112) |
| Loại dữ liệu | **Mô phỏng** (synthetic), giao dịch thẻ tín dụng hợp lệ và gian lận |
| Công cụ sinh dữ liệu | Sparkov Data Generation (tác giả Brandon Harris) |
| Khoảng thời gian | 01/01/2019 – 31/12/2020 |
| Quy mô mô tả | 1.000 khách hàng, 800 merchant |
| Tệp | `fraudTrain.csv`, `fraudTest.csv` |
| Số dòng | ~1,85 triệu (train + test) — **cần xác nhận sau khi tải** |
| Tỉ lệ gian lận | ~0,5% — **cần xác nhận bằng EDA** |
| Giấy phép | **Cần ghi lại từ trang Kaggle** |
| Link | _<dán link Kaggle của nhóm>_ |

## 2. Lý do chọn

- Có `cc_num` (định danh thẻ) và timestamp thật, cho phép tính historical/derived feature theo từng thẻ (số giao dịch trong N phút, khoảng cách tới merchant, chênh lệch thời gian giữa hai giao dịch).
- Có thể replay theo `trans_date_trans_time` thành luồng Kafka.
- Đủ lớn để dùng HDFS, Parquet, Spark; tỉ lệ gian lận thấp phù hợp đánh giá bằng PR-AUC và chỉnh ngưỡng.
- Feature dễ hiểu (số tiền, danh mục, vị trí), thuận lợi cho phân tích và viết báo cáo.

## 3. Hạn chế cần ghi trong báo cáo

- Dữ liệu mô phỏng: kết quả không thể suy ra trực tiếp cho dữ liệu ngân hàng thật.
- Chỉ có 1.000 khách hàng nên mỗi thẻ có rất nhiều giao dịch; đây là điểm thuận lợi cho feature lịch sử nhưng không giống phân bố thật.
- Các cột định danh cá nhân giả (`first`, `last`, `street`, `dob`, `trans_num`) không nên đưa vào mô hình.
- Cần kiểm tra trong EDA: `unix_time` có khớp `trans_date_trans_time` không, có giao dịch trùng không, phân bố `is_fraud` theo thời gian.

## 4. Cách tải và đặt file

```text
data/raw/fraudTrain.csv
data/raw/fraudTest.csv
```

Thư mục `data/` đã nằm trong `.gitignore`, không commit dữ liệu lên Git. Mỗi thành viên tự tải về (hoặc dùng Kaggle CLI).

## 5. Phương án dự phòng

- ULB Credit Card Fraud (Kaggle: mlg-ulb/creditcardfraud): 284.807 giao dịch, 492 gian lận (0,172%), dùng để thử nhanh pipeline vì nhỏ.
- PaySim: dùng nếu cần dữ liệu lớn hơn (lưu ý `step` chỉ là chỉ số giờ, không phải timestamp).
