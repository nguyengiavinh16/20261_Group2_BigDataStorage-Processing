# Evaluation Report — <model_version>

> Template theo `docs/evaluation-plan.md`. Điền mọi ô `<…>`; không xóa mục, ghi "N/A + lý do" nếu không áp dụng.

## 0. Thông tin truy vết

| Mục | Giá trị |
|---|---|
| Model / version | `<baseline_lr_v1>` |
| Feature set | `<A / A+time / B>` — danh sách feature: `<…>` |
| Dataset + SHA-256 | PaySim `PS_20174392719_1491204439457_log.csv` · `<sha256>` |
| Split | `<time-based: train 1–323 / val 324–378 / test 379–743>` hoặc `<stratified random, seed=42>` |
| Prevalence (train / val / test) | `<0,082% / 0,058% / 0,436%>` |
| Class imbalance handling | `<none / class weights / undersampling …>` |
| Threshold | `<giá trị>` — quy tắc chọn: `<precision max với recall ≥ 0,80 trên val>` |
| Predictions file | `<reports/predictions/<model_version>_<split>.parquet>` |
| Script / notebook + commit | `<path>` @ `<commit hash>` |
| Người chạy / ngày | `<…>` |

## 1. So sánh model (threshold-independent)

| Model | Feature set | PR-AUC val | PR-AUC test | ROC-AUC test (tham khảo) |
|---|---|---|---|---|
| Dự đoán tất cả hợp lệ | — | = prevalence | = prevalence | 0,5 |
| `isFlaggedFraud` | — | | | |
| Logistic Regression | A | | | |
| `<Random Forest>` | A | | | |
| `<Random Forest>` | B | | | |

## 2. Kết quả tại threshold đã chọn (test, đánh giá một lần)

| Model | Threshold | Precision | Recall | F1 | Recall theo tiền | Số alert |
|---|---|---|---|---|---|---|
| | | | | | | |

**Confusion matrix — `<model>` @ `<threshold>` (test)**

| | Dự đoán 0 | Dự đoán 1 |
|---|---|---|
| **Thực tế 0** | TN = | FP = |
| **Thực tế 1** | FN = | TP = |

## 3. Threshold trade-off (val)

| Threshold | Số alert | Precision | Recall | F1 | F2 |
|---|---|---|---|---|---|
| | | | | | |

Hình: `<reports/figures/pr_curve_<model_version>.png>`

## 4. Ablation số dư (set A vs set B)

| Model | PR-AUC A | PR-AUC B | Chênh lệch | Nhận xét |
|---|---|---|---|---|
| | | | | |

## 5. Phân tích lỗi

- FP/FN theo `type`: `<bảng>`
- FP/FN theo khoảng amount: `<bảng>`
- FP/FN theo `hour_of_day`: `<bảng>`
- Ví dụ FN amount lớn / FP score cao: `<link file>`

## 6. Giới hạn

- Dữ liệu PaySim là mô phỏng; kết quả không đại diện trực tiếp cho giao dịch ngân hàng thật.
- `fraud_score` `<đã / chưa>` kiểm tra calibration.
- `<giới hạn khác: prevalence shift giữa split, artifact số dư, …>`

## 7. Kết luận & đề xuất

- Model đề xuất triển khai: `<…>` vì `<…>`
- Việc tiếp theo: `<…>`
