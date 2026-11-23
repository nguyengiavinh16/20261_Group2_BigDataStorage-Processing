# Evaluation Plan — v1.1 (W1, cập nhật W2)

**Owner:** Thái · **Áp dụng cho:** W3 (batch model evaluation), W5–W6 (streaming evaluation), W7–W8 (báo cáo)
**Bằng chứng:** `notebooks/w1_eda_label_evaluation.ipynb` mục 9–10; v1.1: `notebooks/w2_feature_draft_test_plan.ipynb`, `reports/eda-report-v1.md`

**Thay đổi ở v1.1 (W2)**
1. §3: thêm *quần thể đánh giá* (P1 toàn bộ giao dịch, P2 chỉ TRANSFER + CASH_OUT); mọi bảng số ghi rõ quần thể.
2. §4: split theo thời gian được kiểm tra lại trên dữ liệu thật và **giữ nguyên**; bổ sung prevalence trong TC và đặc điểm theo ngày.
3. §5: quy tắc chọn threshold thêm `min_precision` (nhánh F2 của v1 không bao giờ chạy); chốt quy ước `alert_triggered = fraud_score >= threshold`.
4. §6, §10: ablation đủ 5 feature set A, A+T, A+H, A+H+T, B (`feature-list.md` mục 4).
5. §8: replay giai đoạn test cần warm-up 24 step để feature lịch sử khớp batch.

## 1. Bài toán

Phân loại nhị phân từng giao dịch: `isFraud ∈ {0,1}`; model trả về `fraud_score` (càng cao càng rủi ro). Một **threshold** biến score thành cảnh báo (`alert_triggered`). Positive class chỉ chiếm **0,129%**, nên mục tiêu là bắt được nhiều gian lận nhất với số cảnh báo sai chấp nhận được.

## 2. Vì sao accuracy không đủ (số liệu thật trên PaySim)

| "Model" (rule, không huấn luyện) | Accuracy | Precision | Recall | PR-AUC | Số alert |
|---|---|---|---|---|---|
| Dự đoán tất cả hợp lệ | **99,87%** | 0 | **0** | 0,0013 | 0 |
| `isFlaggedFraud` (rule có sẵn) | 99,87% | 100% | 0,19% | 0,0032 | 16 |
| `type ∈ {TRANSFER, CASH_OUT}` | 56,59% | 0,30% | 100% | 0,0030 | 2.770.409 |
| `amount = oldbalanceOrg` (artifact) | 99,997% | 100% | 97,6% | 0,976 | 8.018 |

- Model "không làm gì" đã có accuracy 99,87% nhưng recall = 0 → accuracy bị chi phối bởi lớp đa số.
- Accuracy gần như không đổi (99,87% → 99,997%) trong khi recall đi từ 0 đến 97,6% → không dùng accuracy để chọn model hay threshold.
- Dòng cuối cho thấy một phép so sánh trên cột số dư gần như "giải" bài toán → mọi kết quả rất cao phải đi kèm ablation (xem `feature-inventory.md`, set A vs B).

## 3. Bộ metrics

| Loại | Metric | Dùng để | Ghi chú |
|---|---|---|---|
| **Chính (chọn model)** | **PR-AUC** (Average Precision) | So sánh model, không phụ thuộc threshold | Baseline ngẫu nhiên = prevalence của split → luôn báo cáo kèm prevalence. |
| Chính (tại threshold) | **Precision**, **Recall**, **F1** | Đánh giá điểm vận hành | Precision = tỷ lệ alert đúng; Recall = tỷ lệ fraud bị bắt. |
| Chính (tại threshold) | **Confusion matrix** (TP, FP, FN, TN) | Hiểu số alert thật/sai tuyệt đối | Luôn ghi số tuyệt đối, không chỉ %. |
| Phụ | F2 | Khi ưu tiên recall hơn precision | Chọn nếu nhóm thống nhất "bỏ sót fraud đắt hơn báo nhầm". |
| Phụ | Recall theo giá trị tiền (`fraud_amount_recall`) | % số tiền gian lận bị chặn | Gần với góc nhìn thiệt hại. |
| Phụ | Alert rate, Precision@k / Recall@k | Khi có ngân sách xử lý alert (vd. top 0,5% giao dịch) | Hữu ích cho dashboard. |
| Tham khảo | ROC-AUC | So sánh với tài liệu khác | Dễ lạc quan khi mất cân bằng mạnh — không dùng làm tiêu chí chính. |
| **Không dùng** | Accuracy (làm tiêu chí) | — | Chỉ ghi để minh họa. |

**Nhất quán cách tính:** Spark `BinaryClassificationEvaluator(metricName="areaUnderPR")` và `sklearn.average_precision_score` tính PR-AUC theo cách khác nhau (nội suy hình thang vs. tổng bậc thang) nên có thể lệch nhẹ. Số liệu đưa vào báo cáo cuối được tính lại từ **file predictions đã lưu** (`transaction_id, fraud_score, isFraud, split, model_version`) bằng **một** script đánh giá duy nhất.

**Quần thể đánh giá (v1.1).** Báo cáo chính trên **P1** = mọi giao dịch (đúng như hệ thống chấm điểm), báo cáo phụ trên **P2** = chỉ TRANSFER + CASH_OUT. Nếu giao dịch ngoài TC nhận score thấp hơn mọi score của TC thì PR-AUC, precision và recall tại một threshold của P1 và P2 **bằng nhau**; chỉ prevalence dùng làm mốc so sánh khác nhau (0,129% ở P1 so với 0,296% ở P2). Mỗi bảng số ghi rõ quần thể.

## 4. Protocol chia dữ liệu

**Chính — split theo thời gian (~70/15/15 theo số dòng):**

| Split | Step | Số dòng | Fraud | Prevalence |
|---|---|---|---|---|
| train | 1–323 | 4.463.587 | 3.643 | 0,082% |
| val | 324–378 | 980.416 | 564 | 0,058% |
| test | 379–743 | 918.617 | 4.006 | 0,436% |

- Lý do: khớp kịch bản hệ thống (học quá khứ → chấm điểm tương lai qua Kafka replay), tránh dùng thông tin tương lai, nhất là với feature lịch sử.
- Rủi ro đã biết: prevalence ở test cao hơn train ~5 lần và test chứa ~49% số ca gian lận (giao dịch hợp lệ thưa dần cuối tháng). Precision trên test sẽ cao hơn val một phần chỉ vì prevalence → luôn so sánh PR-AUC với prevalence của chính split đó.
- **Phụ — stratified random split 70/15/15 (seed cố định = 42)**: báo cáo song song để xem kết quả có ổn định không. Không dùng split ngẫu nhiên nếu có feature lịch sử.
- Kafka replay cho streaming evaluation dùng **dữ liệu của test period** (step 379–743), không replay dữ liệu đã dùng để train.
- **Kiểm tra lại ở W2 (giữ nguyên split):** prevalence trong TC là 0,187% (train), 0,132% (val), 1,026% (test). Giao dịch hợp lệ 349.478–573.984 mỗi ngày ở ngày 1–2 và 6–17 (ngày 3–5 gần như không có), 8.298–57.613 mỗi ngày ở ngày 18–30 và 0 ở ngày 31; gian lận đều 216–320 ca mỗi ngày. Giai đoạn test chia thành 16 cửa sổ 24 step; cửa sổ cuối (chỉ 5 step) không có giao dịch hợp lệ nên precision theo cửa sổ vô nghĩa ở đó. Chi tiết: `reports/eda-report-v1.md` mục 3.

## 5. Chọn threshold

1. Train trên train, tính score trên val.
2. Vẽ đường precision–recall trên val; chọn threshold theo quy tắc đã chốt **trước khi nhìn test**. Đề xuất mặc định (v1.1): *threshold cho precision cao nhất với điều kiện recall ≥ `min_recall` (0,80) **và** precision ≥ `min_precision`*; nếu không có threshold nào thỏa → chọn threshold có F2 cao nhất. (Nhóm có thể thay `min_recall`, `min_precision` hoặc dùng ngân sách alert thay cho `min_precision`, nhưng phải ghi vào `decisions.md`.)
   - **Lý do sửa v1:** nếu không có `min_precision`, luôn tồn tại ứng viên thỏa (threshold bằng score nhỏ nhất: alert tất cả, recall = 1 ≥ 0,80) nên nhánh F2 của v1 không bao giờ chạy và quy tắc có thể chọn ngưỡng cho alert một phần rất lớn giao dịch. Trên điểm số thật của feature set A (tập val, TC): quy tắc v1 chọn threshold 0,0009, alert 34,0% giao dịch TC để đạt recall 80,3% với precision 0,31%; thêm `min_precision` = 0,05 (**giá trị minh họa, nhóm chốt**) chuyển sang nhánh F2: threshold 0,0345, 287 alert, precision 34,5%, recall 17,6%. Ca kiểm thử: TH-25, TH-26 trong `docs/feature-test-plan.md`; cài đặt tham chiếu: `src/evaluation/feature_reference.select_threshold`.
3. Cố định threshold, đánh giá **một lần** trên test; ghi threshold vào `model_version`/model card.
4. Báo cáo bảng trade-off: số alert, precision, recall tại 3–5 threshold quanh điểm chọn.
5. `fraud_score` **không** được coi là xác suất đã hiệu chuẩn trừ khi đã kiểm tra calibration (reliability curve) — đặc biệt khi dùng class weight hoặc undersampling.
6. **Quy ước alert (v1.1):** `alert_triggered = fraud_score >= threshold` (bằng ngưỡng vẫn là alert). Ứng viên threshold là các giá trị score khác nhau trong val; hòa precision thì chọn recall cao hơn, rồi threshold cao hơn. `fraud_score` và `threshold` nằm trong [0, 1].

## 6. Baselines bắt buộc trong bảng so sánh W3

| Baseline | Vai trò |
|---|---|
| Dự đoán tất cả hợp lệ | Mốc accuracy "vô nghĩa" |
| `isFlaggedFraud` | Rule có sẵn của hệ thống |
| Logistic Regression (feature set A; thêm A+T, A+H, A+H+T, B để ablation) | Baseline ML |
| Random Forest / model ứng viên (đủ 5 set A, A+T, A+H, A+H+T, B; xem `feature-list.md` mục 4) | Ứng viên chính + ablation số dư, giờ trong ngày và feature lịch sử |

## 7. Phân tích lỗi (W3)

- FP/FN theo `type`, theo khoảng amount (log-bins), theo `hour_of_day`, theo split.
- Liệt kê 20 FN có amount lớn nhất và 20 FP có score cao nhất để review.
- Ghi rõ: dữ liệu là mô phỏng; kết quả không đại diện trực tiếp cho hiệu quả ngân hàng thật.

## 8. Streaming evaluation (W5–W6)

- Scoring input không chứa nhãn; nhãn nằm ở bảng `labels(transaction_id, isFraud)` và chỉ join ở evaluation job.
- Metrics theo cửa sổ thời gian (vd. mỗi 24 step mô phỏng) khi đủ fraud để có ý nghĩa; ghi số fraud mỗi cửa sổ.
- Kiểm tra cùng `transaction_id` cho cùng score ở batch và streaming (sai số cho phép do float ghi rõ).
- Dashboard chỉ hiển thị metric có định nghĩa ở tài liệu này.
- **Warm-up (v1.1):** trước giai đoạn test phải replay step 355–378 để nạp state cho feature lịch sử; các event warm-up không chấm điểm và không tính vào metrics. Nếu bỏ warm-up, `dest_txn_count_past_24h` sai ở 38.589/184.520 dòng TC của 24 step đầu (20,9%), và `dest_seen_before` sai ở 48,7% dòng TC của cả giai đoạn test (cần nạp state từ batch layer hoặc bỏ feature này). Xem `feature-list.md` mục 5.3.

## 9. Quy tắc báo cáo (áp dụng cho mọi bảng số)

Mỗi con số metrics phải ghi kèm: `model_version` · feature set (A/B) · split + khoảng step · prevalence · threshold · script/notebook + commit tạo ra số đó. Template: `reports/templates/evaluation-report-template.md`.

## 10. Tiêu chí nghiệm thu W3 (đề xuất)

- [ ] Có bảng so sánh đủ baseline ở mục 6, trên cùng split.
- [ ] PR-AUC, precision, recall, F1, confusion matrix trên val và test.
- [ ] Threshold chọn trên val theo quy tắc mục 5, có bảng trade-off.
- [ ] Có kết quả ablation đủ 5 set A, A+T, A+H, A+H+T, B; phần tín hiệu đến từ artifact (số dư, trần 10M, giờ trong ngày) được nêu rõ.
- [ ] Threshold chọn theo quy tắc v1.1 (có `min_precision` hoặc ngân sách alert do nhóm chốt) và ghi `alert_triggered = fraud_score >= threshold` vào `decisions.md`.
- [ ] Mọi số liệu truy ngược được về predictions file + commit.
