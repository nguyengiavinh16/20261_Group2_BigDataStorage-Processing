# R5: ML, Serving & Evaluation

> Dành cho: thành viên R5 và AI agent của họ. **Đọc `team/00-shared-context.md` trước.**
> Người dự phòng (backup): R3 (model), R4 (đánh giá, dữ liệu Gold).

## 1. Sứ mệnh

Biến kết quả của hệ thống thành **thứ người dùng nhìn thấy và giảng viên chấm được**:
- API và dashboard: feed cảnh báo, trang case, trang ví (O1–O5).
- **Khung đánh giá trung thực** trên các vụ pump có thật (O6), câu trả lời cho câu hỏi "hệ thống có thực sự hoạt động không?".
- Ở P4: model ML trong MLflow để cải thiện rule của R3.

R5 cũng là người tổng hợp báo cáo cuối cùng.

## 2. Phạm vi

**Sở hữu:**
- `ml/**`: evaluation, training, notebooks
- `serving/**`: sink, API, dashboard, Mongo schema
- Định nghĩa protocol đánh giá (`ml/README.md`, `cg_ml.evaluation.metrics`)

**Phối hợp:**
- Contract `alert.v1`, `symbol_features_1m.v1`: R5 là consumer chính, duyệt mọi thay đổi.
- `features.py`: review.
- Superset/Grafana dashboards dữ liệu (R1 cài đặt, R5 thiết kế nội dung O5/O7).

**Ngoài phạm vi:**
- Tính feature real-time và rule (R3)
- Bảng Gold và wallet risk (R4)
- Hạ tầng MongoDB/MLflow (R1 cài, R5 dùng)

## 3. Môi trường theo giai đoạn

| Phase | Phạm vi |
|---|---|
| P0–P3 (kind) | Mongo + API chạy local; dashboard đọc dữ liệu seed/replay; đánh giá trên 3–10 vụ pump replay ở local |
| P4 (GCP) | API/dashboard deploy trong cluster; train model trên Gold 3–6 tháng (node Spot); đánh giá đầy đủ, so sánh rule và model |

## 4. Đầu vào (Inputs)

| Input | Từ ai | Contract | Nếu chưa có |
|---|---|---|---|
| `alerts.v1`, `replay.alerts.v1` | R3 | `alert.v1` | `make seed-samples`; tự sinh alert giả theo schema |
| `features.symbol_1m.v1` | R3 | `symbol_features_1m.v1` | Sample |
| `gold.pump_cases` | R4 (BAT-06) | DDL | Danh sách tạm từ P0 (`pump_events_candidates.csv`) |
| `gold.symbol_features_1m`, `silver.labels` | R4 | DDL, `label.v1` | Notebook trên file binance.vision |
| `wallet_risk` (Mongo) | R4 (BAT-09) | `wallet_risk.v1` | Sample `wallet_risk.valid.jsonl` |
| Replay run (`replay_run_id`) | R2 (ING-07) + R3 (DET-07) | n/a | Chạy evaluation trên alert giả để kiểm tra code |
| MongoDB, MLflow, Grafana/Superset | R1 | n/a | Mongo local qua `make up-serving` |

## 5. Đầu ra (Outputs) và tiêu chí chấp nhận

| Output | Contract | Người dùng | Tiêu chí chấp nhận |
|---|---|---|---|
| Sink Kafka → Mongo | collections trong `mongo_schema.py` | API | Validate từng message (sai thì log và đếm, không crash); upsert idempotent (`_id=alert_id`); case được upsert (status, max_risk, alert_count); replay vào `alerts_replay`, **không lẫn** với live |
| API (`serving/src/cg_serving/api.py`) | Trả document theo contract | dashboard, giảng viên | Mọi route trong skeleton hoạt động; p95 < 300 ms trên dữ liệu local; phân trang bằng cursor; OpenAPI docs ở `/docs` |
| O1 Feed cảnh báo | WS `/ws/alerts` | người xem | Alert mới hiện trên UI ≤ 2 s sau khi vào Mongo; lọc theo loại, severity, symbol |
| O2 Trang case | `/cases/{id}` | người điều tra | Biểu đồ giá/volume ±30 phút, bảng reasons, các alert liên quan, mở < 2 s |
| O3 Trang/API ví | `/wallets/{addr}/risk` | AML | Hiển thị risk, đường tới ví bẩn gần nhất, nguồn nhãn |
| O5 Dashboard thị trường | Superset hoặc Grafana | tổng quan | Top coin bất thường, heatmap volume_z theo giờ, so sánh sàn (P4) |
| **O6 Báo cáo đánh giá** | `cg_ml.evaluation.metrics` | giảng viên | Cho từng rule/model version: recall, precision, precision@10, trung vị độ trễ, alert sai/ngày; ghi rõ cửa sổ match, danh sách vụ, `replay_run_id`; kết quả lưu vào MLflow |
| Model (P4) | MLflow `cg-pump-model` | R3 (DET-11) | Split theo **sự kiện/thời gian** (không random theo dòng); so với rule trên cùng tập; có giải thích (feature importance/SHAP) |
| O7 hiển thị | dashboard | báo cáo | Đọc `gold.data_quality_daily`, biểu đồ theo nguồn và ngày |

## 6. Task board

### P0: Khởi động
- [ ] **MLS-01** Viết protocol đánh giá chi tiết (`ml/README.md`): chọn vụ pump (cùng R3, R4), số ngày bình thường đối chứng, cửa sổ match, metric. Lập `pump_events_candidates.csv` cùng BAT-01.
  - *Done khi:* cả nhóm đồng ý protocol (ghi trong PR).
- [ ] **MLS-02** Thiết kế API và wireframe dashboard (3 trang + link Grafana); quyết định stack dashboard (ADR ngắn). Xem lại mock dashboard W1 trong `_backup/` để tái dùng ý tưởng.

### P1: Serving nền
- [ ] **MLS-03** Mongo schema/index (`python -m cg_serving.mongo_schema`), handler `/alerts`, `/alerts/{id}` đọc Mongo, test với dữ liệu seed.

### P2: MVP end-to-end
- [ ] **MLS-04** Sink Kafka → Mongo (`--mode live|replay`), case upsert, TTL cho features.
- [ ] **MLS-05** Dashboard v1: feed qua WebSocket, trang case với biểu đồ; route `/cases`, `/symbols/{s}/features`.
- [ ] **MLS-06** Chạy evaluation end-to-end lần đầu: replay (R2) → detector replay (R3) → `alerts_replay` → `metrics.evaluate` → báo cáo v0 (markdown + MLflow). Gửi kết quả cho R3 để chỉnh rule.

### P3: Ví
- [ ] **MLS-07** `/wallets/{addr}/risk` + trang ví (đường tới ví bẩn). (Tùy chọn) `/wallets/{addr}/graph` từ `gold.wallet_edges` (qua Trino) hoặc Neo4j.

### P4: Model, báo cáo, cloud
- [ ] **MLS-08** Train IsolationForest (baseline không giám sát) và XGBoost (có nhãn, class weight) trên Gold, split theo sự kiện; log MLflow; đăng ký `cg-pump-model`.
- [ ] **MLS-09** Cùng R3 tích hợp model (DET-11): định nghĩa input (danh sách feature, thứ tự) và output; test rằng điểm số offline và online khớp nhau.
- [ ] **MLS-10** Báo cáo O6 cuối cùng (rule v1 so với rule v2 so với model), dashboard O5 và O7, tổng hợp báo cáo và slide của nhóm.

## 7. Quy tắc kỹ thuật riêng

- API trả **đúng shape contract** (bỏ `_id`), thời gian giữ `*_ts_us`; UI tự format.
- Không tính lại feature trong API. Dùng dữ liệu đã có (Mongo hoặc Gold).
- Evaluation dùng **event-time**, không dùng wall clock, để kết quả không phụ thuộc tốc độ replay.
- Split train/test theo sự kiện hoặc thời gian; báo cáo luôn kèm `replay_run_id`, version rule/model và tham số match. Không "chọn kết quả đẹp".
- Notebook phải strip output trước khi commit; dữ liệu không nằm trong git.

## 8. Metrics phải expose

`cg_sink_messages_total{topic,result}`, `cg_sink_invalid_total{topic}`, `cg_api_requests_total{route,status}`, `cg_api_latency_ms{route}` (histogram), `cg_ws_clients`.

## 9. Kiểm thử bắt buộc

- Unit: `metrics.py` (đã có), mapping message → Mongo document, case upsert logic, API với Mongo giả hoặc mongomock.
- Contract: document trả về từ API validate được với `alert.v1` / `wallet_risk.v1` (sau khi bỏ `_id`).
- Integration: seed → sink → API trả đúng số alert.

## 10. Cần tìm hiểu

FastAPI (+ WebSocket), MongoDB (index, change streams, TTL), Kafka consumer (commit offset), đánh giá phát hiện sự kiện (event-based precision/recall, độ trễ), xử lý mất cân bằng lớp, IsolationForest/XGBoost, MLflow tracking và registry, Superset/Grafana.

## 11. Giao tiếp với các vai trò khác

| Khi nào | Với ai | Nội dung |
|---|---|---|
| MLS-01 | R3, R4 | Protocol đánh giá, danh sách vụ pump |
| MLS-03/04 | R3 | Alert thật có hiển thị đúng không, reason nào thiếu thông tin |
| MLS-06 (mỗi lần) | R3 | Kết quả O6, alert sai điển hình |
| MLS-07 | R4 | Shape `wallet_risk` trong Mongo, nhu cầu truy vấn graph |
| MLS-09 | R3 | Giao diện model |
| P5 | Tất cả | Thu thập phần báo cáo của từng người |

## 12. Rủi ro và câu hỏi mở

- Quá ít vụ pump nên metric dao động mạnh: báo cáo kèm số lượng và khoảng tin cậy, bổ sung nhãn bán tự động có ghi chú.
- Dashboard tốn thời gian: ưu tiên chức năng (feed + case) trước thẩm mỹ.
- **Hỏi giảng viên:** demo bằng replay có được tính là streaming không? (Luồng live vẫn chạy song song.)

## 13. Prompt mẫu cho AI agent

> Tôi là R5 (ML, Serving & Evaluation) của CryptoGuard. Đọc `AGENTS.md`, `team/00-shared-context.md`, `team/05-ml-serving-evaluation.md`, `contracts/schemas/alert.v1.schema.json`, `serving/src/cg_serving/` và `ml/src/cg_ml/evaluation/metrics.py`. Làm task **MLS-04**: sink Kafka → Mongo theo docstring trong `sink.py` (live và replay, validate contract, upsert idempotent, case upsert). Chỉ sửa trong `serving/`. Viết unit test cho logic chuyển alert → document và case upsert, và hướng dẫn chạy với `make seed-samples`.
