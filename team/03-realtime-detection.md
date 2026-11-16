# R3: Real-time Detection

> Dành cho: thành viên R3 và AI agent của họ. **Đọc `team/00-shared-context.md` trước.**
> Người dự phòng (backup): R2 (phần Flink), R5 (phần rule/model).

## 1. Sứ mệnh

Biến luồng `canonical.trades.v1` thành **cảnh báo có lý do trong vài giây**: tính feature theo cửa sổ thời gian, so với baseline, áp rule (sau đó là model), gom thành **case** (pump, rồi dump) và phát `alerts.v1`. Đây là "trái tim" của hệ thống. Chất lượng của R3 được đo trực tiếp ở báo cáo O6 (recall, precision, độ trễ trên các vụ pump có thật).

## 2. Phạm vi

**Sở hữu:**
- `streaming/detector/**`
- **Định nghĩa feature** `libs/common/src/cryptoguard_common/features.py` (đồng sở hữu với R4; R5 review)
- Danh mục reason code trong `docs/data-contracts.md` §3

**Phối hợp:**
- Contract `symbol_features_1m.v1`, `alert.v1`: R3 là producer, đổi cần R5 duyệt.
- Khóa Redis `baseline:*`, `wallet_risk:*`: R4 ghi, R3 đọc.
- Cấu hình `detector.*` trong `config/base.yaml`.

**Ngoài phạm vi:**
- Chuẩn hóa dữ liệu sàn (R2)
- Tính baseline lịch sử và wallet risk trong batch (R4)
- Train model (R5). R3 chỉ *tích hợp* model vào luồng.
- Lưu alert vào Mongo và API (R5)

## 3. Môi trường theo giai đoạn

| Phase | Phạm vi |
|---|---|
| P0–P3 (kind) | Watchlist ≤ 10 symbol, 2 sàn, cửa sổ 60 s; replay 1–3 vụ pump thật + vài ngày bình thường; parallelism 1 |
| P4 (GCP) | 50+ symbol, 4 sàn, cửa sổ 60 s + 300 s, cross-venue, model; tối ưu parallelism; đo throughput và latency |

Vòng phát triển: (1) notebook trên file binance.vision → (2) unit test hàm thuần → (3) PyFlink local mini-cluster đọc `localhost:32100` → (4) FlinkDeployment.

## 4. Đầu vào (Inputs)

| Input | Từ ai | Contract | Nếu chưa có |
|---|---|---|---|
| `canonical.trades.v1` (live) | R2 | `canonical_trade.v1` | `make seed-samples`; hoặc tự đọc file binance.vision bằng `cryptoguard_common.binance_vision` |
| `replay.canonical.trades.v1` | R2 (ING-07) | `canonical_trade.v1` | Như trên |
| Baseline volume theo symbol/exchange/giờ | R4 (BAT-05) → Redis `baseline:<symbol>:<exchange>:<hour>` | `docs/data-contracts.md` §4 | **Fallback DET-03**: EMA trong stream (ghi rõ trong alert evidence) |
| Wallet risk | R4 (BAT-09) → Redis `wallet_risk:<addr>` | `wallet_risk.v1` | Bỏ qua reason on-chain cho tới P3 |
| `canonical.onchain_transfers.v1` (P3) | R2 (ING-11) | `onchain_transfer.v1` | n/a |
| Model đã đăng ký (P4) | R5 (MLS-08/09) | MLflow `cg-pump-model` | Chỉ dùng rule |
| Danh sách vụ pump để hiệu chỉnh | R4 + R5 (`gold.pump_cases`) | bảng Iceberg | Danh sách tạm từ P0 (DET-01) |

## 5. Đầu ra (Outputs) và tiêu chí chấp nhận

> Ngưỡng số là **mục tiêu ban đầu**, chỉnh sau khi có kết quả replay đầu tiên (MLS-06).

| Output | Contract | Consumer | Tiêu chí chấp nhận |
|---|---|---|---|
| `features.symbol_1m.v1` | `symbol_features_1m.v1` | R5 (biểu đồ case), archive → R4 | Mỗi (symbol, exchange) và (symbol, `ALL`) có 1 bản ghi mỗi cửa sổ có giao dịch; giá trị **trùng khớp** với batch R4 trên cùng dữ liệu (parity test, sai số < 1e-9); `is_complete=false` khi có gap hoặc late |
| `alerts.v1` / `replay.alerts.v1` | `alert.v1` | R5 | Mọi alert có ≥ 1 reason có `code`, `value`, `threshold`; severity khớp risk; `case_id` gom pump + dump của cùng một đợt |
| Hiệu năng live (local) | n/a | O1, O8 | p95 (`detected_at_ts_us − window_end_ts_us`) **< 10 s**; không backpressure kéo dài với watchlist local |
| Chất lượng phát hiện (replay) | n/a | O6 | **MVP:** phát hiện ≥ 60% vụ pump trong tập đánh giá, trung vị độ trễ ≤ 3 phút sau thời điểm công bố, ≤ 5 alert sai/ngày/10 symbol trên ngày bình thường. P4: cải thiện bằng cross-venue và model, báo cáo so sánh |
| Late events | `dlq_record.v1` (`LATE_EVENT`) trên `dlq.detector.v1` | R4 (O7) | Có metric đếm; không làm crash job |

## 6. Task board

### P0: Khởi động
- [ ] **DET-01** Đọc bài báo đi kèm pump-and-dump dataset (La Morgia et al.). Cùng R4 và R5 chọn 3–10 vụ pump **có dữ liệu trên binance.vision**. Notebook (`ml/notebooks/`, strip output) tính feature 1 phút quanh 2–3 vụ, vẽ volume/giá, ước lượng ngưỡng ban đầu. Đề xuất thêm coin nhỏ vào watchlist local.
  - *Done khi:* có biểu đồ và bảng ngưỡng đề xuất; `config/base.yaml` `detector.pump_rule` được cập nhật qua PR.

### P1: Feature và baseline
- [ ] **DET-02** Hoàn thiện `features.py` (window features, z-score); thêm feature nếu cần (ví dụ số trade/giây, độ lệch qty). Unit test + đặc tả trong docstring. Phối hợp R4 để batch gọi cùng hàm.
- [ ] **DET-03** Chiến lược baseline: định dạng khóa Redis (đã có trong data-contracts §4), fallback EMA trong stream, xử lý coin mới chưa có baseline (không alert, `volume_z=null`).

### P2: Detector MVP
- [ ] **DET-04** PyFlink job (`detector/job.py`): event-time, watermark, keyBy (symbol, exchange) + nhánh `ALL`, tumbling 60 s, ProcessWindowFunction gọi `window_features`, đọc baseline Redis (có cache), ghi `features.symbol_1m.v1`.
- [ ] **DET-05** Rule `pump-v1` và `dump-v1` (đã có bản đầu trong `rules.py`): hiệu chỉnh trên dữ liệu DET-01, thêm test cho từng nhánh.
- [ ] **DET-06** Case: keyed state tương đương `CaseTracker`, TTL, đóng case; alert `DUMP_PHASE` gắn đúng case.
- [ ] **DET-07** Chế độ `--mode replay` (topic `replay.*` qua `contracts.replay_topic`), FlinkDeployment riêng `cg-detector-replay`; metric latency và alert count.

### P3: On-chain
- [ ] **DET-08** Reason `RISKY_INFLOW_TO_EXCHANGE`: theo dõi `canonical.onchain_transfers.v1` đi vào ví sàn (`exchange_wallet:*` trong Redis), tra `wallet_risk` của ví gửi, tổng USD rủi ro theo sàn trong N phút. Tín hiệu này ở **cấp sàn** (không biết coin nào), nên chỉ cộng nhẹ vào risk và ghi rõ trong description.

### P4: Nâng cao (cloud)
- [ ] **DET-09** Cross-venue divergence: so volume và giá cùng symbol giữa các sàn trong cùng cửa sổ.
- [ ] **DET-10** Wash-trading heuristics (entropy của qty, volume/biến động giá, nhịp mua bán đều đặn). Lưu ý: dữ liệu public không có ID tài khoản.
- [ ] **DET-11** Tích hợp model R5: nạp từ MLflow khi khởi động hoặc reload định kỳ, chấm điểm feature, reason `MODEL_SCORE_HIGH`, ghi `model_version`.
- [ ] **DET-12** (Tùy chọn) Spoofing từ depth stream (ING-13).
- [ ] **DET-13** Tối ưu trên cloud: parallelism, state backend, checkpoint; đo throughput tối đa bằng replay x100 cùng R1.

## 7. Quy tắc kỹ thuật riêng

- Rule là **hàm thuần, xác định, cấu hình được**. Không hard-code ngưỡng trong code.
- Mỗi rule có `version` (ví dụ `pump-v2`). Đổi logic thì tăng version, để báo cáo O6 so sánh được giữa các version.
- **Không bao giờ alert khi thiếu baseline** (trừ khi dùng fallback EMA và ghi rõ trong evidence).
- Feature cho batch và stream dùng **cùng một hàm**. Không viết lại công thức trong Flink.
- Live và replay chạy 2 deployment khác nhau, không dùng chung state.
- Mọi reason code mới phải được thêm vào `docs/data-contracts.md` §3 trong cùng PR.

## 8. Metrics phải expose

`cg_detector_windows_total{exchange}`, `cg_detector_alerts_total{alert_type,severity}`, `cg_detector_latency_ms` (histogram, live), `cg_detector_late_events_total`, `cg_detector_baseline_missing_total`, `cg_detector_redis_errors_total`.

## 9. Kiểm thử bắt buộc

- Unit: mọi nhánh của rule (đủ hoặc thiếu từng điều kiện, thiếu baseline, window không đầy đủ), CaseTracker (gộp, tách case, dump), `build_alert` validate `alert.v1`.
- Parity test với R4: cùng một file trade thì `window_features` từ batch và từ stream cho kết quả như nhau.
- Replay test (integration): 1 vụ pump thật cho ra ≥ 1 alert `PUMP_SUSPECTED` trong khoảng ±30 phút.

## 10. Cần tìm hiểu

Cơ chế pump-and-dump (Telegram group, coin thanh khoản thấp), event-time/watermark/allowed lateness, window và ProcessWindowFunction, keyed state + TTL + timers trong PyFlink, Redis client, z-score/EMA và mùa vụ theo giờ trong ngày, đánh giá với dữ liệu mất cân bằng.

## 11. Giao tiếp với các vai trò khác

| Khi nào | Với ai | Nội dung |
|---|---|---|
| DET-01 | R4, R5 | Danh sách vụ pump, coin nhỏ cho watchlist, ngưỡng ban đầu |
| DET-02 | R4 | Hàm feature chốt, cách gọi từ Spark (applyInPandas) |
| DET-04/05 xong | R5 | Topic `features.*` và `alerts.*` có dữ liệu thật; ví dụ alert |
| Sau mỗi lần replay | R5 | Kết quả O6, quyết định đổi rule (tăng version) |
| DET-08 | R4 | Khóa Redis `wallet_risk:*`, `exchange_wallet:*` |
| DET-11 | R5 | Giao diện model (input feature, output xác suất), cách reload |

## 12. Rủi ro và câu hỏi mở

- Vụ pump trong dataset là quá khứ, có thể coin đã bị delist khỏi Binance. Phải chọn sớm ở P0 (DET-01 + BAT-01).
- Coin nhỏ ít giao dịch nên volume_z nhiễu. Giải pháp: `min_trades`, sàn std, baseline theo giờ.
- PyFlink keyed state/timers trong Python có thể chậm. Giải pháp: đo sớm, giảm số key, fallback Spark (ADR-0003).

## 13. Prompt mẫu cho AI agent

> Tôi là R3 (Real-time Detection) của CryptoGuard. Đọc `AGENTS.md`, `team/00-shared-context.md`, `team/03-realtime-detection.md`, `contracts/schemas/symbol_features_1m.v1.schema.json`, `contracts/schemas/alert.v1.schema.json`, `libs/common/src/cryptoguard_common/features.py` và `streaming/detector/src/detector/`. Làm task **DET-04**: PyFlink job theo docstring trong `detector/job.py`, gọi `window_features` và `evaluate_pump` (không viết lại công thức), hỗ trợ `--mode live|replay`. Chỉ sửa trong `streaming/detector/` và `streaming/k8s/`. Viết test cho phần logic mới và hướng dẫn chạy replay 1 vụ pump.
