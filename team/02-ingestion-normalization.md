# R2: Ingestion & Stream Normalization

> Dành cho: thành viên R2 và AI agent của họ. **Đọc `team/00-shared-context.md` trước.**
> Người dự phòng (backup): R3.

## 1. Sứ mệnh

Đưa dữ liệu **real-time từ các sàn và từ Ethereum** vào Kafka một cách bền bỉ: tự reconnect, không âm thầm mất dữ liệu, có đo đếm. Sau đó **chuẩn hóa dữ liệu bẩn và khác schema** thành một ngôn ngữ chung (`canonical_trade.v1`) mà R3 và R4 tin dùng được. R2 cũng làm **công cụ replay**, nền tảng của toàn bộ phần đánh giá, và **lưu trữ mọi topic xuống Bronze**.

Đây là nơi chứng minh rõ nhất tiêu chí "dữ liệu bẩn, nhiều nguồn": lệch schema giữa các sàn, trùng bản ghi khi reconnect, mất gói, đơn vị thời gian khác nhau, ngữ nghĩa `side` ngược nhau.

## 2. Phạm vi

**Sở hữu:** `ingest/**` (collectors, replay, archiver), `streaming/normalizer/**`, `docs/exchanges.md` (tạo ở ING-01), các sample thô tại `contracts/samples/raw_*` (nếu thêm).

**Phối hợp:**
- `contracts/schemas/raw_envelope.v1`, `canonical_trade.v1`, `onchain_transfer.v1`: R2 là producer, đổi cần R3 và R4 duyệt.
- `contracts/reference/symbol_map.csv`: R4 sở hữu, R2 đề xuất thêm dòng.
- `libs/common/timeutil.py`, `binance_vision.py`: dùng chung, sửa cần review.

**Ngoài phạm vi:**
- Tính feature và phát hiện gian lận (R3)
- Bảng Silver/Gold (R4). R2 chỉ ghi Bronze.
- Hạ tầng Kafka (R1). R2 chỉ thiết kế topic qua contract.

## 3. Môi trường theo giai đoạn

| Phase | Phạm vi dữ liệu |
|---|---|
| P0–P3 (kind) | 2 sàn (Binance, OKX), watchlist ≤ 10 symbol (`config/local.yaml`), replay vài ngày quanh 1–3 vụ pump; ETH: chỉ USDT/USDC/WETH Transfer logs |
| P4 (GCP) | 4 sàn, 50+ symbol, replay x100 để test tải, thêm depth stream nếu kịp |

Vòng phát triển nhanh: chạy collector hoặc normalizer **trên host** trỏ vào Kafka `localhost:32100`. Chỉ đưa lên cluster khi đã chạy ổn.

## 4. Đầu vào (Inputs)

| Input | Nguồn | Ghi chú |
|---|---|---|
| Binance WS `@trade` | `wss://stream.binance.com:9443/stream?streams=...` | ms timestamp, `m`=buyer is maker, ngắt kết nối sau 24h |
| OKX WS `trades` | `wss://ws.okx.com:8443/ws/v5/public` | ts là chuỗi ms, cần gửi `ping` mỗi < 30 s, VERIFY `trades` vs `trades-all` |
| Coinbase `matches`, Kraken v2 `trade` (P4) | xem `ingest/src/collectors/*.py` | Coinbase `side` = **maker** side |
| Ethereum logs (P3) | `wss://ethereum-rpc.publicnode.com` | rate limit, `removed=true` khi reorg |
| data.binance.vision trades | Bronze (R4 tải về) hoặc file local | CSV không header; timestamp ms (cũ) hoặc µs (2025+) |
| `symbol_map.csv`, `token_registry.csv` | R4, `contracts/reference/` | symbol chưa map thì đưa vào DLQ `UNKNOWN_SYMBOL` |
| Kafka, MinIO, Flink operator | R1 | Trước khi có: chạy pure logic + unit test |

## 5. Đầu ra (Outputs) và tiêu chí chấp nhận

> Các ngưỡng số là **mục tiêu ban đầu**. Đo thực tế ở P1–P2 rồi chỉnh trong file này.

| Output | Contract | Consumer | Tiêu chí chấp nhận |
|---|---|---|---|
| `raw.trades.<exchange>.v1` | `raw_envelope.v1` | normalizer, archiver | Collector chạy **liên tục ≥ 1 giờ** (local) không crash; tự reconnect khi rút mạng (đã thử); `seq` không gap trong 1 `conn_id`; xoay kết nối Binance trước 24h; metric msg/s, reconnects và last message có trên Prometheus |
| `canonical.trades.v1` | `canonical_trade.v1` | R3, archiver → R4 | 100% sample hợp lệ chuẩn hóa đúng (unit test với **sample thật đã capture** cho từng sàn); `taker_side` đúng ngữ nghĩa theo tài liệu chính thức (ghi nguồn trong `docs/exchanges.md`); không có bản ghi trùng `event_id` trong output (test bằng cách đẩy trùng); p95 (ingest → canonical) < 1 s ở local |
| `dlq.normalizer.v1` | `dlq_record.v1` | R4 (data quality), R5 | Mọi lỗi (parse, schema, unknown symbol, giá trị sai, late) đi vào DLQ, **không lỗi nào bị nuốt im lặng**; có metric đếm theo `error_type` |
| Replay tool | `canonical_trade.v1` trên `replay.canonical.trades.v1` | R3 (replay mode), R5 (evaluation) | Nhận tham số symbol, ngày, speed, run-id; thứ tự event-time tăng dần; có `replay_run_id` trên mọi bản ghi; ghi manifest JSON; speed=0 để test tải; **không bao giờ ghi vào topic live** |
| Bronze archive | `bronze/kafka/<topic>/dt=YYYY-MM-DD/hour=HH/*.jsonl.gz` | R4 | Mọi topic `raw.*`, `canonical.*`, `features.*`, `alerts.*`, `dlq.*` đều được lưu; at-least-once (commit offset sau khi ghi object); file mới xuất hiện trong ≤ 15 phút |
| `canonical.onchain_transfers.v1` (P3) | `onchain_transfer.v1` | R3, R4 | Áp dụng decimals từ `token_registry`; địa chỉ viết thường; `status` pending → confirmed sau `ethereum.confirmations` block; reorg sinh bản ghi `retracted` |
| `docs/exchanges.md` | n/a | cả nhóm | Mỗi sàn có: endpoint, message mẫu thật, ý nghĩa từng field dùng đến, heartbeat, giới hạn, cách phát hiện gap, **link tài liệu chính thức** |

## 6. Task board

### P0: Khởi động
- [ ] **ING-01** Spike API từng sàn: kết nối Binance và OKX (Coinbase/Kraken đọc tài liệu), capture 100–200 message thật mỗi sàn (gồm ack, ping, trade) vào `contracts/samples/captured/<exchange>.jsonl` (< 200 KB). Xác minh **ngữ nghĩa side** và đơn vị thời gian; viết `docs/exchanges.md`.
  - *Done khi:* các comment `VERIFY` liên quan side/timestamp trong `adapters.py` và collectors được thay bằng trích dẫn tài liệu.

### P1: Thu thập live
- [ ] **ING-02** Hoàn thiện `collectors/base.py`: chạy thật 1 giờ, test reconnect (tắt mạng hoặc kill pod), xoay kết nối, metric. Ghi log JSON.
- [ ] **ING-03** Binance collector chạy trên cluster (`ingest/k8s/collector.yaml`) với watchlist local.
- [ ] **ING-04** OKX collector (ping, kênh đúng) chạy trên cluster.

### P2: Chuẩn hóa, replay, lưu trữ
- [ ] **ING-05** Adapters Binance và OKX dùng sample thật; test edge case: trường thiếu, giá 0, symbol lạ, payload không phải JSON, OKX nhiều trade trong 1 message.
- [ ] **ING-06** PyFlink normalizer job (`normalizer/job.py`): Kafka source theo pattern, map `normalize_envelope`, side output DLQ, watermark, dedup theo `event_id` (state TTL 10 phút), sink AT_LEAST_ONCE, checkpoint. Chạy local mini-cluster rồi FlinkDeployment.
  - *Fallback:* nếu PyFlink chặn tiến độ quá 1 tuần thì báo nhóm và dùng Spark Structured Streaming với cùng hàm (ADR-0003).
- [ ] **ING-07** Replay tool: đọc zip từ Bronze (S3 API), merge nhiều symbol theo event time, pacing theo speed, ghi manifest.
- [ ] **ING-08** Archiver Kafka → Bronze (tự viết consumer hoặc Kafka Connect S3 sink; ghi quyết định vào ADR).
- [ ] **ING-09** Metric chất lượng phía stream: duplicates, gaps (theo `seq`/`trade_id`), DLQ theo loại, ingest lag (`ingest_ts_us - event_ts_us`) p50/p95. Bàn giao danh sách metric cho R1 (Grafana) và R4 (O7).

### P3: On-chain
- [ ] **ING-10** ETH collector: `eth_subscribe logs` (Transfer topic, các contract trong `token_registry` có `tracked=true`) + `newHeads`.
- [ ] **ING-11** Normalizer on-chain: decode Transfer log (topics[1], topics[2] → from, to; data → amount_raw), decimals, status/confirmations, reorg → `retracted`. Unit test bằng log thật đã capture.

### P4: Mở rộng (cloud)
- [ ] **ING-12** Coinbase + Kraken (collector + adapter + test). Thêm dòng `symbol_map.csv` (qua R4).
- [ ] **ING-13** (Tùy chọn) Binance depth stream `@depth@100ms` cho spoofing (R3 DET-12). Test tải: replay x100 trên cloud, đo throughput normalizer cùng R1.

## 7. Quy tắc kỹ thuật riêng

- **Collector không biến đổi payload**, chỉ bọc envelope. Mọi diễn giải nằm trong adapter (để có thể chạy lại từ Bronze).
- Adapter là **hàm thuần**, lỗi thì raise `AdapterError(error_type, msg)`. Không gọi Kafka hay mạng trong adapter.
- Thời gian luôn đi qua `cryptoguard_common.timeutil`. Không tự nhân 1000.
- Topic chỉ lấy từ `contracts/kafka-topics.yaml` (qua `cryptoguard_common.contracts.topic()`), không gõ chuỗi tự do.
- Mỗi sàn **1 replica** collector (tránh nhân đôi dữ liệu).
- Không thêm sàn hoặc symbol ngoài `config/` + `symbol_map.csv`.

## 8. Metrics phải expose

`cg_collector_messages_total{source}`, `cg_collector_reconnects_total{source,reason}`, `cg_collector_last_message_ts_seconds{source}`, `cg_normalizer_records_total{exchange}`, `cg_normalizer_dlq_total{error_type}`, `cg_normalizer_duplicates_total{exchange}`, `cg_normalizer_ingest_lag_ms` (histogram), `cg_archiver_objects_written_total{topic}`.

## 9. Kiểm thử bắt buộc

- Unit: adapters (sample thật mỗi sàn), `normalize_envelope` (mọi nhánh DLQ), envelope builder, parser binance.vision (ms và µs).
- Mọi output trong test đều chạy `cryptoguard_common.contracts.validate`.
- Integration (`-m integration`, cần cluster): đẩy sample vào raw, đọc thấy canonical; đẩy bản trùng, không thấy trùng.

## 10. Cần tìm hiểu

WebSocket (ping/pong, reconnect, backoff), API WebSocket của Binance/OKX/Coinbase/Kraken, Kafka producer (acks, idempotence, key, partition), PyFlink DataStream (KafkaSource/Sink, WatermarkStrategy, keyed state + TTL, side output, checkpoint), Ethereum JSON-RPC (`eth_subscribe`, logs, reorg), S3 API (boto3).

## 11. Giao tiếp với các vai trò khác

| Khi nào | Với ai | Nội dung |
|---|---|---|
| ING-01 xong | Tất cả | `docs/exchanges.md` + sample thật. R3 và R4 dùng ngay |
| ING-05/06 xong | R3 | `canonical.trades.v1` sẵn sàng, tốc độ msg/s thực tế |
| ING-07 xong | R3, R5 | Cách chạy replay, quy ước `replay_run_id` |
| ING-08 xong | R4 | Đường dẫn Bronze, định dạng file, độ trễ |
| ING-09 | R1, R4 | Danh sách metric và ý nghĩa cho Grafana và O7 |
| Mỗi lần cần symbol mới | R4 | PR thêm dòng `symbol_map.csv` |

## 12. Rủi ro và câu hỏi mở

- Sàn đổi API hoặc chặn IP: adapter tách riêng từng sàn, có metric msg/s = 0 để cảnh báo.
- PyFlink + connector jar khó cài: spike sớm trong P1, có fallback Spark.
- Coin nhỏ ít giao dịch nên ít message: thêm cả coin lớn để có tải thật.

## 13. Prompt mẫu cho AI agent

> Tôi là R2 (Ingestion & Normalization) của CryptoGuard. Đọc `AGENTS.md`, `team/00-shared-context.md`, `team/02-ingestion-normalization.md`, `contracts/schemas/raw_envelope.v1.schema.json`, `contracts/schemas/canonical_trade.v1.schema.json` và `streaming/normalizer/src/normalizer/`. Làm task **ING-06**: viết PyFlink job trong `normalizer/job.py` theo đúng mô tả trong docstring, dùng lại `normalize_envelope`, không đưa logic parse vào job. Chỉ sửa trong `streaming/normalizer/` và `streaming/k8s/`. Cho tôi lệnh chạy local (Kafka `localhost:32100`) và cách kiểm tra dedup.
