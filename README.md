# CryptoGuard

Hệ thống Big Data **phát hiện thao túng thị trường crypto** (pump-and-dump, wash trading, spoofing) trên nhiều sàn và **phát hiện rửa tiền on-chain** trên Ethereum (truy dòng tiền từ ví bị trừng phạt hoặc lừa đảo).

Toàn bộ hệ thống chạy trên **Kubernetes**: trước mắt chạy trên **laptop bằng kind**, đến giai đoạn P4 mới chuyển lên **GCP VM** để tiết kiệm chi phí.

> **Người mới: đọc theo thứ tự này**
> 1. File README này (10 phút)
> 2. [docs/brief/CryptoGuard_Project_Brief.html](docs/brief/CryptoGuard_Project_Brief.html): bối cảnh, nguồn dữ liệu, luồng dữ liệu, output (mở bằng trình duyệt)
> 3. [team/00-shared-context.md](team/00-shared-context.md): những gì cả nhóm phải nắm
> 4. File vai trò của bạn trong [team/](team/)
> 5. [CONTRIBUTING.md](CONTRIBUTING.md): cách đặt tên branch, commit, mở PR, những gì được và không được commit

---

## 1. Repo này đã có gì

Repo này thay thế hoàn toàn code W1 (PaySim). Code W1 được lưu dạng zip ở `_backup/W1_backup_2026-11-16.zip`, nằm ngoài repo. Một số quy ước của W1 vẫn được giữ lại: tên topic kết thúc bằng `.v<major>`, message dạng JSON kèm JSON Schema, cấu trúc DLQ.

Trạng thái hiện tại:
- Phần **logic thuần** (parse, chuẩn hóa, feature, rule, taint, metric đánh giá) **đã có code thật và có test**: 63 test pass, ruff sạch.
- Phần **wiring với engine** (Flink job, Spark job, Airflow DAG, Kafka sink, API handler) mới là **khung**. Docstring mô tả rõ yêu cầu, còn hàm thì `raise NotImplementedError("<TASK-ID>")`.
- Manifest K8s và script `make up` **chưa được chạy thử trên cluster thật**. R1 kiểm chứng ở task INF-03.

| Thư mục | Nội dung | Trạng thái | Người phụ trách |
|---|---|---|---|
| `contracts/` | 8 JSON Schema, danh sách 14 Kafka topic, sample đúng và sai, bảng tham chiếu (symbol, token, ví sàn). **Mọi giao tiếp giữa các phần đều đi qua đây** | ✅ v1 | Chung |
| `libs/common/` | Thư viện `cryptoguard_common`: config, thời gian (µs), validate contract, đọc bảng tham chiếu, **định nghĩa feature dùng chung cho real-time và batch** | ✅ | Chung |
| `config/` | `base.yaml` + `local.yaml` / `cloud.yaml` (chọn bằng `CG_PROFILE`) | ✅ | Chung |
| `infra/` | kind, Kafka (Strimzi), MinIO, Postgres, MongoDB, Redis, Helm values, `up-local.sh` theo profile, script sinh topic, build image | 🟡 chưa chạy thật | R1 |
| `ingest/` | Collector Binance + OKX (có test), khung Coinbase/Kraken/ETH, replay, archiver | 🟡 | R2 |
| `streaming/normalizer/` | Adapter từng sàn, chuẩn hóa + DLQ (có test); Flink job dạng khung | 🟡 | R2 |
| `streaming/detector/` | Rule `pump-v1`, `dump-v1`, gom case (có test); Flink job dạng khung | 🟡 | R3 |
| `batch/` | DDL Iceberg (Silver/Gold), cấu hình Spark, DAG mẫu, thuật toán taint tham chiếu (có test) | 🟡 | R4 |
| `ml/` | Metric đánh giá: recall, precision, độ trễ (có test); khung training | 🟡 | R5 |
| `serving/` | Index MongoDB, khung API FastAPI, khung sink, ghi chú dashboard | 🟡 | R5 |
| `docs/` | Kiến trúc, contract, lộ trình, cài đặt local, kế hoạch cloud, 5 ADR | ✅ | Chung |
| `team/` | 6 file context: 1 file chung + 5 file theo vai trò, dùng cho cả người và AI agent | ✅ | Chung |
| `data/` | Dữ liệu tạm trên máy. **Không commit** (git-ignored) | n/a | n/a |

## 2. Kiến trúc tổng quan

```
 Sàn WS (Binance, OKX → P4: Coinbase, Kraken) ─┐
 Ethereum RPC (Transfer logs, P3) ─────────────┤  ingest/collectors (R2) ─▶ Kafka raw.*.v1
                                               │                                │
                                               │            streaming/normalizer (R2, Flink)
                                               │                                ▼
                                               │            canonical.*.v1 ──▶ lưu xuống Bronze
                                               │                                │
                                               │   streaming/detector (R3, Flink) ◀── Redis (baseline, wallet risk)
                                               │                                ▼
                                               │            alerts.v1 / features.symbol_1m.v1
                                               │                                │
                                               │   serving/sink (R5) ─▶ MongoDB ─▶ API ─▶ dashboard
 binance.vision, AWS blockchain, OFAC,         │
 darklist, pump dataset, (GDELT) ──────────────┴─▶ batch/airflow (R4) ─▶ Bronze (MinIO/GCS)
                                                   Spark (R4) ─▶ Silver ─▶ Gold (Iceberg)
                                                   graph ─▶ wallet_risk ─▶ Redis + MongoDB
                                                   ml/ (R5): train, đánh giá trên các vụ pump thật ─▶ MLflow
```

Có 4 luồng: **hot path** (vài giây), **cold path** (giờ/ngày), **vòng phản hồi** (batch đẩy kết quả lên Redis cho detector dùng), **replay** (phát lại vụ pump thật để đánh giá). Chi tiết ở [docs/architecture.md](docs/architecture.md).

## 3. Năm vai trò

| Role | Tên | Thư mục sở hữu | File context |
|---|---|---|---|
| R1 | Platform & Infra | `infra/`, `.github/`, `Makefile` | [team/01-platform-infra.md](team/01-platform-infra.md) |
| R2 | Ingestion & Stream Normalization | `ingest/`, `streaming/normalizer/` | [team/02-ingestion-normalization.md](team/02-ingestion-normalization.md) |
| R3 | Real-time Detection | `streaming/detector/`, `features.py` | [team/03-realtime-detection.md](team/03-realtime-detection.md) |
| R4 | Batch, Lakehouse & Graph | `batch/`, `contracts/reference/` | [team/04-batch-lakehouse-graph.md](team/04-batch-lakehouse-graph.md) |
| R5 | ML, Serving & Evaluation | `ml/`, `serving/` | [team/05-ml-serving-evaluation.md](team/05-ml-serving-evaluation.md) |

Mỗi file vai trò có các mục:
- Sứ mệnh và phạm vi: được sửa thư mục nào, phải phối hợp ở đâu, không được đụng vào đâu.
- Môi trường theo từng giai đoạn.
- **Bảng đầu vào**, ghi rõ nếu chưa có thì dùng gì thay thế để không phải ngồi chờ người khác.
- **Bảng đầu ra kèm tiêu chí chấp nhận có số đo.**
- Task board có mã task (INF / ING / DET / BAT / MLS), chia theo giai đoạn.
- Test bắt buộc, metric, kiến thức cần học, điểm bàn giao với người khác, rủi ro.
- Prompt mẫu để giao việc cho AI agent.

## 4. Lộ trình

| Phase | Môi trường | Mục tiêu | Demo khi xong |
|---|---|---|---|
| P0 | laptop | Hiểu bài toán, thử từng nguồn dữ liệu, chốt contract | `make test` xanh trên cả 5 máy; có danh sách vụ pump dùng được để đánh giá |
| P1 | kind | Dựng nền tảng, có dữ liệu thật đầu tiên | `make up` chạy được; dữ liệu Binance + OKX vào Kafka |
| P2 | kind | **MVP end-to-end** | Replay một vụ pump thật thì cảnh báo hiện lên dashboard |
| P3 | kind | On-chain và graph | API trả về đường đi từ một ví tới ví OFAC/darklist |
| P4 | **GCP** | Lên cloud, tăng quy mô, tính năng nâng cao, đánh giá | 3–6 tháng dữ liệu; có báo cáo đánh giá; demo chịu lỗi |
| P5 | GCP | Hoàn thiện | Báo cáo, slide, video demo dự phòng |

Chi tiết ở [docs/roadmap.md](docs/roadmap.md).

## 5. Bắt đầu nhanh (local)

Yêu cầu: laptop 16 GB RAM trở lên. **Người dùng Windows làm việc trong WSL2 Ubuntu.** Cần cài Docker Desktop (cấp ≥ 8 GB RAM), `kind`, `kubectl`, `helm`, Python 3.11, `make`. Hướng dẫn đầy đủ: [docs/local-setup.md](docs/local-setup.md).

```bash
cp .env.example .env
make dev-install        # tạo venv, cài libs/common và công cụ dev
make test               # unit test + kiểm tra contract (không cần cluster)
make up                 # kind + Kafka + MinIO + Postgres
make up-stream          # + Flink operator           (R2, R3)
make up-batch           # + Spark operator, Airflow  (R4, R5)
make up-serving         # + MongoDB, Redis           (R3, R5)
make up-monitoring      # + Prometheus, Grafana      (R1)
make seed-samples       # đẩy message mẫu vào Kafka để dev khi chưa có dữ liệu thật
make status
make down               # xóa cluster local
```

Chỉ bật những profile bạn cần, vì máy 16 GB không chạy nổi tất cả cùng lúc.

Endpoint local:

| Dịch vụ | Địa chỉ |
|---|---|
| Kafka | `localhost:32100` |
| MinIO | `:30900` (API) / `:30901` (console) |
| Postgres | `:30432` |
| MongoDB | `:30017` |
| Redis | `:30379` |
| Grafana | `:30300` |
| Airflow | `:30808` |
| MLflow | `:30500` |

## 6. Quy tắc quan trọng nhất

1. **Contract là luật.** Các phần chỉ giao tiếp qua `contracts/` và `batch/ddl/`. Muốn đổi contract thì làm PR và người dùng contract đó phải duyệt. Thay đổi phá tương thích cần có ADR.
2. **Local trước, cloud sau.** Không tạo tài nguyên cloud trước P4. Không hard-code host; mọi thứ đi qua `config/*.yaml`. Tuân thủ `data_budget` khi tải dữ liệu về máy.
3. **Logic thuần, engine mỏng.** Logic nghiệp vụ viết bằng hàm Python có test. Flink, Spark và Airflow chỉ lo nối đầu vào, đầu ra.
4. **Thời gian luôn là số nguyên micro giây UTC**, tên field kết thúc bằng `_ts_us`.
5. **Live và replay không bao giờ trộn lẫn**: dùng topic `replay.*` và collection hoặc deployment riêng.
6. **Không lỗi nào bị nuốt im lặng**: mọi bản ghi lỗi đi vào DLQ kèm lý do và có metric đếm.
7. **Không commit** dữ liệu, file `.env`, secret, model binary hay output notebook. Xem bảng chi tiết trong [CONTRIBUTING.md](CONTRIBUTING.md).

## 7. Làm việc với AI agent

Mở agent **tại thư mục gốc repo**. Agent sẽ tự đọc [AGENTS.md](AGENTS.md) (file `CLAUDE.md` trỏ tới file này). AGENTS.md buộc agent:
- đọc context trước khi làm,
- chỉ sửa trong phạm vi vai trò,
- không tự đổi contract,
- không tạo tài nguyên cloud,
- validate output theo schema.

Câu mở đầu mẫu:

> Tôi là R2. Đọc `team/00-shared-context.md` và `team/02-ingestion-normalization.md`, sau đó làm task **ING-03**. Trước khi code, tóm tắt lại input, output và tiêu chí chấp nhận của task.

Sau khi agent làm xong: chạy `make lint test`, đối chiếu với tiêu chí chấp nhận, đánh dấu `[x]` trên Task board trong file vai trò, rồi mở PR.

## 8. Những điểm cần kiểm chứng (comment `# VERIFY` trong code)

Các điểm dưới đây được viết theo hiểu biết hiện tại và **phải đối chiếu với tài liệu chính thức hoặc dữ liệu thật** trước khi tin dùng:

| Điểm cần kiểm chứng | Task |
|---|---|
| Ý nghĩa trường `side` / `m` của từng sàn (Coinbase ghi chiều của **maker**) | ING-01 |
| Kênh `trades` hay `trades-all` của OKX, và cơ chế ping của OKX | ING-01, ING-04 |
| Timestamp của data.binance.vision spot đổi từ ms sang **µs** từ năm 2025 | BAT-01 |
| Schema thật của AWS `token_transfers` | BAT-01 |
| Phiên bản Strimzi, Flink operator, Spark operator, chart Airflow (Airflow 2 hay 3) | INF-03, INF-06, INF-10 |
| Image MinIO còn pull được không (MinIO đổi cách phân phối bản community năm 2025) | INF-05 |
| Phiên bản jar Iceberg, Kafka connector và tên file `flink-python` trong image | BAT-02, ING-06 |

Ngưỡng của rule và chỉ tiêu (ví dụ recall ≥ 60%) chỉ là **mục tiêu ban đầu**. Nhóm sẽ chỉnh sau lần replay đầu tiên.

## 9. Việc cần làm ngay (tuần đầu, P0)

| Ai | Việc |
|---|---|
| R1 | INF-01: đẩy repo lên GitHub, bật branch protection, điền username thật vào `.github/CODEOWNERS`. INF-02: kiểm tra cài đặt trên cả 5 máy |
| Tất cả | `make dev-install test` phải xanh trên máy mình; đọc brief và file context |
| R2 | ING-01: kết nối thử từng sàn, lưu message thật làm sample, viết `docs/exchanges.md` |
| R3 + R4 + R5 | DET-01 / BAT-01 / MLS-01: chọn **≥ 5 vụ pump còn dữ liệu trên Binance** (rủi ro lớn nhất của đề tài); viết protocol đánh giá |
| R4 | BAT-01: thử tải binance.vision, AWS ETH, OFAC; viết `docs/source-notes.md` |
| R5 | MLS-02: thiết kế API và wireframe dashboard |

**Câu hỏi cần hỏi giảng viên:** HDFS có bắt buộc không? Trường có hỗ trợ credit cloud không? Demo bằng replay có được tính là streaming không?

## 10. Tài liệu khác

- [docs/data-contracts.md](docs/data-contracts.md): quy ước contract, reason code của alert, khóa Redis, collection Mongo
- [docs/local-setup.md](docs/local-setup.md): cài đặt và xử lý lỗi thường gặp
- [docs/cloud-migration.md](docs/cloud-migration.md): kế hoạch chuyển lên GCP và cách tiết kiệm chi phí
- [docs/decisions/](docs/decisions/): các quyết định kiến trúc (ADR)
- README riêng trong từng thư mục `infra/`, `ingest/`, `streaming/`, `batch/`, `ml/`, `serving/`
