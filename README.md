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
# Fraud Detection PaySim batch ETL

Project này đọc CSV PaySim, chuẩn hóa và kiểm tra dữ liệu bằng Spark, lưu Parquet,
và tạo hai đầu ra riêng cho replay và nhãn đánh giá. Mỗi lần chạy lưu manifest,
logs và báo cáo kiểm tra đọc lại Parquet.

## Các bước chạy bằng Docker Compose

Mở Docker Desktop và đợi Docker Engine chạy. ETL chạy trong Linux container,
nên không cần cài Hadoop/winutils trên Windows hoặc kích hoạt Conda.
`docker-compose.yml` và `Dockerfile.jupyter` phải nằm tại thư mục gốc project.

1. Mở PowerShell và vào thư mục project:

   ```powershell
   cd C:\Users\lenovo\project\bigdata
   ```

2. Build và khởi động container:

   ```powershell
   docker compose -p fraud-detection up -d --build
   ```

3. Kiểm tra service `spark` có trạng thái `Up`:

   ```powershell
   docker compose -p fraud-detection ps
   ```

4. Chạy thử ETL trên 10.000 dòng:

   ```powershell
   docker compose -p fraud-detection exec spark python ./src/processing/etl.py --limit 10000
   ```

5. Khi sample hoàn thành với `"status": "verified"`, chạy toàn bộ dataset:

   ```powershell
   docker compose -p fraud-detection exec spark python ./src/processing/etl.py
   ```

Compose tạo container `fraud-spark`, dựa trên môi trường Spark 4.2.0 và Jupyter
của Spark lab. Thư mục project được mount vào `/workspace`, nên các file code,
dataset và output trong container cũng hiện trên máy của bạn. Dataset cần nằm
ở `data/paysim dataset.csv` trước khi chạy job.

Jupyter dùng cổng 8890. Lấy URL có token bằng lệnh:

```powershell
docker compose -p fraud-detection exec spark jupyter server list
```

Trong URL được in, đổi port `8888` thành `8890`, giữ nguyên token và mở trên trình
duyệt. Spark UI ở `http://localhost:4041` chỉ có khi SparkSession đang chạy.
Job batch dùng `local[2]` và Kryo, không cần checkpoint streaming.

## Kiểm thử trước khi chạy dataset đầy đủ

```powershell
docker compose -p fraud-detection exec spark python -m unittest discover -s tests -p test_etl.py -v
```

Fixture có 28 dòng dữ liệu: 6 hợp lệ, 22 quarantine và 1 duplicate được giữ lại.
Test kiểm tra lỗi parse/kiểu/domain, chuẩn hóa, đọc lại Parquet, ID ổn định khi
chạy lại, duplicate, replay tách nhãn, giới hạn dòng và header sai. Output kiểm
thử nằm trong thư mục tạm bên trong container và được dọn sau test.

Kết quả đã xác nhận ngày 17/10/2026: **7/7 test pass trong 22,752 giây**,
kết thúc bằng `OK`. Môi trường: Python `3.10.12`, Spark `4.2.0`, Scala
`2.13.18`, OpenJDK `21.0.12.1`. Xem [log test](reports/week2-unit-tests.log)
và [báo cáo kiểm thử tổng hợp](reports/etl-test-report.md).
Test rerun đã xác nhận ID và số dòng ổn định; chưa so sánh toàn bộ giá trị
trước/sau rerun.

## Chạy ETL trên sample

```powershell
docker compose -p fraud-detection exec spark python ./src/processing/etl.py --limit 10000
```

`--limit` lấy N dòng dữ liệu đầu tiên theo vị trí nguồn, gồm cả dòng lỗi. Job
vẫn tính checksum toàn file và có thể đọc file để đánh số dòng; đây là giới hạn
số dòng được xử lý, không phải cam kết chỉ đọc N dòng từ ổ đĩa.

## Chạy ETL đầy đủ

```powershell
docker compose -p fraud-detection exec spark python ./src/processing/etl.py
```

Job dùng đường dẫn và cấu hình từ `configs/etl.json`. Có thể thay đường dẫn bằng
tham số; đường dẫn tuyệt đối phải là đường dẫn bên trong container:

```powershell
docker compose -p fraud-detection exec spark python ./src/processing/etl.py --input "/workspace/data/paysim dataset.csv" --output-root /workspace/data/processed
```

Output được lưu tại `data/processed/paysim-v1/<run-id>/`:

Thư mục này nằm trên máy Windows vì project được mount vào `/workspace`.
Lần chạy sample mới nhất hoàn tất ngày 17/10/2026 lúc 20:11:25 (Asia/Saigon), có run ID
`57d04fcb134a`, nên đường dẫn tương ứng là:

```text
C:\Users\lenovo\project\bigdata\data\processed\paysim-v1\57d04fcb134a\
```

Manifest của lần chạy này xác nhận `status: verified`, raw/valid/processed/replay/labels
đều có 10.000 dòng, quarantine và duplicate đều bằng 0, runtime 24,894 giây.
Run ID của lần chạy khác được in trong log và phần `outputs` cuối terminal.

Full run mới nhất `5b18e10106f3` hoàn tất ngày 17/10/2026 lúc 20:24:04
(Asia/Saigon), có `status: verified`, runtime **310,308 giây**:

| Chỉ số | Kết quả full run |
|---|---:|
| raw / valid | 6.362.620 / 6.362.620 |
| quarantine / duplicate | 0 / 0 |
| processed / replay / labels | Mỗi output 6.362.620 dòng |
| Nhãn fraud (`label = 1`) | 8.213 |

Đã đọc lại cả bốn output để xác minh schema và số dòng, kiểm tra ID processed
duy nhất và phân bố label/type không đổi sau khi ghi Parquet. Bằng chứng:
[full manifest](data/processed/paysim-v1/5b18e10106f3/manifest.json),
[ETL log](data/processed/paysim-v1/5b18e10106f3/etl.log) và
[báo cáo tổng hợp](reports/etl-test-report.md).
Dataset/Parquet nằm ngoài Git; liên kết đến `data/processed/` chỉ hoạt động
trên máy có output tương ứng.

| Đầu ra | Nội dung |
|---|---|
| `processed/` | Parquet đã chuẩn hóa, gồm nhãn và metadata nguồn phục vụ training. |
| `quarantine/` | Dòng gốc, chuỗi trường đã parse, số dòng nguồn và `error_reasons`. |
| `replay/` | ID, step, loại giao dịch, amount và tài khoản nguồn/đích. |
| `labels/` | `transaction_id` và `label`, dành riêng cho evaluation. |
| `manifest.json` | Identity/checksum input, config, phiên bản, counts và kết quả readback. |
| `etl.log` | Logs các bước Extract, Transform, Load và kết quả kiểm tra. |
| `etl-test-report.md` | Báo cáo sinh sau một lần chạy thành công. |

Khi hoàn thành, terminal in `status: verified`. Kiểm tra manifest có
`raw = valid + quarantine`; counts và schema của bốn output đã được đối chiếu
khi đọc lại. Với dataset đã profiling, mốc tham khảo là 6.362.620 dòng và
8.213 nhãn fraud. Kết quả chạy thật là nguồn xác nhận, không mặc định dùng mốc
tham khảo để tuyên bố job thành công.

## Policy và phạm vi

- Input hiện là một CSV local UTF-8, có header đúng thứ tự 11 cột và mỗi record
  trên một dòng vật lý. Multiline CSV và HDFS chưa được hỗ trợ trong ingestion này.
- Các trường integer cần chuỗi số nguyên; `1.5` và `1.0` được quarantine.
  Giá trị số khác phải parse được và hữu hạn. Nhãn/cờ phải thuộc 0 hoặc 1.
- Required fields, step dương, amount không âm và category theo config được kiểm
  tra. Type được trim và viết hoa; account ID được trim, giữ nguyên hoa/thường.
- Duplicate toàn dòng được thống kê và giữ lại. Zero, outlier, số dư âm và balance
  residual không bị tự động loại hoặc impute. Validation defaults còn cần nhóm review.
- `transaction_id = SHA256(input_checksum + ':' + số dòng vật lý)`, nên duplicate
  có ID riêng và cùng snapshot cho cùng ID khi chạy lại. Thay nội dung file làm
  đổi checksum và các ID của snapshot.
- Replay dùng allowlist cố định, không có nhãn, cờ có sẵn hoặc số dư. `step` là
  chỉ số mô phỏng; chưa tự tạo `event_time` khi chưa chốt timestamp contract.
- Một run directory được xác định từ input/config/schema/code. Chạy lại cùng
  run ghi đè các output đó, không append. Sample và full có run ID khác nhau.
  File khóa ngăn hai job cùng ghi một run. Nếu container dừng đột ngột, xác nhận
  không còn job chạy trước khi dọn file `.etl.lock` của run bị gián đoạn.

## Dừng và xử lý lỗi

```powershell
docker compose -p fraud-detection logs --tail 50 spark
docker compose -p fraud-detection down
```

`down` dừng container; dữ liệu nằm trong thư mục project vẫn giữ nguyên. Nếu
8890 hoặc 4041 đã bị dùng, sửa phần `ports` trong `docker-compose.yml` rồi chạy
lại `up`. Nếu dataset thiếu, đặt CSV vào `data/` hoặc dùng `--input` đúng đường
dẫn. Kết quả sample, full dataset và bộ test đã được xác nhận theo các bằng
chứng nêu trên; kết quả này áp dụng cho input/config/schema/code của các lần
chạy đã ghi nhận.
