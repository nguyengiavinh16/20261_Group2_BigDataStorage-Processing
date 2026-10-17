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
Lần chạy sample thành công ngày 2026-10-13 lúc 22:44 (Asia/Saigon) có run ID
`57d04fcb134a`, nên đường dẫn tương ứng là:

```text
C:\Users\lenovo\project\bigdata\data\processed\paysim-v1\57d04fcb134a\
```

Log của lần chạy này xác nhận `status: verified`, raw/valid/processed/replay/labels
đều có 10.000 dòng, quarantine và duplicate đều bằng 0, runtime 26,823 giây.
Đây là kết quả sample; chưa xác nhận lần chạy toàn bộ dataset hoặc bộ test lỗi.
Run ID của lần chạy khác được in trong log và phần `outputs` cuối terminal.

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
dẫn. Kết quả sample đã được xác nhận theo log; full dataset và bộ test lỗi
cần được chạy riêng để xác nhận.
