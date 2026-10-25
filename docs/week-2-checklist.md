# Checklist Week 2 — Spark ETL và bàn giao dữ liệu

Cập nhật: 17/10/2026 (Asia/Saigon).

Theo dõi theo [kế hoạch Week 2](Ke_hoach_chi_tiet_thanh_vien_2_Dat.html#muc-5).
Đổi `- [ ]` thành `- [x]` khi hoàn thành và ghi link log, report, commit hoặc xác nhận review bên cạnh.
Các mục đã đánh dấu dựa trên code và bằng chứng lưu trong workspace; checklist này không phải kết quả chạy test mới.

## Phiên 1 — Schema, config và ETL skeleton

- [x] Có schema `StructType` rõ ràng: `src/processing/schema.py`.
- [x] Có entry point ETL: `src/processing/etl.py`.
- [x] Có config: `configs/etl.json`; hỗ trợ tham số input, output, limit và master local.
- [x] Đọc và xử lý sample 10.000 dòng thành công: run `57d04fcb134a`.
- [x] Có hướng dẫn chạy bằng Docker Compose trong `README.md`.

Tên `schema.py` và `etl.json` khác tên đề xuất trong kế hoạch nhưng đã đảm nhiệm đúng vai trò.

## Phiên 2 — Chuẩn hóa và validation

- [x] Chuẩn hóa tên cột, cast kiểu và lưu column mapping trong manifest.
- [x] Kiểm tra malformed CSV, required fields, kiểu số hữu hạn, step, amount, category và nhãn.
- [x] Tách valid/quarantine; giữ dòng nguồn và nhiều `error_reasons` cho một dòng lỗi.
- [x] Thống kê duplicate và giữ lại; không tự động loại zero/outlier/số dư âm.
- [x] Tạo `transaction_id` từ checksum input và số dòng nguồn.
- [ ] Đức Anh và Thái review, chốt validation policy v1; ghi quyết định cho null, zero, outlier, duplicate và category.
- [ ] Cập nhật trạng thái policy đang là `profiling_reference_pending_team_contract_review` sau khi được chốt.

## Phiên 3 — Chạy full và kiểm tra Parquet

- [x] Có run full `5b18e10106f3` với `status: verified`.
- [x] Có đủ bốn output: `processed/`, `quarantine/`, `replay/`, `labels/`.
- [x] Đối chiếu raw = valid + quarantine: 6.362.620 = 6.362.620 + 0; duplicate = 0.
- [x] Đọc lại bốn output để so schema/count; kiểm tra ID duy nhất và phân bố label/type của processed.
- [x] Lưu manifest, log và báo cáo theo run; có checksum, config, schema/code version và runtime.

Bằng chứng full: [manifest](../data/processed/paysim-v1/5b18e10106f3/manifest.json),
[log](../data/processed/paysim-v1/5b18e10106f3/etl.log),
[báo cáo run](../data/processed/paysim-v1/5b18e10106f3/etl-test-report.md).
Run hoàn tất ngày 13/10/2026 lúc 22:53 (Asia/Saigon), runtime 340,766 giây; có 8.213 nhãn fraud.
Các file dữ liệu này nằm ngoài Git nên liên kết chỉ hoạt động khi máy có output tương ứng.

## Phiên 4 — Replay và bàn giao cho Công Anh

- [x] Xuất replay chỉ gồm ID, step, loại giao dịch, amount và account nguồn/đích.
- [x] Xuất ground truth riêng: `transaction_id` + `label`.
- [x] Replay không chứa label, cờ fraud có sẵn hoặc số dư.
- [x] Có output sample 10.000 dòng tại run `57d04fcb134a`.
- [ ] Viết hướng dẫn bàn giao: processed/replay/labels URI, schema, cách đọc Parquet và cách đối chiếu nhãn bằng ID.
- [ ] Chốt với Công Anh/Lead contract replay: required fields, quy tắc ID, thứ tự phát và mapping `step` sang thời gian sự kiện.
- [ ] Bàn giao sample phù hợp để Công Anh thử producer; bổ sung sample event theo format đã thống nhất nếu cần.
- [ ] Công Anh xác nhận producer đọc được sample và không gửi nhãn vào scoring; lưu log hoặc xác nhận nhận bàn giao.

## Phiên 5 — Test, review và PR W2

- [x] Có fixture 28 dòng: 6 hợp lệ, 22 quarantine, 1 duplicate được giữ lại.
- [x] Có `tests/test_etl.py` kiểm tra parse/type/domain, chuẩn hóa, Parquet, ID, duplicate, replay/labels, limit và header sai.
- [x] Chạy bộ test trong Docker thành công; lưu log, lệnh chạy và phiên bản runtime.
- [ ] Xác nhận rerun cùng input giữ ID, nội dung logic và số dòng; không append gây nhân đôi. Bổ sung kiểm tra nội dung nếu cần vì test hiện tập trung so ID/count/schema.
- [ ] Đức Anh review test lỗi và expected behavior; ghi kết quả review.
- [ ] Thái chốt split theo step, cách kiểm tra nhãn từng split và feature MVP với Công Anh; ghi quyết định trước khi training Week 3.
- [x] Tạo `reports/etl-test-report.md` tổng hợp run full, test fixture, rerun, expected/actual counts và hạn chế.
- [ ] Cập nhật README: bổ sung run full đã verified; sửa câu còn nói chưa xác nhận full; chỉ cập nhật trạng thái test sau khi có log pass.
- [ ] Đưa code/config/tests và báo cáo cần thiết vào commit; giữ dataset/Parquet ngoài Git.
- [ ] Đảm bảo README và các file Docker cần bàn giao được đưa vào Git; `.gitignore` hiện đang loại trừ chúng.
- [ ] Hoàn tất PR W2, ghi owner/reviewer/deadline, lệnh tái tạo, evidence và các mục còn mở.

Lệnh chạy test từ thư mục gốc project khi service `spark` đang Up:

```powershell
docker compose -p fraud-detection exec spark python -m unittest discover -s tests -p test_etl.py -v
```

`fixture-check` cũ có trạng thái failed; không dùng nó làm bằng chứng test pass.

## HDFS — Theo điều kiện hạ tầng

- [ ] Ghi rõ HDFS chưa được nghiệm thu và tiếp tục dùng local Docker filesystem khi endpoint chưa sẵn sàng.
- [ ] Khi Lead bàn giao endpoint: bổ sung hỗ trợ đường dẫn HDFS, kiểm tra đọc/ghi và lưu evidence. ETL hiện chỉ hỗ trợ một CSV local.

HDFS chưa sẵn sàng được phép để mở theo kế hoạch; ghi owner và dependency để theo dõi.

## Ghi nhận hoàn thành và bàn giao

| Ngày | Mục hoàn thành / quyết định | Link evidence / commit / PR | Người review hoặc nhận |
|---|---|---|---|
| | | | |

Thứ tự ưu tiên: chạy test và rerun → tổng hợp báo cáo → bàn giao replay → chốt policy/split/feature → cập nhật README và PR.
Training model thuộc Week 3.
