# Tuần 6 — Kịch bản demo và câu hỏi bảo vệ (10–15 phút)

**Mode A — live integration** chỉ khi G3/G4 và staging đã PASS. **Mode B — fixture fallback** khi cluster không sẵn: nói rõ *fixture*, chỉ demo semantics/code; không biến bản ghi fixture thành kết quả PaySim thật.

| Thời lượng dự kiến (không phải thời gian đã đo) | Người trình bày | Cảnh demo | Bằng chứng phải mở |
|---|---|---|---|
| 0–2 phút | P1 | Kiến trúc thực được freeze; Spark/HDFS/Iceberg/Kafka/Mongo/API; Redis/Airflow chỉ optional | final inventory, revision/config |
| 2–4 phút | P2 | Bronze giữ duplicate → Silver dedup → Gold từ pinned cutoff; query before/after compaction | source checksum, Bronze/Silver/Gold run manifests |
| 4–6 phút | P3 | Model A và A+H, window `[step-24,step-1]`, validation threshold, test AP/PR nếu có nguồn | model card, golden parity, locked evaluation report |
| 6–9 phút | P4 | Replay với manifest; Mongo alert ID; stop/restart một worker/query và resume checkpoint | Kafka offsets, sink ACK, recovery/run id reconciliation |
| 9–11 phút | P5 | API/dashboard từ Mongo run thật, lag/latency nếu đo thật, validate PASS/FAIL/INCONCLUSIVE | `api_sample`, collector, G3/G4/G5 evidence index |
| 11–15 phút | cả nhóm | 5 câu hỏi: leakage, duplicate/late, failover, benchmark fairness, synthetic-data limitation | SOURCE URI có hash, không chỉ screenshot |

## Thủ tục diễn tập

1. Freeze revision/run/config/model/feature và ghi đồng thời vào bảng `rehearsal`.
2. Người dự phòng không phải tác giả thử lệnh từ quickstart; ghi blocker và thời lượng thật theo từng bước.
3. Cho một report FAIL cố ý để người dự phòng tìm nguyên nhân; **giữ cả evidence FAIL** trong bundle.
4. Nếu chuyển sang fixture fallback, cập nhật nhãn màn hình và lời thuyết minh. Không dùng p95/AP synthetic làm benchmark.
5. Mỗi thành viên tự trình bày module của mình và một contract nhận/giao; reviewer ký **sau** khi tự chạy.

## Phần report cần ghi rõ

- Data scale được đo với nguồn PaySim/snapshot checksum, không suy ra tăng tốc nếu tổng CPU/RAM thay đổi.
- Benchmark W5 cần 3 repetitions/arm, correctness PASS ở từng sample, median/MAD; ghi contention riêng.
- Fault recovery đo thời điểm lỗi, phát hiện, khởi động và finalized IDs — không tự suy thời gian recovery.
- PaySim mô phỏng **giờ**, không phải transaction stream thực; 1 broker, single-host và NameNode đơn là giới hạn.
- Không tuyên bố Redis/Airflow đã làm nếu không có code/runtime proof; diagram chỉ là mô tả, không phải evidence.

## Checklist sau freeze

`git status` sạch; hash model/code/config; người chạy độc lập; 30 checklist cases; G5 gate; evidence index; backup plan; danh sách blocker/limitations; signer P1–P5. Nếu thiếu một cổng, báo **INCONCLUSIVE**, không sửa tay JSON thành PASS.
