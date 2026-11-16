# G5 — Checklist 30 ca nghiệm thu (trích từ HTML, TUẦN 6)

**Trạng thái khi bàn giao: CHƯA CHẠY / chưa nghiệm thu G5.** Tên ca giữ đúng theo kế hoạch nguồn. Mỗi ca cần operator, revision, lệnh, expected/actual, manifest/report SHA, reviewer và link evidence thật. Không đánh dấu PASS từ file JSON điền mẫu.

Exit semantics: `PASS=0`, `FAIL=1`, `INCONCLUSIVE=2`. Gate cần **G4 live PASS**, clean Git checkout chạy bởi người khác, toàn bộ 30 ca PASS và signoff P1–P5.

## P1 — Lead

- [ ] **P1-W6A-C1 — Checkout sạch từ hướng dẫn** — Chưa chạy. Evidence: `reports/evidence/week6/P1-W6A-C1.json` + stdout/URI thực + reviewer.
- [ ] **P1-W6A-C2 — Luồng fixture đầu-cuối** — Chưa chạy. Evidence: `reports/evidence/week6/P1-W6A-C2.json` + stdout/URI thực + reviewer.
- [ ] **P1-W6A-C3 — Cấu hình thiếu và hướng dẫn phục hồi** — Chưa chạy. Evidence: `reports/evidence/week6/P1-W6A-C3.json` + stdout/URI thực + reviewer.
- [ ] **P1-W6B-C1 — Kiến trúc khớp implementation** — Chưa chạy. Evidence: `reports/evidence/week6/P1-W6B-C1.json` + stdout/URI thực + reviewer.
- [ ] **P1-W6B-C2 — Diễn tập demo và bàn giao** — Chưa chạy. Evidence: `reports/evidence/week6/P1-W6B-C2.json` + stdout/URI thực + reviewer.
- [ ] **P1-W6B-C3 — Freeze và evidence truy vết** — Chưa chạy. Evidence: `reports/evidence/week6/P1-W6B-C3.json` + stdout/URI thực + reviewer.

## P2 — Data engineering

- [ ] **P2-W6A-C1 — Người khác ingest lại** — Chưa chạy. Evidence: `reports/evidence/week6/P2-W6A-C1.json` + stdout/URI thực + reviewer.
- [ ] **P2-W6A-C2 — Data dictionary dùng được** — Chưa chạy. Evidence: `reports/evidence/week6/P2-W6A-C2.json` + stdout/URI thực + reviewer.
- [ ] **P2-W6A-C3 — Nguồn sai và retention guidance** — Chưa chạy. Evidence: `reports/evidence/week6/P2-W6A-C3.json` + stdout/URI thực + reviewer.
- [ ] **P2-W6B-C1 — Demo dedup và cutoff** — Chưa chạy. Evidence: `reports/evidence/week6/P2-W6B-C1.json` + stdout/URI thực + reviewer.
- [ ] **P2-W6B-C2 — Evidence Big Data truy vết** — Chưa chạy. Evidence: `reports/evidence/week6/P2-W6B-C2.json` + stdout/URI thực + reviewer.
- [ ] **P2-W6B-C3 — Người nhận lặp demo** — Chưa chạy. Evidence: `reports/evidence/week6/P2-W6B-C3.json` + stdout/URI thực + reviewer.

## P3 — Machine learning

- [ ] **P3-W6A-C1 — Truy vết số liệu model card** — Chưa chạy. Evidence: `reports/evidence/week6/P3-W6A-C1.json` + stdout/URI thực + reviewer.
- [ ] **P3-W6A-C2 — Gói tái lập đủ metadata** — Chưa chạy. Evidence: `reports/evidence/week6/P3-W6A-C2.json` + stdout/URI thực + reviewer.
- [ ] **P3-W6A-C3 — Bàn giao job I/O** — Chưa chạy. Evidence: `reports/evidence/week6/P3-W6A-C3.json` + stdout/URI thực + reviewer.
- [ ] **P3-W6B-C1 — Independent reload** — Chưa chạy. Evidence: `reports/evidence/week6/P3-W6B-C1.json` + stdout/URI thực + reviewer.
- [ ] **P3-W6B-C2 — Bản release nhất quán** — Chưa chạy. Evidence: `reports/evidence/week6/P3-W6B-C2.json` + stdout/URI thực + reviewer.
- [ ] **P3-W6B-C3 — Evidence demo có dự phòng** — Chưa chạy. Evidence: `reports/evidence/week6/P3-W6B-C3.json` + stdout/URI thực + reviewer.

## P4 — Streaming

- [ ] **P4-W6A-C1 — Clean run theo checklist** — Chưa chạy. Evidence: `reports/evidence/week6/P4-W6A-C1.json` + stdout/URI thực + reviewer.
- [ ] **P4-W6A-C2 — Resume khác fresh-run** — Chưa chạy. Evidence: `reports/evidence/week6/P4-W6A-C2.json` + stdout/URI thực + reviewer.
- [ ] **P4-W6A-C3 — Package và lỗi cấu hình** — Chưa chạy. Evidence: `reports/evidence/week6/P4-W6A-C3.json` + stdout/URI thực + reviewer.
- [ ] **P4-W6B-C1 — Regression release nhỏ** — Chưa chạy. Evidence: `reports/evidence/week6/P4-W6B-C1.json` + stdout/URI thực + reviewer.
- [ ] **P4-W6B-C2 — Demo đầu-cuối có bằng chứng** — Chưa chạy. Evidence: `reports/evidence/week6/P4-W6B-C2.json` + stdout/URI thực + reviewer.
- [ ] **P4-W6B-C3 — Giới hạn và bàn giao người dự phòng** — Chưa chạy. Evidence: `reports/evidence/week6/P4-W6B-C3.json` + stdout/URI thực + reviewer.

## P5 — API & Monitoring

- [ ] **P5-W6A-C1 — Người khác tái lập fixture mode** — Chưa chạy. Evidence: `reports/evidence/week6/P5-W6A-C1.json` + stdout/URI thực + reviewer.
- [ ] **P5-W6A-C2 — Tái lập run thật hữu hạn** — Chưa chạy. Evidence: `reports/evidence/week6/P5-W6A-C2.json` + stdout/URI thực + reviewer.
- [ ] **P5-W6A-C3 — Gói bàn giao sạch và gate nhạy lỗi** — Chưa chạy. Evidence: `reports/evidence/week6/P5-W6A-C3.json` + stdout/URI thực + reviewer.
- [ ] **P5-W6B-C1 — Demo theo kịch bản hữu hạn** — Chưa chạy. Evidence: `reports/evidence/week6/P5-W6B-C1.json` + stdout/URI thực + reviewer.
- [ ] **P5-W6B-C2 — Số liệu báo cáo truy được nguồn** — Chưa chạy. Evidence: `reports/evidence/week6/P5-W6B-C2.json` + stdout/URI thực + reviewer.
- [ ] **P5-W6B-C3 — Bàn giao đọc được kết quả fail** — Chưa chạy. Evidence: `reports/evidence/week6/P5-W6B-C3.json` + stdout/URI thực + reviewer.

## Dạng record cần bàn giao

```json
{
  "case": "P1-W6A-C1",
  "status": "INCONCLUSIVE",
  "operator": null,
  "reviewer": null,
  "revision": null,
  "command": null,
  "expected": null,
  "actual": null,
  "evidence_files": {},
  "reason": "NOT_RUN"
}
```

**Ghi chú:** Ca có failure chủ đích phải ghi `negative_test_rejected: true` và bằng chứng đúng loại, không coi bản ghi FAIL lẽ ra phải bị chặn là G5 FAIL nếu validator đã phát hiện chính xác. Người review cần xác nhận điều này theo checklist case.
