# team/: context files cho từng thành viên (và AI agent của họ)

| File | Dành cho |
|---|---|
| [00-shared-context.md](00-shared-context.md) | **Tất cả mọi người** (đọc trước) |
| [01-platform-infra.md](01-platform-infra.md) | R1: Platform & Infra |
| [02-ingestion-normalization.md](02-ingestion-normalization.md) | R2: Ingestion & Stream Normalization |
| [03-realtime-detection.md](03-realtime-detection.md) | R3: Real-time Detection |
| [04-batch-lakehouse-graph.md](04-batch-lakehouse-graph.md) | R4: Batch, Lakehouse & Graph |
| [05-ml-serving-evaluation.md](05-ml-serving-evaluation.md) | R5: ML, Serving & Evaluation |

## Cách dùng với AI agent (Claude Code, Cursor, Copilot…)

1. Mở agent **tại thư mục gốc repo** (để agent đọc được `AGENTS.md` / `CLAUDE.md`).
2. Câu mở đầu mẫu:

   > Tôi là R2. Đọc `team/00-shared-context.md` và `team/02-ingestion-normalization.md`, sau đó làm task **ING-03**.
   > Chỉ sửa file trong phạm vi của R2. Trước khi code, tóm tắt lại input/output và acceptance criteria của task.

3. Khi agent xong: chạy `make lint test`, đối chiếu checklist acceptance trong file vai trò, cập nhật mục **Task board** (đánh dấu `[x]`), mở PR theo `CONTRIBUTING.md`.

## Quy ước cập nhật file

- Mỗi người **tự cập nhật Task board** trong file của mình (trạng thái, link PR, ghi chú).
- Thay đổi phạm vi hoặc input/output giữa các vai trò: sửa cả hai file liên quan trong cùng một PR và tag người kia review.
- `00-shared-context.md` chỉ sửa khi cả nhóm đã thống nhất (ghi vào biên bản họp hoặc ADR).
