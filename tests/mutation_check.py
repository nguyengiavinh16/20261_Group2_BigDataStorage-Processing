"""Kiểm tra bộ test có "răng": cố ý làm hỏng src/evaluation/feature_reference.py theo từng cách rồi xác nhận test bắt được.

Chạy từ thư mục gốc project:  python tests/mutation_check.py
Mỗi lỗi được áp dụng lên một bản sao trong thư mục tạm; mã nguồn thật không bị sửa. Thoát với mã 1 nếu có lỗi lọt qua.
"""
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE = Path("src") / "evaluation" / "feature_reference.py"

# (mô tả, đoạn mã gốc, đoạn mã bị làm hỏng)
MUTATIONS = [
    ("alert dùng > thay vì >=",
     'return _unit_interval(fraud_score, "fraud_score") >= _unit_interval(threshold, "threshold")',
     'return _unit_interval(fraud_score, "fraud_score") > _unit_interval(threshold, "threshold")'),
    ("cửa sổ lịch sử gồm cả cùng step", "step - HISTORY_WINDOW_STEPS <= s <= step - 1", "step - HISTORY_WINDOW_STEPS <= s <= step"),
    ("cửa sổ lịch sử lệch 1 ở cận dưới", "step - HISTORY_WINDOW_STEPS <= s <= step - 1", "step - HISTORY_WINDOW_STEPS + 1 <= s <= step - 1"),
    ("dung sai rút sạch quá rộng", "DRAIN_TOLERANCE = 0.005", "DRAIN_TOLERANCE = 0.01"),
    ("chấp nhận boolean làm số", "if isinstance(v, bool) or not isinstance(v, numbers.Real):", "if not isinstance(v, numbers.Real):"),
    ("hour_of_day lệch 1", "(resolve_step(event, t0) - 1) % 24", "(resolve_step(event, t0)) % 24"),
    ("hòa precision chọn recall thấp hơn", "np.lexsort((thr[idx], recall[idx], precision[idx]))[-1]", "np.lexsort((thr[idx], -recall[idx], precision[idx]))[-1]"),
    ("không loại transaction_id trùng", "        if event[F_ID] in seen_ids:\n            continue\n", ""),
    ("tự chuẩn hóa chữ hoa", "if v not in TRANSACTION_TYPES:", "if v.strip().upper() not in TRANSACTION_TYPES:"),
    ("không yêu cầu số dư ở set B (thiếu số dư bị bỏ qua)", "required += [F_OLD_SRC, F_OLD_DEST]", "pass"),
    ("đổi múi giờ sai khi đọc event_time", "return dt.astimezone(timezone.utc)", "return dt.replace(tzinfo=timezone.utc)"),
    ("bỏ kiểm tra trường cấm", "    found = forbidden_fields(event)\n    if found:", "    found = ()\n    if found:"),
]


def run_tests(workdir: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    return subprocess.run([sys.executable, "-m", "pytest", "tests/test_feature_reference.py", "-q", "-x", "--no-header", "--color=no",
                           "-p", "no:cacheprovider"], cwd=workdir, capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)


def fresh_copy(dest: Path) -> None:
    ignore = shutil.ignore_patterns("__pycache__", ".pytest_cache", "mutation_check.py")
    shutil.copytree(ROOT / "src", dest / "src", ignore=ignore)
    shutil.copytree(ROOT / "tests", dest / "tests", ignore=ignore)


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    survivors = []
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp) / "copy"
        fresh_copy(work)
        baseline = run_tests(work)
        print("Bản gốc:", baseline.stdout.strip().splitlines()[-1])
        if baseline.returncode != 0:
            print("Bản gốc phải qua toàn bộ test trước khi kiểm tra đột biến.")
            return 1
        for name, old, new in MUTATIONS:
            shutil.rmtree(work)
            fresh_copy(work)
            target = work / MODULE
            text = target.read_text(encoding="utf-8")
            if old not in text:
                print(f"[KHÔNG ÁP DỤNG ĐƯỢC] {name}: mã nguồn đã đổi, cập nhật MUTATIONS")
                survivors.append(name)
                continue
            target.write_text(text.replace(old, new, 1), encoding="utf-8")
            result = run_tests(work)
            caught = result.returncode != 0
            print(f"[{'BẮT ĐƯỢC' if caught else 'LỌT QUA  '}] {name}")
            if not caught:
                survivors.append(name)
    print(f"\n{len(MUTATIONS) - len(survivors)}/{len(MUTATIONS)} lỗi cố ý bị test bắt được.")
    return 1 if survivors else 0


if __name__ == "__main__":
    raise SystemExit(main())
