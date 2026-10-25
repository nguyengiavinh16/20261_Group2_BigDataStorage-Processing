"""Validate thông điệp giao dịch theo JSON Schema trước khi đưa vào Kafka."""
import json
from datetime import datetime
from pathlib import Path

from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "transaction.schema.json"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

with open(SCHEMA_PATH, encoding="utf-8") as f:
    _VALIDATOR = Draft202012Validator(json.load(f))


def validate_transaction(message):
    """Trả về danh sách lỗi (rỗng nếu hợp lệ).

    Gồm lỗi theo schema và kiểm tra thêm mà schema không biểu diễn được
    (ngày giờ có tồn tại thật không).
    """
    errors = []
    for err in sorted(_VALIDATOR.iter_errors(message), key=lambda e: list(e.path)):
        path = ".".join(str(p) for p in err.path) or "<root>"
        errors.append(f"{path}: {err.message}")

    if not errors:
        try:
            datetime.strptime(message["trans_date_trans_time"], TIME_FORMAT)
        except ValueError:
            errors.append("trans_date_trans_time: không phải ngày giờ hợp lệ")
    return errors


def is_valid(message):
    return not validate_transaction(message)


def validate_json_string(raw):
    """Parse chuỗi JSON rồi validate. Trả về (message hoặc None, danh sách lỗi)."""
    try:
        message = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, [f"JSON không hợp lệ: {exc}"]
    return message, validate_transaction(message)


if __name__ == "__main__":
    import sys

    for path in sys.argv[1:]:
        _, errs = validate_json_string(Path(path).read_text(encoding="utf-8"))
        print(f"{path}: {'OK' if not errs else 'LỖI'}")
        for e in errs:
            print("  -", e)
