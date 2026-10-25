import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.ingestion.validation import validate_json_string, validate_transaction  # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures"


def load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_valid_transaction_passes():
    assert validate_transaction(load("valid_transaction.json")) == []


@pytest.mark.parametrize(
    "fixture, expected_fragment",
    [
        ("invalid_missing_field.json", "amt"),
        ("invalid_wrong_type.json", "amt"),
        ("invalid_out_of_range.json", "lat"),
        ("invalid_bad_timestamp.json", "trans_date_trans_time"),
        ("invalid_extra_field.json", "debug_flag"),
    ],
)
def test_invalid_transactions_are_rejected(fixture, expected_fragment):
    errors = validate_transaction(load(fixture))
    assert errors, f"{fixture} lẽ ra phải bị từ chối"
    assert any(expected_fragment in e for e in errors)


def test_negative_amount_is_rejected():
    errors = validate_transaction(load("invalid_out_of_range.json"))
    assert any("amt" in e for e in errors)


def test_impossible_date_is_rejected():
    msg = load("valid_transaction.json")
    msg["trans_date_trans_time"] = "2019-02-30 10:00:00"
    assert any("trans_date_trans_time" in e for e in validate_transaction(msg))


def test_broken_json_string():
    message, errors = validate_json_string("{not json")
    assert message is None and errors
