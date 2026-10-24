from pathlib import Path
import sys

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from inspect_data import inspect_csv  # noqa: E402


def test_inspector_reports_schema_and_label_distribution(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    pd.DataFrame(
        {
            "step": [1, 2, 2],
            "type": ["PAYMENT", "TRANSFER", "TRANSFER"],
            "isFraud": [0, 1, 1],
        }
    ).to_csv(csv_path, index=False)

    report = inspect_csv(csv_path)
    assert report["rows_inspected"] == 3
    assert [item["name"] for item in report["columns"]] == ["step", "type", "isFraud"]
    assert report["target_distribution_in_sample"] == {"1": 2, "0": 1}
    assert report["duplicate_rows_in_sample"] == 1


def test_inspector_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        inspect_csv(tmp_path / "missing.csv")


def test_inspector_rejects_empty_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("step,type,isFraud\\n", encoding="utf-8")
    with pytest.raises(ValueError):
        inspect_csv(csv_path)
