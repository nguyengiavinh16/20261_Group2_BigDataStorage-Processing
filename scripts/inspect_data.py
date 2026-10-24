"""Inspect a CSV schema and basic data-quality indicators without training a model."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys

import pandas as pd

# Allow direct execution from a source checkout without requiring installation.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from fraud_detection.logging_config import configure_logging  # noqa: E402

LOGGER = logging.getLogger("inspect_data")


def inspect_csv(path: Path, sample_rows: int = 100_000) -> dict[str, object]:
    """Inspect a CSV using a bounded sample for basic profiling."""
    if not path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    if sample_rows < 1:
        raise ValueError("sample_rows must be greater than zero")

    # Read the full header and a bounded number of rows to keep this Week 1 tool light.
    frame = pd.read_csv(path, nrows=sample_rows)
    if frame.empty:
        raise ValueError(f"CSV contains no data rows: {path}")

    result: dict[str, object] = {
        "file": str(path),
        "rows_inspected": int(len(frame)),
        "sample_limit": sample_rows,
        "columns": [
            {"name": str(column), "dtype": str(frame[column].dtype)}
            for column in frame.columns
        ],
        "missing_values_in_sample": {
            str(column): int(count)
            for column, count in frame.isna().sum().items()
        },
        "duplicate_rows_in_sample": int(frame.duplicated().sum()),
    }

    target = "isFraud"
    if target in frame.columns:
        counts = frame[target].value_counts(dropna=False)
        result["target_distribution_in_sample"] = {
            str(label): int(count) for label, count in counts.items()
        }
    else:
        result["target_distribution_in_sample"] = None

    return result


def main() -> int:
    """Parse CLI arguments, inspect the file, and print JSON results."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="Path to a CSV file")
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=100_000,
        help="Maximum rows to inspect (default: 100000)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the report as JSON",
    )
    args = parser.parse_args()
    configure_logging("INFO")

    try:
        report = inspect_csv(args.input, args.sample_rows)
    except (FileNotFoundError, ValueError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        LOGGER.error("Could not inspect dataset: %s", exc)
        return 2

    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
        LOGGER.info("Wrote inspection report to %s", args.output)
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
