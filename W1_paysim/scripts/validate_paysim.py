#!/usr/bin/env python3
"""PaySim dataset validator and data quality reporter.

Usage:
    python validate_paysim.py data.csv
    python validate_paysim.py data.json --report reports/dq.md

Exit codes: 0 = no ERROR-level violations, 1 = ERROR-level violations found,
            2 = input could not be read.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]
BALANCE_COLS = ["oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]
TYPES = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
STEP_MIN, STEP_MAX = 1, 744
FLAG_AMOUNT_THRESHOLD = 200_000.0
BALANCE_TOLERANCE = 0.01
MAX_EXAMPLES = 5


def load_dataset(path: str, fmt: str = "auto") -> pd.DataFrame:
    if fmt == "auto":
        lower = path.lower()
        fmt = "csv" if lower.endswith(".csv") else "ndjson" if lower.endswith((".ndjson", ".jsonl")) else "json"
    if fmt == "csv":
        return pd.read_csv(path, dtype={"nameOrig": str, "nameDest": str, "type": str})
    if fmt == "ndjson":
        with open(path, encoding="utf-8") as f:
            return pd.DataFrame([json.loads(line) for line in f if line.strip()])
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict) and "records" in payload:
        payload = payload["records"]
    if not isinstance(payload, list):
        raise ValueError("JSON input must be an array of records or an object with a 'records' array")
    return pd.DataFrame(payload)


def structural_problems(raw: pd.DataFrame) -> list[str]:
    problems = []
    missing = [c for c in COLUMNS if c not in raw.columns]
    extra = [c for c in raw.columns if c not in COLUMNS]
    if missing:
        problems.append(f"missing columns: {missing}")
    if extra:
        problems.append(f"unexpected columns: {extra}")
    return problems


def _coerce(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    for c in ["step", "amount", "isFraud", "isFlaggedFraud", *BALANCE_COLS]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["type", "nameOrig", "nameDest"]:
        df[c] = df[c].fillna("").astype(str)
    return df


def run_checks(raw: pd.DataFrame, max_examples: int = MAX_EXAMPLES) -> list[dict]:
    """Run every rule. Returns one result dict per rule (ERROR or WARN)."""
    df = _coerce(raw)
    results: list[dict] = []

    def add(code: str, severity: str, description: str, mask) -> None:
        mask = pd.Series(mask, index=df.index).fillna(False).astype(bool)
        results.append({
            "code": code,
            "severity": severity,
            "description": description,
            "violations": int(mask.sum()),
            "example_rows": [int(i) for i in df.index[mask][:max_examples]],
        })

    is_cash_in = df["type"].eq("CASH_IN")
    is_transfer = df["type"].eq("TRANSFER")
    to_customer = df["nameDest"].str.startswith("C")
    to_merchant = df["nameDest"].str.startswith("M")

    # ---- ERROR rules: a violation makes the dataset fail ----
    add("NULL_VALUES", "ERROR", "No missing values in any required column",
        raw[COLUMNS].isna().any(axis=1))
    add("TYPE_DOMAIN", "ERROR", f"type must be one of {', '.join(TYPES)}",
        ~df["type"].isin(TYPES))
    add("STEP_RANGE", "ERROR", f"step must be an integer in [{STEP_MIN}, {STEP_MAX}]",
        ~df["step"].between(STEP_MIN, STEP_MAX) | (df["step"] % 1 != 0))
    add("AMOUNT_NONNEGATIVE", "ERROR", "amount must not be negative",
        df["amount"] < 0)
    add("BALANCE_NONNEGATIVE", "ERROR", "All balance fields must be >= 0",
        (df[BALANCE_COLS] < 0).any(axis=1))
    add("FRAUD_LABEL_DOMAIN", "ERROR", "isFraud must be 0 or 1",
        ~df["isFraud"].isin([0, 1]))
    add("FLAG_LABEL_DOMAIN", "ERROR", "isFlaggedFraud must be 0 or 1",
        ~df["isFlaggedFraud"].isin([0, 1]))
    add("ORIG_ID_FORMAT", "ERROR", "nameOrig must be 'C' followed by digits",
        ~df["nameOrig"].str.fullmatch(r"C\d+"))
    add("DEST_ID_FORMAT", "ERROR", "nameDest must be 'C' or 'M' followed by digits",
        ~df["nameDest"].str.fullmatch(r"[CM]\d+"))
    add("FLAG_RULE", "ERROR",
        f"isFlaggedFraud=1 only for TRANSFER with amount > {FLAG_AMOUNT_THRESHOLD:,.0f}",
        (df["isFlaggedFraud"] == 1) & ~(is_transfer & (df["amount"] > FLAG_AMOUNT_THRESHOLD)))

    # ---- WARN rules: suspicious but do not fail the dataset ----
    signed_amount = np.where(is_cash_in, 1.0, -1.0) * df["amount"]
    expected_new_orig = df["oldbalanceOrg"] + signed_amount
    add("ORIG_BALANCE_MATCH", "WARN",
        "newbalanceOrig should equal oldbalanceOrg + amount (CASH_IN) or - amount (others)",
        (df["newbalanceOrig"] - expected_new_orig).abs() > BALANCE_TOLERANCE)
    add("DEST_BALANCE_MATCH", "WARN",
        "TRANSFER to a customer: newbalanceDest should equal oldbalanceDest + amount",
        is_transfer & to_customer &
        ((df["newbalanceDest"] - (df["oldbalanceDest"] + df["amount"])).abs() > BALANCE_TOLERANCE))
    add("MERCHANT_DEST_NO_BALANCE", "WARN",
        "Merchant destinations (M*) are expected to carry zero destination balances",
        to_merchant & (df[["oldbalanceDest", "newbalanceDest"]] != 0).any(axis=1))
    add("MERCHANT_DEST_TYPE", "WARN",
        "Merchant destinations (M*) are expected only for PAYMENT",
        to_merchant & ~df["type"].eq("PAYMENT"))
    add("FRAUD_TYPE_PROFILE", "WARN",
        "Fraud labels are expected only on TRANSFER and CASH_OUT",
        (df["isFraud"] == 1) & ~df["type"].isin(["TRANSFER", "CASH_OUT"]))
    add("AMOUNT_ZERO", "WARN", "amount equals 0 (observed in source data; review before use)",
        df["amount"] == 0)
    add("DUPLICATE_ROWS", "WARN", "Exact duplicate rows",
        raw.duplicated(keep="first"))
    return results


def profile(raw: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    df = _coerce(raw)
    by_type = df.groupby("type").agg(
        count=("amount", "size"),
        fraud=("isFraud", "sum"),
        amount_median=("amount", "median"),
        amount_sum=("amount", "sum"),
    )
    prof = {
        "rows": int(len(raw)),
        "nulls": {c: int(raw[c].isna().sum()) for c in COLUMNS},
        "duplicate_rows": int(raw.duplicated().sum()),
        "step_min": int(df["step"].min()),
        "step_max": int(df["step"].max()),
        "fraud_total": int(df["isFraud"].sum()),
        "flagged_total": int(df["isFlaggedFraud"].sum()),
        "amount_quantiles": {str(q): float(df["amount"].quantile(q)) for q in (0, 0.5, 0.9, 0.99, 1)},
    }
    return prof, by_type


def render_markdown(source: str, results: list[dict], prof: dict, by_type: pd.DataFrame) -> str:
    errors = [r for r in results if r["severity"] == "ERROR" and r["violations"] > 0]
    warns = [r for r in results if r["severity"] == "WARN" and r["violations"] > 0]
    verdict = "FAIL" if errors else ("PASS (with warnings)" if warns else "PASS")
    L = [
        "# PaySim Data Quality Report", "",
        f"- Source: `{source}`",
        f"- Generated (UTC): {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"- Rows: {prof['rows']:,}",
        f"- Verdict: **{verdict}**", "",
        "## Validation checks", "",
        "| Code | Severity | Violations | Rule |", "|---|---|---:|---|",
    ]
    for r in results:
        L.append(f"| {r['code']} | {r['severity']} | {r['violations']:,} | {r['description']} |")

    flagged = [r for r in results if r["violations"] > 0]
    if flagged:
        L += ["", "## Example violating rows (0-based row index in the file)", ""]
        for r in flagged:
            L.append(f"- `{r['code']}`: {r['example_rows']}")

    L += ["", "## Completeness", "", "| Column | Missing values |", "|---|---:|"]
    for c, n in prof["nulls"].items():
        L.append(f"| {c} | {n:,} |")
    L.append(f"\nDuplicate rows: {prof['duplicate_rows']:,}")

    L += ["", "## Transaction type profile", "",
          "| Type | Rows | Fraud | Fraud rate | Median amount | Total amount |",
          "|---|---:|---:|---:|---:|---:|"]
    for t, row in by_type.iterrows():
        rate = row["fraud"] / row["count"] if row["count"] else 0.0
        L.append(f"| {t} | {int(row['count']):,} | {int(row['fraud']):,} | {rate:.4%} | "
                 f"{row['amount_median']:,.2f} | {row['amount_sum']:,.2f} |")

    L += ["", "## Other statistics", "",
          f"- step range observed: {prof['step_min']} to {prof['step_max']} (allowed {STEP_MIN} to {STEP_MAX})",
          f"- Fraud rows (isFraud=1): {prof['fraud_total']:,}",
          f"- Flagged rows (isFlaggedFraud=1): {prof['flagged_total']:,}",
          "- amount quantiles: " + ", ".join(f"p{float(q)*100:g}={v:,.2f}" for q, v in prof["amount_quantiles"].items())]
    return "\n".join(L) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate a PaySim dataset and write a data quality report.")
    p.add_argument("input")
    p.add_argument("--format", default="auto", choices=["auto", "csv", "json", "ndjson"])
    p.add_argument("--report", help="Write the Markdown report to this path instead of stdout")
    p.add_argument("--max-examples", type=int, default=MAX_EXAMPLES)
    args = p.parse_args(argv)

    try:
        raw = load_dataset(args.input, args.format)
    except Exception as exc:  # noqa: BLE001 - surface any read error to the user
        print(f"ERROR: cannot read input: {exc}", file=sys.stderr)
        return 2

    problems = structural_problems(raw)
    if problems:
        print("STRUCTURAL FAILURE:", *problems, sep="\n  - ", file=sys.stderr)
        return 1

    results = run_checks(raw, args.max_examples)
    prof, by_type = profile(raw)
    md = render_markdown(args.input, results, prof, by_type)
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"Report written to {args.report}")
    else:
        print(md)
    has_error = any(r["severity"] == "ERROR" and r["violations"] > 0 for r in results)
    return 1 if has_error else 0


if __name__ == "__main__":
    sys.exit(main())
