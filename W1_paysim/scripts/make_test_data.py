#!/usr/bin/env python3
"""Generate SYNTHETIC PaySim-shaped test data. These are NOT real PaySim records.

Writes:
  test_data/paysim_valid_sample.csv / .json  - rows that satisfy every rule
  test_data/paysim_invalid_cases.csv         - one deliberately broken row per case
  test_data/invalid_cases_expected.json      - which rule each broken row must trigger
"""
import json
import os

import numpy as np
import pandas as pd

from validate_paysim import COLUMNS, FLAG_AMOUNT_THRESHOLD, STEP_MAX

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "test_data")
rng = np.random.default_rng(20240601)
TYPE_WEIGHTS = {"CASH_IN": 0.22, "CASH_OUT": 0.35, "DEBIT": 0.07, "PAYMENT": 0.22, "TRANSFER": 0.14}


def _id(prefix: str) -> str:
    return f"{prefix}{int(rng.integers(10**8, 10**10))}"


def valid_rows(n: int) -> pd.DataFrame:
    rows = []
    for t in rng.choice(list(TYPE_WEIGHTS), size=n, p=list(TYPE_WEIGHTS.values())):
        amount = round(float(rng.lognormal(10, 1.2)), 2)
        step = int(rng.integers(1, STEP_MAX + 1))
        if t == "CASH_IN":
            old_o = round(float(rng.uniform(0, 5e5)), 2)
            new_o = round(old_o + amount, 2)
        else:
            old_o = round(amount + float(rng.uniform(0, 5e5)), 2)
            new_o = round(old_o - amount, 2)
        if t == "PAYMENT":
            dest, old_d, new_d = _id("M"), 0.0, 0.0
        else:
            dest = _id("C")
            old_d = round(float(rng.uniform(0, 1e6)), 2)
            new_d = round(old_d + amount, 2) if t == "TRANSFER" else old_d
        is_fraud = int(t in ("TRANSFER", "CASH_OUT") and rng.random() < 0.02)
        is_flagged = int(t == "TRANSFER" and amount > FLAG_AMOUNT_THRESHOLD and rng.random() < 0.5)
        rows.append([step, t, amount, _id("C"), old_o, new_o, dest, old_d, new_d, is_fraud, is_flagged])
    return pd.DataFrame(rows, columns=COLUMNS)


def invalid_cases(valid: pd.DataFrame):
    base = valid[valid["type"] == "TRANSFER"].iloc[0].to_dict()
    cases = [
        ("null_amount", "NULL_VALUES", lambda r: {**r, "amount": None}),
        ("bad_type", "TYPE_DOMAIN", lambda r: {**r, "type": "PAY"}),
        ("step_zero", "STEP_RANGE", lambda r: {**r, "step": 0}),
        ("negative_amount", "AMOUNT_NONNEGATIVE", lambda r: {**r, "amount": -5.0}),
        ("negative_balance", "BALANCE_NONNEGATIVE", lambda r: {**r, "oldbalanceOrg": -10.0}),
        ("fraud_label_2", "FRAUD_LABEL_DOMAIN", lambda r: {**r, "isFraud": 2}),
        ("bad_orig_prefix", "ORIG_ID_FORMAT", lambda r: {**r, "nameOrig": "M123"}),
        ("bad_dest_prefix", "DEST_ID_FORMAT", lambda r: {**r, "nameDest": "X999"}),
        ("flag_on_payment", "FLAG_RULE", lambda r: {**r, "type": "PAYMENT", "isFlaggedFraud": 1, "amount": 300000.0}),
        ("flag_under_threshold", "FLAG_RULE", lambda r: {**r, "isFlaggedFraud": 1, "amount": 1000.0}),
        ("origin_balance_mismatch", "ORIG_BALANCE_MATCH", lambda r: {**r, "newbalanceOrig": r["newbalanceOrig"] + 500.0}),
        ("transfer_dest_mismatch", "DEST_BALANCE_MATCH", lambda r: {**r, "newbalanceDest": r["oldbalanceDest"]}),
        ("merchant_dest_on_transfer", "MERCHANT_DEST_TYPE", lambda r: {**r, "nameDest": "M555"}),
        ("fraud_on_payment", "FRAUD_TYPE_PROFILE", lambda r: {**r, "type": "PAYMENT", "isFraud": 1}),
    ]
    rows, expected = [], []
    for idx, (name, code, mutate) in enumerate(cases):
        rows.append(mutate(base))
        expected.append({"row": idx, "case": name, "expected_code": code})
    return pd.DataFrame(rows, columns=COLUMNS), expected


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    valid = valid_rows(300)
    valid.to_csv(os.path.join(OUT, "paysim_valid_sample.csv"), index=False)
    with open(os.path.join(OUT, "paysim_valid_sample.json"), "w", encoding="utf-8") as f:
        json.dump(valid.astype(object).where(valid.notna(), None).to_dict(orient="records"), f, indent=2)
    bad, expected = invalid_cases(valid)
    bad.to_csv(os.path.join(OUT, "paysim_invalid_cases.csv"), index=False)
    with open(os.path.join(OUT, "invalid_cases_expected.json"), "w", encoding="utf-8") as f:
        json.dump(expected, f, indent=2)
    print(f"Wrote {len(valid)} valid rows and {len(bad)} invalid cases to {os.path.abspath(OUT)}")


if __name__ == "__main__":
    main()
