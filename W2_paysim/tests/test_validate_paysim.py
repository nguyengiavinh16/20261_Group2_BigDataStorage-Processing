"""Tests for the PaySim validator. Run: python -m unittest discover -s tests -v"""
import json
import os
import sys
import unittest

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from validate_paysim import COLUMNS, load_dataset, run_checks, structural_problems  # noqa: E402

DATA = os.path.join(HERE, "..", "test_data")


def errors(results):
    return [r for r in results if r["severity"] == "ERROR" and r["violations"] > 0]


def warns(results):
    return [r for r in results if r["severity"] == "WARN" and r["violations"] > 0]


class ValidSampleTests(unittest.TestCase):
    def test_csv_has_no_errors_or_warnings(self):
        raw = load_dataset(os.path.join(DATA, "paysim_valid_sample.csv"))
        self.assertEqual(structural_problems(raw), [])
        results = run_checks(raw)
        self.assertEqual(errors(results), [])
        self.assertEqual(warns(results), [])

    def test_json_matches_csv(self):
        csv_raw = load_dataset(os.path.join(DATA, "paysim_valid_sample.csv"))
        json_raw = load_dataset(os.path.join(DATA, "paysim_valid_sample.json"))
        self.assertEqual(len(csv_raw), len(json_raw))
        self.assertEqual(list(json_raw.columns), COLUMNS)
        self.assertEqual(errors(run_checks(json_raw)), [])

    def test_duplicate_row_is_warned(self):
        raw = load_dataset(os.path.join(DATA, "paysim_valid_sample.csv"))
        dup = pd.concat([raw, raw.iloc[:1]], ignore_index=True)
        codes = [r["code"] for r in warns(run_checks(dup))]
        self.assertIn("DUPLICATE_ROWS", codes)


class InvalidCaseTests(unittest.TestCase):
    def test_each_broken_row_triggers_its_rule(self):
        raw = load_dataset(os.path.join(DATA, "paysim_invalid_cases.csv"))
        with open(os.path.join(DATA, "invalid_cases_expected.json"), encoding="utf-8") as f:
            expected = json.load(f)
        by_code = {r["code"]: r for r in run_checks(raw, max_examples=len(raw))}
        for case in expected:
            with self.subTest(case=case["case"]):
                self.assertIn(case["row"], by_code[case["expected_code"]]["example_rows"])


class StructureTests(unittest.TestCase):
    def test_missing_column_is_reported(self):
        raw = load_dataset(os.path.join(DATA, "paysim_valid_sample.csv")).drop(columns=["isFraud"])
        self.assertTrue(any("isFraud" in p for p in structural_problems(raw)))


if __name__ == "__main__":
    unittest.main()
