"""End-to-end PaySim ETL checks; run with PySpark inside the Spark container.

    python -m unittest discover -s tests -p 'test_etl.py' -v

The fixture deliberately includes malformed CSV and domain violations. Tests
use temporary output folders, so the original dataset and ETL outputs are safe.
"""

import hashlib
import shutil
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from src.processing.etl import DEFAULT_CONFIG_PATH, run_etl


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "paysim_etl.csv"
VALID_SOURCE_LINES = {2, 3, 4, 5, 23, 26}
EXPECTED_COUNTS = {
    "raw": 28,
    "valid": 6,
    "quarantine": 22,
    "duplicate_rows": 1,
    "processed": 6,
    "replay": 6,
    "labels": 6,
}
REPLAY_FIELDS = {
    "transaction_id",
    "step",
    "transaction_type",
    "amount",
    "source_account",
    "destination_account",
}


class PaySimEtlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_directory = tempfile.TemporaryDirectory(prefix="paysim etl tests ")
        cls.addClassCleanup(cls.temp_directory.cleanup)
        cls.temp_root = Path(cls.temp_directory.name)
        cls.fixture_input = cls.temp_root / "paysim fixture.csv"
        shutil.copyfile(FIXTURE, cls.fixture_input)
        cls.output_root = cls.temp_root / "outputs"

        cls.first_manifest = run_etl(
            DEFAULT_CONFIG_PATH,
            input_path=cls.fixture_input,
            output_root=cls.output_root,
            master="local[2]",
        )
        # Keep the first processed output, then rerun into the same destination.
        # Comparing these snapshots verifies IDs and overwrite behavior together.
        cls.first_processed_path = cls.temp_root / "first-processed"
        shutil.copytree(
            cls.first_manifest["outputs"]["processed"], cls.first_processed_path
        )
        cls.rerun_manifest = run_etl(
            DEFAULT_CONFIG_PATH,
            input_path=cls.fixture_input,
            output_root=cls.output_root,
            master="local[2]",
        )
        cls.limited_manifest = run_etl(
            DEFAULT_CONFIG_PATH,
            input_path=cls.fixture_input,
            output_root=cls.temp_root / "limited",
            limit=7,
            master="local[2]",
        )

        spark = (
            SparkSession.builder.appName("PaySimEtlReadbackTests")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        try:
            cls.first_processed_rows = spark.read.parquet(
                str(cls.first_processed_path)
            ).collect()
            cls.rows = {}
            cls.schemas = {}
            for name, path in cls.rerun_manifest["outputs"].items():
                if name not in {"processed", "quarantine", "replay", "labels"}:
                    continue
                dataframe = spark.read.parquet(str(path))
                cls.rows[name] = [row.asDict(recursive=True) for row in dataframe.collect()]
                cls.schemas[name] = {
                    field.name: field.dataType.simpleString()
                    for field in dataframe.schema.fields
                }
            limited = spark.read.parquet(
                str(cls.limited_manifest["outputs"]["processed"])
            )
            cls.limited_processed_rows = [row.asDict() for row in limited.collect()]
            cls.limited_quarantine_rows = [
                row.asDict(recursive=True)
                for row in spark.read.parquet(
                    str(cls.limited_manifest["outputs"]["quarantine"])
                ).collect()
            ]
        finally:
            spark.stop()

    def test_reconciled_counts_and_parquet_readback(self):
        for manifest in (self.first_manifest, self.rerun_manifest):
            for name, expected in EXPECTED_COUNTS.items():
                with self.subTest(run=manifest.get("run_id"), count=name):
                    self.assertEqual(manifest["counts"][name], expected)
            self.assertEqual(
                manifest["counts"]["raw"],
                manifest["counts"]["valid"] + manifest["counts"]["quarantine"],
            )
        for name in ("processed", "quarantine", "replay", "labels"):
            with self.subTest(output=name):
                self.assertEqual(len(self.rows[name]), EXPECTED_COUNTS[name])

    def test_quarantine_preserves_all_invalid_physical_records(self):
        original_lines = FIXTURE.read_text(encoding="utf-8").splitlines()
        expected_invalid_lines = set(range(2, 30)) - VALID_SOURCE_LINES
        self.assertEqual(
            {row["source_line"] for row in self.rows["quarantine"]},
            expected_invalid_lines,
        )
        for row in self.rows["quarantine"]:
            with self.subTest(source_line=row["source_line"]):
                self.assertEqual(row["raw_record"], original_lines[row["source_line"] - 1])
                self.assertTrue(row["error_reasons"], "Invalid rows need audit reasons")
                self.assertTrue(all(row["error_reasons"]))

    def test_normalization_and_non_destructive_validation(self):
        processed = {row["source_line"]: row for row in self.rows["processed"]}
        self.assertEqual(set(processed), VALID_SOURCE_LINES)
        self.assertEqual(processed[5]["transaction_type"], "TRANSFER")
        self.assertEqual(processed[5]["source_account"], "C400")
        self.assertEqual(processed[5]["destination_account"], "C500")
        self.assertEqual(processed[4]["amount"], 0.0)
        self.assertEqual(processed[23]["source_account"], "C,quoted")
        self.assertEqual(processed[26]["old_balance_origin"], -5.0)
        self.assertEqual(processed[26]["new_balance_origin"], -6.0)
        expected_types = {
            "transaction_id": "string",
            "step": "int",
            "transaction_type": "string",
            "amount": "double",
            "source_account": "string",
            "old_balance_origin": "double",
            "new_balance_origin": "double",
            "destination_account": "string",
            "old_balance_destination": "double",
            "new_balance_destination": "double",
            "label": "int",
            "existing_fraud_flag": "int",
        }
        for field, expected_type in expected_types.items():
            with self.subTest(field=field):
                self.assertEqual(self.schemas["processed"][field], expected_type)

    def test_ids_are_stable_and_duplicate_records_remain_distinct(self):
        first_ids = {
            row["source_line"]: row["transaction_id"]
            for row in self.first_processed_rows
        }
        rerun_ids = {
            row["source_line"]: row["transaction_id"]
            for row in self.rows["processed"]
        }
        self.assertEqual(first_ids, rerun_ids)
        self.assertEqual(len(set(rerun_ids.values())), EXPECTED_COUNTS["valid"])
        self.assertNotEqual(rerun_ids[2], rerun_ids[3])
        checksum = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
        for source_line, transaction_id in rerun_ids.items():
            with self.subTest(source_line=source_line):
                expected_id = hashlib.sha256(
                    f"{checksum}:{source_line}".encode("utf-8")
                ).hexdigest()
                self.assertEqual(transaction_id, expected_id)

    def test_replay_allowlist_and_ground_truth_join(self):
        self.assertEqual(set(self.schemas["replay"]), REPLAY_FIELDS)
        self.assertEqual(set(self.schemas["labels"]), {"transaction_id", "label"})
        processed_labels = {
            row["transaction_id"]: row["label"] for row in self.rows["processed"]
        }
        labels = {row["transaction_id"]: row["label"] for row in self.rows["labels"]}
        self.assertEqual(labels, processed_labels)
        self.assertEqual(
            {row["transaction_id"] for row in self.rows["replay"]},
            set(processed_labels),
        )

    def test_limit_counts_invalid_rows_and_preserves_original_ids(self):
        counts = self.limited_manifest["counts"]
        for name, expected in {
            "raw": 7,
            "valid": 4,
            "quarantine": 3,
            "duplicate_rows": 1,
            "processed": 4,
            "replay": 4,
            "labels": 4,
        }.items():
            with self.subTest(count=name):
                self.assertEqual(counts[name], expected)
        self.assertEqual(
            {row["source_line"] for row in self.limited_processed_rows}, {2, 3, 4, 5}
        )
        self.assertEqual(
            {row["source_line"] for row in self.limited_quarantine_rows}, {6, 7, 8}
        )
        full_ids = {
            row["source_line"]: row["transaction_id"] for row in self.rows["processed"]
        }
        for row in self.limited_processed_rows:
            self.assertEqual(row["transaction_id"], full_ids[row["source_line"]])

    def test_wrong_header_fails_before_parquet_output(self):
        wrong_header = self.temp_root / "wrong-header.csv"
        fixture_text = FIXTURE.read_text(encoding="utf-8")
        wrong_header.write_text(
            fixture_text.replace("step,type,", "step,wrong_type,", 1), encoding="utf-8"
        )
        bad_output = self.temp_root / "wrong-header-output"
        with self.assertRaises(ValueError):
            run_etl(
                DEFAULT_CONFIG_PATH,
                input_path=wrong_header,
                output_root=bad_output,
                master="local[2]",
            )
        self.assertFalse(list(bad_output.rglob("*.parquet")))


if __name__ == "__main__":
    unittest.main()
