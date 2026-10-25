import json, unittest
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
TX = json.loads((ROOT/"schemas/transaction-event.json").read_text(encoding="utf-8"))
PRED = json.loads((ROOT/"schemas/prediction-event.json").read_text(encoding="utf-8"))

def fixture(rel):
    return json.loads((ROOT/"fixtures"/rel).read_text(encoding="utf-8"))

def errors(record, schema):
    return list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(record))

class SchemaTests(unittest.TestCase):
    def test_valid_transaction_has_required_fields(self):
        row = fixture("valid/transaction-001.json")
        self.assertTrue(set(TX["required"]).issubset(row))
        self.assertEqual(errors(row, TX), [])

    def test_valid_prediction(self):
        row = fixture("valid/prediction-001.json")
        self.assertTrue(set(PRED["required"]).issubset(row))
        self.assertEqual(errors(row, PRED), [])

    def test_missing_required_field(self):
        row = fixture("valid/transaction-001.json"); row.pop("transaction_id")
        self.assertTrue(errors(row, TX))

    def test_amount_string_rejected(self):
        row = fixture("valid/transaction-001.json"); row["amount"] = "125.75"
        self.assertTrue(errors(row, TX))

    def test_invalid_timestamp_rejected(self):
        row = fixture("valid/transaction-001.json"); row["event_time"] = "not-a-time"
        self.assertTrue(errors(row, TX))

    def test_malformed_json_rejected(self):
        raw = (ROOT/"fixtures/invalid/malformed-json.json").read_text(encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError): json.loads(raw)

    def test_unknown_category_warns_but_schema_accepts(self):
        from src.ingestion.validation import validate_transaction
        row = fixture("valid/transaction-001.json"); row["transaction_type"] = "UNKNOWN_TYPE"
        self.assertEqual(errors(row, TX), [])
        result = validate_transaction(row, {"PAYMENT", "CASH_OUT"})
        self.assertEqual(result["status"], "WARN")
        self.assertIn("unknown_transaction_type", result["warnings"])

if __name__ == "__main__": unittest.main()
