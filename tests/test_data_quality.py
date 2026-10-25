import json, unittest
from pathlib import Path
from src.ingestion.validation import validate_transaction, classify_duplicate, canonical_fingerprint
ROOT = Path(__file__).resolve().parents[1]
def valid():
    return json.loads((ROOT/"fixtures/valid/transaction-001.json").read_text(encoding="utf-8"))

class DataQualityTests(unittest.TestCase):
    def test_valid_record_accepted(self):
        self.assertEqual(validate_transaction(valid(), {"PAYMENT","CASH_OUT"})["status"], "ACCEPT")
    def test_negative_amount_rejected(self):
        row=valid(); row["amount"]=-1
        result=validate_transaction(row, {"PAYMENT","CASH_OUT"})
        self.assertEqual(result["status"],"REJECT")
        self.assertIn("amount_must_be_non_negative",result["errors"])
    def test_null_amount_rejected(self):
        row=valid(); row["amount"]=None
        self.assertIn("amount_must_be_number",validate_transaction(row)["errors"])
    def test_unknown_category_warns(self):
        row=valid(); row["transaction_type"]="UNKNOWN"
        self.assertEqual(validate_transaction(row,{"PAYMENT"})["status"],"WARN")
    def test_same_transaction_id_is_duplicate(self):
        a=valid(); b=dict(a,amount=999)
        self.assertEqual(classify_duplicate(a,b),"DUPLICATE_TRANSACTION_ID")
    def test_same_payload_different_ids_not_assumed_same_transaction(self):
        a=valid(); b=dict(a,transaction_id="other-id",source_row_id="other-row")
        self.assertEqual(canonical_fingerprint(a),canonical_fingerprint(b))
        self.assertEqual(classify_duplicate(a,b),"SAME_PAYLOAD_DIFFERENT_ID")

if __name__ == "__main__": unittest.main()
