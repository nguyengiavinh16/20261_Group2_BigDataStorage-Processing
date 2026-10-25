import hashlib, json, math

REQUIRED = {"schema_version","transaction_id","event_time","event_time_step","transaction_type","amount","source_dataset","source_row_id"}

def canonical_fingerprint(record):
    # Diagnostic only: equal payloads are not proof of the same real transaction.
    payload={k:v for k,v in record.items() if k not in {"transaction_id","source_row_id"}}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(",",":")).encode()).hexdigest()

def classify_duplicate(a,b):
    if a.get("transaction_id") == b.get("transaction_id"):
        return "DUPLICATE_TRANSACTION_ID"
    if canonical_fingerprint(a) == canonical_fingerprint(b):
        return "SAME_PAYLOAD_DIFFERENT_ID"
    return "DISTINCT"

def validate_transaction(record, known_categories=None):
    errors=[]; warnings=[]
    if not isinstance(record,dict):
        return {"status":"REJECT","errors":["event_must_be_json_object"],"warnings":[]}
    for key in sorted(REQUIRED):
        if key not in record: errors.append("missing_required_field:"+key)
    if "transaction_id" in record and (not isinstance(record["transaction_id"],str) or not record["transaction_id"].strip()):
        errors.append("transaction_id_must_be_non_empty_string")
    if "transaction_type" in record and (not isinstance(record["transaction_type"],str) or not record["transaction_type"].strip()):
        errors.append("transaction_type_must_be_non_empty_string")
    amount=record.get("amount")
    if isinstance(amount,bool) or not isinstance(amount,(int,float)) or not math.isfinite(float(amount)):
        errors.append("amount_must_be_number")
    elif amount < 0: errors.append("amount_must_be_non_negative")
    step=record.get("event_time_step")
    if "event_time_step" in record and (isinstance(step,bool) or not isinstance(step,int) or step<0):
        errors.append("event_time_step_must_be_non_negative_integer")
    if known_categories is not None and isinstance(record.get("transaction_type"),str) and record["transaction_type"] not in known_categories:
        warnings.append("unknown_transaction_type")
    return {"status":"REJECT" if errors else ("WARN" if warnings else "ACCEPT"),"errors":errors,"warnings":warnings}
