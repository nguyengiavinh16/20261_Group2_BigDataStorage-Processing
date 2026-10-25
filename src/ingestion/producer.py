"""Replay historical CSV rows to Kafka; this is not a live feed."""
import argparse, csv, hashlib, json, logging, os, time
from datetime import datetime, timedelta
from pathlib import Path
from src.ingestion.validation import validate_transaction

def map_row(row, file_id, row_number, anchor):
    missing={"step","type","amount"}-set(row)
    if missing: raise ValueError(f"Missing source columns: {sorted(missing)}")
    step=int(row["step"])
    if step<0: raise ValueError("step must be non-negative")
    event={
      "schema_version":"1.0","transaction_id":f"paysim-{file_id}-row-{row_number:012d}",
      "event_time":(anchor+timedelta(hours=step)).isoformat().replace("+00:00","Z"),
      "event_time_step":step,"transaction_type":row["type"],"amount":float(row["amount"]),
      "source_dataset":"paysim","source_row_id":f"{file_id}:{row_number}"}
    mapping={"nameOrig":"origin_account_id","nameDest":"destination_account_id",
      "oldbalanceOrg":"origin_balance_before","newbalanceOrig":"origin_balance_after",
      "oldbalanceDest":"destination_balance_before","newbalanceDest":"destination_balance_after"}
    for src,dst in mapping.items():
      if src in row:
        if "balance_" in dst: event[dst]=float(row[src]) if row[src].strip() else None
        else: event[dst]=row[src] or None
    # Do not include isFraud/isFlaggedFraud in the scoring event.
    return event

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--csv",required=True); p.add_argument("--bootstrap-servers",default=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"))
    p.add_argument("--topic",default=os.getenv("KAFKA_TOPIC","transactions")); p.add_argument("--rate",type=float,default=5)
    p.add_argument("--max-records",type=int,default=50); p.add_argument("--simulation-start",default="2026-01-01T00:00:00+00:00")
    p.add_argument("--known-categories",default="CASH_IN,CASH_OUT,DEBIT,PAYMENT,TRANSFER")
    p.add_argument("--log-file",default="logs/producer.log"); a=p.parse_args()
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s",
      handlers=[logging.StreamHandler(),logging.FileHandler(a.log_file,encoding="utf-8")])
    if a.rate<0 or a.max_records<0: p.error("--rate and --max-records must be non-negative")
    path=Path(a.csv)
    if not path.is_file(): p.error(f"CSV not found: {path}")
    try: from kafka import KafkaProducer
    except ImportError: raise SystemExit("Install dependencies: pip install -r requirements-ingestion.txt")
    digest=hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    anchor=datetime.fromisoformat(a.simulation_start.replace("Z","+00:00"))
    if anchor.tzinfo is None: p.error("--simulation-start must include timezone")
    known={x.strip() for x in a.known_categories.split(",") if x.strip()}
    producer=KafkaProducer(bootstrap_servers=a.bootstrap_servers.split(","),key_serializer=lambda k:k.encode(),
      value_serializer=lambda v:json.dumps(v,separators=(",",":")).encode(),acks="all",retries=3)
    sent=rejected=warned=0; logging.info("HISTORICAL REPLAY (not live): topic=%s",a.topic)
    try:
      with path.open(encoding="utf-8-sig",newline="") as f:
        reader=csv.DictReader(f)
        for n,row in enumerate(reader,1):
          if a.max_records and sent>=a.max_records: break
          try:
            event=map_row(row,digest,n,anchor); result=validate_transaction(event,known or None)
            if result["status"]=="REJECT":
              rejected+=1; logging.warning("Reject row=%s errors=%s",n,result["errors"]); continue
            if result["warnings"]:
              warned+=1; logging.warning("Unknown category row=%s",n)
            producer.send(a.topic,key=event["transaction_id"],value=event); sent+=1
            if a.rate>0: time.sleep(1/a.rate)
          except (ValueError,TypeError) as e:
            rejected+=1; logging.warning("Reject row=%s reason=%s",n,e)
      producer.flush(timeout=30)
    finally: producer.close(timeout=10)
    print(json.dumps({"sent":sent,"rejected":rejected,"warned":warned},indent=2))

if __name__=="__main__": main()
