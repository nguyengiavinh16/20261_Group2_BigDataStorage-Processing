"""Bounded smoke-test consumer: read, print, and count Kafka messages."""
import argparse, json, os, time
def main():
    p=argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers",default=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"))
    p.add_argument("--topic",default=os.getenv("KAFKA_TOPIC","transactions"))
    p.add_argument("--count",type=int,default=20); p.add_argument("--timeout",type=float,default=30)
    p.add_argument("--group-id",default="fraud-consumer-smoke-test"); a=p.parse_args()
    if a.count<=0 or a.timeout<=0: p.error("--count and --timeout must be positive")
    try: from kafka import KafkaConsumer
    except ImportError: raise SystemExit("Install dependencies: pip install -r requirements-ingestion.txt")
    c=KafkaConsumer(a.topic,bootstrap_servers=a.bootstrap_servers.split(","),group_id=a.group_id,
      auto_offset_reset="earliest",enable_auto_commit=False,consumer_timeout_ms=500,
      key_deserializer=lambda k:k.decode() if k else None,value_deserializer=lambda v:json.loads(v.decode()))
    count=0; deadline=time.monotonic()+a.timeout
    print(f"Listening to {a.topic}; target={a.count}; timeout={a.timeout}s")
    try:
      while count<a.count and time.monotonic()<deadline:
        for m in c:
          count+=1
          print(json.dumps({"count":count,"partition":m.partition,"offset":m.offset,"key":m.key,"value":m.value}))
          if count>=a.count: break
    finally: c.close()
    result={"topic":a.topic,"received":count,"target":a.count,"status":"PASS" if count>=a.count else "FAIL"}
    print(json.dumps(result,indent=2))
    if count<a.count: raise SystemExit(1)
if __name__=="__main__": main()
