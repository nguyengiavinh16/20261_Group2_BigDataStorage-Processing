"""Replay CSV transactions into Kafka in source-file order."""
import argparse, json, time
import pandas as pd
from kafka import KafkaProducer

def main():
    p=argparse.ArgumentParser(); p.add_argument('--input',default='data/raw/paysim_dataset.csv')
    p.add_argument('--bootstrap-servers',default='localhost:9092'); p.add_argument('--topic',default='transactions')
    p.add_argument('--limit',type=int,default=10000); p.add_argument('--delay-ms',type=float,default=0)
    a=p.parse_args()
    producer=KafkaProducer(bootstrap_servers=a.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode('utf-8'),
        key_serializer=lambda key: key.encode('utf-8') if key else None,
        retries=5, acks='all')
    for i, row in enumerate(pd.read_csv(a.input, nrows=a.limit).to_dict(orient='records')):
        key=str(row.get('nameOrig', i))
        producer.send(a.topic, key=key, value=row)
        if i and i % 1000 == 0: producer.flush(); print(f'Published {i} transactions')
        if a.delay_ms: time.sleep(a.delay_ms/1000)
    producer.flush(); producer.close(); print(f'Published {min(a.limit, i+1 if "i" in locals() else 0)} records')
if __name__=='__main__': main()
