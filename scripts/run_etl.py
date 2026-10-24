import argparse, json
from fraud_detection.processing.etl import convert_csv_to_parquet
p=argparse.ArgumentParser(); p.add_argument('--input',default='data/raw/paysim_dataset.csv'); p.add_argument('--output',default='data/processed/paysim.parquet'); p.add_argument('--chunksize',type=int,default=250000)
a=p.parse_args(); print(json.dumps(convert_csv_to_parquet(a.input,a.output,a.chunksize),indent=2))
