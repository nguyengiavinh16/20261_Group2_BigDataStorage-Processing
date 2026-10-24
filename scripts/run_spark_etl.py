"""Spark CSV-to-Parquet job for larger files; run where Spark can access the input path."""
import argparse
from pyspark.sql import SparkSession, functions as F
p=argparse.ArgumentParser(); p.add_argument('--input',default='data/raw/paysim_dataset.csv'); p.add_argument('--output',default='data/processed/paysim_parquet'); p.add_argument('--mode',default='overwrite',choices=['overwrite','append','errorifexists']); a=p.parse_args()
spark=SparkSession.builder.appName('paysim-etl').getOrCreate()
df=spark.read.option('header',True).option('inferSchema',True).csv(a.input)
required={'step','type','amount','oldbalanceOrg','newbalanceOrig','oldbalanceDest','newbalanceDest','isFraud','isFlaggedFraud','nameOrig','nameDest'}
missing=required-set(df.columns)
if missing: raise ValueError(f'Missing columns: {sorted(missing)}')
for c in ['amount','oldbalanceOrg','newbalanceOrig','oldbalanceDest','newbalanceDest']:
    df=df.withColumn(c,F.col(c).cast('double'))
df=df.withColumn('isFraud',F.col('isFraud').cast('int')).withColumn('isFlaggedFraud',F.col('isFlaggedFraud').cast('int'))
print('rows=',df.count()); df.write.mode(a.mode).parquet(a.output); spark.stop()
