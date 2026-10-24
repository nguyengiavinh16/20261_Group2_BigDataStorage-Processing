"""Spark Structured Streaming scorer. Requires Kafka, PySpark, and a trained model artifact."""
import os
from pyspark.sql import SparkSession, functions as F, types as T
import joblib
import pandas as pd

SCHEMA = T.StructType([
 T.StructField("step",T.IntegerType()), T.StructField("type",T.StringType()),
 T.StructField("amount",T.DoubleType()), T.StructField("nameOrig",T.StringType()),
 T.StructField("oldbalanceOrg",T.DoubleType()), T.StructField("newbalanceOrig",T.DoubleType()),
 T.StructField("nameDest",T.StringType()), T.StructField("oldbalanceDest",T.DoubleType()),
 T.StructField("newbalanceDest",T.DoubleType()), T.StructField("isFraud",T.IntegerType()),
 T.StructField("isFlaggedFraud",T.IntegerType())])


def main():
    bootstrap=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
    topic=os.getenv('KAFKA_TOPIC','transactions')
    model_path=os.getenv('MODEL_PATH','artifacts/fraud_model.joblib')
    mongo_uri=os.getenv('MONGODB_URI','mongodb://localhost:27017')
    database=os.getenv('MONGODB_DATABASE','fraud_detection')
    collection=os.getenv('MONGODB_COLLECTION','predictions')
    bundle=joblib.load(model_path); model=bundle['model']; threshold=float(os.getenv('FRAUD_THRESHOLD',bundle.get('threshold',0.5)))
    spark=(SparkSession.builder.appName('fraud-streaming-scoring').getOrCreate())
    schema_json=SCHEMA.json()
    raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',bootstrap)
         .option('subscribe',topic).option('startingOffsets','latest').load())
    parsed=(raw.select(F.from_json(F.col('value').cast('string'),schema_json).alias('v'),F.col('timestamp').alias('kafka_timestamp'))
            .select('v.*','kafka_timestamp').filter(F.col('amount').isNotNull()))
    @F.udf(T.DoubleType())
    def probability(step, typ, amount, old_org, new_org, old_dest, new_dest, flagged):
        row={"step":step,"type":typ,"amount":amount,"oldbalanceOrg":old_org,"newbalanceOrig":new_org,
             "oldbalanceDest":old_dest,"newbalanceDest":new_dest,"isFlaggedFraud":flagged}
        frame=pd.DataFrame([row])
        from fraud_detection.processing.features import prepare_features
        return float(model.predict_proba(prepare_features(frame))[0,1])
    scored=(parsed.withColumn('fraud_probability',probability('step','type','amount','oldbalanceOrg','newbalanceOrig','oldbalanceDest','newbalanceDest','isFlaggedFraud'))
            .withColumn('is_fraud',F.col('fraud_probability')>=F.lit(threshold))
            .withColumn('threshold',F.lit(threshold)))
    def persist_batch(batch_df, batch_id):
        records=[r.asDict(recursive=True) for r in batch_df.toLocalIterator()]
        if not records: return
        from pymongo import MongoClient, UpdateOne
        for r in records:
            r['stream_batch_id']=int(batch_id)
            r['transaction_key']=f"{r.get('step')}:{r.get('nameOrig')}:{r.get('nameDest')}:{r.get('amount')}"
        with MongoClient(mongo_uri,serverSelectionTimeoutMS=3000) as client:
            coll=client[database][collection]
            coll.bulk_write([UpdateOne({'transaction_key':r['transaction_key']},{'$set':r},upsert=True) for r in records])
        print(f'Persisted batch={batch_id} records={len(records)}')
    query=(scored.writeStream.foreachBatch(persist_batch).option('checkpointLocation','data/checkpoints/fraud-stream').start())
    query.awaitTermination()
if __name__=='__main__': main()
