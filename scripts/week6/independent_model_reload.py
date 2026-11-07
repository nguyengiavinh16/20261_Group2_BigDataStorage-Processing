"""Independent Spark process: pinned model -> frozen feature rows -> golden score comparator.

Run with spark-submit and matching Iceberg/HDFS Python environment. An absent
artifact or mismatched ID set MUST not be counted as a successful reload.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path


def compare(spark, *, artifact, features, expected, model_kind, tolerance=1e-8):
    from pyspark.sql import functions as F
    if not artifact or not features or not expected:raise ValueError('EXPLICIT_ARTIFACT_FEATURE_GOLD_URIS_REQUIRED')
    if tolerance<0 or tolerance>1e-4:raise ValueError('TOLERANCE_OUTSIDE_FROZEN_SCOPE')
    if model_kind=='A':
        from src.ml.lr_w2 import load,predict_features
        model,manifest=load(artifact)
        predictor=lambda d:predict_features(model,manifest,d)
    elif model_kind=='A+H':
        from src.ml.lr_w4 import load,score
        model,manifest=load(artifact)
        predictor=lambda d:score(model,manifest,d)
    else:raise ValueError('UNSUPPORTED_MODEL_KIND')
    feat=spark.read.parquet(features)
    gold=spark.read.parquet(expected).select('transaction_id',F.col('score').alias('ref_score'),
                                              F.col('decision').alias('ref_decision'))
    for label,frame in (('features',feat),('gold',gold)):
        if frame.groupBy('transaction_id').count().filter(F.col('count')>1).limit(1).count():
            raise ValueError('DUPLICATE_'+label.upper()+'_IDS')
    scored=predictor(feat)
    if scored.groupBy('transaction_id').count().filter('count>1').limit(1).count():
        raise ValueError('DUPLICATE_SCORED_IDS')
    paired=gold.join(scored.select('transaction_id','score','decision'),'transaction_id','full_outer')
    missing=paired.filter(F.col('ref_score').isNull() | F.col('score').isNull()).count()
    bad=paired.filter((F.abs(F.col('score')-F.col('ref_score'))>tolerance) |
                      (F.col('decision')!=F.col('ref_decision'))).count()
    row=paired.select(F.max(F.abs(F.col('score')-F.col('ref_score'))).alias('delta')).first()
    actual=paired.count()
    status='PASS' if actual>0 and missing==0 and bad==0 else 'FAIL'
    return {'status':status,'evidence_mode':'integration','stage':'independent_spark_model_reload',
            'model_version':manifest.get('model_version'),'feature_version':manifest.get('feature_version'),
            'model_kind':model_kind,'artifact_uri':artifact,'features_uri':features,'gold_uri':expected,
            'total_ids':actual,'missing_extra_ids':missing,'score_or_decision_mismatch':bad,
            'max_score_delta':float(row['delta']) if row and row['delta'] is not None else None,
            'tolerance':tolerance,'spark_version':spark.version}


def main():
    p=argparse.ArgumentParser()
    for a in ('artifact','features','expected','model-kind','report'):p.add_argument('--'+a,required=True)
    p.add_argument('--tolerance',type=float,default=1e-8);a=p.parse_args()
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName('fraud-w6-independent-reload').getOrCreate()
    try:r=compare(spark,artifact=a.artifact,features=a.features,expected=a.expected,
                  model_kind=a.model_kind,tolerance=a.tolerance)
    except Exception as exc:r={'status':'FAIL','reason':type(exc).__name__+':'+str(exc)}
    finally:spark.stop()
    target=Path(a.report);target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(r,indent=2)+'\n',encoding='utf8')
    print(json.dumps(r));return 0 if r['status']=='PASS' else 1
if __name__=='__main__':raise SystemExit(main())
