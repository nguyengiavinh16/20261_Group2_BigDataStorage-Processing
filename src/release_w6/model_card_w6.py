"""Machine-checkable model-card and release consistency audit; no guessed scores."""
from __future__ import annotations
import json
from pathlib import Path

A_FIELDS=['transaction_type','log_amount']
AH_FIELDS=A_FIELDS+['dest_prior24_count','dest_prior24_amount_cents']
REQUIRED=('model_version','feature_version','feature_set','feature_order','threshold','threshold_selection_split',
          'silver_snapshot_id','dataset_sha256','git_commit','seed','spark_version','model_type','artifact_uri',
          'artifact_sha256','run_id','config_sha256','warmup_steps','test_steps')

def assess_release(model, *, score_config=None, api_sample=None):
    missing=[k for k in REQUIRED if model.get(k) in (None,'')]
    if missing:return {'status':'INCONCLUSIVE','reason':'MODEL_METADATA_MISSING','fields':missing}
    failures=[]
    expected={'A_stateless':A_FIELDS,'A_plus_H':AH_FIELDS}.get(model['feature_set'])
    if expected is None or model['feature_order']!=expected:failures.append('FEATURE_ORDER_MISMATCH')
    if model['threshold_selection_split']!='validation':failures.append('THRESHOLD_TEST_LEAKAGE')
    if type(model['threshold']) not in (int,float) or not 0<=model['threshold']<=1:failures.append('INVALID_THRESHOLD')
    if model['test_steps']!=[379,743] or model['warmup_steps']!=[355,378]:failures.append('WRONG_EVALUATION_SPLIT')
    if any(not isinstance(model[k],str) or len(model[k])!=64 or any(c not in '0123456789abcdef' for c in model[k]) for k in ('dataset_sha256','artifact_sha256','config_sha256')):
        failures.append('INVALID_SHA256')
    if not str(model['artifact_uri']).startswith(('hdfs://','s3://','file:///','dbfs:/')):
        failures.append('ARTIFACT_URI_NOT_EXPLICIT')
    if type(model['seed']) is not int:failures.append('INVALID_SEED')
    if model.get('artifact_status') not in ('COMPLETE','RELEASED'):failures.append('ARTIFACT_NOT_COMPLETE')
    if score_config:
        for k in ('model_version','feature_version','threshold'):
            if score_config.get(k)!=model[k]:failures.append('SCORER_'+k.upper()+'_MISMATCH')
    if api_sample:
        for k in ('model_version','feature_version','run_id'):
            if api_sample.get(k)!=model[k]:failures.append('API_'+k.upper()+'_MISMATCH')
    return {'status':'FAIL' if failures else 'PASS','failures':failures,
            'scope':'METADATA_CONSISTENCY_ONLY_NOT_MODEL_RELOAD',
            'run_id':model['run_id'],'model_version':model['model_version'],
            'feature_order':model['feature_order']}

def render_card(model, metrics=None, assessment=None):
    """All unproven metrics show `unavailable`; no fake AP/recall."""
    metrics=metrics or {};assessment=assessment or assess_release(model)
    lines=['# Model card — Fraud Analytics W6','',
           f"**Model:** {model.get('model_version','unavailable')}",
           f"**Validation status:** {assessment['status']} (metadata audit only)",
           '', '## Provenance',
           f"- Dataset SHA-256: `{model.get('dataset_sha256','unavailable')}`",
           f"- Silver snapshot: `{model.get('silver_snapshot_id','unavailable')}`",
           f"- Git revision: `{model.get('git_commit','unavailable')}`",
           f"- HDFS/storage artifact: `{model.get('artifact_uri','unavailable')}`",
           f"- Artifact checksum: `{model.get('artifact_sha256','unavailable')}`",
           f"- Spark version: `{model.get('spark_version','unavailable')}`",
           f"- Seed: `{model.get('seed','unavailable')}`",'',
           '## Features, threshold and evaluation',
           f"- Feature order: `{json.dumps(model.get('feature_order','unavailable'))}`",
           f"- Threshold: `{model.get('threshold','unavailable')}` selected on `{model.get('threshold_selection_split','unavailable')}`",
           f"- Evaluation test: `{model.get('test_steps','unavailable')}`; warm-up: `{model.get('warmup_steps','unavailable')}`",
           f"- AP: `{metrics.get('average_precision','unavailable')}`",
           f"- Precision: `{metrics.get('precision','unavailable')}`",
           f"- Recall: `{metrics.get('recall','unavailable')}`",
           f"- F1: `{metrics.get('f1','unavailable')}`",
           '- All reported numbers require a run/report/manifest link and an evidence review.',
           '', '## Limitations and responsible interpretation',
           '- PaySim is a synthetic hourly-step simulator, not representative of live banking on its own.',
           '- Feature history requires ordered replay, 24-step warm-up and point-in-time availability.',
           '- Single-host cluster, Kafka broker replication and multi-sink consistency limits must be disclosed.',
           '- Not a certified fraud decision system; threshold and false-positive costs need domain review.',
           '- Do not infer model calibration or host high availability without separate evidence.','']
    return '\n'.join(lines)
