"""W6 operational safety: distinguish resume from fresh run; do not delete state."""
from __future__ import annotations
import re

REQUIRED=('run_id','model_version','feature_version','model_uri','mongo_uri','archive_checkpoint',
          'scoring_checkpoint','replay_topic','warmup_start_step','score_start_step')

def check_stream_config(config,prior=None):
    absent=[k for k in REQUIRED if config.get(k) in (None,'')]
    if absent:return {'status':'INCONCLUSIVE','reason':'MISSING_CONFIG','fields':absent}
    errs=[];mode=config.get('mode')
    if mode not in ('fresh','resume'):errs.append('MODE_REQUIRED')
    if config['archive_checkpoint']==config['scoring_checkpoint']:errs.append('CHECKPOINT_COLLISION')
    for k in ('archive_checkpoint','scoring_checkpoint'):
        if not str(config[k]).startswith(('hdfs://','file:///')):errs.append('INVALID_CHECKPOINT_URI:'+k)
    if not re.match(r'^(hdfs://|s3://|file:///)',config['model_uri']):errs.append('MODEL_URI_REQUIRED')
    if not re.match(r'^mongodb(\+srv)?://',config['mongo_uri']):errs.append('MONGO_URI_INVALID')
    if '@' in config['mongo_uri'] and ':' in config['mongo_uri'].split('@')[0]:errs.append('MONGO_CREDENTIALS_SHOULD_USE_ENV')
    warm=config['warmup_start_step'];scored=config['score_start_step']
    if type(warm) is not int or type(scored) is not int or not 1<=warm<=scored<=743:
        errs.append('INVALID_WARMUP_RANGE')
    elif mode=='fresh' and scored>1 and warm>max(1,scored-24):
        errs.append('WARMUP_MISSING_HISTORY')
    if mode=='resume':
        if not prior:errs.append('PRIOR_CHECKPOINT_MANIFEST_REQUIRED')
        else:
            for field in ('run_id','model_version','feature_version','replay_topic','archive_checkpoint','scoring_checkpoint'):
                if prior.get(field)!=config[field]:errs.append('RESUME_PIN_MISMATCH:'+field)
    if config.get('reset_checkpoint') or config.get('delete_volumes'):errs.append('DESTRUCTIVE_RESET_FORBIDDEN')
    return {'status':'FAIL' if errs else 'PASS','errors':errs,
            'scope':'CONFIG_GUARD_ONLY_NOT_KAFKA_RESTART_PROOF'}
