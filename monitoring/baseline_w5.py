"""W5 baseline-derived alert configuration: no SLA thresholds without measurements."""
from __future__ import annotations
import math
from statistics import median

def thresholds_from_baseline(runs, *, multiplier=2.0):
    if type(multiplier) not in (float,int) or not math.isfinite(multiplier) or multiplier<=1:
        raise ValueError('INVALID_MULTIPLIER')
    if len(runs)<3:return {'status':'INCONCLUSIVE','reason':'BASELINE_REQUIRES_THREE_SAMPLES','thresholds':None}
    times=[];backlogs=[]
    for r in runs:
        if r.get('correctness')!='PASS' or not r.get('manifest_uri'):
            return {'status':'INCONCLUSIVE','reason':'UNVERIFIED_BASELINE','thresholds':None}
        for k in ('p95_latency_ms','max_query_backlog'):
            if type(r.get(k)) not in (int,float) or not math.isfinite(r[k]) or r[k]<0:
                return {'status':'INCONCLUSIVE','reason':'UNAVAILABLE_BASELINE_METRIC:'+k,'thresholds':None}
        times.append(r['p95_latency_ms']);backlogs.append(r['max_query_backlog'])
    return {'status':'PASS','sample_count':len(runs),'thresholds':{
        'p95_latency_ms_warning':median(times)*multiplier,
        'query_backlog_warning':max(1,math.ceil(median(backlogs)*multiplier))},
        'method':'median_baseline_times_multiplier','multiplier':multiplier,
        'caveat':'warning thresholds require subsequent tuning with live observability'}
