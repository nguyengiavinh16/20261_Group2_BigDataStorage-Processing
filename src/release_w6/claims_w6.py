"""Trace report claims to immutable evidence files and JSON pointers.

Never turn an illustration/fixture metric into a real measured claim.
"""
from __future__ import annotations
import json
import math
from pathlib import Path
from .package_w6 import sha256_file

ALLOWED_UNITS={'ms','seconds','count','percent','ratio','transactions/s','bytes','cents','AP','F1','steps'}

def _resolve(root,path):
    r=Path(root).resolve();p=Path(path)
    if p.is_absolute() or '..' in p.parts:raise ValueError('UNSAFE_EVIDENCE_PATH')
    q=(r/p).resolve()
    if not q.is_relative_to(r) or q.is_symlink():raise ValueError('EVIDENCE_OUTSIDE_ROOT')
    return q

def pointer(obj, ptr):
    if ptr=='':return obj
    if not ptr.startswith('/'):raise ValueError('INVALID_JSON_POINTER')
    cur=obj
    for raw in ptr[1:].split('/'):
        k=raw.replace('~1','/').replace('~0','~')
        if isinstance(cur,list) and k.isdigit():cur=cur[int(k)]
        elif isinstance(cur,dict):cur=cur[k]
        else:raise KeyError(k)
    return cur

def verify_claims(root,claims, *, require_live=True):
    results={};overall='PASS'
    for idx,claim in enumerate(claims):
        key=claim.get('claim_id',str(idx))
        try:
            if claim.get('unit') not in ALLOWED_UNITS:raise ValueError('UNIT_REQUIRED')
            if claim.get('evidence_mode') not in ('integration','fixture'):raise ValueError('INVALID_EVIDENCE_MODE')
            if require_live and claim['evidence_mode']!='integration':
                results[key]={'status':'INCONCLUSIVE','reason':'FIXTURE_NOT_EMPIRICAL'};continue
            p=_resolve(root,claim['report_path'])
            if not p.is_file():
                results[key]={'status':'INCONCLUSIVE','reason':'REPORT_MISSING'};continue
            if sha256_file(p)!=claim['report_sha256']:
                results[key]={'status':'FAIL','reason':'REPORT_HASH_MISMATCH'};continue
            obj=json.loads(p.read_text(encoding='utf8'))
            if obj.get('status')!='PASS':
                results[key]={'status':'INCONCLUSIVE','reason':'REPORT_NOT_PASS'};continue
            if obj.get('evidence_mode') in ('fixture','synthetic') or obj.get('fixture_only') is True:
                results[key]={'status':'INCONCLUSIVE','reason':'REPORT_IS_FIXTURE'};continue
            for k in ('run_id','model_version','feature_version','cutoff'):
                if claim.get(k) is None or obj.get(k)!=claim[k]:raise ValueError('REPORT_PIN_MISMATCH:'+k)
            value=pointer(obj,claim['json_pointer'])
            if type(value) not in (int,float) or not math.isfinite(value):raise ValueError('NON_NUMERIC_METRIC')
            if claim.get('reported_value')!=value:raise ValueError('CLAIM_VALUE_MISMATCH')
            results[key]={'status':'PASS','value':value,'unit':claim['unit'],'source':claim['report_path']}
        except (ValueError,KeyError,IndexError,TypeError,UnicodeDecodeError, OSError,json.JSONDecodeError) as exc:
            results[key]={'status':'FAIL','reason':str(exc)}
    for r in results.values():
        if r['status']=='FAIL':overall='FAIL';break
        if r['status']!='PASS':overall='INCONCLUSIVE'
    return {'status':overall if claims else 'INCONCLUSIVE','checks':results,'checked':len(claims)}
