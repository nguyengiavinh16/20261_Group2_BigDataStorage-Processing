"""Validate final diagram service inventory against explicitly confirmed reality."""
from __future__ import annotations

CORE={'spark','hdfs','iceberg','kafka','mongodb','fastapi','dashboard'}
OPTIONAL={'redis','airflow'}

def check_inventory(config):
    if not isinstance(config,dict) or not isinstance(config.get('services'),dict):
        return {'status':'INCONCLUSIVE','reason':'SERVICE_INVENTORY_MISSING'}
    services=config['services'];errors=[];unknown=[]
    for name in CORE:
        s=services.get(name)
        if not isinstance(s,dict) or not s.get('implementation_ref') or s.get('status') not in ('implemented','not_verified','not_implemented'):
            unknown.append(name)
    for name in OPTIONAL:
        s=services.get(name,{'status':'not_implemented'})
        if s.get('status')=='implemented' and not (s.get('implementation_ref') and s.get('runtime_evidence')):
            errors.append('OPTIONAL_SERVICE_UNPROVEN:'+name)
    for name,s in services.items():
        if s.get('status')=='implemented' and not s.get('implementation_ref'):
            errors.append('IMPLEMENTATION_REFERENCE_REQUIRED:'+name)
    if errors:return {'status':'FAIL','issues':errors,'unverified':unknown}
    if unknown or any(services[name]['status']!='implemented' for name in CORE):
        return {'status':'INCONCLUSIVE','issues':[],'unverified':unknown+[n for n in CORE if services.get(n,{}).get('status')!='implemented']}
    return {'status':'PASS','issues':[], 'scope':'STATIC_REFERENCE_CHECK_ONLY'}
