"""Trace one canonical ID through source locator, Silver and destination-step Gold.

Proof from the real cluster requires corresponding pinned manifest/Gold report;
this reference is a **local fixture test** only.
"""
from __future__ import annotations
from src.common.contracts_v2 import stable_id
from src.processing.gold_w3 import aggregate_reference


def verify_trace(*,source_checksum,source_line,transaction_id,silver_rows,gold_rows,cutoff):
    if stable_id(source_checksum,source_line)!=transaction_id:
        return {'status':'FAIL','reason':'SOURCE_LOCATOR_ID_MISMATCH'}
    actual=[r for r in silver_rows if r.get('transaction_id')==transaction_id]
    if len(actual)!=1:return {'status':'FAIL','reason':'SILVER_ID_MISSING_OR_DUPLICATE'}
    row=actual[0]
    if row['step']>cutoff:return {'status':'INCONCLUSIVE','reason':'TRANSACTION_OUTSIDE_CUTOFF'}
    expected=aggregate_reference(silver_rows,cutoff=cutoff)
    index={(r['destination_account_id'],r['step']):r for r in gold_rows}
    key=(row['destination_account_id'],row['step'])
    if key not in index:return {'status':'FAIL','reason':'GOLD_GROUP_MISSING'}
    gold=index[key];ref=next(r for r in expected if (r['destination_account_id'],r['step'])==key)
    for field in ('transaction_count','amount_sum_cents','feature_version','cutoff_step'):
        if gold.get(field)!=ref[field]:return {'status':'FAIL','reason':'GOLD_'+field.upper()+'_MISMATCH'}
    return {'status':'PASS','scope':'LOCAL_LINEAGE_REFERENCE_ONLY','transaction_id':transaction_id,
            'source_line':source_line,'silver_step':row['step'],'gold_key':list(key),'gold':gold}
