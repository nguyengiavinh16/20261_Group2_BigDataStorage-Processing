import hashlib,json
from pathlib import Path
import pytest
from src.common.contracts_v2 import stable_id
from src.processing.gold_w3 import aggregate_reference
from src.release_w6.lineage_w6 import verify_trace
from src.release_w6.reproduction_w6 import workspace_from,fixture_run


def cases():
    checksum='b'*64
    def e(i,dest,step,cents):return {'transaction_id':stable_id(checksum,i),'destination_account_id':dest,'step':step,
                                    'amount_cents':cents,'transaction_type':'TRANSFER'}
    rows=[e(1,'D',1,100),e(2,'D',1,200),e(3,'D',2,300)]
    return checksum,rows,aggregate_reference(rows,cutoff=3)

def test_lineage_good():
    checksum,rows,gold=cases()
    assert verify_trace(source_checksum=checksum,source_line=2,transaction_id=rows[1]['transaction_id'],
                        silver_rows=rows,gold_rows=gold,cutoff=3)['status']=='PASS'

def test_lineage_bad_locator():
    checksum,rows,gold=cases()
    assert verify_trace(source_checksum=checksum,source_line=1,transaction_id=rows[1]['transaction_id'],
                        silver_rows=rows,gold_rows=gold,cutoff=3)['status']=='FAIL'

def test_lineage_bad_gold():
    checksum,rows,gold=cases();gold[0]['amount_sum_cents']+=100
    assert verify_trace(source_checksum=checksum,source_line=2,transaction_id=rows[1]['transaction_id'],
                        silver_rows=rows,gold_rows=gold,cutoff=3)['status']=='FAIL'

def test_copy_is_not_git_checkout(tmp_path):
    root=tmp_path/'src';root.mkdir();(root/'a.txt').write_text('hello')
    r=workspace_from(root,tmp_path/'dst')
    assert r['workspace_mode']=='filesystem_copy_not_checkout'
    assert (tmp_path/'dst'/'a.txt').read_text()=='hello'

def test_cannot_copy_inside_source(tmp_path):
    with pytest.raises(ValueError):workspace_from(tmp_path,tmp_path/'child')

def test_copy_ignores_large_artifacts(tmp_path):
    root=tmp_path/'src';(root/'data').mkdir(parents=True);(root/'data'/'huge.csv').write_text('sensitive')
    (root/'a.py').write_text('pass')
    workspace_from(root,tmp_path/'isolated')
    assert not (tmp_path/'isolated'/'data').exists()
    assert (tmp_path/'isolated'/'a.py').exists()
