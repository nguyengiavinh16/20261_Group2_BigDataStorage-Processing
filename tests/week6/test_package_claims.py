import hashlib,json
from pathlib import Path
import pytest
from src.release_w6.package_w6 import audit_files,write_release,verify_release,sha256_file
from src.release_w6.claims_w6 import verify_claims,pointer


def test_package_and_integrity(tmp_path):
    root=tmp_path/'src';root.mkdir();(root/'a.py').write_text('print(1)')
    report=audit_files(root);assert report['status']=='PASS'
    res=write_release(root,tmp_path/'pkg.zip',tmp_path/'manifest.json')
    assert res['status']=='PASS'
    assert verify_release(root,json.loads((tmp_path/'manifest.json').read_text()))['status']=='PASS'
    (root/'a.py').write_text('print(2)')
    assert verify_release(root,report)['errors'][0]['reason']=='TAMPERED'

@pytest.mark.parametrize('name', ['.env','foo.parquet','a.sqlite','secret.json','credentials.json','checkpoints/a.json','data/raw/file.csv','x.pkl'])
def test_audit_rejects_forbidden(tmp_path,name):
    p=tmp_path/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('harmless')
    assert audit_files(tmp_path)['status']=='FAIL'

@pytest.mark.parametrize('secret', ['-----BEGIN '+'PRIVATE KEY-----','AK'+'IA1234567890123456','password = mySuperSecretPass'])
def test_secret_rejected(tmp_path,secret):
    (tmp_path/'config.txt').write_text(secret)
    assert audit_files(tmp_path)['issues'][0]['reason']=='POTENTIAL_SECRET'

def test_too_large(tmp_path):
    (tmp_path/'x.dat').write_bytes(b'abc')
    assert audit_files(tmp_path,max_file_size=2)['status']=='FAIL'

def test_symlink_guard(tmp_path):
    external=tmp_path.parent/'external-test-w6'
    external.write_text('test')
    try:
        (tmp_path/'evil.txt').symlink_to(external)
        assert audit_files(tmp_path)['status']=='FAIL'
    finally:external.unlink(missing_ok=True)

def test_build_rejects_child_output(tmp_path):
    (tmp_path/'a.txt').write_text('ok')
    with pytest.raises(ValueError,match='OUTPUT_MUST_BE_OUTSIDE'):
        write_release(tmp_path,tmp_path/'out.zip',tmp_path/'out.json')

def claim(root, *,mode='integration'):
    p=root/'r.json';p.write_text(json.dumps({'status':'PASS','evidence_mode':mode,'run_id':'r','model_version':'m',
             'feature_version':'f','cutoff':379,'latency':{'p95':10}}))
    return {'claim_id':'p95','unit':'ms','evidence_mode':mode,'report_path':'r.json','report_sha256':sha256_file(p),
            'run_id':'r','model_version':'m','feature_version':'f','cutoff':379,'json_pointer':'/latency/p95','reported_value':10}

def test_claim_good(tmp_path):
    assert verify_claims(tmp_path,[claim(tmp_path)])['status']=='PASS'

def test_claim_tampered(tmp_path):
    c=claim(tmp_path);(tmp_path/'r.json').write_text('{}')
    assert verify_claims(tmp_path,[c])['status']=='FAIL'

def test_claim_value_mismatch(tmp_path):
    c=claim(tmp_path);c['reported_value']=12
    assert verify_claims(tmp_path,[c])['status']=='FAIL'

def test_claim_fixture_cannot_be_empirical(tmp_path):
    c=claim(tmp_path,mode='fixture')
    assert verify_claims(tmp_path,[c])['status']=='INCONCLUSIVE'

def test_claim_wrong_pin(tmp_path):
    c=claim(tmp_path);c['model_version']='other'
    assert verify_claims(tmp_path,[c])['status']=='FAIL'

def test_claim_missing(tmp_path):
    c=claim(tmp_path);(tmp_path/'r.json').unlink()
    assert verify_claims(tmp_path,[c])['status']=='INCONCLUSIVE'

def test_claim_unsafe_path(tmp_path):
    c=claim(tmp_path);c['report_path']='../escape.json'
    assert verify_claims(tmp_path,[c])['status']=='FAIL'

def test_pointer_escape():
    assert pointer({'a/b':[{'x':3}]},'/a~1b/0/x')==3
