import copy,json
from pathlib import Path
from validation.gate_w6 import g5_gate,TASKS,PINS,REPRO_STEPS,file_digest
from src.release_w6.model_card_w6 import assess_release,render_card,AH_FIELDS
from src.release_w6.architecture_w6 import check_inventory
from src.release_w6.stream_release_w6 import check_stream_config


def context():
    return {'run_id':'r','cutoff':379,'model_version':'m','feature_version':'history-v1',
            'silver_snapshot_id':'1','commit':'abcdef0123456789','config_sha256':'a'*64,'input_sha256':'b'*64,
            'author':'author','signoffs':{f'P{i}':'signed-after-review' for i in range(1,6)}}

def valid(tmp_path):
    c=context();ev=tmp_path/'proof.json';ev.write_text('{"status":"PASS"}\n')
    hash={'proof.json':file_digest(ev)}
    g4={'gate':'G4','status':'PASS','run_id':'r','cutoff':379,
        'evidence_requirement':'runtime_integration_finalized_no_synthetic_results',
        'tasks':{f'P{i}-W5{x}':{'status':'PASS'} for i in range(1,6) for x in ('A','B')}}
    repro={**{k:c[k] for k in PINS},'status':'PASS','operator':'different',
           'evidence_mode':'integration','workspace_mode':'git_clean_checkout','fixture_only':False,
           'steps':{s:{'status':'PASS'} for s in REPRO_STEPS},'evidence_files':hash}
    tasks={name:{**{k:c[k] for k in PINS},'status':'PASS','evidence_mode':'integration',
           'implemented_by':'owner','reviewed_by':'other',
           'checks':{f'{name}-C{i}':'PASS' for i in (1,2,3)},'evidence_files':hash} for name in TASKS}
    return c,tasks,g4,repro

def test_g5_all_complete(tmp_path):
    assert g5_gate(*valid(tmp_path),root=tmp_path)['status']=='PASS'

def test_g5_g4_not_pass(tmp_path):
    c,t,g,r=valid(tmp_path);g['status']='INCONCLUSIVE'
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='INCONCLUSIVE'

def test_g5_repro_fixture_not_pass(tmp_path):
    c,t,g,r=valid(tmp_path);r['fixture_only']=True
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='INCONCLUSIVE'

def test_g5_repro_author_not_independent(tmp_path):
    c,t,g,r=valid(tmp_path);r['operator']='author'
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='INCONCLUSIVE'

def test_g5_missing_step(tmp_path):
    c,t,g,r=valid(tmp_path);r['steps'].pop('validator')
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='INCONCLUSIVE'

def test_g5_mismatched_pin(tmp_path):
    c,t,g,r=valid(tmp_path);r['model_version']='different'
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='FAIL'

def test_g5_no_signoffs(tmp_path):
    c,t,g,r=valid(tmp_path);c['signoffs']={}
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='INCONCLUSIVE'

def test_g5_missing_acceptance_case(tmp_path):
    c,t,g,r=valid(tmp_path);t[TASKS[0]]['checks'].pop(TASKS[0]+'-C3')
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='INCONCLUSIVE'

def test_g5_tamper(tmp_path):
    c,t,g,r=valid(tmp_path);(tmp_path/'proof.json').write_text('tampered')
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='FAIL'

def test_g5_reviewer_cannot_be_author(tmp_path):
    c,t,g,r=valid(tmp_path);t[TASKS[0]]['reviewed_by']='owner'
    assert g5_gate(c,t,g,r,root=tmp_path)['status']=='INCONCLUSIVE'

def test_g5_missing_task(tmp_path):
    c,t,g,r=valid(tmp_path);t.pop(TASKS[0])
    assert g5_gate(c,t,g,r,root=tmp_path)['status'] in ('FAIL','INCONCLUSIVE')

def test_g5_bad_context():
    assert g5_gate({}, {}, {}, {})['status']=='INCONCLUSIVE'

def model():
    return {'model_version':'m','feature_version':'history-v1','feature_set':'A_plus_H','feature_order':AH_FIELDS,
            'threshold':.6,'threshold_selection_split':'validation','silver_snapshot_id':'1',
            'dataset_sha256':'a'*64,'git_commit':'abcdef123456','seed':42,'spark_version':'3.5.6',
            'model_type':'SparkML_LogisticRegression','artifact_uri':'hdfs://namenode/model/1',
            'artifact_sha256':'b'*64,'run_id':'r','config_sha256':'c'*64,'warmup_steps':[355,378],
            'test_steps':[379,743],'artifact_status':'COMPLETE'}

def test_model_good():
    m=model();assert assess_release(m)['status']=='PASS'
    assert 'unavailable' in render_card(m)

def test_model_wrong_feature_order():
    m=model();m['feature_order']=list(reversed(m['feature_order']))
    assert assess_release(m)['status']=='FAIL'

def test_model_test_threshold_leakage():
    m=model();m['threshold_selection_split']='test'
    assert 'THRESHOLD_TEST_LEAKAGE' in assess_release(m)['failures']

def test_model_scorer_mismatch():
    m=model();assert assess_release(m,score_config={'model_version':'other','threshold':.6,'feature_version':'history-v1'})['status']=='FAIL'

def test_model_missing_artifact():
    m=model();del m['artifact_sha256']
    assert assess_release(m)['status']=='INCONCLUSIVE'

def stream():
    return {'mode':'fresh','run_id':'r','model_version':'m','feature_version':'history-v1',
            'model_uri':'hdfs://name/model','mongo_uri':'mongodb://mongo:27017',
            'archive_checkpoint':'hdfs://name/a','scoring_checkpoint':'hdfs://name/b',
            'replay_topic':'events','warmup_start_step':355,'score_start_step':379}

def test_stream_good():assert check_stream_config(stream())['status']=='PASS'

def test_stream_missing_history():
    x=stream();x['warmup_start_step']=378
    assert check_stream_config(x)['status']=='FAIL'

def test_stream_rejects_checkpoint_collision():
    x=stream();x['archive_checkpoint']=x['scoring_checkpoint']
    assert check_stream_config(x)['status']=='FAIL'

def test_stream_resume_requires_pin():
    x=stream();x['mode']='resume'
    assert check_stream_config(x)['status']=='FAIL'
    assert check_stream_config(x,x)['status']=='PASS'

def test_stream_resume_wrong_model():
    x=stream();x['mode']='resume';previous=copy.deepcopy(x);previous['model_version']='old'
    assert check_stream_config(x,previous)['status']=='FAIL'

def test_stream_no_reset():
    x=stream();x['delete_volumes']=True
    assert check_stream_config(x)['status']=='FAIL'

def test_architecture_optional_not_implemented():
    assert check_inventory({'services':{'redis':{'status':'not_implemented'}}})['status']=='INCONCLUSIVE'

def test_architecture_optional_false_claim():
    x={k:{'status':'implemented','implementation_ref':'something.py'} for k in ('spark','hdfs','iceberg','kafka','mongodb','fastapi','dashboard')}
    x['redis']={'status':'implemented','implementation_ref':'redis.py'}
    assert check_inventory({'services':x})['status']=='FAIL'
