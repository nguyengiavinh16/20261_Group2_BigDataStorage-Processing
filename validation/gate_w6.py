"""G5 acceptance gate: real independent reproduction, ten W6 tasks, traceable proof.

This is a deterministic evidence checker, NOT a signature authority or deployment.
Never award G5 PASS for fixture/demo/dry-run. Missing proof => INCONCLUSIVE;
incorrect pinned versions or evidence corruption => FAIL.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

TASKS=tuple(f'P{i}-W6{x}' for i in range(1,6) for x in ('A','B'))
PINS=('run_id','cutoff','model_version','feature_version','silver_snapshot_id','commit','config_sha256','input_sha256')
REPRO_STEPS=('bootstrap','batch','replay','score','validator','api_ui','restart','final_reconciliation')


def file_digest(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for block in iter(lambda:f.read(1<<20),b''):h.update(block)
    return h.hexdigest()


def _proofs(files,root):
    if not isinstance(files,dict) or not files:return 'MISSING_EVIDENCE_FILES'
    base=Path(root).resolve()
    for rel,digest in files.items():
        if not isinstance(rel,str) or not isinstance(digest,str) or len(digest)!=64:return 'INVALID_PROOF_REFERENCE'
        p=Path(rel)
        if p.is_absolute() or '..' in p.parts:return 'UNSAFE_EVIDENCE_PATH'
        resolved=(base/p).resolve()
        if not resolved.is_relative_to(base) or (base/p).is_symlink():return 'UNSAFE_EVIDENCE_PATH'
        if not resolved.is_file():return 'EVIDENCE_FILE_MISSING'
        if file_digest(resolved)!=digest:return 'EVIDENCE_TAMPERED'
    return None


def g5_gate(context,tasks,g4,reproduction,*,root='.'):
    if not isinstance(context,dict) or any(context.get(k) in ('',None) for k in PINS):
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'RELEASE_CONTEXT_NOT_PINNED'}
    if type(context['cutoff']) is not int or not 1<=context['cutoff']<=743:
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'CUTOFF_INVALID'}
    if not isinstance(tasks,dict) or not isinstance(g4,dict) or not isinstance(reproduction,dict):
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'REPORTS_UNAVAILABLE'}
    if g4.get('gate')!='G4' or g4.get('status')!='PASS':
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'G4_LIVE_GATE_NOT_PASSED'}
    if any(g4.get(k)!=context[k] for k in ('run_id','cutoff')):
        return {'gate':'G5','status':'FAIL','reason':'G4_RELEASE_PIN_MISMATCH'}
    if g4.get('evidence_requirement')!='runtime_integration_finalized_no_synthetic_results':
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'G4_UNVERIFIED_SOURCE'}
    if not isinstance(g4.get('tasks'),dict) or set(g4['tasks'])!=set(f'P{i}-W5{x}' for i in range(1,6) for x in ('A','B')) or any(x.get('status')!='PASS' for x in g4['tasks'].values()):
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'G4_TASKS_NOT_ALL_PASSED'}
    if any(reproduction.get(k)!=context[k] for k in PINS):
        return {'gate':'G5','status':'FAIL','reason':'REPRODUCTION_PIN_MISMATCH'}
    if reproduction.get('status')=='FAIL':
        return {'gate':'G5','status':'FAIL','reason':'INDEPENDENT_REPRODUCTION_FAILED'}
    if (reproduction.get('status')!='PASS' or reproduction.get('evidence_mode')!='integration' or
        reproduction.get('workspace_mode')!='git_clean_checkout' or reproduction.get('fixture_only') is not False):
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'REAL_CLEAN_REPRODUCTION_REQUIRED'}
    if (not reproduction.get('operator') or not context.get('author') or
        reproduction['operator']==context['author']):
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'INDEPENDENT_OPERATOR_REQUIRED'}
    steps=reproduction.get('steps')
    if not isinstance(steps,dict) or any(steps.get(s,{}).get('status')!='PASS' for s in REPRO_STEPS):
        return {'gate':'G5','status':'INCONCLUSIVE','reason':'REPRODUCTION_STEPS_INCOMPLETE'}
    repro_proof=_proofs(reproduction.get('evidence_files'),root)
    if repro_proof:
        return {'gate':'G5','status':'FAIL' if repro_proof in ('EVIDENCE_TAMPERED','UNSAFE_EVIDENCE_PATH') else 'INCONCLUSIVE',
                'reason':'REPRODUCTION_'+repro_proof}
    reviews={};overall='PASS'
    for name in TASKS:
        task=tasks.get(name)
        if task is None:
            reviews[name]={'status':'INCONCLUSIVE','reason':'TASK_MISSING'}
        else:
            mismatched=[k for k in PINS if task.get(k)!=context[k]]
            if mismatched:reviews[name]={'status':'FAIL','reason':'RELEASE_PINS_MISMATCH','fields':mismatched}
            elif task.get('status')=='FAIL':reviews[name]={'status':'FAIL','reason':'TASK_FAILED'}
            elif task.get('status')!='PASS' or task.get('evidence_mode')!='integration':
                reviews[name]={'status':'INCONCLUSIVE','reason':'REAL_TASK_EVIDENCE_REQUIRED'}
            elif not task.get('reviewed_by') or task.get('reviewed_by')==task.get('implemented_by'):
                reviews[name]={'status':'INCONCLUSIVE','reason':'CROSS_REVIEW_REQUIRED'}
            elif (not isinstance(task.get('checks'),dict) or
                  any(task['checks'].get(f'{name}-C{i}')!='PASS' for i in (1,2,3))):
                reviews[name]={'status':'INCONCLUSIVE','reason':'THREE_ACCEPTANCE_CASES_REQUIRED'}
            else:
                proof=_proofs(task.get('evidence_files'),root)
                if proof:reviews[name]={'status':'FAIL' if proof in ('EVIDENCE_TAMPERED','UNSAFE_EVIDENCE_PATH') else 'INCONCLUSIVE','reason':proof}
                else:reviews[name]={'status':'PASS','evidence_count':len(task['evidence_files'])}
        if reviews[name]['status']=='FAIL':overall='FAIL'
        elif reviews[name]['status']!='PASS' and overall=='PASS':overall='INCONCLUSIVE'
    if set(tasks)!=set(TASKS):
        overall='FAIL';unexpected=sorted(set(tasks)-set(TASKS))
    else:unexpected=[]
    signoffs=context.get('signoffs',{})
    if not isinstance(signoffs,dict) or set(signoffs)!={f'P{i}' for i in range(1,6)} or not all(isinstance(v,str) and v.strip() for v in signoffs.values()):
        if overall=='PASS':overall='INCONCLUSIVE'
        signoff_issue='HUMAN_SIGNOFFS_INCOMPLETE'
    else:signoff_issue=None
    return {'gate':'G5','status':overall,'run_id':context['run_id'],'commit':context['commit'],
            'tasks':reviews,'unexpected_tasks':unexpected,'signoff_issue':signoff_issue,
            'scope':'LIVE_INDEPENDENT_REPRODUCTION_REQUIRES_HUMAN_VERIFICATION'}


def main():
    p=argparse.ArgumentParser()
    for x in ('context','tasks','g4','reproduction','root','output'):
        p.add_argument('--'+x,required=x!='root',default='.')
    a=p.parse_args()
    try:
        inputs=[json.loads(Path(getattr(a,k)).read_text(encoding='utf8')) for k in ('context','tasks','g4','reproduction')]
        result=g5_gate(*inputs,root=a.root)
    except (OSError,ValueError,TypeError,json.JSONDecodeError) as exc:
        result={'gate':'G5','status':'INCONCLUSIVE','reason':'INPUT_NOT_READABLE:'+type(exc).__name__}
    target=Path(a.output);target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n',encoding='utf8')
    print(json.dumps({'gate':'G5','status':result['status'],'reason':result.get('reason','SEE_REPORT')}))
    raise SystemExit({'PASS':0,'FAIL':1,'INCONCLUSIVE':2}[result['status']])

if __name__=='__main__':main()
