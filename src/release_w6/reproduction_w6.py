"""Fixture-mode isolated workspace smoke, optionally clean Git checkout.

Fixture/synthetic success is NEVER accepted as real end-to-end G5 proof.
No automatic Docker volume reset, checkpoint deletion, or secret import.
"""
from __future__ import annotations
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime,timezone
from pathlib import Path

IGNORE={'.git','__pycache__','.pytest_cache','.venv','venv','data','reports','spark-warehouse','.mypy_cache','checkpoints'}

def utc_now():return datetime.now(timezone.utc).isoformat()

def workspace_from(source,destination,*,git=False):
    source=Path(source).resolve();destination=Path(destination).resolve()
    if not source.is_dir() or destination.exists() or destination.is_relative_to(source) or source.is_relative_to(destination):
        raise ValueError('WORKSPACE_MUST_BE_NEW_AND_OUTSIDE_SOURCE')
    if git:
        if not (source/'.git').exists():raise ValueError('GIT_CHECKOUT_REQUIRED')
        subprocess.run(['git','-C',str(source),'diff','--quiet'],check=True)
        subprocess.run(['git','-C',str(source),'diff','--cached','--quiet'],check=True)
        subprocess.run(['git','clone','--local','--no-hardlinks','--',str(source),str(destination)],check=True,
                       timeout=120,capture_output=True,text=True)
        revision=subprocess.run(['git','-C',str(destination),'rev-parse','HEAD'],capture_output=True,text=True,check=True).stdout.strip()
        mode='git_clean_checkout'
    else:
        shutil.copytree(source,destination,ignore=lambda p,names: [n for n in names if n in IGNORE or n.endswith(('.pyc','.sqlite','.db','.log','.parquet'))])
        revision='UNVERIFIED_COPY_NO_GIT_COMMIT'
        mode='filesystem_copy_not_checkout'
    return {'workspace':str(destination),'workspace_mode':mode,'revision':revision}


def fixture_run(root,output,*,operator='local-developer',git=False):
    """Runs 3 existing source CLIs in isolated copy; no Spark/Kafka/Mongo claims."""
    root=Path(root).resolve();output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
    if output.is_relative_to(root):raise ValueError('OUTPUT_MUST_BE_OUTSIDE_SOURCE')
    import tempfile
    with tempfile.TemporaryDirectory(prefix='fraud-w6-checkout-') as tmp:
        checkout=workspace_from(root,Path(tmp)/'checkout',git=git)
        workspace=Path(checkout['workspace'])
        cases={
            'w3_stateful_fixture':(['-m','scripts.week3.demo_local','--output',str(output/'w3')],output/'w3'/'local-smoke.json'),
            'w4_history_parity':(['-m','scripts.week4.cli','local-parity','--events',str(workspace/'fixtures/week4/boundary-events.json'),
                                 '--output',str(output/'w4')],None),
            'w5_synthetic_evaluator':(['-m','scripts.week5.demo_fixture','--out',str(output/'w5')],output/'w5'/'locked-evaluation.json')
        }
        results={};all_ok=True
        for name,(args,report_path) in cases.items():
            env={k:v for k,v in os.environ.items() if k in ('PATH','PATHEXT','SYSTEMROOT','WINDIR','TEMP','TMP','HOME','USERPROFILE','VIRTUAL_ENV','LANG')}
            env['PYTHONPATH']=str(workspace)
            try:
                p=subprocess.run([sys.executable,*args],cwd=workspace,env=env,
                                 capture_output=True,text=True,timeout=120)
                code=p.returncode;stdout=p.stdout[-20000:];stderr=p.stderr[-20000:]
                ok=code==0 and (report_path is None or report_path.is_file())
            except subprocess.TimeoutExpired as e:
                code=124;stdout='';stderr='TIMEOUT';ok=False
            log_path=output/f'{name}.log'
            log_path.write_text('COMMAND: python '+' '.join(args)+'\nEXIT: '+str(code)+'\n\nSTDOUT:\n'+stdout+'\nSTDERR:\n'+stderr,encoding='utf8')
            results[name]={'status':'PASS' if ok else 'FAIL','exit_code':code,'log':str(log_path)}
            all_ok=all_ok and ok
    record={'status':'PASS' if all_ok else 'FAIL','evidence_mode':'fixture',
            'fixture_only':True,'g5_eligible':False,'operator':operator,
            'workspace_mode':checkout['workspace_mode'],'revision':checkout['revision'],
            'created_at_utc':utc_now(),'steps':results,
            'not_tested':['Spark/Iceberg/HDFS','Kafka archive/scoring','Mongo durable ACK','API real dashboard','restart on distributed cluster']}
    (output/'reproduction-fixture.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf8')
    return record
