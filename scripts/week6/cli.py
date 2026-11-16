"""W6 commands: package audit, fixture repro, evidence claims and model card."""
from __future__ import annotations
import argparse,json
from pathlib import Path
from src.release_w6.package_w6 import audit_files,write_release,verify_release
from src.release_w6.claims_w6 import verify_claims
from src.release_w6.model_card_w6 import assess_release,render_card
from src.release_w6.reproduction_w6 import fixture_run
from src.release_w6.stream_release_w6 import check_stream_config

def main():
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='operation',required=True)
    a=s.add_parser('package-audit');a.add_argument('--root',required=True);a.add_argument('--output',required=True)
    a=s.add_parser('build-release');a.add_argument('--root',required=True);a.add_argument('--zip',required=True);a.add_argument('--manifest',required=True)
    a=s.add_parser('fixture-reproduce');a.add_argument('--root',default='.');a.add_argument('--output',required=True)
    a.add_argument('--operator',default='local-developer');a.add_argument('--git',action='store_true')
    a=s.add_parser('verify-claims');a.add_argument('--root',required=True);a.add_argument('--claims',required=True)
    a.add_argument('--output',required=True);a.add_argument('--allow-fixture',action='store_true')
    a=s.add_parser('model-card');a.add_argument('--manifest',required=True);a.add_argument('--metrics');a.add_argument('--scorer')
    a.add_argument('--api-sample');a.add_argument('--output',required=True);a.add_argument('--audit-output',required=True)
    a=s.add_parser('stream-guard');a.add_argument('--config',required=True);a.add_argument('--prior');a.add_argument('--output',required=True)
    a=p.parse_args()
    def j(path):return json.loads(Path(path).read_text('utf8'))
    try:
        if a.operation=='package-audit':result=audit_files(a.root)
        elif a.operation=='build-release':result=write_release(a.root,a.zip,a.manifest)
        elif a.operation=='fixture-reproduce':result=fixture_run(a.root,a.output,operator=a.operator,git=a.git)
        elif a.operation=='verify-claims':result=verify_claims(a.root,j(a.claims),require_live=not a.allow_fixture)
        elif a.operation=='model-card':
            model=j(a.manifest);result=assess_release(model,score_config=j(a.scorer) if a.scorer else None,
                                                       api_sample=j(a.api_sample) if a.api_sample else None)
            target=Path(a.output);target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(render_card(model,metrics=j(a.metrics) if a.metrics else None,assessment=result),encoding='utf8')
        else:result=check_stream_config(j(a.config),j(a.prior) if a.prior else None)
    except Exception as exc:
        result={'status':'INCONCLUSIVE','reason':'COMMAND_ERROR:'+type(exc).__name__,'detail':str(exc)}
    report=getattr(a,'audit_output',None) or getattr(a,'output',None)
    if a.operation!='fixture-reproduce' and report:
        t=Path(report);t.parent.mkdir(parents=True,exist_ok=True)
        t.write_text(json.dumps(result,indent=2)+'\n',encoding='utf8')
    print(json.dumps({'status':result['status'],'reason':result.get('reason','SEE_REPORT')},ensure_ascii=False))
    return {'PASS':0,'FAIL':1,'INCONCLUSIVE':2}[result['status']]
if __name__=='__main__':raise SystemExit(main())
