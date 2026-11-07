"""W6 deterministic release manifest and staged-file safety checks.

A manifest proves file integrity, NOT that a workflow ran successfully.
No large dataset, .env, checkpoint, model or Git internals are packaged.
"""
from __future__ import annotations
import hashlib
import json
import os
import re
import zipfile
from pathlib import Path

BLOCKED_COMPONENTS={'.git','.venv','venv','__pycache__','.pytest_cache','checkpoints','checkpoint','warehouse','spark-warehouse','raw'}
BLOCKED_SUFFIXES={'.pyc','.pyo','.parquet','.arrow','.orc','.avro','.sqlite','.db','.pkl','.pickle','.jar','.class','.log','.pem','.key','.p12','.pfx','.bin'}
BLOCKED_FILENAMES={'.env','credentials.json','id_rsa','id_ed25519','secret.json','secrets.yaml','secrets.yml'}
MAX_FILE=10*1024*1024
TEXT_SUFFIXES={'.py','.md','.json','.toml','.yaml','.yml','.txt','.ini','.env','.sh','.ps1','.cfg','.html','.xml'}
SECRET_PATTERNS=[
    re.compile(r'-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----'),
    re.compile(r'AKIA[0-9A-Z]{16}'),
    re.compile(r'(?im)^\s*(?:password|api_key|secret_key|access_token|mongo_password)\s*[:=]\s*[\'"]?(?!<|YOUR_|CHANGEME|PLACEHOLDER|\$\{|\$\(|\s*(?:\#|$))[^\s\'"#]{8,}')
]


def sha256_file(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for block in iter(lambda:f.read(1<<20),b''):h.update(block)
    return h.hexdigest()


def audit_files(root, *, max_file_size=MAX_FILE):
    """Audit source tree, including forbidden tracked candidates; do not traverse symlinks."""
    root=Path(root)
    if not root.is_dir():raise ValueError('ROOT_NOT_DIRECTORY')
    files=[];issues=[]
    for path in sorted(root.rglob('*')):
        rel=path.relative_to(root); parts=set(rel.parts)
        if path.is_symlink():
            issues.append({'file':rel.as_posix(),'reason':'SYMLINK_NOT_ALLOWED'});continue
        if not path.is_file():continue
        if parts&BLOCKED_COMPONENTS or path.name.lower() in BLOCKED_FILENAMES or path.suffix.lower() in BLOCKED_SUFFIXES:
            issues.append({'file':rel.as_posix(),'reason':'FORBIDDEN_ARTIFACT'});continue
        if path.stat().st_size>max_file_size:
            issues.append({'file':rel.as_posix(),'reason':'TOO_LARGE'});continue
        if path.suffix.lower() in TEXT_SUFFIXES or path.name=='.env.example':
            try:
                sample=path.read_text(encoding='utf8')
                if any(p.search(sample) for p in SECRET_PATTERNS):
                    issues.append({'file':rel.as_posix(),'reason':'POTENTIAL_SECRET'});continue
            except UnicodeDecodeError:
                issues.append({'file':rel.as_posix(),'reason':'NON_UTF8_TEXT'});continue
        files.append({'path':rel.as_posix(),'bytes':path.stat().st_size,'sha256':sha256_file(path)})
    return {'status':'FAIL' if issues else 'PASS','file_count':len(files),'files':files,'issues':issues,
            'scope':'STATIC_RELEASE_PACKAGE_AUDIT_ONLY'}


def write_release(root, zip_path, manifest_path):
    """Fails closed; reproducible ZIP metadata; verifies contents before success."""
    root=Path(root).resolve();zip_path=Path(zip_path).resolve();manifest_path=Path(manifest_path).resolve()
    if root in zip_path.parents or root in manifest_path.parents:
        raise ValueError('OUTPUT_MUST_BE_OUTSIDE_RELEASE_TREE')
    report=audit_files(root)
    if report['status']!='PASS':raise ValueError('RELEASE_AUDIT_FAILED:'+json.dumps(report['issues']))
    manifest_path.parent.mkdir(parents=True,exist_ok=True)
    manifest_path.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n',encoding='utf8')
    zip_path.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as z:
        for item in report['files']:
            info=zipfile.ZipInfo(item['path'],date_time=(2026,1,1,0,0,0))
            info.compress_type=zipfile.ZIP_DEFLATED
            info.external_attr=0o644<<16
            z.writestr(info,(root/item['path']).read_bytes(),compress_type=zipfile.ZIP_DEFLATED,compresslevel=9)
    assert verify_release(root,report)['status']=='PASS'
    return {'status':'PASS','file_count':len(report['files']),'zip_sha256':sha256_file(zip_path),
            'manifest_sha256':sha256_file(manifest_path),'zip_path':str(zip_path)}


def verify_release(root,manifest):
    root=Path(root).resolve();m=manifest if isinstance(manifest,dict) else json.loads(Path(manifest).read_text())
    errors=[]
    for e in m.get('files',[]):
        rel=Path(e['path'])
        if rel.is_absolute() or '..' in rel.parts or (root/rel).is_symlink() or not (root/rel).resolve().is_relative_to(root):
            errors.append({'path':str(rel),'reason':'UNSAFE_PATH'});continue
        p=root/rel
        if not p.is_file():errors.append({'path':str(rel),'reason':'MISSING'});continue
        if sha256_file(p)!=e['sha256'] or p.stat().st_size!=e['bytes']:
            errors.append({'path':str(rel),'reason':'TAMPERED'})
    return {'status':'FAIL' if errors else 'PASS','errors':errors,'checked_files':len(m.get('files',[]))}
