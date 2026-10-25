"""Verify the restored W2 support files with the user's Conda bigdata runtime."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('project_checks', HERE/'run_checks.py')
checks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checks)
checks.OUT = HERE/'after-import'
checks.OUT.mkdir(exist_ok=True)


def main():
    os.environ['PYTHONUTF8'] = '1'
    os.environ['MPLBACKEND'] = 'Agg'
    os.environ['MPLCONFIGDIR'] = str(checks.WORK/'matplotlib')
    os.environ['IPYTHONDIR'] = str(checks.WORK/'ipython')
    sys.path.insert(0, str(checks.ROOT))
    checks.RESULTS['environment'] = {'python': sys.version, 'executable': sys.executable,
        'conda_prefix': os.environ.get('CONDA_PREFIX'),
        'base_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checks.ROOT, text=True).strip(),
        'source': 'working tree after importing selected dat/week2-spark-etl support files',
        'checked_at': datetime.now(timezone(timedelta(hours=7))).isoformat()}
    source = checks.snapshot()
    restored = ['.dockerignore', 'Dockerfile.jupyter', 'docker-compose.yml', 'README.md',
        'configs/README.md', 'configs/etl.json', 'configs/paysim_schema.json',
        'configs/data_profiling.json', 'src/processing/schema.py', 'tests/test_etl.py',
        'tests/fixtures/paysim_etl.csv', 'reports/week2-unit-tests.log']
    for rel in restored:
        target = source/rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(checks.ROOT/rel, target)
    checks.RESULTS['restored_files'] = restored
    checks.command('etl-help', [sys.executable, '-m', 'src.processing.etl', '--help'])
    checks.command('data-quality-tests', [sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test_data_quality.py', '-v'])
    checks.command('schema-tests', [sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test_schema.py', '-v'])
    checks.note('RESTORE original PaySim dataset from preserved local backup into temporary notebook snapshot')
    dataset = source/'data/paysim dataset.csv'
    dataset.parent.mkdir(exist_ok=True)
    with dataset.open('wb') as output:
        subprocess.run(['git', 'show', 'backup/main-before-sync-2026-10-25:data/paysim dataset.csv'],
            cwd=checks.ROOT, stdout=output, check=True)
    raw_alias = source/'data/raw/PS_20174392719_1491204439457_log.csv'
    raw_alias.parent.mkdir(exist_ok=True)
    os.link(dataset, raw_alias)
    os.environ['PAYSIM_DATA_PATH'] = str(dataset)
    checks.RESULTS['notebook_data'] = {'source': 'existing local backup branch, not a download',
        'file': 'backup/main-before-sync-2026-10-25:data/paysim dataset.csv',
        'bytes': dataset.stat().st_size, 'temporary_path': str(dataset),
        'alias_for_w1_notebook': str(raw_alias),
        'note': 'Original notebooks, local historical reports and outputs remain untouched.'}
    checks.notebooks(source)
    (checks.OUT/'results.json').write_text(json.dumps(checks.RESULTS, indent=2, ensure_ascii=False), encoding='utf-8')
    checks.note('COMPLETE '+str(checks.OUT/'results.json'))
    checks.note(json.dumps([{'name':c['name'], 'status':c['status']} for c in checks.RESULTS['checks']]))


if __name__ == '__main__':
    main()
