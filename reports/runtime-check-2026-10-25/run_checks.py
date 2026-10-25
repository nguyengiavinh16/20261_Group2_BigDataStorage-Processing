"""Read-only project checks; run with conda run -n bigdata python run_checks.py.

Runtime outputs and notebook execution copies are isolated from source inputs.
"""
from __future__ import annotations

import ast
import contextlib
import csv
import importlib.metadata
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback
from datetime import datetime, timedelta, timezone
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
WORK = Path(tempfile.mkdtemp(prefix='fraud-conda-bigdata-check-'))
RESULTS = {'environment': {}, 'checks': [], 'findings': [], 'work_directory': str(WORK)}


def note(message):
    print(message, flush=True)


def command(name, argv, timeout=45, cwd=ROOT):
    started = time.perf_counter()
    note(f'RUN {name}')
    process = subprocess.Popen(argv, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, encoding='utf-8', errors='replace',
                               creationflags=subprocess.CREATE_NO_WINDOW)
    timed_out = False
    try:
        output, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'],
                       capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        output, _ = process.communicate(timeout=10)
    log = OUT / f'{name}.log'
    log.write_text(output, encoding='utf-8')
    result = {'name': name, 'argv': argv, 'returncode': process.returncode,
              'timed_out': timed_out, 'seconds': round(time.perf_counter()-started, 3),
              'log': log.name, 'status': 'PASS' if process.returncode == 0 and not timed_out else 'FAIL'}
    RESULTS['checks'].append(result)
    note(f'{result["status"]} {name} ({result["seconds"]}s)')
    if result['status'] == 'FAIL':
        note(output[-1800:])
    return result


def section(name, function):
    note(f'RUN {name}')
    started = time.perf_counter()
    try:
        detail = function()
        result = {'name': name, 'status': 'PASS', 'detail': detail}
    except Exception:
        result = {'name': name, 'status': 'FAIL', 'traceback': traceback.format_exc()}
    result['seconds'] = round(time.perf_counter()-started, 3)
    RESULTS['checks'].append(result)
    note(f'{result["status"]} {name}')
    return result


def snapshot():
    data = subprocess.check_output(['git', 'archive', '--format=tar', 'HEAD'], cwd=ROOT)
    destination = WORK / 'source'
    destination.mkdir()
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        # Only extract regular repository files into the verified temporary root.
        for member in archive.getmembers():
            target = (destination / member.name).resolve()
            if not target.is_relative_to(destination.resolve()) or not (member.isfile() or member.isdir()):
                raise ValueError(f'Unexpected archive member: {member.name}')
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.extractfile(member).read())
    return destination


def static_checks():
    import yaml
    import nbformat
    from IPython.core.interactiveshell import InteractiveShell
    from jsonschema import Draft202012Validator, FormatChecker
    paths = subprocess.check_output(['git', 'ls-files'], cwd=ROOT, text=True).splitlines()
    paths += [str(p.relative_to(ROOT)).replace('\\', '/') for p in (ROOT/'docs/diagrams').glob('*.py')]
    summary = {'python_files': 0, 'json_files': 0, 'yaml_files': 0,
               'notebooks': 0, 'code_cells': 0, 'intentional_invalid_json': [], 'valid_event_fixtures': 0}
    for rel in paths:
        path = ROOT / rel
        if path.suffix == '.py':
            ast.parse(path.read_text(encoding='utf-8-sig'), filename=rel)
            summary['python_files'] += 1
        elif path.suffix == '.json':
            try:
                json.loads(path.read_text(encoding='utf-8-sig'))
            except json.JSONDecodeError:
                if rel == 'fixtures/invalid/malformed-json.json':
                    summary['intentional_invalid_json'].append(rel)
                else:
                    raise
            summary['json_files'] += 1
        elif path.suffix in {'.yaml', '.yml'}:
            yaml.safe_load(path.read_text(encoding='utf-8-sig'))
            summary['yaml_files'] += 1
        elif path.suffix == '.ipynb':
            notebook = nbformat.read(path, as_version=4)
            nbformat.validate(notebook)
            summary['notebooks'] += 1
            for i, cell in enumerate(notebook.cells):
                if cell.cell_type == 'code':
                    transformed = InteractiveShell.instance().transform_cell(cell.source)
                    compile(transformed, f'{rel}:cell-{i}', 'exec')
                    summary['code_cells'] += 1
    for schema in (ROOT/'schemas').glob('*.json'):
        Draft202012Validator.check_schema(json.loads(schema.read_text(encoding='utf-8')))
    for fixture in (ROOT/'fixtures/valid').glob('*.json'):
        schema = ROOT/'schemas'/('prediction-event.json' if fixture.name.startswith('prediction') else 'transaction-event.json')
        validator = Draft202012Validator(json.loads(schema.read_text()), format_checker=FormatChecker())
        validator.validate(json.loads(fixture.read_text()))
        summary['valid_event_fixtures'] += 1
    return summary


def producer_logic():
    from src.ingestion import producer
    from jsonschema import Draft202012Validator, FormatChecker
    rows = list(csv.DictReader((ROOT/'fixtures/sample_paysim.csv').open(encoding='utf-8')))
    schema = json.loads((ROOT/'schemas/transaction-event.json').read_text())
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    produced = []

    class FakeProducer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def send(self, topic, key, value):
            # Execute the actual serializers and validate the resulting message.
            serialized = self.kwargs['value_serializer'](value)
            self.kwargs['key_serializer'](key)
            decoded = json.loads(serialized)
            validator.validate(decoded)
            assert 'isFraud' not in decoded and 'isFlaggedFraud' not in decoded
            produced.append({'topic': topic, 'key': key, 'event': decoded})

        def flush(self, **kwargs):
            pass

        def close(self, **kwargs):
            pass

    import kafka
    argv = ['producer', '--csv', str(ROOT/'fixtures/sample_paysim.csv'), '--max-records', '2',
            '--rate', '0', '--log-file', str(WORK/'producer-mock.log')]
    with mock.patch.object(kafka, 'KafkaProducer', FakeProducer), mock.patch.object(sys, 'argv', argv):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            producer.main()
    assert len(rows) == len(produced) == 2
    (OUT/'producer-mock-messages.json').write_text(json.dumps(produced, indent=2), encoding='utf-8')
    return {'mode': 'mock broker; no network publish', 'sent': len(produced),
            'schema_valid_messages': len(produced), 'producer_summary': output.getvalue().strip()}


def contract_probes():
    from src.ingestion.producer import map_row
    from src.ingestion.validation import validate_transaction
    from jsonschema import Draft202012Validator, FormatChecker
    valid = json.loads((ROOT/'fixtures/valid/transaction-001.json').read_text())
    schema = json.loads((ROOT/'schemas/transaction-event.json').read_text())
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    probes = []
    for title, updates in [('invalid_timestamp', {'event_time': 'not-a-time'}),
                           ('wrong_schema_version', {'schema_version': '9.9'}),
                           ('empty_source_dataset', {'source_dataset': ''}),
                           ('unexpected_label_field', {'isFraud': 1})]:
        record = dict(valid, **updates)
        probes.append({'case': title, 'helper': validate_transaction(record),
                       'schema_errors': [e.message for e in validator.iter_errors(record)]})
    try:
        validate_transaction(dict(valid, amount=10**1000))
    except Exception as error:
        probes.append({'case': 'huge_integer_amount', 'exception': type(error).__name__, 'message': str(error)})
    rows = list(csv.DictReader((ROOT/'fixtures/sample_paysim.csv').open(encoding='utf-8')))
    anchor = datetime(2026, 1, 1, tzinfo=timezone.utc)
    try:
        map_row(dict(rows[0], oldbalanceOrg=None), 'fixture', 1, anchor)
    except Exception as error:
        probes.append({'case': 'truncated_optional_balance', 'exception': type(error).__name__, 'message': str(error)})
    event = map_row(dict(rows[0], oldbalanceOrg='NaN'), 'fixture', 1, anchor)
    result = validate_transaction(event)
    try:
        json.dumps(event, allow_nan=False)
        strict_json = True
    except ValueError:
        strict_json = False
    probes.append({'case': 'nan_optional_balance', 'helper': result, 'valid_standard_json': strict_json})
    (OUT/'contract-probes.json').write_text(json.dumps(probes, indent=2), encoding='utf-8')
    RESULTS['findings'].extend(probes)
    return {'probes': len(probes), 'details': 'contract-probes.json',
            'note': 'These are observed defects/gaps, not passing regression tests.'}


def notebooks(source):
    import nbformat
    from nbclient import NotebookClient
    from jupyter_client import KernelManager
    from jupyter_client.kernelspec import KernelSpecManager
    kernel_root = WORK/'kernels'
    kernel_dir = kernel_root/'bigdata-check'
    kernel_dir.mkdir(parents=True)
    (kernel_dir/'kernel.json').write_text(json.dumps({
        'argv': [sys.executable, '-m', 'ipykernel_launcher', '-f', '{connection_file}'],
        'display_name': 'Conda bigdata check', 'language': 'python'}), encoding='utf-8')
    destination = OUT/'notebook-runs'
    destination.mkdir(exist_ok=True)
    for path in sorted((source/'notebooks').glob('*.ipynb')):
        started = time.perf_counter()
        notebook = nbformat.read(path, as_version=4)
        skipped = []
        for i, cell in enumerate(notebook.cells):
            if cell.cell_type == 'code':
                cell.execution_count = None
                cell.outputs = []
                if cell.source.lstrip().startswith(('%pip install', '!pip install')):
                    skipped.append(i)
                    cell.source = '# Installation cell skipped: dependencies already present in Conda bigdata.'
        ksm = KernelSpecManager(kernel_dirs=[str(kernel_root)])
        km = KernelManager(kernel_name='bigdata-check', kernel_spec_manager=ksm)
        client = NotebookClient(notebook, km=km, timeout=60, startup_timeout=30,
                                resources={'metadata': {'path': str(source/'notebooks')}},
                                allow_errors=False, record_timing=True)
        note('RUN notebook ' + path.name)
        try:
            client.execute()
            status, error = 'PASS', None
        except Exception as exc:
            status, error = 'FAIL', str(exc)
        finally:
            if km.has_kernel:
                with contextlib.suppress(Exception):
                    km.shutdown_kernel(now=True)
        nbformat.write(notebook, destination/path.name)
        result = {'name': 'notebook-' + path.stem, 'status': status,
                  'seconds': round(time.perf_counter()-started, 3), 'error': error,
                  'executed_code_cells': sum(c.cell_type == 'code' and c.execution_count is not None for c in notebook.cells),
                  'code_cells': sum(c.cell_type == 'code' for c in notebook.cells),
                  'installation_cells_skipped': skipped,
                  'kernel': sys.executable, 'working_directory': str(source/'notebooks'),
                  'executed_copy': f'notebook-runs/{path.name}'}
        RESULTS['checks'].append(result)
        note(f'{status} notebook {path.name} ({result["executed_code_cells"]}/{result["code_cells"]} cells)')
        if error:
            note(error[-1800:])


def main():
    os.environ['PYTHONUTF8'] = '1'
    os.environ['MPLBACKEND'] = 'Agg'
    os.environ['MPLCONFIGDIR'] = str(WORK/'matplotlib')
    os.environ['IPYTHONDIR'] = str(WORK/'ipython')
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    sys.path.insert(0, str(ROOT))
    packages = ['pyspark', 'pandas', 'numpy', 'pytest', 'jsonschema', 'kafka-python',
                'nbformat', 'nbclient', 'ipykernel', 'matplotlib', 'scikit-learn', 'PyYAML']
    versions = {}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = 'MISSING'
    RESULTS['environment'] = {'python': sys.version, 'executable': sys.executable,
        'conda_prefix': os.environ.get('CONDA_PREFIX'), 'packages': versions,
        'java_home': os.environ.get('JAVA_HOME'),
        'git_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'checked_at': datetime.now(timezone(timedelta(hours=7))).isoformat()}
    note(json.dumps(RESULTS['environment'], indent=2))
    source = snapshot()
    section('static-source-config-and-schema', static_checks)
    command('unittest', [sys.executable, '-X', 'utf8', '-m', 'unittest', 'discover', '-s', 'tests', '-v'])
    command('etl-help', [sys.executable, '-X', 'utf8', '-m', 'src.processing.etl', '--help'])
    command('producer-help', [sys.executable, '-m', 'src.ingestion.producer', '--help'])
    command('consumer-help', [sys.executable, '-m', 'src.ingestion.consumer_test', '--help'])
    if not (ROOT/'logs').exists():
        command('producer-default-log', [sys.executable, '-m', 'src.ingestion.producer',
            '--csv', 'fixtures/sample_paysim.csv', '--max-records', '1', '--rate', '0'])
    with socket.socket() as probe:
        probe.settimeout(1)
        broker_available = probe.connect_ex(('127.0.0.1', 9092)) == 0
    RESULTS['checks'].append({'name': 'kafka-local-port', 'status': 'PASS' if broker_available else 'BLOCKED',
                              'endpoint': '127.0.0.1:9092', 'reachable': broker_available})
    # The consumer uses enable_auto_commit=False and does not publish data.
    command('consumer-runtime', [sys.executable, '-m', 'src.ingestion.consumer_test',
            '--count', '1', '--timeout', '3', '--group-id', 'codex-bigdata-check-20261025'], timeout=15)
    section('producer-mapping-and-serialization-mock', producer_logic)
    section('contract-edge-case-probes', contract_probes)
    command('java-version', ['java', '-version'], timeout=10)
    spark_probe = WORK/'spark_probe.py'
    spark_probe.write_text('''import sys
from pyspark.sql import SparkSession
spark = None
try:
    spark = (SparkSession.builder.master("local[1]").appName("bigdata-environment-check")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable).getOrCreate())
    count = spark.range(3).count()
    print({"spark_version":spark.version, "range_count":count}, flush=True)
    assert count == 3
finally:
    if spark is not None: spark.stop()
''', encoding='utf-8')
    command('spark-environment-smoke', [sys.executable, str(spark_probe)], timeout=50, cwd=WORK)
    notebooks(source)
    skill = Path.home()/'.codex/skills/drawio-skill/scripts'
    command('diagram-structural', [sys.executable, str(skill/'validate.py'),
            str(ROOT/'docs/diagrams/system-architecture.drawio'), '--score'])
    command('diagram-semantic', [sys.executable, str(skill/'diagramctl.py'), 'test',
            str(ROOT/'docs/diagrams/system-architecture.ir.json')])
    RESULTS['missing_project_files'] = [rel for rel in [
        'src/processing/schema.py', 'configs/etl.json', 'configs/paysim_schema.json',
        'configs/data_profiling.json', 'tests/test_etl.py', 'tests/fixtures/paysim_etl.csv',
        'requirements-ingestion.txt', 'docker-compose.yml',
        'data/paysim dataset.csv', 'data/raw/PS_20174392719_1491204439457_log.csv'] if not (ROOT/rel).is_file()]
    (OUT/'results.json').write_text(json.dumps(RESULTS, indent=2, ensure_ascii=False), encoding='utf-8')
    note('COMPLETE ' + str(OUT/'results.json'))
    note(json.dumps({'checks': [{'name':c['name'], 'status':c['status']} for c in RESULTS['checks']]}))


if __name__ == '__main__':
    main()
