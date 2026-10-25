"""Exercise the real producer/consumer in Conda bigdata on a temporary topic."""
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent/'after-import'
OUT.mkdir(exist_ok=True)
TOPIC = 'codex-check-20261025-' + uuid.uuid4().hex[:12]


def main():
    result = {'topic': TOPIC, 'bootstrap_servers': '127.0.0.1:9092',
              'python': sys.executable, 'status': 'FAIL', 'created': False}
    admin = None
    try:
        deadline = time.monotonic()+30
        while admin is None:
            try:
                admin = KafkaAdminClient(bootstrap_servers='127.0.0.1:9092', request_timeout_ms=10000)
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        assert TOPIC not in admin.list_topics()
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)], timeout_ms=15000)
        result['created'] = True
        commands = [
            ('producer', [sys.executable, '-m', 'src.ingestion.producer', '--csv',
                'fixtures/sample_paysim.csv', '--bootstrap-servers', '127.0.0.1:9092',
                '--topic', TOPIC, '--max-records', '2', '--rate', '0',
                '--log-file', str(OUT/'kafka-producer-runtime.log')]),
            ('consumer', [sys.executable, '-m', 'src.ingestion.consumer_test',
                '--bootstrap-servers', '127.0.0.1:9092', '--topic', TOPIC,
                '--count', '2', '--timeout', '15', '--group-id', TOPIC]),
        ]
        for name, argv in commands:
            print('RUN Kafka ' + name, flush=True)
            completed = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True,
                                       encoding='utf-8', errors='replace', timeout=40)
            (OUT/f'kafka-{name}.log').write_text(completed.stdout+'\nSTDERR\n'+completed.stderr, encoding='utf-8')
            result[name] = {'returncode': completed.returncode, 'stdout': completed.stdout,
                            'stderr': completed.stderr}
            if completed.returncode:
                raise RuntimeError(f'{name} failed: {completed.stderr}')
        rows = [json.loads(line) for line in result['consumer']['stdout'].splitlines()
                if line.startswith('{"count"')]
        assert len(rows) == 2, rows
        validator = Draft202012Validator(json.loads((ROOT/'schemas/transaction-event.json').read_text()),
                                         format_checker=FormatChecker())
        for row in rows:
            validator.validate(row['value'])
            assert row['key'] == row['value']['transaction_id']
            assert 'isFraud' not in row['value'] and 'isFlaggedFraud' not in row['value']
        assert len({row['key'] for row in rows}) == 2
        result.update(status='PASS', received=2, schema_valid_messages=2)
        print('PASS Kafka producer -> broker -> consumer: 2/2 messages', flush=True)
    except Exception as error:
        result['error'] = repr(error)
        raise
    finally:
        if admin is not None and result['created']:
            try:
                admin.delete_topics([TOPIC], timeout_ms=10000)
                for _ in range(10):
                    if TOPIC not in admin.list_topics():
                        result['temporary_topic_deleted'] = True
                        break
                    time.sleep(.5)
            except Exception as error:
                result['cleanup_error'] = repr(error)
        if admin is not None:
            admin.close()
        (OUT/'kafka-results.json').write_text(json.dumps(result, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
