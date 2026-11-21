import json
import os
from collections import defaultdict
from contextlib import closing
from pathlib import Path
from time import monotonic
from uuid import uuid4

import pytest

from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import validate
from cryptoguard_common.paths import contracts_dir
from detector.job import main
from detector.outputs import encode_record, kafka_outputs

FIXTURES = Path(__file__).with_name("fixtures")


@pytest.mark.parametrize(
    ("kind", "schema", "name", "key_field"),
    [
        ("features", "symbol_features_1m.v1", "features.symbol_1m.v1", "symbol"),
        ("alerts", "alert.v1", "alerts.v1", "case_id"),
        ("dlq", "dlq_record.v1", "dlq.detector.v1", "source_topic"),
    ],
)
def test_kafka_keys_and_contract_payload(kind, schema, name, key_field):
    config = load_config("local", env={})
    sample_file = f"{schema.removesuffix('.v1')}.valid.jsonl"
    sample = (contracts_dir() / "samples" / sample_file).read_text(encoding="utf-8")
    record = json.loads(sample.splitlines()[0])
    output = kafka_outputs(config, "live")[kind]
    assert output.topic == name
    key, value = encode_record(record, output)
    assert key == record[key_field]
    decoded = json.loads(value.encode("utf-8"))
    validate(decoded, schema)
    assert decoded == record


def test_replay_routes_only_to_declared_mirror_topics():
    config = load_config("local", env={})
    outputs = kafka_outputs(config, "replay")
    assert {kind: (output.topic, output.key_field) for kind, output in outputs.items()} == {
        "features": ("replay.features.symbol_1m.v1", "symbol"),
        "alerts": ("replay.alerts.v1", "case_id"),
    }


def test_publish_requires_checkpoint_interval(monkeypatch, capsys):
    monkeypatch.setenv("CG_FLINK__CHECKPOINT_INTERVAL_SECONDS", "0")
    with pytest.raises(SystemExit, match="2"):
        main([])
    assert "flink.checkpoint_interval_seconds" in capsys.readouterr().err


@pytest.mark.integration
def test_replay_job_publishes_features_and_ordered_case_alerts(monkeypatch):
    bootstrap = os.getenv("CG_DETECTOR_TEST_KAFKA")
    redis_url = os.getenv("CG_DETECTOR_TEST_REDIS_URL")
    if not (bootstrap and redis_url and os.getenv("CG_FLINK_JARS")):
        pytest.skip("Set detector test Kafka, Redis and Flink connector options")
    kafka = pytest.importorskip("confluent_kafka")
    redis = pytest.importorskip("redis")
    pytest.importorskip("pyflink")
    monkeypatch.setenv("CG_KAFKA__BOOTSTRAP_SERVERS", bootstrap)
    monkeypatch.setenv("CG_REDIS__URL", redis_url)
    run_id = f"m09-test-{uuid4().hex}"
    feature_topic, alert_topic = "replay.features.symbol_1m.v1", "replay.alerts.v1"
    input_topic = "replay.canonical.trades.v1"
    producer = kafka.Producer({"bootstrap.servers": bootstrap})

    with closing(redis.Redis.from_url(redis_url)) as client:
        for exchange, mean in (("ALL", 1200), ("binance", 1000)):
            snapshot = {
                "mean_volume_usd": mean,
                "std_volume_usd": 200,
                "as_of_date": "2025-12-31",
                "n_windows": 20,
            }
            client.set(f"baseline:BTC-USDT:{exchange}:10", json.dumps(snapshot), ex=180)

    settings = {"bootstrap.servers": bootstrap, "group.id": run_id}
    with closing(kafka.Consumer(settings)) as consumer:
        starts = []
        for topic in (feature_topic, alert_topic):
            metadata = consumer.list_topics(topic, timeout=5).topics[topic]
            assert not metadata.error
            for partition in metadata.partitions:
                target = kafka.TopicPartition(topic, partition)
                _, end = consumer.get_watermark_offsets(target, timeout=5)
                starts.append(kafka.TopicPartition(topic, partition, end))
        consumer.assign(starts)

        producer.produce(input_topic, key=b"BTC-USDT", value=b"invalid-json")
        for line in (FIXTURES / "pump_dump_trades.jsonl").read_bytes().splitlines():
            trade = json.loads(line) | {"replay_run_id": run_id}
            validate(trade, "canonical_trade.v1")
            producer.produce(input_topic, key=trade["symbol"].encode(), value=json.dumps(trade))
        assert producer.flush(10) == 0
        main(["--mode", "replay"])

        remaining = sum(
            consumer.get_watermark_offsets(start, timeout=5)[1] - start.offset for start in starts
        )
        counts = {feature_topic: 0, alert_topic: 0}
        cases = defaultdict(list)
        deadline = monotonic() + 15
        while remaining and monotonic() < deadline:
            message = consumer.poll(0.25)
            if message is None:
                continue
            assert not message.error(), message.error()
            remaining -= 1
            record = json.loads(message.value())
            if record.get("replay_run_id") != run_id:
                continue
            topic = message.topic()
            counts[topic] += 1
            if topic == feature_topic:
                validate(record, "symbol_features_1m.v1")
                assert message.key() == record["symbol"].encode()
            else:
                validate(record, "alert.v1")
                assert message.key() == record["case_id"].encode()
                cases[record["case_id"]].append(
                    (message.partition(), message.offset(), record["alert_type"])
                )

    assert remaining == 0
    assert counts == {feature_topic: 8, alert_topic: 4}
    assert len(cases) == 2
    for messages in cases.values():
        assert len({partition for partition, _, _ in messages}) == 1
        assert [kind for _, _, kind in sorted(messages)] == ["PUMP_SUSPECTED", "DUMP_PHASE"]
