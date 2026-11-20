import json
import os
from contextlib import closing
from pathlib import Path

import pytest

from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import validate
from detector.job import main
from detector.source import kafka_source_options, kafka_trade_source


def test_modes_use_separate_topics_groups_and_offsets():
    config = load_config("local", env={"CG_KAFKA__BOOTSTRAP_SERVERS": "broker.test:9092"})
    live = kafka_source_options(config, "live")
    replay = kafka_source_options(config, "replay")

    assert live["topic"] == "canonical.trades.v1"
    assert replay["topic"] == "replay.canonical.trades.v1"
    assert live["properties.group.id"] != replay["properties.group.id"]
    assert live["scan.startup.mode"] == "group-offsets"
    assert live["properties.auto.offset.reset"] == "earliest"
    assert "scan.bounded.mode" not in live
    assert replay["scan.startup.mode"] == "earliest-offset"
    assert replay["scan.bounded.mode"] == "latest-offset"
    assert replay["properties.commit.offsets.on.checkpoint"] == "false"
    for options in (live, replay):
        assert options["properties.bootstrap.servers"] == "broker.test:9092"
        assert options["properties.enable.auto.commit"] == "false"
        assert options["key.format"] == options["value.format"] == "raw"
        assert options["value.fields-include"] == "EXCEPT_KEY"


def test_unknown_replay_topic_fails_before_connecting():
    config = load_config("local", env={"CG_REPLAY__TOPIC_PREFIX": "unknown."})
    with pytest.raises(KeyError, match=r"unknown\.canonical\.trades\.v1"):
        kafka_source_options(config, "replay")


def test_invalid_mode_rejected_at_cli(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--mode", "unknown"])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


@pytest.mark.integration
def test_replay_source_preserves_raw_bytes_and_kafka_coordinates():
    bootstrap = os.getenv("CG_DETECTOR_TEST_KAFKA")
    jar = os.getenv("CG_FLINK_JARS")
    if not bootstrap or not jar:
        pytest.skip("Set CG_DETECTOR_TEST_KAFKA and CG_FLINK_JARS for a disposable Kafka broker")
    kafka = pytest.importorskip("confluent_kafka")
    pytest.importorskip("pyflink")
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    config = load_config("local", env={"CG_KAFKA__BOOTSTRAP_SERVERS": bootstrap})
    options = kafka_source_options(config, "replay")
    input_topic = options["topic"]
    path = Path(__file__).with_name("fixtures") / "pump_dump_trades.jsonl"
    payload = path.read_bytes().splitlines()[-3]  # Event time is at the minute boundary minus 1 µs.
    trade = json.loads(payload)
    validate(trade, "canonical_trade.v1")
    expected = {}
    errors = []

    def delivery(error, message):
        if error:
            errors.append(error)
        else:
            expected[message.partition(), message.offset()] = (message.key(), message.value())

    producer = kafka.Producer({"bootstrap.servers": bootstrap})
    producer.produce(input_topic, key=trade["symbol"].encode(), value=payload, on_delivery=delivery)
    producer.produce(input_topic, value=b"\xff\x00", on_delivery=delivery)
    producer.produce(input_topic, key=b"", value=None, on_delivery=delivery)
    assert producer.flush(10) == 0
    assert not errors
    assert len(expected) == 3

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(*jar.split(";"))
    records = kafka_trade_source(StreamTableEnvironment.create(env), options)
    actual = {}
    # Collect through a Python operator like the downstream validator. PyFlink's
    # direct collection of Java rows fails on nullable byte arrays in 1.20.
    with closing(records.map(lambda row: row).execute_and_collect()) as reader:
        for row in reader:
            coordinates = row.source_partition, row.source_offset
            if coordinates in expected:
                assert row.source_topic == input_topic
                actual[coordinates] = row.record_key, row.record_value

    assert actual == expected
