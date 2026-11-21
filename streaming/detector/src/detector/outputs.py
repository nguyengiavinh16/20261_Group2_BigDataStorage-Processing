"""Kafka routing and serialization for detector outputs."""

import json
from dataclasses import dataclass

from cryptoguard_common.contracts import replay_topic, topic
from detector.source import MODES


@dataclass(frozen=True)
class KafkaOutput:
    topic: str
    key_field: str


def kafka_outputs(config: dict, mode: str) -> dict[str, KafkaOutput]:
    if mode not in MODES:
        raise ValueError(f"Unsupported detector mode: {mode!r}")
    names = {"features": "features.symbol_1m.v1", "alerts": "alerts.v1"}
    if mode == "live":
        names["dlq"] = "dlq.detector.v1"
    else:
        prefix = config["replay"]["topic_prefix"]
        names = {kind: replay_topic(name, prefix=prefix) for kind, name in names.items()}
    outputs = {}
    for kind, name in names.items():
        entry = topic(name)
        outputs[kind] = KafkaOutput(entry["name"], entry["key"])
    return outputs


def encode_record(record: dict, output: KafkaOutput) -> tuple[str, str]:
    """Return the Kafka key and the complete JSON payload as UTF-8 text."""
    return record[output.key_field], json.dumps(record, allow_nan=False, separators=(",", ":"))


def attach_kafka_outputs(table_env, streams, outputs, kafka_config: dict) -> None:
    """Attach all Kafka writers to the same DataStream job."""
    from pyflink.common import Row, Types
    from pyflink.table import DataTypes, Schema, TableDescriptor

    row_type = Types.ROW_NAMED(["record_key", "record_value"], [Types.STRING(), Types.STRING()])
    schema = (
        Schema.new_builder()
        .column("record_key", DataTypes.STRING())
        .column("record_value", DataTypes.STRING())
        .build()
    )
    producer_options = kafka_config.get("producer", {})
    statements = table_env.create_statement_set()
    for kind, output in outputs.items():
        descriptor = (
            TableDescriptor.for_connector("kafka")
            .schema(schema)
            .option("topic", output.topic)
            .option("properties.bootstrap.servers", kafka_config["bootstrap_servers"])
            .option("key.format", "raw")
            .option("key.fields", "record_key")
            .option("value.format", "raw")
            .option("value.fields-include", "EXCEPT_KEY")
            .option("sink.delivery-guarantee", "at-least-once")
        )
        for key, value in producer_options.items():
            encoded = str(value).lower() if isinstance(value, bool) else str(value)
            descriptor.option(f"properties.{key.replace('_', '.')}", encoded)
        records = streams[kind].map(
            lambda record, target=output: Row(*encode_record(record, target)),
            output_type=row_type,
        )
        statements.add_insert(descriptor.build(), table_env.from_data_stream(records))
    statements.attach_as_datastream()
