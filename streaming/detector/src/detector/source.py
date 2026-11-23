"""Kafka input shared by the live and bounded replay detector jobs."""

from cryptoguard_common.contracts import replay_topic, topic

LIVE_TRADE_TOPIC = "canonical.trades.v1"
MODES = ("live", "replay")


def kafka_source_options(config: dict, mode: str) -> dict[str, str]:
    """Resolve the Kafka topic, consumer group and startup policy."""
    if mode not in MODES:
        raise ValueError(f"Unsupported detector mode: {mode!r}")

    bootstrap_servers = config["kafka"]["bootstrap_servers"]
    if not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip():
        raise ValueError("kafka.bootstrap_servers must be a non-empty string")

    if mode == "replay":
        prefix = config["replay"]["topic_prefix"]
        if not isinstance(prefix, str) or not prefix.strip():
            raise ValueError("replay.topic_prefix must be a non-empty string")
        input_topic = replay_topic(LIVE_TRADE_TOPIC, prefix=prefix)
    else:
        input_topic = topic(LIVE_TRADE_TOPIC)["name"]

    options = {
        "topic": input_topic,
        "properties.bootstrap.servers": bootstrap_servers,
        "properties.group.id": f"cg-detector-{mode}",
        "properties.enable.auto.commit": "false",
        "key.format": "raw",
        "key.fields": "record_key",
        "value.format": "raw",
        "value.fields-include": "EXCEPT_KEY",
    }
    if mode == "replay":
        options.update(
            {
                "scan.startup.mode": "earliest-offset",
                "scan.bounded.mode": "latest-offset",
                "properties.commit.offsets.on.checkpoint": "false",
            }
        )
    else:
        options.update(
            {
                "scan.startup.mode": "group-offsets",
                "properties.auto.offset.reset": "earliest",
            }
        )
    return options


def kafka_trade_source(table_env, options: dict[str, str]):
    """Expose Kafka record bytes and source coordinates as a DataStream of Rows."""
    from pyflink.table import DataTypes, Schema, TableDescriptor

    schema = (
        Schema.new_builder()
        .column("record_key", DataTypes.BYTES())
        .column("record_value", DataTypes.BYTES())
        .column_by_metadata("source_topic", DataTypes.STRING(), "topic", True)
        .column_by_metadata("source_partition", DataTypes.INT(), "partition", True)
        .column_by_metadata("source_offset", DataTypes.BIGINT(), "offset", True)
        .build()
    )
    descriptor = TableDescriptor.for_connector("kafka").schema(schema)
    for name, value in options.items():
        descriptor.option(name, value)
    table = table_env.from_descriptor(descriptor.build())
    return table_env.to_data_stream(table)
