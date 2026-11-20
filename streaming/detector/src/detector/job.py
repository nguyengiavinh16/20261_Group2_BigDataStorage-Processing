"""Detector Kafka input, validation and deduplication for DET-04 and DET-07.

The current entry point previews accepted and rejected source coordinates.

See team/03-realtime-detection.md for the subsequent feature, rule, case and sink stages.
"""

import argparse
import json
import os

from cryptoguard_common.config import load_config
from detector.source import MODES, kafka_source_options, kafka_trade_source

DEFAULT_DEDUP_TTL_SECONDS = 600


def _accepted_preview(record) -> str:
    return json.dumps(
        {
            "topic": record.source.source_topic,
            "partition": record.source.source_partition,
            "offset": record.source.source_offset,
            "status": "accepted",
        }
    )


def _rejected_preview(record: dict) -> str:
    return json.dumps(
        {
            "topic": record["source_topic"],
            "partition": record["source_partition"],
            "offset": record["source_offset"],
            "status": "rejected",
            "error_type": record["error_type"],
        }
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Read the CryptoGuard detector Kafka source")
    parser.add_argument("--mode", choices=MODES, default="live")
    args = parser.parse_args(argv)
    config = load_config()
    try:
        options = kafka_source_options(config, args.mode)
        ttl_seconds = config["detector"].get("dedup_ttl_seconds", DEFAULT_DEDUP_TTL_SECONDS)
        if type(ttl_seconds) is not int or ttl_seconds <= 0:
            raise ValueError("detector.dedup_ttl_seconds must be a positive integer")
    except (KeyError, ValueError) as exc:
        parser.error(str(exc))

    from pyflink.common import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    from detector.input_stream import prepare_trades

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config["flink"]["parallelism"])
    jars = [uri.strip() for uri in os.getenv("CG_FLINK_JARS", "").split(";") if uri.strip()]
    if jars:
        env.add_jars(*jars)

    table_env = StreamTableEnvironment.create(env)
    records = kafka_trade_source(table_env, options)
    trades, dlq = prepare_trades(records, args.mode, ttl_seconds)
    accepted = trades.map(_accepted_preview, output_type=Types.STRING())
    rejected = dlq.map(_rejected_preview, output_type=Types.STRING())
    accepted.union(rejected).print()
    env.execute(f"cg-detector-{args.mode}-input")


if __name__ == "__main__":
    main()
