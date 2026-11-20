"""Detector Kafka input for DET-04 and DET-07.

The current entry point previews source coordinates. Kafka key and value bytes stay
on the DataStream for the validation stage.

See team/03-realtime-detection.md for the subsequent feature, rule, case and sink stages.
"""

import argparse
import json
import os

from cryptoguard_common.config import load_config
from detector.source import MODES, kafka_source_options, kafka_trade_source


def _source_position(record) -> str:
    return json.dumps(
        {
            "topic": record.source_topic,
            "partition": record.source_partition,
            "offset": record.source_offset,
        }
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Read the CryptoGuard detector Kafka source")
    parser.add_argument("--mode", choices=MODES, default="live")
    args = parser.parse_args(argv)
    config = load_config()
    try:
        options = kafka_source_options(config, args.mode)
    except (KeyError, ValueError) as exc:
        parser.error(str(exc))

    from pyflink.common import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config["flink"]["parallelism"])
    jars = [uri.strip() for uri in os.getenv("CG_FLINK_JARS", "").split(";") if uri.strip()]
    if jars:
        env.add_jars(*jars)

    table_env = StreamTableEnvironment.create(env)
    records = kafka_trade_source(table_env, options)
    records.map(_source_position, output_type=Types.STRING()).print()
    env.execute(f"cg-detector-{args.mode}-source")


if __name__ == "__main__":
    main()
