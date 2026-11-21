"""Detector Kafka input, event-time windows and scored features.

The current entry point previews source coordinates, window snapshots and feature records.

See team/03-realtime-detection.md for the subsequent rule, case and sink stages.
"""

import argparse
import json
import os

from cryptoguard_common.config import load_config
from cryptoguard_common.timeutil import now_us
from detector.baseline import BaselineSettings, ScoredFeature
from detector.features import features_for_window
from detector.source import MODES, kafka_source_options, kafka_trade_source

DEFAULT_DEDUP_TTL_SECONDS = 600
DEFAULT_WATERMARK_IDLE_TIMEOUT_SECONDS = 10


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


def _window_preview(window) -> str:
    return json.dumps(
        {
            "status": "window",
            "symbol": window.symbol,
            "replay_run_id": window.replay_run_id,
            "window_start_ts_us": window.window_start_ts_us,
            "window_end_ts_us": window.window_end_ts_us,
            "trade_count": len(window.trades),
            "is_complete": window.is_complete,
            "late_exchanges": window.late_exchanges,
        }
    )


def _feature_preview(scored: ScoredFeature) -> str:
    return json.dumps(
        {
            "status": "feature",
            "feature": scored.feature,
            "baseline_source": scored.baseline_source,
            "redis_status": scored.redis_status,
        }
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Read the CryptoGuard detector Kafka source")
    parser.add_argument("--mode", choices=MODES, default="live")
    args = parser.parse_args(argv)
    config = load_config()
    try:
        options = kafka_source_options(config, args.mode)
        detector = config["detector"]
        ttl_seconds = detector.get("dedup_ttl_seconds", DEFAULT_DEDUP_TTL_SECONDS)
        if type(ttl_seconds) is not int or ttl_seconds <= 0:
            raise ValueError("detector.dedup_ttl_seconds must be a positive integer")
        watermark_seconds = detector["watermark_seconds"]
        allowed_lateness_seconds = detector["allowed_lateness_seconds"]
        idle_timeout_seconds = detector.get(
            "watermark_idle_timeout_seconds", DEFAULT_WATERMARK_IDLE_TIMEOUT_SECONDS
        )
        if type(idle_timeout_seconds) is not int or idle_timeout_seconds <= 0:
            raise ValueError("detector.watermark_idle_timeout_seconds must be a positive integer")
        for name in ("watermark_seconds", "allowed_lateness_seconds"):
            if type(detector[name]) is not int or detector[name] < 0:
                raise ValueError(f"detector.{name} must be a non-negative integer")
        baseline = BaselineSettings(**detector.get("baseline", {}))
        redis_url = config["redis"]["url"]
        if not isinstance(redis_url, str) or not redis_url.strip():
            raise ValueError("redis.url must be a non-empty string")
    except (KeyError, TypeError, ValueError) as exc:
        parser.error(str(exc))

    from pyflink.common import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    from detector.baseline_stream import enrich_features
    from detector.input_stream import prepare_trades
    from detector.windows import assign_trade_watermarks, window_trades

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config["flink"]["parallelism"])
    jars = [uri.strip() for uri in os.getenv("CG_FLINK_JARS", "").split(";") if uri.strip()]
    if jars:
        env.add_jars(*jars)

    table_env = StreamTableEnvironment.create(env)
    records = kafka_trade_source(table_env, options)
    trades, dlq = prepare_trades(records, args.mode, ttl_seconds)
    timestamped = assign_trade_watermarks(trades, watermark_seconds, idle_timeout_seconds)
    windows, late_dlq = window_trades(timestamped, allowed_lateness_seconds)
    accepted = trades.map(_accepted_preview, output_type=Types.STRING())
    rejected = dlq.union(late_dlq).map(_rejected_preview, output_type=Types.STRING())
    snapshots = windows.map(_window_preview, output_type=Types.STRING())
    features = windows.flat_map(
        lambda window: features_for_window(window, computed_at_ts_us=now_us())
    )
    scored = enrich_features(features, redis_url, baseline)
    accepted.union(
        rejected, snapshots, scored.map(_feature_preview, output_type=Types.STRING())
    ).print()
    env.execute(f"cg-detector-{args.mode}-input")


if __name__ == "__main__":
    main()
