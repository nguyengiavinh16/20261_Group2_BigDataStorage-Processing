"""Read trades and publish detector features, alerts and live DLQ records."""

import argparse
import json
import os

from cryptoguard_common.config import load_config
from detector.baseline import BaselineSettings, ScoredFeature
from detector.outputs import attach_kafka_outputs, kafka_outputs
from detector.source import MODES, kafka_source_options, kafka_trade_source

DEFAULT_DEDUP_TTL_SECONDS = 600
DEFAULT_WATERMARK_IDLE_TIMEOUT_SECONDS = 10


def _accepted_preview(record) -> str:
    return json.dumps(
        {
            "topic": record.source.source_topic,
            "partition": record.source.source_partition,
            "offset": record.source.source_offset,
            "replay_run_id": record.trade.get("replay_run_id"),
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


def _alert_preview(alert: dict) -> str:
    return json.dumps({"status": "alert", "alert": alert})


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the CryptoGuard detector")
    parser.add_argument("--mode", choices=MODES, default="live")
    parser.add_argument(
        "--preview", action="store_true", help="Print pipeline output without writing to Kafka"
    )
    parser.add_argument(
        "--replay-run-id", default=os.getenv("CG_REPLAY_RUN_ID"), help="Process one replay run"
    )
    args = parser.parse_args(argv)
    config = load_config()
    try:
        options = kafka_source_options(config, args.mode)
        if args.preview:
            options["properties.commit.offsets.on.checkpoint"] = "false"
        else:
            outputs = kafka_outputs(config, args.mode)
            checkpoint_interval = config["flink"]["checkpoint_interval_seconds"]
            if type(checkpoint_interval) is not int or checkpoint_interval <= 0:
                raise ValueError("flink.checkpoint_interval_seconds must be a positive integer")
        detector = config["detector"]
        replay_run_id = args.replay_run_id
        if replay_run_id is not None:
            if args.mode != "replay":
                raise ValueError("--replay-run-id requires --mode replay")
            if not replay_run_id.strip() or len(replay_run_id) > 64:
                raise ValueError("--replay-run-id must contain 1 to 64 characters")
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
        pump_rule, dump_rule = detector["pump_rule"], detector["dump_rule"]
        case_merge_minutes = detector["case_merge_minutes"]
        if type(case_merge_minutes) is not int or case_merge_minutes < 0:
            raise ValueError("detector.case_merge_minutes must be a non-negative integer")
        redis_url = config["redis"]["url"]
        if not isinstance(redis_url, str) or not redis_url.strip():
            raise ValueError("redis.url must be a non-empty string")
        metrics_port = detector.get("metrics_port")
        if metrics_port is not None and (
            type(metrics_port) is not int or not 1 <= metrics_port < 65535
        ):
            raise ValueError("detector.metrics_port must be an integer between 1 and 65534")
    except (KeyError, TypeError, ValueError) as exc:
        parser.error(str(exc))

    from pyflink.common import Configuration, Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    from detector.baseline_stream import enrich_features
    from detector.case_stream import case_alerts
    from detector.input_stream import prepare_trades
    from detector.windows import (
        WINDOW_SECONDS,
        BuildWindowFeatures,
        assign_trade_watermarks,
        window_trades,
    )

    flink_config = Configuration()
    if metrics_port is not None:
        reporter = "metrics.reporter.prom"
        flink_config.set_string(
            f"{reporter}.factory.class",
            "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
        )
        flink_config.set_string(f"{reporter}.port", f"{metrics_port}-{metrics_port + 1}")
        flink_config.set_boolean(f"{reporter}.filterLabelValueCharacters", False)
    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_parallelism(config["flink"]["parallelism"])
    if not args.preview:
        env.enable_checkpointing(checkpoint_interval * 1000)
    jars = [uri.strip() for uri in os.getenv("CG_FLINK_JARS", "").split(";") if uri.strip()]
    if jars:
        env.add_jars(*jars)

    table_env = StreamTableEnvironment.create(env)
    records = kafka_trade_source(table_env, options)
    trades, dlq = prepare_trades(records, args.mode, ttl_seconds, replay_run_id=replay_run_id)
    timestamped = assign_trade_watermarks(trades, watermark_seconds, idle_timeout_seconds)
    windows, late_dlq = window_trades(timestamped, allowed_lateness_seconds)
    errors = dlq.union(late_dlq)
    # Keep metric increments in record processing, outside the Python window timer.
    features = (
        windows.flat_map(BuildWindowFeatures()).name("Build window features").disable_chaining()
    )
    scored = enrich_features(features, redis_url, baseline)
    alerts = case_alerts(
        scored,
        pump_rule,
        dump_rule,
        case_merge_minutes=case_merge_minutes,
        window_seconds=WINDOW_SECONDS,
        allowed_lateness_seconds=allowed_lateness_seconds,
    )
    if args.preview:
        trades.map(_accepted_preview, output_type=Types.STRING()).union(
            errors.map(_rejected_preview, output_type=Types.STRING()),
            windows.map(_window_preview, output_type=Types.STRING()),
            scored.map(_feature_preview, output_type=Types.STRING()),
            alerts.map(_alert_preview, output_type=Types.STRING()),
        ).print()
    else:
        if args.mode == "replay":
            errors.map(_rejected_preview, output_type=Types.STRING()).print().set_parallelism(1)
        attach_kafka_outputs(
            table_env,
            {"features": scored.map(lambda row: row.feature), "alerts": alerts, "dlq": errors},
            outputs,
            config["kafka"],
        )
    env.execute(f"cg-detector-{args.mode}")


if __name__ == "__main__":
    main()
