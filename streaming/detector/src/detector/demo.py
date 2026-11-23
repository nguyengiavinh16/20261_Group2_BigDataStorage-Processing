"""Run the real historical detector replay and render its local HTML demo."""

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from uuid import uuid4

from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import validate
from cryptoguard_common.timeutil import now_us
from detector.historical_data import (
    BASELINE_DAY,
    CONTROL_DAY,
    EVENT_DAY,
    MINUTE_US,
    SYMBOL,
    atomic_write,
    load_historical_data,
)
from detector.outputs import kafka_outputs
from detector.source import kafka_source_options

DEMO_DIR = Path(__file__).resolve().parents[2] / "demo"
TEMPLATE = DEMO_DIR / "index.html"


def topic_offsets(consumer, topics):
    from confluent_kafka import TopicPartition

    offsets = {}
    for topic in topics:
        metadata = consumer.list_topics(topic=topic, timeout=8).topics[topic]
        if metadata.error is not None:
            raise RuntimeError(f"Kafka topic {topic}: {metadata.error}")
        for partition in metadata.partitions:
            key = (topic, partition)
            offsets[key] = consumer.get_watermark_offsets(TopicPartition(*key), timeout=8)[1]
    return offsets


def _ranges(before, after):
    return [
        {"topic": key[0], "partition": key[1], "start": before[key], "end_exclusive": offset}
        for key, offset in sorted(after.items())
    ]


def _publish(producer, topic: str, trades: list[dict], run_id: str, speed: float) -> float:
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    first_ts = trades[0]["event_ts_us"]
    started = time.monotonic()
    for trade in trades:
        record = trade | {"replay_run_id": run_id, "ingest_ts_us": now_us()}
        validate(record, "canonical_trade.v1")
        if speed:
            target = started + (trade["event_ts_us"] - first_ts) / 1_000_000 / speed
            time.sleep(max(0.0, target - time.monotonic()))
        producer.produce(
            topic, key=trade["symbol"].encode(), value=json.dumps(record), on_delivery=delivered
        )
        producer.poll(0)
    if producer.flush(30) or errors:
        raise RuntimeError(f"Kafka replay publish failed: {errors[:3]}")
    return time.monotonic() - started


def _read(consumer, before, after, run_id, contracts):
    from confluent_kafka import KafkaError, TopicPartition

    ends = {key: end for key, end in after.items() if end > before[key]}
    consumer.assign([TopicPartition(*key, before[key]) for key in ends])
    rows = {topic: [] for topic in contracts}
    deadline = time.monotonic() + 40
    while ends and time.monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue
        key = (message.topic(), message.partition())
        if key not in ends:
            continue
        error, offset = message.error(), message.offset()
        if error and error.code() != KafkaError._PARTITION_EOF:
            raise RuntimeError(str(error))
        end = ends[key]
        if not error and offset < end:
            record = json.loads(message.value())
            if record.get("replay_run_id") == run_id:
                schema, key_field = contracts[key[0]]
                validate(record, schema)
                if message.key() != record[key_field].encode():
                    raise ValueError(f"Invalid Kafka output key at {key}/{offset}")
                rows[key[0]].append(
                    {"topic": key[0], "partition": key[1], "offset": offset, "record": record}
                )
        if offset + (0 if error else 1) >= end:
            del ends[key]
    if ends:
        raise TimeoutError(f"Kafka output was not fully read: {ends}")
    return rows


def equivalence_key(records: list[dict]) -> list[str]:
    stable = {"replay_run_id", "computed_at_ts_us", "detected_at_ts_us", "alert_id"}
    return sorted(
        json.dumps({k: v for k, v in row["record"].items() if k not in stable}, sort_keys=True)
        for row in records
    )


def _seed_baselines(redis_url: str, snapshots: dict) -> None:
    from redis import Redis

    with Redis.from_url(redis_url) as redis:
        for key, snapshot in snapshots.items():
            if not redis.set(key, json.dumps(snapshot), nx=True, ex=3600):
                previous = redis.get(key)
                if previous is None or json.loads(previous) != snapshot:
                    raise RuntimeError(f"Demo baseline differs from existing Redis key {key}")


def _run_once(config: dict, data: dict, speed: float) -> tuple[dict, dict]:
    from confluent_kafka import Consumer, Producer

    source = kafka_source_options(config, "replay")
    outputs = kafka_outputs(config, "replay")
    input_topic = source["topic"]
    trades = data["trades"]
    features, alerts = outputs["features"], outputs["alerts"]
    contracts = {
        features.topic: ("symbol_features_1m.v1", features.key_field),
        alerts.topic: ("alert.v1", alerts.key_field),
    }
    run_id = f"m12-gvt-{uuid4().hex[:12]}"
    settings = {"bootstrap.servers": source["properties.bootstrap.servers"]}
    producer = Producer(settings)
    consumer = Consumer(
        settings
        | {
            "group.id": f"demo-reader-{run_id}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    try:
        before = topic_offsets(consumer, (input_topic, *contracts))
        print(f"{run_id} publishing {len(trades)} trades at speed {speed:g}", flush=True)
        send_seconds = _publish(producer, input_topic, trades, run_id, speed)
        _seed_baselines(config["redis"]["url"], data["snapshots"])
        started = time.monotonic()
        subprocess.run(
            [sys.executable, "-m", "detector.job", "--mode", "replay", "--replay-run-id", run_id],
            env=os.environ | {"CG_PROFILE": "local"},
            check=True,
            timeout=240,
        )
        flink_seconds = time.monotonic() - started
        after = topic_offsets(consumer, (input_topic, *contracts))
        rows = _read(
            consumer,
            {key: offset for key, offset in before.items() if key[0] in contracts},
            {key: offset for key, offset in after.items() if key[0] in contracts},
            run_id,
            contracts,
        )
    finally:
        consumer.close()
    records = {"features": rows[features.topic], "alerts": rows[alerts.topic]}
    return {
        "run_id": run_id,
        "speed": speed,
        "trades": len(trades),
        "features": len(records["features"]),
        "alerts": len(records["alerts"]),
        "send_seconds": round(send_seconds, 3),
        "flink_seconds": round(flink_seconds, 3),
        "offsets": _ranges(before, after),
    }, records


def run_demo(cache: Path, output: Path, paced_speed: float) -> dict:
    from cg_ml.evaluation.metrics import PumpEvent, evaluate, precision_at_k

    config = load_config("local")
    data = load_historical_data(cache, config["data_budget"])
    paced, original = _run_once(config, data, paced_speed)
    fast, records = _run_once(config, data, 0)
    parity = {
        kind: equivalence_key(original[kind]) == equivalence_key(rows)
        for kind, rows in records.items()
    }
    label = PumpEvent("la-morgia-gvt-2020-09-17", SYMBOL, "binance", data["label_ts_us"])
    alerts = [row["record"] for row in records["alerts"] if row["record"]["exchange"] == "binance"]
    metrics = evaluate(alerts, [label], days_covered=2.0)
    smoke = evaluate(alerts, [label], days_covered=2.0, before_minutes=30)
    control_start = data["label_ts_us"] - 24 * 60 * MINUTE_US
    ranked = min(5, metrics.n_alerts)
    featured = [
        row["record"]
        for row in records["features"]
        if any(
            abs(row["record"]["window_start_ts_us"] - center) <= 30 * MINUTE_US
            for center in (data["label_ts_us"], control_start)
        )
    ]
    report = {
        "generated_at_ts_us": now_us(),
        "symbol": SYMBOL,
        "event_day": str(EVENT_DAY),
        "control_day": str(CONTROL_DAY),
        "baseline_day": str(BASELINE_DAY),
        "label_ts_us": data["label_ts_us"],
        "label_times": data["label_times"],
        "control_ts_us": control_start,
        "source_files": data["sources"],
        "trade_counts": data["trade_counts"],
        "runs": [paced, fast],
        "parity": parity,
        "smoke_pass": smoke.detected_events == 1,
        "rules": {key: config["detector"][key] for key in ("pump_rule", "dump_rule")},
        "baseline": data["snapshots"].get(f"baseline:{SYMBOL}:binance:17"),
        "features": sorted(featured, key=lambda row: row["window_start_ts_us"]),
        "alerts": sorted(records["alerts"], key=lambda row: row["record"]["window_start_ts_us"]),
        "evaluation": {
            "recall": metrics.recall,
            "detected_events": metrics.detected_events,
            "events": metrics.n_events,
            "precision_at_5": precision_at_k(alerts, [label], 5),
            "ranked_alerts": ranked,
            "median_delay_s": metrics.median_delay_s,
            "false_alerts_per_day": metrics.false_alerts_per_day,
            "control_pump_alerts": sum(
                row["alert_type"] == "PUMP_SUSPECTED"
                and row["window_start_ts_us"] // (24 * 60 * MINUTE_US)
                == control_start // (24 * 60 * MINUTE_US)
                for row in alerts
            ),
        },
    }
    payload = json.dumps(report, ensure_ascii=False, allow_nan=False).replace("<", r"\u003c")
    html = TEMPLATE.read_text(encoding="utf-8").replace("<!--REPLAY_DATA-->null", payload)
    atomic_write(output, html.encode("utf-8"))
    print(f"Demo written to {output}. Smoke={report['smoke_pass']} parity={parity}", flush=True)
    return report


def serve(cache: Path, output: Path, speed: float, port: int) -> None:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    status = {"state": "idle"}
    lock = threading.Lock()

    def replay():
        try:
            run_demo(cache, output, speed)
        except Exception:
            traceback.print_exc()
            result = "error"
        else:
            result = "done"
        with lock:
            status["state"] = result

    class Handler(BaseHTTPRequestHandler):
        def send(self, payload, content_type, code=200):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == "/":
                self.send(
                    (output if output.is_file() else TEMPLATE).read_bytes(),
                    "text/html; charset=utf-8",
                )
            elif self.path == "/api/status":
                with lock:
                    body = json.dumps(status).encode()
                self.send(body, "application/json")
            else:
                self.send_error(404)

        def do_POST(self):
            if self.path != "/api/replay":
                self.send_error(404)
                return
            if self.headers.get("Origin") != f"http://127.0.0.1:{port}":
                self.send_error(403)
                return
            with lock:
                if status["state"] == "running":
                    self.send_error(409)
                    return
                status["state"] = "running"
            threading.Thread(target=replay, daemon=True).start()
            self.send(b'{"state":"running"}', "application/json", 202)

    with ThreadingHTTPServer(("127.0.0.1", port), Handler) as server:
        print(f"Detector replay demo: http://127.0.0.1:{port}", flush=True)
        server.serve_forever()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=Path.home() / ".cache" / "cg-historical")
    parser.add_argument("--output", type=Path, default=DEMO_DIR / "replay-local.html")
    parser.add_argument("--paced-speed", type=float, default=3600)
    parser.add_argument("--serve", type=int, metavar="PORT")
    args = parser.parse_args(argv)
    if not 1 <= args.paced_speed < float("inf"):
        parser.error("--paced-speed must be finite and at least 1")
    if args.output.resolve() == TEMPLATE.resolve():
        parser.error("--output must be separate from index.html")
    if args.serve is not None:
        if not 1 <= args.serve <= 65535:
            parser.error("--serve PORT must be between 1 and 65535")
        serve(args.cache_dir, args.output, args.paced_speed, args.serve)
    else:
        result = run_demo(args.cache_dir, args.output, args.paced_speed)
        if not result["smoke_pass"] or not all(result["parity"].values()):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
