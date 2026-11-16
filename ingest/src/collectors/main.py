"""Entry point: python -m collectors.main --source binance [--metrics-port 9108]

Reads config (CG_PROFILE), resolves the watchlist to native symbols, publishes RawEnvelopes to raw.trades.<source>.v1.
"""

import argparse
import asyncio
import json

from confluent_kafka import Producer
from prometheus_client import start_http_server

from collectors.binance import BinanceCollector
from collectors.coinbase import CoinbaseCollector
from collectors.kraken import KrakenCollector
from collectors.okx import OkxCollector
from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import topic
from cryptoguard_common.reference import native_symbols

COLLECTORS = {
    "binance": BinanceCollector,
    "okx": OkxCollector,
    "coinbase": CoinbaseCollector,
    "kraken": KrakenCollector,
}


def kafka_sink(cfg: dict):
    pcfg = cfg["kafka"]["producer"]
    producer = Producer(
        {
            "bootstrap.servers": cfg["kafka"]["bootstrap_servers"],
            "acks": pcfg["acks"],
            "enable.idempotence": pcfg["enable_idempotence"],
            "linger.ms": pcfg["linger_ms"],
            "compression.type": pcfg["compression_type"],
        }
    )

    def sink(topic_name: str, key: str | None, envelope: dict) -> None:
        producer.produce(topic_name, key=key, value=json.dumps(envelope, separators=(",", ":")))
        producer.poll(0)

    return sink


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=sorted(COLLECTORS), required=True)
    parser.add_argument("--metrics-port", type=int, default=9108)
    args = parser.parse_args()

    cfg = load_config()
    topic_name = f"raw.trades.{args.source}.v1"
    topic(topic_name)  # fail fast if not in the registry
    symbols = native_symbols(args.source, cfg["symbols"]["watchlist"])
    if not symbols:
        raise SystemExit(f"no symbols for {args.source}: extend contracts/reference/symbol_map.csv")

    start_http_server(args.metrics_port)
    collector = COLLECTORS[args.source](
        cfg["exchanges"][args.source], symbols, kafka_sink(cfg), topic_name
    )
    asyncio.run(collector.run_forever())


if __name__ == "__main__":
    main()
