"""Shared Kafka settings for CryptoGuard I02."""
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_BINANCE_TOPIC = os.getenv("RAW_BINANCE_TOPIC", "raw.trades.binance")
RAW_SECOND_TOPIC = os.getenv("RAW_SECOND_TOPIC", "raw.trades.kraken")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq.trades")
CANONICAL_TOPIC = os.getenv("CANONICAL_TOPIC", "canonical.trades")
