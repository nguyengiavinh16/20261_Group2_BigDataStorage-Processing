"""Publish a small synthetic record to verify Kafka connectivity."""
from datetime import datetime, timezone
from producer import JsonKafkaProducer
from config import RAW_BINANCE_TOPIC




def main():
    sample = {
        "source": "binance",
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "raw_message": {
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade", "s": "BTCUSDT", "t": 999000001,
                "p": "65000.10", "q": "0.001", "T": 1760000000000,
                "m": False
            }
        }
    }
    producer = JsonKafkaProducer()
    try:
        producer.send(RAW_BINANCE_TOPIC, sample, key="btcusdt@trade")
        producer.flush()
        print("Smoke-test record published.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
