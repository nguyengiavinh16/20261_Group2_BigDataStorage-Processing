"""Publish I01 JSONL raw collector records to a Kafka raw topic."""
import argparse
import json
from pathlib import Path
from producer import JsonKafkaProducer
from config import RAW_BINANCE_TOPIC




def main():
    parser = argparse.ArgumentParser(description="Publish Binance raw JSONL records to Kafka")
    parser.add_argument("--file", default="data/raw/binance/trades.jsonl", help="I01 JSONL file")
    parser.add_argument("--topic", default=RAW_BINANCE_TOPIC)
    parser.add_argument("--limit", type=int, default=0, help="0 means publish all records")
    args = parser.parse_args()
    path = Path(args.file)
    if not path.exists():
        raise FileNotFoundError(f"Input JSONL file not found: {path}. Run I01 collector first.")

    producer = JsonKafkaProducer()
    sent = 0
    try:
        with path.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                record = json.loads(line)
                # Preserve the I01 raw record as-is. Use the stream name as the Kafka key
                # for simple partition affinity by stream/symbol at this stage.
                raw_message = record.get("raw_message", {})
                stream_key = raw_message.get("stream", "unknown")
                producer.send(args.topic, record, key=stream_key)
                sent += 1
                if args.limit and sent >= args.limit:
                    break
        producer.flush()
        print(f"Finished publishing {sent} record(s) to {args.topic}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
