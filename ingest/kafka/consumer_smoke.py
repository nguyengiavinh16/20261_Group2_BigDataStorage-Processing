"""Read messages from a topic for a basic Kafka smoke test."""
import argparse
import json
from kafka import KafkaConsumer
from config import BOOTSTRAP_SERVERS, RAW_BINANCE_TOPIC




def main():
    parser = argparse.ArgumentParser(description="Kafka consumer smoke test")
    parser.add_argument("--topic", default=RAW_BINANCE_TOPIC)
    parser.add_argument("--max-messages", type=int, default=10)
    parser.add_argument("--group", default="cryptoguard-i02-smoke")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=15000,
    )
    count = 0
    try:
        print(f"Listening topic={args.topic} (max {args.max_messages})...")
        for message in consumer:
            count += 1
            print(f"\n[{count}] partition={message.partition} offset={message.offset} key={message.key!r}")
            print(json.dumps(message.value, ensure_ascii=False, indent=2))
            if count >= args.max_messages:
                break
        print(f"\nConsumer finished. Read {count} message(s).")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
