# CryptoGuard I02 - Kafka Ingestion

This package creates the first Kafka topic layout and demonstrates a Python JSON producer/consumer. It can also replay the raw JSONL output from I01 into the Binance raw topic.


## Quick start
1. Start your Kafka broker, configured to listen at `localhost:9092`.
2. From this project root, install Python dependencies:
   `python -m pip install -r requirements.txt`
3. Create topics:
   `python ingest/kafka/create_topics.py`
4. Test with two terminals:
   - Terminal A: `python ingest/kafka/consumer_smoke.py --topic raw.trades.binance --max-messages 1`
   - Terminal B: `python ingest/kafka/producer_smoke.py`
5. If I01 raw file exists, publish it:
   `python ingest/kafka/publish_raw_file.py --file data/raw/binance/trades.jsonl --topic raw.trades.binance --limit 20`

Read `docs/i02_kafka_ingestion.md` for detailed setup, run steps, and troubleshooting notes.
