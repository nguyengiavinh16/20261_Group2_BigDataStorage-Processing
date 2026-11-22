# I02 - Kafka Ingestion Setup & Smoke Test

## Goal
Verify a local Kafka broker, create CryptoGuard topic layout, and prove that Python can publish JSON messages and a separate consumer can read them. This stage is a smoke test; it is not the production ingestion service.

## Topics
See `config/kafka/topic_design.md`.

## Prerequisites
- Kafka is installed and a local broker is running (example bootstrap: `localhost:9092`).
- Run the commands from the repository root.
- I01 collector output exists at `data/raw/binance/trades.jsonl` for file replay test (optional for synthetic smoke test).

## Windows local Kafka (KRaft) quick start
Use the Kafka distribution's own scripts and the config file that your local installation was set up with. Kafka 4.x uses KRaft and does not need ZooKeeper. If your broker is already formatted and running, do not format it again.

1. Start broker in a separate terminal from Kafka installation folder. Example:
   `bin\windows\kafka-server-start.bat config\server.properties`
2. Confirm the configured listener is reachable on `localhost:9092`.
3. If the broker has not been initialized yet, follow Kafka's official quickstart for storage UUID generation and formatting. **Formatting is an initialization operation; do not run it against an existing broker data directory.**

## Python environment
```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

## Create topics
```bash
python ingest/kafka/create_topics.py
```
List topics using Kafka CLI (run from Kafka install folder):
```bash
bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```

## Producer smoke test
Terminal A:
```bash
python ingest/kafka/consumer_smoke.py --topic raw.trades.binance --max-messages 1
```
Terminal B:
```bash
python ingest/kafka/producer_smoke.py
```
Terminal A should display the sample event.

## Publish I01 data file (optional)
After running I01 collector and creating `data/raw/binance/trades.jsonl`:
```bash
python ingest/kafka/publish_raw_file.py --file data/raw/binance/trades.jsonl --topic raw.trades.binance --limit 20
```
To publish all records, omit `--limit` or set it to 0:
```bash
python ingest/kafka/publish_raw_file.py --file data/raw/binance/trades.jsonl --topic raw.trades.binance
```
Then start/restart consumer with a fresh group id if you want to read the beginning of the topic:
```bash
python ingest/kafka/consumer_smoke.py --topic raw.trades.binance --max-messages 10 --group cryptoguard-i02-check
```

## Scripts
- `create_topics.py`: create four project topics; existing topics are reported and skipped.
- `producer.py`: shared JSON producer with acknowledgements and retries.
- `producer_smoke.py`: send one synthetic test record.
- `consumer_smoke.py`: read messages and print JSON.
- `publish_raw_file.py`: publish the raw records from I01 JSONL file.

## Expected result
A producer prints the topic, partition, and offset of the published message; a separate consumer prints the payload. The I02 milestone passes when topic creation is verified and producer/consumer successfully exchange a record.

## Limitations / decisions to revisit
- Replication factor 1 is for a single local broker only.
- Partitions = 3 are an initial development choice, not benchmarked or tuned.
- Kafka key for raw message is the stream name in this smoke test.
- No schema registry, retry topic, production ACL, security, monitoring, or high availability in I02.
- Confirm topic names, partition count, and retention with N1/team before integration.
