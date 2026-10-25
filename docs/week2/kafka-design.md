# Kafka design and runbook

- Topic: `transactions` (if the team chose `transaction-events`, change producer/consumer/docs consistently).
- Key: `transaction_id`; value: UTF-8 JSON transaction event.
- MVP local partitions: 1. Record the actual retention setting from broker config; do not assume a duration.
- Replay means replaying historical dataset rows, not receiving live bank traffic.

## Windows Kafka 4.x KRaft example
From the Kafka installation directory, only format storage on first setup:
```bat
bin\\windows\\kafka-storage.bat random-uuid
bin\\windows\\kafka-storage.bat format --standalone -t <CLUSTER_ID> -c config\\server.properties
bin\\windows\\kafka-server-start.bat config\\server.properties
```
Stop with Ctrl+C. Do not reformat existing storage on each restart.

Create/inspect topic:
```bat
bin\\windows\\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic transactions --partitions 1 --replication-factor 1
bin\\windows\\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic transactions
```
If topic already exists, inspect it instead of recreating. Confirm commands against the team's actual Kafka version/config.
