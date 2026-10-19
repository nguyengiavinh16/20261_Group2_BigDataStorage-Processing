#!/usr/bin/env bash
# DRAFT W1 - create the topics defined in kafka-design.md.
# Run INSIDE the broker container:
#   docker cp configs/kafka/create-topics.sh kafka:/tmp/create-topics.sh
#   docker exec kafka bash /tmp/create-topics.sh
# Check from the host:
#   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
KT=/opt/kafka/bin/kafka-topics.sh
RF="${KAFKA_REPLICATION_FACTOR:-1}"   # dev: 1 broker

create() {
  local topic=$1 partitions=$2 retention_ms=$3
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor "$RF" \
    --config retention.ms="$retention_ms" \
    --config cleanup.policy=delete
}

# Required topics (MVP)
create fraud.transactions.v1      3  604800000    # 7 days
create fraud.transactions.dlq.v1  1  1209600000   # 14 days, kept longer for error investigation
# Optional topic (enable when results need to fan out to consumers other than the NoSQL sink)
create fraud.predictions.v1       3  604800000

"$KT" --bootstrap-server "$BOOTSTRAP" --list
for t in fraud.transactions.v1 fraud.transactions.dlq.v1 fraud.predictions.v1; do
  "$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic "$t"
done
