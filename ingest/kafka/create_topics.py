"""Create CryptoGuard Kafka topics (safe to rerun; existing topics are skipped)."""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config import BOOTSTRAP_SERVERS, RAW_BINANCE_TOPIC, RAW_SECOND_TOPIC, DLQ_TOPIC, CANONICAL_TOPIC



def main():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="cryptoguard-topic-admin")
    topics = [
        NewTopic(RAW_BINANCE_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(RAW_SECOND_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(DLQ_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(CANONICAL_TOPIC, num_partitions=3, replication_factor=1),
    ]
    try:
        results = admin.create_topics(new_topics=topics, validate_only=False)
        for name, future in results.items():
            try:
                future.result()
                print(f"[CREATED] {name}")
            except TopicAlreadyExistsError:
                print(f"[EXISTS]  {name} (left unchanged)")
            except Exception as exc:
                print(f"[ERROR]   {name}: {exc}")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
