"""Small JSON Kafka producer wrapper used in I02."""
import json
from kafka import KafkaProducer
from config import BOOTSTRAP_SERVERS



class JsonKafkaProducer:
    def __init__(self, bootstrap_servers=BOOTSTRAP_SERVERS):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
            acks="all",
            retries=5,
            linger_ms=10,
        )

    def send(self, topic, value, key=None):
        future = self.producer.send(topic, key=key, value=value)
        metadata = future.get(timeout=15)
        print(f"[SENT] topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}")
        return metadata


    def flush(self):
        self.producer.flush()


    def close(self):
        self.producer.close()
