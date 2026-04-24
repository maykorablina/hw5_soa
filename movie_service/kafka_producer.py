import json
import os
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_NAME = "movie-events"

_schema_str = (Path(__file__).parent / "schemas" / "movie_event.avsc").read_text()

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

avro_serializer = AvroSerializer(
    schema_registry_client,
    _schema_str,
    lambda obj, ctx: obj,
)

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def produce_event(event: dict):
    producer.produce(
        topic=TOPIC_NAME,
        key=event["user_id"],
        value=avro_serializer(event, SerializationContext(TOPIC_NAME, MessageField.VALUE)),
        on_delivery=delivery_report,
    )
    producer.flush()
