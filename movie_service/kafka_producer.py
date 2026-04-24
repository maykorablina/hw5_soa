import json
import logging
import os
import time
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("kafka_producer")

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

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "retry.backoff.ms": 500,
        "delivery.timeout.ms": 30000,
    }
)


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for event: %s", err)
    else:
        logger.info(
            "Event delivered: topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def produce_event(event: dict, max_retries: int = 5) -> None:
    delay = 0.5
    for attempt in range(1, max_retries + 1):
        try:
            serialized = avro_serializer(
                event,
                SerializationContext(TOPIC_NAME, MessageField.VALUE),
            )
            producer.produce(
                topic=TOPIC_NAME,
                key=event["user_id"],
                value=serialized,
                on_delivery=_delivery_report,
            )
            producer.flush(timeout=10)
            logger.info(
                "Published event_id=%s event_type=%s timestamp=%s",
                event["event_id"],
                event["event_type"],
                event["timestamp"],
            )
            return
        except KafkaException as exc:
            logger.warning(
                "Attempt %d/%d failed: %s. Retrying in %.1fs...",
                attempt,
                max_retries,
                exc,
                delay,
            )
            if attempt == max_retries:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 30)
