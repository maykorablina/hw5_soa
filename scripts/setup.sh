#!/bin/bash
set -e

echo "Creating Kafka topic movie-events..."
kafka-topics --create --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic movie-events \
  --partitions 3 \
  --replication-factor 1

echo "Creating ClickHouse Kafka Engine tables..."
clickhouse-client --host clickhouse --port 9000 --query "
CREATE TABLE IF NOT EXISTS movie_events_queue (
    event_id  String,
    user_id   String,
    movie_id  String,
    event_type String,
    timestamp String,
    device_type String,
    session_id String,
    progress_seconds Nullable(Int32)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list  = 'movie-events',
    kafka_group_name  = 'clickhouse-consumer',
    kafka_format      = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_events_mv TO movie_events AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    parseDateTimeBestEffort(timestamp) AS timestamp,
    device_type,
    session_id,
    progress_seconds
FROM movie_events_queue;
"

echo "Setup complete."
