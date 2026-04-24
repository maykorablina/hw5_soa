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

CREATE TABLE IF NOT EXISTS movie_events (
    event_id    String,
    user_id     String,
    movie_id    String,
    event_type  String,
    timestamp   DateTime,
    device_type String,
    session_id  String,
    progress_seconds Nullable(Int32)
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp);

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
