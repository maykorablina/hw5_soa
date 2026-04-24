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

CREATE TABLE IF NOT EXISTS daily_dau_agg (
    date     Date,
    user_hll AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
ORDER BY date;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_dau_mv TO daily_dau_agg AS
SELECT
    toDate(timestamp) AS date,
    uniqState(user_id) AS user_hll
FROM movie_events
GROUP BY date;

CREATE TABLE IF NOT EXISTS daily_watch_time_agg (
    date      Date,
    avg_state AggregateFunction(avg, Nullable(Int32))
) ENGINE = AggregatingMergeTree()
ORDER BY date;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_watch_time_mv TO daily_watch_time_agg AS
SELECT
    toDate(timestamp) AS date,
    avgState(progress_seconds) AS avg_state
FROM movie_events
WHERE event_type = 'VIEW_FINISHED'
  AND progress_seconds IS NOT NULL
GROUP BY date;

CREATE TABLE IF NOT EXISTS daily_movie_views_agg (
    date        Date,
    movie_id    String,
    views_count AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (date, movie_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_movie_views_mv TO daily_movie_views_agg AS
SELECT
    toDate(timestamp) AS date,
    movie_id,
    countState() AS views_count
FROM movie_events
WHERE event_type = 'VIEW_STARTED'
GROUP BY date, movie_id;

CREATE TABLE IF NOT EXISTS daily_event_counts_agg (
    date        Date,
    event_type  String,
    event_count AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (date, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_event_counts_mv TO daily_event_counts_agg AS
SELECT
    toDate(timestamp) AS date,
    event_type,
    countState() AS event_count
FROM movie_events
GROUP BY date, event_type;
