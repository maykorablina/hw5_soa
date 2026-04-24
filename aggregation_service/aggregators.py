import logging
import os
from datetime import date, timedelta

import clickhouse_connect

logger = logging.getLogger("aggregators")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))


def _client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


def compute_dau(target_date: date) -> float:
    client = _client()
    r = client.query(
        "SELECT uniqMerge(user_hll) FROM daily_dau_agg WHERE date = {d:Date}",
        parameters={"d": target_date},
    )
    return float(r.first_row[0] or 0)


def compute_avg_watch_time(target_date: date) -> float:
    client = _client()
    r = client.query(
        "SELECT avgMerge(avg_state) FROM daily_watch_time_agg WHERE date = {d:Date}",
        parameters={"d": target_date},
    )
    val = r.first_row[0]
    return float(val) if val else 0.0


def compute_top_movies(target_date: date) -> list:
    client = _client()
    r = client.query(
        "SELECT movie_id, countMerge(views_count) AS views "
        "FROM daily_movie_views_agg WHERE date = {d:Date} "
        "GROUP BY movie_id ORDER BY views DESC LIMIT 10",
        parameters={"d": target_date},
    )
    return [{"movie_id": row[0], "views": int(row[1])} for row in r.result_rows]


def compute_conversion(target_date: date) -> float:
    client = _client()
    r = client.query(
        "SELECT event_type, countMerge(event_count) AS cnt "
        "FROM daily_event_counts_agg WHERE date = {d:Date} "
        "AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED') "
        "GROUP BY event_type",
        parameters={"d": target_date},
    )
    counts = {row[0]: int(row[1]) for row in r.result_rows}
    started = counts.get("VIEW_STARTED", 0)
    finished = counts.get("VIEW_FINISHED", 0)
    return float(finished) / float(started) if started > 0 else 0.0


def _retained_count(client, base_date: date, check_date: date) -> int:
    r = client.query(
        "SELECT uniq(user_id) FROM movie_events "
        "WHERE event_type = 'VIEW_STARTED' "
        "AND toDate(timestamp) = {check:Date} "
        "AND user_id IN ("
        "  SELECT DISTINCT user_id FROM movie_events "
        "  WHERE event_type = 'VIEW_STARTED' AND toDate(timestamp) = {base:Date}"
        ")",
        parameters={"check": check_date, "base": base_date},
    )
    return int(r.first_row[0] or 0)


def compute_retention(target_date: date) -> dict:
    client = _client()
    base_r = client.query(
        "SELECT uniq(user_id) FROM movie_events "
        "WHERE event_type = 'VIEW_STARTED' AND toDate(timestamp) = {d:Date}",
        parameters={"d": target_date},
    )
    base_count = int(base_r.first_row[0] or 0)
    if base_count == 0:
        return {"d1": 0.0, "d7": 0.0}
    d1 = _retained_count(client, target_date, target_date + timedelta(days=1))
    d7 = _retained_count(client, target_date, target_date + timedelta(days=7))
    return {"d1": d1 / base_count, "d7": d7 / base_count}


def count_processed(target_date: date) -> int:
    client = _client()
    r = client.query(
        "SELECT count() FROM movie_events WHERE toDate(timestamp) = {d:Date}",
        parameters={"d": target_date},
    )
    return int(r.first_row[0] or 0)
