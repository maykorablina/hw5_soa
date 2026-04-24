import time
import uuid

import clickhouse_connect
import pytest
import requests

MOVIE_SERVICE_URL = "http://movie-service:8000"
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
POLL_INTERVAL = 2
MAX_WAIT = 60


@pytest.fixture(scope="session", autouse=True)
def wait_for_services():
    _wait_http(f"{MOVIE_SERVICE_URL}/health", timeout=60)
    _wait_clickhouse(timeout=60)


def _wait_http(url: str, timeout: int):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"Service not ready: {url}")


def _wait_clickhouse(timeout: int):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
            client.query("SELECT 1")
            return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("ClickHouse not ready")


def _clickhouse_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


def test_event_pipeline_end_to_end():
    session_id = str(uuid.uuid4())
    user_id = f"test_user_{uuid.uuid4().hex[:8]}"

    payload = {
        "user_id": user_id,
        "movie_id": "test_movie_001",
        "event_type": "VIEW_STARTED",
        "device_type": "DESKTOP",
        "session_id": session_id,
        "progress_seconds": 0,
    }

    response = requests.post(f"{MOVIE_SERVICE_URL}/events", json=payload, timeout=10)
    assert response.status_code == 200, f"Unexpected status: {response.status_code} {response.text}"

    data = response.json()
    assert "event_id" in data
    assert data["status"] == "published"

    event_id = data["event_id"]

    client = _clickhouse_client()
    deadline = time.time() + MAX_WAIT
    row = None
    while time.time() < deadline:
        result = client.query(
            "SELECT event_id, user_id, movie_id, event_type, device_type, session_id, progress_seconds "
            "FROM movie_events WHERE event_id = %(event_id)s",
            parameters={"event_id": event_id},
        )
        if result.row_count > 0:
            row = result.first_row
            break
        time.sleep(POLL_INTERVAL)

    assert row is not None, f"Event {event_id} did not appear in ClickHouse within {MAX_WAIT}s"

    assert row[0] == event_id
    assert row[1] == user_id
    assert row[2] == "test_movie_001"
    assert row[3] == "VIEW_STARTED"
    assert row[4] == "DESKTOP"
    assert row[5] == session_id
    assert row[6] == 0


def test_validation_error_returns_422():
    response = requests.post(
        f"{MOVIE_SERVICE_URL}/events",
        json={"user_id": "u1", "movie_id": "m1", "event_type": "INVALID_TYPE", "device_type": "DESKTOP", "session_id": "s1"},
        timeout=10,
    )
    assert response.status_code == 422


def test_batch_events_pipeline():
    user_id = f"test_user_{uuid.uuid4().hex[:8]}"
    session_id = str(uuid.uuid4())

    batch = [
        {"user_id": user_id, "movie_id": "movie_batch_001", "event_type": "VIEW_STARTED", "device_type": "MOBILE", "session_id": session_id, "progress_seconds": 0},
        {"user_id": user_id, "movie_id": "movie_batch_001", "event_type": "VIEW_FINISHED", "device_type": "MOBILE", "session_id": session_id, "progress_seconds": 3600},
    ]

    response = requests.post(f"{MOVIE_SERVICE_URL}/events/batch", json=batch, timeout=10)
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 2

    client = _clickhouse_client()
    event_ids = [r["event_id"] for r in results]

    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        result = client.query(
            "SELECT event_id FROM movie_events WHERE event_id IN %(ids)s",
            parameters={"ids": event_ids},
        )
        if result.row_count == 2:
            break
        time.sleep(POLL_INTERVAL)
    else:
        pytest.fail(f"Batch events did not appear in ClickHouse within {MAX_WAIT}s")
