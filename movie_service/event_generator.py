import asyncio
import random
import uuid
from datetime import datetime, timezone

from kafka_producer import produce_event

MOVIE_IDS = ["movie_001", "movie_002", "movie_003", "movie_004", "movie_005"]
DEVICE_TYPES = ["MOBILE", "DESKTOP", "TV", "TABLET"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_event(user_id: str, movie_id: str, session_id: str, event_type: str, progress: int | None) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": event_type,
        "timestamp": _now(),
        "device_type": random.choice(DEVICE_TYPES),
        "session_id": session_id,
        "progress_seconds": progress,
    }


async def generate_user_session(user_id: str) -> list[dict]:
    movie_id = random.choice(MOVIE_IDS)
    session_id = str(uuid.uuid4())
    events = []
    progress = 0

    start_event = _make_event(user_id, movie_id, session_id, "VIEW_STARTED", progress)
    events.append(start_event)
    produce_event(start_event)
    await asyncio.sleep(random.uniform(0.05, 0.15))

    pauses = random.randint(0, 2)
    for _ in range(pauses):
        progress += random.randint(30, 300)
        pause_event = _make_event(user_id, movie_id, session_id, "VIEW_PAUSED", progress)
        events.append(pause_event)
        produce_event(pause_event)
        await asyncio.sleep(random.uniform(0.05, 0.1))

        resume_event = _make_event(user_id, movie_id, session_id, "VIEW_RESUMED", progress)
        events.append(resume_event)
        produce_event(resume_event)
        await asyncio.sleep(random.uniform(0.05, 0.1))

    if random.random() < 0.4:
        like_event = _make_event(user_id, movie_id, session_id, "LIKED", progress)
        events.append(like_event)
        produce_event(like_event)
        await asyncio.sleep(random.uniform(0.05, 0.1))

    progress += random.randint(300, 7200)
    finish_event = _make_event(user_id, movie_id, session_id, "VIEW_FINISHED", progress)
    events.append(finish_event)
    produce_event(finish_event)

    return events


async def run_generator(num_users: int, sessions_per_user: int) -> list[dict]:
    all_events = []
    for i in range(num_users):
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        for _ in range(sessions_per_user):
            if random.random() < 0.3:
                search_session = str(uuid.uuid4())
                search_event = _make_event(user_id, random.choice(MOVIE_IDS), search_session, "SEARCHED", None)
                all_events.append(search_event)
                produce_event(search_event)
                await asyncio.sleep(random.uniform(0.02, 0.08))

            session_events = await generate_user_session(user_id)
            all_events.extend(session_events)

    return all_events
