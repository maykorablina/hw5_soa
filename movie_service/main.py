import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel, Field

from kafka_producer import produce_event

app = FastAPI(title="Movie Service - Event Producer")


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEventRequest(BaseModel):
    user_id: str
    movie_id: str
    event_type: EventType
    device_type: DeviceType
    session_id: str
    progress_seconds: Optional[int] = None


class MovieEventResponse(BaseModel):
    event_id: str
    status: str


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/events", response_model=MovieEventResponse)
async def create_event(req: MovieEventRequest):
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": req.user_id,
        "movie_id": req.movie_id,
        "event_type": req.event_type.value,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": req.device_type.value,
        "session_id": req.session_id,
        "progress_seconds": req.progress_seconds,
    }
    produce_event(event)
    return MovieEventResponse(event_id=event["event_id"], status="published")


@app.post("/events/batch", response_model=list[MovieEventResponse])
async def create_events_batch(events: list[MovieEventRequest]):
    results = []
    for req in events:
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": req.user_id,
            "movie_id": req.movie_id,
            "event_type": req.event_type.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_type": req.device_type.value,
            "session_id": req.session_id,
            "progress_seconds": req.progress_seconds,
        }
        produce_event(event)
        results.append(MovieEventResponse(event_id=event["event_id"], status="published"))
    return results
