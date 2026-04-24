import logging
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from event_generator import run_generator
from kafka_producer import KafkaException, produce_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("movie_service")

app = FastAPI(title="Movie Service — Event Producer")


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


class GeneratorRequest(BaseModel):
    num_users: int = 3
    sessions_per_user: int = 2


class GeneratorResponse(BaseModel):
    total_events: int
    status: str


def _build_event(req: MovieEventRequest) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": req.user_id,
        "movie_id": req.movie_id,
        "event_type": req.event_type.value,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": req.device_type.value,
        "session_id": req.session_id,
        "progress_seconds": req.progress_seconds,
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/events", response_model=MovieEventResponse)
async def create_event(req: MovieEventRequest):
    event = _build_event(req)
    try:
        produce_event(event)
    except KafkaException as exc:
        logger.error("Failed to publish event: %s", exc)
        raise HTTPException(status_code=503, detail=f"Kafka publish failed: {exc}")
    return MovieEventResponse(event_id=event["event_id"], status="published")


@app.post("/events/batch", response_model=list[MovieEventResponse])
async def create_events_batch(events: list[MovieEventRequest]):
    results = []
    for req in events:
        event = _build_event(req)
        try:
            produce_event(event)
        except KafkaException as exc:
            logger.error("Failed to publish event %s: %s", event["event_id"], exc)
            raise HTTPException(status_code=503, detail=f"Kafka publish failed: {exc}")
        results.append(MovieEventResponse(event_id=event["event_id"], status="published"))
    return results


@app.post("/generate", response_model=GeneratorResponse)
async def generate_events(req: GeneratorRequest):
    events = await run_generator(
        num_users=req.num_users,
        sessions_per_user=req.sessions_per_user,
    )
    logger.info("Generator produced %d events", len(events))
    return GeneratorResponse(total_events=len(events), status="done")
