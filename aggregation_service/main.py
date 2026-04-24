import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Query

from aggregators import (
    compute_avg_watch_time,
    compute_conversion,
    compute_dau,
    compute_retention,
    compute_top_movies,
    count_processed,
)
from pg_writer import init_db, upsert_metric

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("aggregation_service")

AGGREGATION_INTERVAL_SECONDS = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "300"))


async def run_aggregation(target_date: date) -> dict:
    start = time.monotonic()
    logger.info("Aggregation cycle started for date=%s", target_date)

    processed = count_processed(target_date)

    metrics = {
        "dau": compute_dau(target_date),
        "avg_watch_time_seconds": compute_avg_watch_time(target_date),
        "top_movies": compute_top_movies(target_date),
        "view_conversion": compute_conversion(target_date),
        "retention": compute_retention(target_date),
    }

    for metric_name, value in metrics.items():
        await upsert_metric(target_date, metric_name, value)

    elapsed = time.monotonic() - start
    logger.info(
        "Aggregation cycle finished: date=%s processed=%d records, elapsed=%.2fs",
        target_date,
        processed,
        elapsed,
    )
    return {"date": str(target_date), "processed": processed, "elapsed_seconds": round(elapsed, 3)}


async def scheduled_job():
    yesterday = date.today() - timedelta(days=1)
    await run_aggregation(yesterday)


scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    scheduler.add_job(
        scheduled_job,
        "interval",
        seconds=AGGREGATION_INTERVAL_SECONDS,
        id="aggregation_job",
    )
    scheduler.start()
    logger.info(
        "Scheduler started: interval=%ds", AGGREGATION_INTERVAL_SECONDS
    )
    yield
    scheduler.shutdown()


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/aggregate")
async def aggregate(
    target_date: date = Query(default=None, description="Date to aggregate (YYYY-MM-DD). Defaults to yesterday."),
):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)
    result = await run_aggregation(target_date)
    return result
