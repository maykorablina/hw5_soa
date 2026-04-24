import asyncio
import json
import logging
import os
from datetime import date, datetime

import asyncpg

logger = logging.getLogger("pg_writer")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cinema:cinema@postgres:5432/cinema")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS metrics (
    metric_date  DATE         NOT NULL,
    metric_name  VARCHAR(100) NOT NULL,
    value        JSONB        NOT NULL,
    computed_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_date, metric_name)
);
"""

_UPSERT = """
INSERT INTO metrics (metric_date, metric_name, value, computed_at)
VALUES ($1, $2, $3::jsonb, $4)
ON CONFLICT (metric_date, metric_name)
DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at;
"""


async def init_db() -> None:
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute(_CREATE_TABLE)
        logger.info("PostgreSQL schema initialized")
    finally:
        await conn.close()


async def upsert_metric(
    metric_date: date,
    metric_name: str,
    value: dict | float | list,
    max_retries: int = 3,
) -> None:
    value_json = json.dumps(value)
    computed_at = datetime.utcnow()
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        conn = None
        try:
            conn = await asyncpg.connect(DATABASE_URL)
            await conn.execute(_UPSERT, metric_date, metric_name, value_json, computed_at)
            logger.info("Upserted metric=%s date=%s", metric_name, metric_date)
            return
        except Exception as exc:
            logger.warning(
                "Attempt %d/%d writing %s failed: %s. Retry in %.1fs",
                attempt, max_retries, metric_name, exc, delay,
            )
            if attempt == max_retries:
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)
        finally:
            if conn:
                await conn.close()
