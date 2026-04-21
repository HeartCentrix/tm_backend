"""storage-toggle-worker — orchestrates azure↔onprem toggles.

Consumes `storage.toggle` RabbitMQ queue. Acquires a Postgres advisory
lock so only one instance actually runs the orchestration at a time;
the other is a hot standby.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os

import aio_pika
import asyncpg

from services.storage_toggle_worker.orchestrator import run_toggle
from shared.storage.router import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("storage-toggle-worker")

ADVISORY_LOCK_ID = 9_042_042
QUEUE_NAME = "storage.toggle"


def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USERNAME", "postgres")
    pw = os.getenv("DB_PASSWORD", "")
    db = os.getenv("DB_NAME", "postgres")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def _rmq_url() -> str:
    url = os.getenv("RABBITMQ_URL")
    if url:
        return url
    u = os.getenv("RABBITMQ_USERNAME", os.getenv("RABBITMQ_USER", "guest"))
    p = os.getenv("RABBITMQ_PASSWORD", "guest")
    h = os.getenv("RABBITMQ_HOST", "localhost")
    port = os.getenv("RABBITMQ_PORT", "5672")
    return f"amqp://{u}:{p}@{h}:{port}/"


async def _acquire_advisory_lock_forever(dsn: str) -> asyncpg.Connection:
    while True:
        conn = await asyncpg.connect(dsn)
        locked = await conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", ADVISORY_LOCK_ID,
        )
        if locked:
            log.info("acquired advisory lock %s", ADVISORY_LOCK_ID)
            return conn
        await conn.close()
        log.info("lock held by another instance; retrying in 15s")
        await asyncio.sleep(15)


async def consume() -> None:
    lock_conn = await _acquire_advisory_lock_forever(_dsn())
    await router.load(db_dsn=_dsn())

    connection = await aio_pika.connect_robust(_rmq_url())
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        log.info("toggle worker ready, consuming %s", QUEUE_NAME)
        async with queue.iterator() as it:
            async for message in it:
                async with message.process():
                    payload = json.loads(message.body)
                    log.info("toggle message: %s", payload)
                    try:
                        await run_toggle(payload)
                    except Exception as e:
                        log.exception("orchestrator crashed: %s", e)
    finally:
        await lock_conn.close()
        await connection.close()
        await router.close()


if __name__ == "__main__":
    asyncio.run(consume())
