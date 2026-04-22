"""chat-export-worker entrypoint.

Consumes q.export.chat.thread, renders HTML/JSON/PDF, streams ZIP to blob.
v1: one job = one ZIP. v2 will add parent/merge consumers alongside.
"""
import asyncio
import logging
import signal

from aiohttp import web
from prometheus_client import start_http_server
import aio_pika

from shared.config import settings

from workers.chat_export_worker.consumers.thread import consume_thread

log = logging.getLogger("chat-export-worker")
Q_THREAD = "q.export.chat.thread"


async def health_server() -> None:
    async def ok(_r):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/health", ok)
    app.router.add_get("/ready", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8081)
    await site.start()


async def main() -> None:
    from shared.storage.startup import startup_router
    logging.basicConfig(
        level=logging.INFO,
        format='{"ts":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s","svc":"chat-export-worker"}',
    )
    start_http_server(9102)
    await health_server()
    await startup_router()

    conn = await aio_pika.connect_robust(
        settings.RABBITMQ_URL,
        heartbeat=settings.RABBITMQ_CONSUMER_HEARTBEAT_SECONDS,
    )
    channel = await conn.channel()
    await channel.set_qos(prefetch_count=1)
    # Bind to the shared tm.exchange (DIRECT) used by message_bus.publish so
    # messages routed with key=Q_THREAD land in our queue.
    exchange = await channel.declare_exchange(
        "tm.exchange", aio_pika.ExchangeType.DIRECT, durable=True,
    )
    dlq_rk = f"dlq.{Q_THREAD.split('.', 1)[1]}"
    queue = await channel.declare_queue(
        Q_THREAD,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "tm.exchange",
            "x-dead-letter-routing-key": dlq_rk,
        },
    )
    await queue.bind(exchange, routing_key=Q_THREAD)
    dlq = await channel.declare_queue(dlq_rk, durable=True)
    await dlq.bind(exchange, routing_key=dlq_rk)
    parent_q = await channel.declare_queue("q.export.chat.parent", durable=True)
    await parent_q.bind(exchange, routing_key="q.export.chat.parent")
    merge_q = await channel.declare_queue("q.export.chat.merge", durable=True)
    await merge_q.bind(exchange, routing_key="q.export.chat.merge")

    log.info("worker started queue=%s", Q_THREAD)
    stop = asyncio.Event()

    def _sigterm(*_):
        stop.set()

    for s in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(s, _sigterm)

    async with queue.iterator() as it:
        async for message in it:
            if stop.is_set():
                break
            await consume_thread(message)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
