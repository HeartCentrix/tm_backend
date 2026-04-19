"""Single-thread export consumer. Implementation in Task 15 (wires render + packager)."""
import logging
import aio_pika

log = logging.getLogger("chat-export.thread")


async def consume_thread(message: aio_pika.IncomingMessage) -> None:
    async with message.process(requeue=False):
        log.info("stub_consumer body=%s", message.body[:200])
