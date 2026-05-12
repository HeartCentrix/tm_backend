"""Process-wide asyncio exception handler.

aio-pika's robust-connection layer manages broker disconnects via
internal background tasks; their failures (ECONNRESET, ConnectionError
"Connection lost", channel-closed-by-broker) are caught + recovered
inside aio_pika.RobustConnection but are not surfaced as awaited
exceptions, so asyncio's default handler logs them as

    Future exception was never retrieved
    future: <Future finished exception=ConnectionError(...)>

This is cosmetic noise — the library has already reconnected by the
time the warning fires. The handler below downgrades known transient
broker exceptions to a single-line debug log and lets every other
exception fall through to the default loud handler.

Wire it into every long-running worker right before ``asyncio.run``::

    from shared.asyncio_handlers import install_robust_loop_handler
    install_robust_loop_handler()
    asyncio.run(main())
"""
from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

# Exception classes whose ECONNRESET / "Connection lost" warnings are
# expected during broker restarts and handled by aio-pika's robust
# reconnect. Match by class name so we don't import aio-pika here
# (avoids a hard dep for services that don't use it).
_TRANSIENT_BROKER_EXC = {
    "ClientOSError",        # aiohttp underlying TCP reset
    "ConnectionError",      # asyncio / aio-pika synthesised
    "ChannelClosed",        # aio-pika
    "ChannelInvalidStateError",
    "AMQPConnectionError",
}

# Exception classes whose orphan-future noise comes from the Azure
# Storage SDK's aiohttp transport during connector cleanup under high
# upload concurrency. The SDK's own retry policy plus our
# upload_blob_with_retry wrapper already handle these at the API
# boundary — what reaches the loop handler is the *connector cleanup
# task* dying after the upload result has already been returned to the
# caller. Demoting these to debug prevents the cascade from killing the
# worker via a downstream TaskGroup that catches "any" exception.
_TRANSIENT_HTTP_TRANSPORT_EXC = {
    "ClientConnectionError",   # aiohttp connector cleanup
    "ServerDisconnectedError", # aiohttp peer-closed mid-stream
    "ClientPayloadError",
}

# Messages that indicate the same cleanup-cascade category even when
# the exception class is generic (TimeoutError, OSError, etc.).
_TRANSIENT_HTTP_MSG_PATTERNS = (
    "SSL shutdown timed out",
    "Timeout on reading data from socket",
    "Cannot write to closing transport",
    "Connection lost",
)


def _is_transient_http_transport(exc: BaseException, msg: str) -> bool:
    """True when the exception/message pair matches an Azure-SDK-aiohttp
    cleanup-cascade pattern that's already been handled at the upload
    retry layer."""
    if exc is not None and exc.__class__.__name__ in _TRANSIENT_HTTP_TRANSPORT_EXC:
        return True
    text = str(exc) if exc is not None else ""
    haystack = f"{msg} {text}"
    return any(p in haystack for p in _TRANSIENT_HTTP_MSG_PATTERNS)


def _handle(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    exc = context.get("exception")
    msg = context.get("message", "")
    if exc is not None and exc.__class__.__name__ in _TRANSIENT_BROKER_EXC:
        # Demote to debug — the connection layer has already started
        # reconnecting at the point this fires.
        logger.debug(
            "[asyncio] suppressed transient broker exception %s: %s",
            exc.__class__.__name__, exc,
        )
        return
    if "Future exception was never retrieved" in msg and exc is not None and exc.__class__.__name__ in _TRANSIENT_BROKER_EXC:
        logger.debug("[asyncio] suppressed orphan future: %s", exc)
        return
    if _is_transient_http_transport(exc, msg):
        # Azure SDK's aiohttp transport cleanup-cascade. Already retried
        # at the upload layer; the orphan exception here would otherwise
        # propagate via the default handler and trip a TaskGroup that
        # exits the worker cleanly with ExitCode=0 (root cause of
        # worker-2 mid-run cycles documented in benchmarks/comparison-
        # 2026-05-11.md). Demote to debug.
        logger.debug(
            "[asyncio] suppressed transient HTTP transport noise %s: %s | msg=%s",
            exc.__class__.__name__ if exc is not None else "<no-exc>",
            exc, msg,
        )
        return
    # Anything else — preserve the default loud behaviour so real bugs
    # still get caught.
    loop.default_exception_handler(context)


def install_robust_loop_handler() -> None:
    """Install the handler on the running loop. Idempotent — safe to
    call from multiple bootstrap paths."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.set_exception_handler(_handle)
