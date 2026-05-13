"""Helpers services call from their FastAPI lifespan (or worker main())
to load the StorageRouter at startup and close it at shutdown.

Safe to call before Phase 2 migrations have applied — router.load()
logs a warning and no-ops when storage_backends/system_config tables
don't exist yet.

Boot-race resilience: in compose / k8s startups the DB hostname can
fail DNS for the first few seconds while postgres' init scripts run.
We retry with exponential backoff so workers don't end up in a silent
"no-router" zombie state where OneDrive metadata is written but blobs
are never uploaded.
"""
from __future__ import annotations

import asyncio
import logging
import os

from shared.storage.router import router

log = logging.getLogger("tmvault.storage.startup")


def _build_dsn() -> str:
    """Build the DSN the router uses. Prefers DATABASE_URL; otherwise
    assembles from DB_HOST/DB_PORT/DB_USERNAME/DB_PASSWORD/DB_NAME the
    same way shared/database.py does."""
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USERNAME", os.getenv("DB_USER", "postgres"))
    pw = os.getenv("DB_PASSWORD", "")
    db = os.getenv("DB_NAME", "postgres")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


async def startup_router() -> None:
    """Load the storage router, retrying on transient connection errors.

    Boot order is not guaranteed between containers in compose — postgres
    may take 10-30s to be ready. Without retries we silently degrade to
    "no router" and the worker's fallback path uploads to a stale Azure
    account / raw shard that doesn't have the `upload_blob_stream` facade.
    That manifests as OneDrive files persisted with metadata only and
    zero blob bytes — data loss disguised as a healthy snapshot.

    Knobs:
      STORAGE_ROUTER_MAX_RETRIES (default 12) — total attempts
      STORAGE_ROUTER_BACKOFF_BASE_S (default 1.0)
      STORAGE_ROUTER_BACKOFF_CAP_S (default 10.0)
    Worst case wait: ~110s with defaults. After exhaustion we still
    log + return (non-fatal) so the worker can come up and at least
    serve health probes.
    """
    max_retries = int(os.getenv("STORAGE_ROUTER_MAX_RETRIES", "12"))
    base = float(os.getenv("STORAGE_ROUTER_BACKOFF_BASE_S", "1.0"))
    cap = float(os.getenv("STORAGE_ROUTER_BACKOFF_CAP_S", "10.0"))
    last_err: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            await router.load(db_dsn=_build_dsn())
            log.info(
                "storage router loaded on attempt %d: %d backend(s), active=%s, state=%s",
                attempt,
                len(router.list_backends()),
                router.active_backend_id(),
                router.transition_state(),
            )
            return
        except Exception as e:
            last_err = e
            if attempt >= max_retries:
                break
            sleep_s = min(cap, base * (2 ** (attempt - 1)))
            log.warning(
                "storage router load attempt %d/%d failed (%s); retrying in %.1fs",
                attempt, max_retries, type(e).__name__, sleep_s,
            )
            await asyncio.sleep(sleep_s)
    log.error(
        "storage router load gave up after %d attempts (last error: %s) — "
        "process will run WITHOUT an active backend and any blob upload "
        "via the facade will fall back to the legacy raw-shard path",
        max_retries, last_err,
    )


async def shutdown_router() -> None:
    try:
        await router.close()
    except Exception as e:
        log.warning("storage router close error: %s", e)
