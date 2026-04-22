"""Helpers services call from their FastAPI lifespan (or worker main())
to load the StorageRouter at startup and close it at shutdown.

Safe to call before Phase 2 migrations have applied — router.load()
logs a warning and no-ops when storage_backends/system_config tables
don't exist yet.
"""
from __future__ import annotations

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
    try:
        await router.load(db_dsn=_build_dsn())
        log.info(
            "storage router loaded: %d backend(s), active=%s, state=%s",
            len(router.list_backends()),
            router.active_backend_id(),
            router.transition_state(),
        )
    except Exception as e:
        log.warning("storage router load failed at startup (non-fatal): %s", e)


async def shutdown_router() -> None:
    try:
        await router.close()
    except Exception as e:
        log.warning("storage router close error: %s", e)
