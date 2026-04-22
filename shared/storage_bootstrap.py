"""Storage backend + system_config bootstrap.

Runs from init_db() on every service startup. Idempotent — seeds the minimum
set of backends (azure-primary + seaweedfs-local) and the system_config
singleton when missing, installs the NOTIFY trigger, and asserts the end
state is usable. A fresh DB (schema drop/re-create) becomes self-healing.
"""
from __future__ import annotations

import json
import logging
import os
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

log = logging.getLogger(__name__)


# asyncpg / SQLAlchemy text() runs one statement per execute(), so each
# CREATE FUNCTION / TRIGGER is its own entry.
_NOTIFY_TRIGGER_STATEMENTS = (
    """CREATE OR REPLACE FUNCTION notify_system_config_changed()
    RETURNS trigger AS $$
    BEGIN
      PERFORM pg_notify('system_config_changed', NEW.id::text);
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS trg_system_config_notify ON system_config",
    """CREATE TRIGGER trg_system_config_notify
      AFTER UPDATE OR INSERT ON system_config
      FOR EACH ROW
      EXECUTE FUNCTION notify_system_config_changed()""",
    """CREATE OR REPLACE FUNCTION notify_storage_backends_changed()
    RETURNS trigger AS $$
    BEGIN
      PERFORM pg_notify('storage_backends_changed',
                        COALESCE(NEW.id::text, OLD.id::text));
      RETURN COALESCE(NEW, OLD);
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS trg_storage_backends_notify ON storage_backends",
    """CREATE TRIGGER trg_storage_backends_notify
      AFTER INSERT OR UPDATE OR DELETE ON storage_backends
      FOR EACH ROW
      EXECUTE FUNCTION notify_storage_backends_changed()""",
)


def _azure_config() -> dict:
    account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
    return {
        "shards": [
            {
                "account": account or "PLACEHOLDER",
                "key_ref": "env://AZURE_STORAGE_ACCOUNT_KEY",
            }
        ]
    }


def _azure_endpoint() -> str:
    ep = os.getenv("AZURE_STORAGE_BLOB_ENDPOINT", "").strip()
    if ep:
        return ep
    account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
    if account:
        return f"https://{account}.blob.core.windows.net/"
    return "https://PLACEHOLDER.blob.core.windows.net/"


def _seaweed_endpoint() -> str:
    return os.getenv("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs:8333").strip()


def _seaweed_config() -> dict:
    return {
        "access_key": os.getenv("SEAWEEDFS_ACCESS_KEY", "testaccess"),
        "secret_key": os.getenv("SEAWEEDFS_SECRET_KEY", "testsecret"),
    }


async def ensure_storage_bootstrap(engine: AsyncEngine) -> None:
    """Seed azure-primary + seaweedfs-local + system_config, install triggers,
    assert sanity. Idempotent and safe to call from every service."""
    async with engine.begin() as conn:
        # Triggers first — installed independently so a seed row failure
        # downstream can't roll them back.
        for stmt in _NOTIFY_TRIGGER_STATEMENTS:
            try:
                await conn.execute(text(stmt))
            except Exception as exc:
                log.warning(
                    "[storage-bootstrap] trigger stmt failed: %s (%s...)",
                    exc, stmt[:60],
                )

        azure_id = str(uuid.uuid4())
        await conn.execute(
            text(
                "INSERT INTO storage_backends "
                "(id, kind, name, endpoint, secret_ref, config, is_enabled, "
                " created_at, updated_at) "
                "VALUES (:id, 'azure_blob', 'azure-primary', :endpoint, "
                " 'env://AZURE_STORAGE_ACCOUNT_KEY', CAST(:config AS JSONB), "
                " true, NOW(), NOW()) "
                "ON CONFLICT (name) DO NOTHING"
            ),
            {
                "id": azure_id,
                "endpoint": _azure_endpoint(),
                "config": json.dumps(_azure_config()),
            },
        )

        seaweed_id = str(uuid.uuid4())
        await conn.execute(
            text(
                "INSERT INTO storage_backends "
                "(id, kind, name, endpoint, secret_ref, config, is_enabled, "
                " created_at, updated_at) "
                "VALUES (:id, 'seaweedfs', 'seaweedfs-local', :endpoint, "
                " 'env://SEAWEEDFS_ACCESS_KEY', CAST(:config AS JSONB), "
                " true, NOW(), NOW()) "
                "ON CONFLICT (name) DO NOTHING"
            ),
            {
                "id": seaweed_id,
                "endpoint": _seaweed_endpoint(),
                "config": json.dumps(_seaweed_config()),
            },
        )

        # system_config singleton — point at whichever backend is named by
        # STORAGE_DEFAULT_BACKEND (azure-primary by default).
        default_name = os.getenv("STORAGE_DEFAULT_BACKEND", "azure-primary")
        await conn.execute(
            text(
                "INSERT INTO system_config "
                "(id, active_backend_id, transition_state, updated_at) "
                "SELECT 1, b.id, 'stable', NOW() "
                "FROM storage_backends b WHERE b.name = :name "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"name": default_name},
        )

        # Startup assertion — fail loud if we still don't have a usable
        # singleton after the seeder ran.
        row = await conn.execute(
            text(
                "SELECT sc.active_backend_id, "
                "       (SELECT count(*) FROM storage_backends "
                "        WHERE is_enabled = true) AS enabled_count "
                "FROM system_config sc WHERE sc.id = 1"
            )
        )
        r = row.first()
        if not r or r.active_backend_id is None or r.enabled_count == 0:
            raise RuntimeError(
                "[storage-bootstrap] invariant failed: "
                "system_config singleton or enabled backends missing after seed"
            )

    log.info("[storage-bootstrap] seed + triggers + invariants OK")
