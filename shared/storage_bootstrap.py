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
    # Legacy worker paths (any pre-router code still using
    # azure_storage_manager directly) INSERT into snapshots/snapshot_items
    # without setting backend_id. This trigger fills in the active backend
    # from system_config so the NOT NULL constraint doesn't kill the job.
    """CREATE OR REPLACE FUNCTION default_snapshot_backend_id()
    RETURNS trigger AS $$
    BEGIN
      IF NEW.backend_id IS NULL THEN
        SELECT active_backend_id INTO NEW.backend_id
        FROM system_config WHERE id = 1;
      END IF;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS trg_snapshots_default_backend ON snapshots",
    """CREATE TRIGGER trg_snapshots_default_backend
      BEFORE INSERT ON snapshots
      FOR EACH ROW EXECUTE FUNCTION default_snapshot_backend_id()""",
    "DROP TRIGGER IF EXISTS trg_snapshot_items_default_backend ON snapshot_items",
    """CREATE TRIGGER trg_snapshot_items_default_backend
      BEFORE INSERT ON snapshot_items
      FOR EACH ROW EXECUTE FUNCTION default_snapshot_backend_id()""",
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
    # Match ONPREM_S3_ENDPOINT (the var the SeaweedStore expects via
    # secret_ref resolution) so operator config flows through one place.
    return os.getenv("ONPREM_S3_ENDPOINT", "http://seaweedfs:8333").strip()


def _seaweed_buckets() -> list[str]:
    raw = os.getenv("ONPREM_S3_BUCKETS", "").strip()
    if not raw:
        return ["tmvault-shard-0"]
    return [b.strip() for b in raw.split(",") if b.strip()]


def _seaweed_config() -> dict:
    """Config blob that SeaweedStore.from_config() consumes.

    - access_key_env: env var name to read the access key from
    - buckets: list of pre-created buckets (object-lock enabled)
    - verify_tls: bool — false for local Azurite/SeaweedFS over plain HTTP
    """
    return {
        "access_key_env": "ONPREM_S3_ACCESS_KEY",
        "buckets": _seaweed_buckets(),
        "verify_tls": os.getenv("ONPREM_S3_VERIFY_TLS", "true").lower() == "true",
        "region": os.getenv("ONPREM_S3_REGION", "us-east-1"),
    }


async def ensure_storage_bootstrap(engine: AsyncEngine) -> None:
    """Seed azure-primary + seaweedfs-local + system_config, install triggers,
    assert sanity. Idempotent and safe to call from every service.

    Serialized via pg_advisory_lock(9042042) — when many services boot
    concurrently, each tries to run the same CREATE OR REPLACE FUNCTION /
    CREATE TRIGGER DDL. Without the lock, two services racing the
    CREATE FUNCTION hit pg_proc_proname_args_nsp_index unique violation,
    which aborts the whole transaction and turns every subsequent stmt
    into InFailedSQLTransactionError. The advisory lock is what the
    storage_toggle_worker already does for its own bootstrap (key 9042042);
    sharing the same key keeps the system serialized end-to-end.
    """
    BOOTSTRAP_LOCK_KEY = 9042042
    async with engine.begin() as conn:
        # Acquire advisory lock first — pg_advisory_lock blocks until granted.
        # Session-scoped: released on connect close OR by the explicit unlock
        # in the finally block at the end of this function.
        await conn.execute(text(f"SELECT pg_advisory_lock({BOOTSTRAP_LOCK_KEY})"))

        # Wrap each DDL in its own SAVEPOINT so a stmt-level failure (rare,
        # since we serialize with the lock above — but possible across PG
        # versions for IF NOT EXISTS edge cases) doesn't abort the whole
        # transaction and bring down the downstream seed.
        for stmt in _NOTIFY_TRIGGER_STATEMENTS:
            sp_name = f"sp_bootstrap_{abs(hash(stmt)) % (10**8)}"
            try:
                await conn.execute(text(f"SAVEPOINT {sp_name}"))
                await conn.execute(text(stmt))
                await conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
            except Exception as exc:
                # Roll back to savepoint so the outer transaction stays alive
                # for the seed rows + invariant check below.
                try:
                    await conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
                except Exception:
                    pass
                log.warning(
                    "[storage-bootstrap] trigger stmt failed: %s (%s...)",
                    exc, stmt[:60],
                )

        azure_id = str(uuid.uuid4())
        # Upsert on re-seed so operator-driven .env changes (e.g.
        # AZURE_STORAGE_ACCOUNT_NAME swap to a new account) actually
        # land in the DB. Previously this used DO NOTHING, which froze
        # the endpoint at whatever .env held the first time bootstrap
        # ran — toggles to "azure-primary" then preflighted against a
        # stale account and timed out.
        await conn.execute(
            text(
                "INSERT INTO storage_backends "
                "(id, kind, name, endpoint, secret_ref, config, is_enabled, "
                " created_at, updated_at) "
                "VALUES (:id, 'azure_blob', 'azure-primary', :endpoint, "
                " 'env://AZURE_STORAGE_ACCOUNT_KEY', CAST(:config AS JSONB), "
                " true, NOW(), NOW()) "
                "ON CONFLICT (name) DO UPDATE SET "
                "  endpoint = EXCLUDED.endpoint, "
                "  secret_ref = EXCLUDED.secret_ref, "
                "  config = EXCLUDED.config, "
                "  updated_at = NOW()"
            ),
            {
                "id": azure_id,
                "endpoint": _azure_endpoint(),
                "config": json.dumps(_azure_config()),
            },
        )

        seaweed_id = str(uuid.uuid4())
        # Upsert on re-seed: SeaweedStore.from_config() reads credentials
        # from env vars referenced by secret_ref / access_key_env, so if the
        # operator later changes ONPREM_S3_* vars, the DB row needs to stay
        # in sync with the expected env names + bucket list.
        await conn.execute(
            text(
                "INSERT INTO storage_backends "
                "(id, kind, name, endpoint, secret_ref, config, is_enabled, "
                " created_at, updated_at) "
                "VALUES (:id, 'seaweedfs', 'seaweedfs-local', :endpoint, "
                " 'env://ONPREM_S3_SECRET_KEY', CAST(:config AS JSONB), "
                " true, NOW(), NOW()) "
                "ON CONFLICT (name) DO UPDATE SET "
                "  endpoint = EXCLUDED.endpoint, "
                "  secret_ref = EXCLUDED.secret_ref, "
                "  config = EXCLUDED.config, "
                "  updated_at = NOW()"
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
            await conn.execute(text(f"SELECT pg_advisory_unlock({BOOTSTRAP_LOCK_KEY})"))
            raise RuntimeError(
                "[storage-bootstrap] invariant failed: "
                "system_config singleton or enabled backends missing after seed"
            )

        # Release the advisory lock before the txn commits, so the next
        # racing waiter unblocks immediately rather than waiting for the
        # session/connection to close.
        await conn.execute(text(f"SELECT pg_advisory_unlock({BOOTSTRAP_LOCK_KEY})"))

    log.info("[storage-bootstrap] seed + triggers + invariants OK")

    # Bucket auto-provisioning — SeaweedStore expects buckets to pre-exist,
    # and preflight probes write to _buckets[0]. A fresh SeaweedFS has none,
    # so every toggle would fail with NoSuchBucket / SignatureDoesNotMatch.
    try:
        await _ensure_seaweed_buckets()
    except Exception as exc:
        log.warning("[storage-bootstrap] seaweed bucket ensure failed: %s", exc)


async def _ensure_seaweed_buckets() -> None:
    """Create any buckets listed in ONPREM_S3_BUCKETS that don't yet exist.
    Idempotent. Skipped when aioboto3 isn't available in this runtime."""
    buckets = _seaweed_buckets()
    if not buckets:
        return
    try:
        import aioboto3  # type: ignore
    except ImportError:
        log.info("[storage-bootstrap] aioboto3 not available; skipping bucket create")
        return

    endpoint = _seaweed_endpoint()
    access = os.getenv("ONPREM_S3_ACCESS_KEY", "")
    secret = os.getenv("ONPREM_S3_SECRET_KEY", "")
    if not access or not secret:
        log.info("[storage-bootstrap] ONPREM_S3 creds not set; skipping bucket create")
        return

    # Retry the endpoint-reachability probe with backoff so a cold
    # docker-compose up doesn't leave the bucket uncreated when the
    # toggle-worker beats seaweedfs to readiness (seaweedfs has no
    # healthcheck so we can't depends_on it). 5 retries × 4s = ~20s
    # cold-boot tolerance.
    import asyncio as _asyncio
    session = aioboto3.Session()
    attempts = int(os.getenv("STORAGE_BUCKET_ENSURE_ATTEMPTS", "5"))
    backoff_s = float(os.getenv("STORAGE_BUCKET_ENSURE_BACKOFF_S", "4"))
    # Two-phase: for each bucket, first verify it exists; if not,
    # try to create it. Track which buckets are still pending AFTER
    # each attempt and ONLY retry the ones that haven't landed yet.
    # This fixes a subtle bug where the previous code swallowed the
    # create_bucket exception inside the inner try/except so the
    # outer retry never fired — cold-start still left buckets
    # uncreated even though the loop "completed".
    pending = set(buckets)
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        if not pending:
            break
        try:
            async with session.client(
                "s3", endpoint_url=endpoint,
                aws_access_key_id=access, aws_secret_access_key=secret,
                region_name=os.getenv("ONPREM_S3_REGION", "us-east-1"),
                verify=os.getenv(
                    "ONPREM_S3_VERIFY_TLS", "true",
                ).lower() == "true",
            ) as s3:
                for bucket in list(pending):
                    try:
                        await s3.head_bucket(Bucket=bucket)
                        pending.discard(bucket)
                        continue
                    except Exception as he:
                        last_err = he  # record but keep trying to create
                    try:
                        await s3.create_bucket(Bucket=bucket)
                        pending.discard(bucket)
                        log.info(
                            "[storage-bootstrap] created seaweed bucket %s",
                            bucket,
                        )
                    except Exception as ce:
                        # Keep bucket in pending, retry on next round.
                        last_err = ce
        except Exception as exc:
            last_err = exc

        if pending and attempt < attempts:
            log.info(
                "[storage-bootstrap] bucket ensure (attempt %d/%d) "
                "still pending %s — retrying in %.1fs",
                attempt, attempts, sorted(pending), backoff_s,
            )
            await _asyncio.sleep(backoff_s)

    if pending:
        log.warning(
            "[storage-bootstrap] gave up after %d attempts — "
            "buckets still missing: %s (last error: %s)",
            attempts, sorted(pending), last_err,
        )
