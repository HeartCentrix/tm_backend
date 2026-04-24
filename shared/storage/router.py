"""StorageRouter — DB-driven backend registry with LISTEN/NOTIFY.

Loaded once at process startup. Listens on Postgres channel
``system_config_changed`` for runtime swaps without restart.

Connection model: each router instance holds a single long-lived
asyncpg connection used for BOTH the LISTEN socket and the periodic
reload queries, serialized through an asyncio.Lock. Older code opened
a fresh asyncpg.connect() on every reload (startup, 60-s heartbeat,
each NOTIFY); under a service-wide toggle every replica fired a NOTIFY-
storm and the hobby-tier Postgres connection cap (~25-50) was easy to
exceed, surfacing as TooManyConnectionsError. One connection per
process closes that gap entirely.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import asyncpg

from shared.storage.azure_blob import AzureBlobStore
from shared.storage.base import BackendStore
from shared.storage.errors import (
    BackendNotFoundError,
    TransitionInProgressError,
)
from shared.storage.seaweedfs import SeaweedStore

log = logging.getLogger("tmvault.storage.router")


@dataclass
class PreflightCheck:
    name: str
    ok: bool
    detail: Optional[str] = None


@dataclass
class PreflightResult:
    ok: bool
    checks: list[PreflightCheck] = field(default_factory=list)


class StorageRouter:
    def __init__(self):
        self._backends: dict[str, BackendStore] = {}
        self._active_backend_id: Optional[str] = None
        self._transition_state: str = "stable"
        self._cooldown_until: Optional[datetime] = None
        self._db_dsn: Optional[str] = None
        self._listener_task: Optional[asyncio.Task] = None
        # Single long-lived asyncpg connection shared by reload + LISTEN.
        # Serialised through _conn_lock so a NOTIFY-triggered reload and a
        # heartbeat reload can't issue two concurrent queries on it.
        self._conn: Optional[asyncpg.Connection] = None
        self._conn_lock: Optional[asyncio.Lock] = None

    # ---- lifecycle

    async def load(self, db_dsn: str) -> None:
        self._db_dsn = db_dsn
        self._conn_lock = asyncio.Lock()
        # Open the long-lived connection up front; fail fast if Postgres
        # isn't reachable rather than discovering it on the first toggle.
        self._conn = await self._open_conn()
        await self._reload_from_db()
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def close(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None
        for s in self._backends.values():
            await s.close()

    # ---- connection helpers

    async def _open_conn(self) -> asyncpg.Connection:
        """Open the long-lived connection and stamp its search_path so
        every subsequent query resolves the tm schema without an extra
        SET round-trip."""
        assert self._db_dsn, "router not loaded"
        schema = os.getenv("DB_SCHEMA", "tm")
        conn = await asyncpg.connect(self._db_dsn)
        await conn.execute(f"SET search_path TO {schema}, public")
        return conn

    async def _ensure_conn(self) -> asyncpg.Connection:
        """Re-open the shared connection if it's been closed (network
        blip, Postgres restart, …). Caller must hold _conn_lock."""
        if self._conn is None or self._conn.is_closed():
            self._conn = await self._open_conn()
        return self._conn

    # ---- reload

    async def _reload_from_db(self) -> None:
        """Reload backends + active config using the shared connection.
        Lock-guarded so concurrent NOTIFY callbacks + heartbeat reloads
        don't race for the single asyncpg connection."""
        assert self._conn_lock is not None, "router not loaded"
        async with self._conn_lock:
            conn = await self._ensure_conn()
            schema = os.getenv("DB_SCHEMA", "tm")
            # Tolerant of pre-Phase-2 schema (tables may not exist during the
            # initial deploy window before migrations apply). In that case we
            # simply no-op — callers get a clear error via get_active_store().
            tables_exist = await conn.fetchval(
                f"SELECT to_regclass('{schema}.storage_backends') IS NOT NULL "
                f"  AND to_regclass('{schema}.system_config') IS NOT NULL"
            )
            if not tables_exist:
                log.warning(
                    "storage_backends / system_config tables not present yet; "
                    "router will reload once migrations have run",
                )
                return
            sb_rows = await conn.fetch(
                "SELECT id::text, kind, name, endpoint, config::text, secret_ref "
                "FROM storage_backends WHERE is_enabled = true"
            )
            sc = await conn.fetchrow(
                "SELECT active_backend_id::text, transition_state, cooldown_until "
                "FROM system_config WHERE id = 1"
            )

        new_backends: dict[str, BackendStore] = {}
        for r in sb_rows:
            cfg_raw = r["config"]
            cfg = json.loads(cfg_raw) if isinstance(cfg_raw, str) else (cfg_raw or {})
            if r["kind"] == "azure_blob":
                new_backends[r["id"]] = AzureBlobStore.from_config(
                    backend_id=r["id"], name=r["name"], config=cfg,
                )
            elif r["kind"] == "seaweedfs":
                new_backends[r["id"]] = SeaweedStore.from_config(
                    backend_id=r["id"], name=r["name"], endpoint=r["endpoint"],
                    secret_ref=r["secret_ref"], config=cfg,
                )
            else:
                log.warning("unknown backend kind %s (id=%s); skipping",
                            r["kind"], r["id"])

        for old_id, old_store in self._backends.items():
            if old_id not in new_backends:
                try:
                    await old_store.close()
                except Exception as e:
                    log.warning("error closing stale backend %s: %s", old_id, e)

        self._backends = new_backends
        if sc:
            self._active_backend_id = sc["active_backend_id"]
            self._transition_state = sc["transition_state"]
            self._cooldown_until = sc["cooldown_until"]

    # ---- listener

    async def _listen_loop(self) -> None:
        """Self-healing: LISTEN on both channels using the shared connection,
        and a periodic safety-net reload so a silent connection drop or a
        missed NOTIFY can't leave the in-memory registry stale indefinitely.

        On any error, drop the shared connection and let _ensure_conn re-
        open it on the next reload; sleeps 5s between reconnects so a Pg
        outage doesn't hot-loop."""
        reload_interval_s = 60.0
        while True:
            try:
                # Make sure the shared connection is alive and re-attach
                # listeners if we just reconnected.
                async with self._conn_lock:
                    conn = await self._ensure_conn()

                def _on_notify(*_args):
                    asyncio.create_task(self._reload_from_db())

                await conn.add_listener("system_config_changed", _on_notify)
                await conn.add_listener("storage_backends_changed", _on_notify)

                while True:
                    await asyncio.sleep(reload_interval_s)
                    # Heartbeat: proves the shared connection is alive AND
                    # catches any NOTIFY we missed. _reload_from_db acquires
                    # the lock and uses the same connection — so this is
                    # zero new connections per heartbeat.
                    try:
                        async with self._conn_lock:
                            await self._conn.fetchval("SELECT 1")
                    except Exception as hb_exc:
                        log.warning(
                            "router heartbeat failed: %s — reconnecting",
                            hb_exc,
                        )
                        break
                    await self._reload_from_db()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("router listen loop error: %s, reconnecting in 5s", e)
            # Drop the dead connection so the next iteration reopens.
            if self._conn is not None:
                try:
                    await self._conn.close()
                except Exception:
                    pass
                self._conn = None
            await asyncio.sleep(5)

    # ---- primary API

    def get_active_store(self) -> BackendStore:
        if self._transition_state != "stable":
            raise TransitionInProgressError(
                f"cannot write during state={self._transition_state}",
            )
        if not self._active_backend_id:
            raise RuntimeError("router not loaded")
        return self.get_store_by_id(self._active_backend_id)

    def get_store_by_id(self, backend_id: str) -> BackendStore:
        bid = str(backend_id)
        if bid not in self._backends:
            raise BackendNotFoundError(f"backend_id not registered: {bid}")
        return self._backends[bid]

    def get_store_for_item(self, item) -> BackendStore:
        return self.get_store_by_id(str(item.backend_id))

    def get_store_for_snapshot(self, snapshot) -> BackendStore:
        return self.get_store_by_id(str(snapshot.backend_id))

    def writable(self) -> bool:
        return self._transition_state == "stable"

    def active_backend_id(self) -> Optional[str]:
        return self._active_backend_id

    def transition_state(self) -> str:
        return self._transition_state

    def list_backends(self) -> list[BackendStore]:
        return list(self._backends.values())


# Module-level singleton
router = StorageRouter()
