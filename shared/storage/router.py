"""StorageRouter — DB-driven backend registry with LISTEN/NOTIFY.

Loaded once at process startup. Listens on Postgres channel
``system_config_changed`` for runtime swaps without restart.
"""
from __future__ import annotations

import asyncio
import json
import logging
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

    # ---- lifecycle

    async def load(self, db_dsn: str) -> None:
        self._db_dsn = db_dsn
        await self._reload_from_db()
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def close(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
        for s in self._backends.values():
            await s.close()

    async def _reload_from_db(self) -> None:
        assert self._db_dsn, "router not loaded"
        conn = await asyncpg.connect(self._db_dsn)
        try:
            sb_rows = await conn.fetch(
                "SELECT id::text, kind, name, endpoint, config::text, secret_ref "
                "FROM storage_backends WHERE is_enabled = true"
            )
            sc = await conn.fetchrow(
                "SELECT active_backend_id::text, transition_state, cooldown_until "
                "FROM system_config WHERE id = 1"
            )
        finally:
            await conn.close()

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

    async def _listen_loop(self) -> None:
        while True:
            try:
                conn = await asyncpg.connect(self._db_dsn)
                await conn.add_listener(
                    "system_config_changed",
                    lambda *_: asyncio.create_task(self._reload_from_db()),
                )
                while True:
                    await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("router listen loop error: %s, reconnecting in 5s", e)
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
