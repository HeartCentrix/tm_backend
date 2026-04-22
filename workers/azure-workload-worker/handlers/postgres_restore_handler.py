"""Azure PostgreSQL Restore Handler — reverses the afi-style streaming backup.

Restore modes:
1. FULL — schema + all table data re-applied
2. SCHEMA_ONLY — run schema.sql only (produces empty tables)
3. DATA_ONLY — run data .sql files only (tables must already exist)

The restore reuses the backup handler's AAD-token connection path so it works
against Flexible Server and Single Server the same way the backup does. The SP
that runs the restore must already be registered as Azure AD admin on the
target server.
"""
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import asyncpg
from azure.identity.aio import ClientSecretCredential

from shared.models import Resource, Tenant, Snapshot
from shared.azure_storage import azure_storage_manager
from shared.config import settings


PG_AAD_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


class PostgresRestoreHandler:
    """Restore Azure PostgreSQL databases from afi-style streaming backups."""

    def __init__(self, worker_id: str = "azure-pg-restore"):
        self.worker_id = worker_id

    def _log(self, message: str, level: str = "INFO"):
        prefix = f"[{self.worker_id}] [PG-RESTORE]"
        if level == "ERROR":
            print(f"{prefix} ERROR: {message}")
        elif level == "WARNING":
            print(f"{prefix} WARNING: {message}")
        else:
            print(f"{prefix} {message}")

    async def restore(self, tenant: Tenant, snapshot: Snapshot,
                      restore_params: Dict) -> Dict:
        target_server = restore_params.get("target_server_name")
        target_db = restore_params.get("target_database_name")
        server_type = (restore_params.get("server_type")
                       or snapshot.extra_data.get("server_type", "FLEXIBLE")).upper()
        mode = (restore_params.get("mode") or "FULL").upper()
        drop_existing = bool(restore_params.get("drop_existing", False))

        if not target_server or not target_db:
            raise ValueError("target_server_name and target_database_name are required")

        self._log(f"Restore start: {target_server}/{target_db} (mode={mode}, server_type={server_type})")

        conn = None
        try:
            conn = await self._connect(target_server, target_db, server_type, restore_params)

            schema_blob = snapshot.extra_data.get("schema_blob")
            data_prefix = snapshot.extra_data.get("blob_path_prefix") or self._derive_data_prefix(snapshot)

            if drop_existing:
                await self._drop_user_tables(conn)

            schema_applied = 0
            if mode in ("FULL", "SCHEMA_ONLY") and schema_blob:
                schema_applied = await self._apply_schema(conn, tenant, schema_blob)

            tables_applied = 0
            rows_applied = 0
            if mode in ("FULL", "DATA_ONLY"):
                stats = await self._apply_data(conn, tenant, data_prefix)
                tables_applied = stats["tables"]
                rows_applied = stats["rows"]

            self._log(f"Restore completed: schema_objects={schema_applied}, tables={tables_applied}, rows={rows_applied}")
            return {
                "success": True,
                "mode": mode,
                "server_type": server_type,
                "target_server": target_server,
                "target_database": target_db,
                "schema_statements_applied": schema_applied,
                "tables_restored": tables_applied,
                "rows_restored": rows_applied,
            }

        except Exception as e:
            self._log(f"Restore FAILED: {e}", "ERROR")
            return {"success": False, "error": str(e)[:1000]}
        finally:
            if conn:
                try:
                    await conn.close()
                except Exception:
                    pass

    async def _connect(self, server_name: str, db_name: str, server_type: str,
                       restore_params: Dict):
        sp_client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
        sp_client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
        sp_tenant_id = settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID
        if not sp_client_id or not sp_client_secret or not sp_tenant_id:
            raise ValueError("SP credentials missing (MICROSOFT_CLIENT_ID/SECRET/TENANT_ID)")

        credential = ClientSecretCredential(
            client_id=sp_client_id, client_secret=sp_client_secret, tenant_id=sp_tenant_id,
        )
        try:
            token = (await credential.get_token(PG_AAD_SCOPE)).token
        finally:
            await credential.close()

        host = f"{server_name}.postgres.database.azure.com"
        if server_type == "FLEXIBLE":
            user = sp_client_id
        else:
            admin_user = restore_params.get("admin_user", "pgadmin")
            user = f"{admin_user}@{server_name}"

        self._log(f"Connecting to {host}/{db_name} as {user} ({server_type})")
        return await asyncpg.connect(
            host=host, port=5432, user=user, password=token,
            database=db_name, ssl="require", timeout=30.0,
        )

    async def _drop_user_tables(self, conn):
        """DROP every user table in the target database so a FULL restore lands
        on a clean slate. Leaves extensions and non-default schemas other than
        the ones containing tables intact."""
        rows = await conn.fetch("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_schema !~ '^pg_toast'
        """)
        for r in rows:
            await conn.execute(f'DROP TABLE IF EXISTS "{r["table_schema"]}"."{r["table_name"]}" CASCADE')
        self._log(f"Dropped {len(rows)} pre-existing tables")

    async def _apply_schema(self, conn, tenant: Tenant, schema_blob: str) -> int:
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-postgresql")
        shard = azure_storage_manager.get_default_shard()
        # Facade download — works on whichever backend the snapshot
        # was captured on (Azure Blob or on-prem SeaweedFS). Raises
        # RuntimeError instead of silently returning None if the blob
        # is missing — a missing schema dump means we can't restore
        # and should fail loudly so the operator notices.
        content_bytes = await shard.download_blob(container, schema_blob)
        if content_bytes is None:
            raise RuntimeError(
                f"schema blob missing for restore: container={container} "
                f"path={schema_blob}"
            )
        content = content_bytes.decode("utf-8")

        # Execute the whole dump in one shot. The backup handler writes a
        # semicolon-separated series of CREATE TABLE / CREATE INDEX statements;
        # asyncpg.execute handles a multi-statement script fine.
        await conn.execute(content)
        # Count statements for reporting only — not authoritative on syntax.
        return sum(1 for ln in content.splitlines() if ln.strip().upper().startswith(("CREATE", "ALTER")))

    async def _apply_data(self, conn, tenant: Tenant, data_prefix: str) -> Dict[str, int]:
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-postgresql")
        shard = azure_storage_manager.get_default_shard()

        # List all per-table .sql blobs under data/ via the facade's
        # prefix-aware list — pushes the filter to the backend so we
        # don't pull millions of metadata rows when a tenant has many
        # DBs in the same container.
        prefix = data_prefix + "data/"
        tables = 0
        rows = 0
        blob_names = []
        async for name in shard.list_blobs(container, name_starts_with=prefix):
            if name.endswith(".sql"):
                blob_names.append(name)

        for blob_name in blob_names:
            content_bytes = await shard.download_blob(container, blob_name)
            if content_bytes is None:
                self._log(f"  ✗ {blob_name}: blob missing, skipping", "WARNING")
                continue
            content = content_bytes.decode("utf-8")

            # Each file is a stream of INSERT statements, one per row.
            insert_count = sum(1 for ln in content.splitlines() if ln.strip().upper().startswith("INSERT"))
            try:
                await conn.execute(content)
                tables += 1
                rows += insert_count
                self._log(f"  ✓ Applied {blob_name} ({insert_count} rows)")
            except Exception as e:
                self._log(f"  ✗ Failed to apply {blob_name}: {e}", "WARNING")
                # Continue with other tables — partial restores beat total failure.

        return {"tables": tables, "rows": rows}

    @staticmethod
    def _derive_data_prefix(snapshot: Snapshot) -> str:
        """Reconstruct the blob-path prefix used by the backup handler when it
        isn't stored on the snapshot. Matches
        `{server_name}/{db_name}/{snapshot_id_12}/`."""
        server = snapshot.extra_data.get("server_name", "")
        db = snapshot.extra_data.get("database_name", "")
        sid = snapshot.id.hex[:12]
        return f"{server}/{db}/{sid}/"
