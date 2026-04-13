"""Azure Database for PostgreSQL Flexible Server Backup Handler.

Two modes:
1. Native backup via Azure Backup API (managed by Azure)
2. pg_dump streaming export to blob storage (portable backup)

For large databases, prefer native Azure backup (faster, point-in-time).
For portability, use pg_dump streaming.
"""
import asyncio
import subprocess
import tempfile
import os
from datetime import datetime, timezone
from typing import Dict, Any

from azure.mgmt.rdbms.postgresql_flexibleservers.aio import PostgreSQLManagementClient
from azure.core.exceptions import HttpResponseError

from shared.models import Resource, Tenant, Snapshot
from shared.azure_storage import azure_storage_manager
from shared.security import decrypt_secret
from shared.config import settings
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from lro import await_lro
from arm_credentials import get_arm_credential


class PostgresBackupHandler:
    """Backup Azure PostgreSQL flexible servers via native backup or pg_dump."""

    def __init__(self, worker_id: str = "azure-pg-worker"):
        self.worker_id = worker_id

    async def backup(self, resource: Resource, tenant: Tenant,
                     snapshot: Snapshot, msg: Dict) -> Dict:
        """
        Two-mode backup:
        - mode=NATIVE: Use Azure's native PostgreSQL backup API
        - mode=PG_DUMP: Stream pg_dump to blob storage
        """
        mode = msg.get("pg_mode", "NATIVE")

        if mode == "NATIVE":
            return await self._native_backup(resource, tenant, snapshot)
        elif mode == "PG_DUMP":
            return await self._pg_dump_backup(resource, tenant, snapshot, msg)
        else:
            raise ValueError(f"Unknown PostgreSQL backup mode: {mode}")

    async def _native_backup(self, resource: Resource, tenant: Tenant,
                              snapshot: Snapshot) -> Dict:
        """
        Azure PostgreSQL Flexible Server doesn't have a direct backup API.
        Instead, we record the point-in-time restore capability which Azure
        manages automatically (7-35 days retention based on tier).
        """
        server_name = resource.extra_data.get("server_name", "")
        if not server_name:
            raise ValueError(f"No server_name found in resource {resource.external_id} metadata")

        # Azure automatically manages PITR for flexible servers (7-35 days based on tier)
        # We just record the earliest/latest restore window
        credential = get_arm_credential()
        pg_client = PostgreSQLManagementClient(credential, resource.azure_subscription_id)

        try:
            server = await pg_client.servers.get(
                resource_group_name=resource.azure_resource_group,
                server_name=server_name,
            )

            # Azure stores backup metadata on the server
            backup_retention = getattr(server, 'backup', None)
            retention_days = getattr(backup_retention, 'backup_retention_days', 7) if backup_retention else 7

            snapshot.extra_data = snapshot.extra_data or {}
            snapshot.extra_data.update({
                "mode": "NATIVE_PITR",
                "server_name": server_name,
                "retention_days": retention_days,
                "backup_storage_redundancy": getattr(backup_retention, 'geo_redundant_backup', 'Disabled') if backup_retention else 'Unknown',
            })

            print(f"[{self.worker_id}] [PG NATIVE PITR] {resource.display_name} — retention={retention_days}d")
            return {"success": True, "mode": "NATIVE_PITR", "size_bytes": 0, "retention_days": retention_days}

        except HttpResponseError as e:
            raise RuntimeError(f"Failed to query PostgreSQL server {server_name}: {e}") from e

    async def _pg_dump_backup(self, resource: Resource, tenant: Tenant,
                               snapshot: Snapshot, msg: Dict) -> Dict:
        """
        Stream pg_dump output to Azure blob storage.
        Requires database credentials stored in tenant.extra_data.
        """
        server_name = resource.extra_data.get("server_name", "")
        db_name = resource.extra_data.get("database_name", resource.external_id)

        if not server_name:
            raise ValueError(f"No server_name found in resource {resource.external_id} metadata")

        # Get credentials
        pg_user = None
        pg_password = None
        user_key = "azure_pg_user_encrypted"
        pass_key = "azure_pg_password_encrypted"

        if tenant.extra_data and user_key in tenant.extra_data:
            pg_user = decrypt_secret(tenant.extra_data[user_key])
        if tenant.extra_data and pass_key in tenant.extra_data:
            pg_password = decrypt_secret(tenant.extra_data[pass_key])

        if not pg_user or not pg_password:
            raise ValueError(
                f"PostgreSQL credentials not found in tenant {tenant.id}. "
                f"Set {user_key} and {pass_key} in tenant.extra_data"
            )

        # Build connection string
        host = f"{server_name}.postgres.database.azure.com"
        conn_str = f"postgresql://{pg_user}:{pg_password}@{host}:5432/{db_name}?sslmode=require"

        # pg_dump to temp file, then upload
        blob_name = f"{resource.external_id}_{snapshot.id.hex[:12]}_{int(time.time())}.sql.gz"
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-postgresql")

        print(f"[{self.worker_id}] [PG DUMP] Starting pg_dump for {resource.display_name}...")

        with tempfile.NamedTemporaryFile(suffix=".sql.gz", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Run pg_dump with gzip compression
            process = await asyncio.create_subprocess_exec(
                "pg_dump",
                "-h", host,
                "-p", "5432",
                "-U", pg_user,
                "-d", db_name,
                "-Fc",  # Custom format (compressed, supports parallel restore)
                "-f", tmp_path,
                env={
                    **os.environ,
                    "PGPASSWORD": pg_password,
                    "PGSSLMODE": "require",
                },
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()

            if process.returncode != 0:
                err_msg = stderr.decode() if stderr else "Unknown error"
                raise RuntimeError(f"pg_dump failed: {err_msg}")

            # Get file size
            dump_size = os.path.getsize(tmp_path)

            # Upload to Azure blob
            shard = azure_storage_manager.get_default_shard()
            await shard.ensure_container(container)
            blob_path = f"{resource.external_id}/{blob_name}"

            with open(tmp_path, 'rb') as f:
                content = f.read()

            await shard.copy_from_url_sync(
                # pg_dump writes to local file, we upload directly
                source_url="",  # Not applicable for local file
                container_name=container,
                blob_path=blob_path,
                source_size=dump_size,
                metadata={
                    "source_server": server_name,
                    "database_name": db_name,
                    "dump_format": "custom",
                },
            )

            # Actually upload the file properly
            blob_client = shard.get_async_client().get_blob_client(container, blob_path)
            await blob_client.upload_blob(content, overwrite=True)

            snapshot.extra_data = snapshot.extra_data or {}
            snapshot.extra_data.update({
                "mode": "PG_DUMP",
                "blob_path": blob_path,
                "dump_size_bytes": dump_size,
                "server_name": server_name,
                "database_name": db_name,
            })

            print(f"[{self.worker_id}] [PG DUMP COMPLETE] {resource.display_name} — {dump_size} bytes")
            return {"success": True, "mode": "PG_DUMP", "size_bytes": dump_size}

        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
