"""Azure SQL Database Backup Handler — PITR + BACPAC export."""
import time
from datetime import datetime, timezone
from typing import Dict, Any

from azure.mgmt.sql.aio import SqlManagementClient
from azure.core.exceptions import HttpResponseError

from shared.models import Resource, Tenant, Snapshot
from shared.azure_storage import azure_storage_manager
from shared.security import decrypt_secret
from shared.config import settings
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from lro import await_lro
from arm_credentials import get_arm_credential


class SqlBackupHandler:
    """Backup Azure SQL databases via PITR metadata and BACPAC export."""

    def __init__(self, worker_id: str = "azure-sql-worker"):
        self.worker_id = worker_id

    async def backup(self, resource: Resource, tenant: Tenant,
                     snapshot: Snapshot, msg: Dict) -> Dict:
        """
        Two-mode backup based on SLA policy:
        - mode=PITR_RESTORE_POINT: register a restorable point in time (metadata only)
        - mode=BACPAC_EXPORT: full logical export to customer blob
        """
        credential = get_arm_credential()
        sql = SqlManagementClient(credential, resource.azure_subscription_id)

        mode = msg.get("sql_mode", "PITR_RESTORE_POINT")

        if mode == "PITR_RESTORE_POINT":
            return await self._register_pitr_point(sql, resource, snapshot)
        elif mode == "BACPAC_EXPORT":
            return await self._export_bacpac(sql, resource, tenant, snapshot)
        else:
            raise ValueError(f"Unknown SQL backup mode: {mode}")

    async def _register_pitr_point(self, sql: SqlManagementClient,
                                    resource: Resource, snapshot: Snapshot) -> Dict:
        """
        PITR is automatic in Azure SQL — this method just records that we
        CAN restore to this moment. No data movement.
        """
        server_name = resource.extra_data.get("server_name", "")
        rg = resource.azure_resource_group or ""
        if not server_name or not rg:
            raise ValueError(f"No server_name or azure_resource_group found in resource {resource.external_id} metadata")

        db = await sql.databases.get(
            resource_group_name=rg,
            server_name=server_name,
            database_name=resource.external_id,
        )
        earliest = db.earliest_restore_date
        now = datetime.now(timezone.utc)

        snapshot.extra_data = snapshot.extra_data or {}
        snapshot.extra_data.update({
            "mode": "PITR_RESTORE_POINT",
            "restore_point_in_time": now.isoformat(),
            "earliest_restorable": earliest.isoformat() if earliest else None,
            "pitr_retention_days": getattr(db, 'earliest_restore_date', None) is not None,
        })

        print(f"[{self.worker_id}] [SQL PITR] Registered restore point for {resource.display_name}")
        return {"success": True, "mode": "PITR", "size_bytes": 0}

    async def _export_bacpac(self, sql: SqlManagementClient, resource: Resource,
                              tenant: Tenant, snapshot: Snapshot) -> Dict:
        """
        BACPAC export — Azure SQL writes directly to our blob.
        Bytes flow Azure SQL -> customer blob, not through this worker.
        """
        server_name = resource.extra_data.get("server_name", "")
        rg = resource.azure_resource_group or ""
        if not server_name or not rg:
            raise ValueError(f"No server_name or azure_resource_group found in resource {resource.external_id} metadata")

        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql")
        shard = azure_storage_manager.get_default_shard()
        await shard.ensure_container(container)

        blob_name = f"{resource.external_id}_{snapshot.id.hex[:12]}_{int(time.time())}.bacpac"
        blob_uri = f"https://{shard.account_name}.blob.core.windows.net/{container}/{blob_name}"

        # Retrieve SQL admin credentials from tenant extra_data
        sql_admin_user = None
        sql_admin_password = None
        admin_user_key = "azure_sql_admin_user_encrypted"
        admin_pass_key = "azure_sql_admin_password_encrypted"

        if tenant.extra_data and admin_user_key in tenant.extra_data:
            sql_admin_user = decrypt_secret(tenant.extra_data[admin_user_key])
        if tenant.extra_data and admin_pass_key in tenant.extra_data:
            sql_admin_password = decrypt_secret(tenant.extra_data[admin_pass_key])

        if not sql_admin_user or not sql_admin_password:
            raise ValueError(
                f"SQL admin credentials not found in tenant {tenant.id}. "
                f"Set {admin_user_key} and {admin_pass_key} in tenant.extra_data"
            )

        export_params = {
            "storageKeyType": "StorageAccessKey",
            "storageKey": shard.account_key,
            "storageUri": blob_uri,
            "administratorLogin": sql_admin_user,
            "administratorLoginPassword": sql_admin_password,
            "authenticationType": "SQL",
        }

        print(f"[{self.worker_id}] [SQL BACPAC] Starting export for {resource.display_name}...")

        try:
            poller = await sql.databases.begin_export(
                resource_group_name=rg,
                server_name=server_name,
                database_name=resource.external_id,
                parameters=export_params,
            )

            # BACPAC exports on large DBs can take hours
            result = await await_lro(
                poller, f"bacpac_export/{resource.external_id}",
                timeout_seconds=14400,  # 4 hours
                poll_interval=60)
        except HttpResponseError as e:
            # Redact sensitive storage key from error messages
            error_msg = str(e).replace(shard.account_key, "***REDACTED***")
            raise RuntimeError(f"BACPAC export failed: {error_msg}") from e

        # Verify blob actually landed
        blob_client = shard.get_async_client().get_blob_client(container, blob_name)
        try:
            props = await blob_client.get_blob_properties()
            blob_size = props.size
        except Exception:
            raise RuntimeError(f"BACPAC export claimed success but blob not found: {blob_uri}")

        snapshot.extra_data = snapshot.extra_data or {}
        snapshot.extra_data.update({
            "mode": "BACPAC_EXPORT",
            "blob_path": blob_name,
            "bacpac_size_bytes": blob_size,
            "export_duration_seconds": (datetime.now(timezone.utc) - snapshot.started_at.replace(tzinfo=timezone.utc)).total_seconds() if snapshot.started_at else 0,
        })

        print(f"[{self.worker_id}] [SQL BACPAC COMPLETE] {resource.display_name} — {blob_size} bytes")

        return {
            "success": True,
            "mode": "BACPAC",
            "size_bytes": blob_size,
            "blob_path": blob_name,
        }
