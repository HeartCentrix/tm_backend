"""
Azure SQL Database Restore Handler — Afi.ai-Equivalent

Restore modes:
1. Full database restore (from BACPAC)
2. Schema-only restore (from SQL scripts)
3. Table-level restore (individual tables from BACPAC)
4. Cross-server restore (different server/subscription/region)
5. Point-in-time restore (using PITR metadata)

Features:
- Automated firewall management during restore
- Full fidelity recovery (original settings)
- Custom database naming (avoid overwriting production)
- Transparent Data Encryption (TDE) support
"""
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from azure.mgmt.sql.aio import SqlManagementClient
from azure.mgmt.compute.aio import ComputeManagementClient
from azure.mgmt.network.aio import NetworkManagementClient
from azure.core.exceptions import HttpResponseError

from shared.models import Resource, Tenant, Snapshot
from shared.azure_storage import azure_storage_manager
from shared.config import settings
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from lro import await_lro
from arm_credentials import get_arm_credential


class SqlRestoreHandler:
    """Production-ready Azure SQL restore handler."""

    def __init__(self, worker_id: str = "azure-sql-restore"):
        self.worker_id = worker_id

    async def restore_full(self, tenant: Tenant, snapshot: Snapshot,
                           restore_params: Dict) -> Dict:
        """
        Full database restore from BACPAC.
        
        Supports:
        - Restore to original server
        - Restore to new server (cross-subscription, cross-region)
        - Automated firewall configuration
        - Custom database name
        
        restore_params:
            target_server_name: str — target SQL server
            target_resource_group: str — target RG
            target_database_name: str — target DB name
            target_subscription_id: str — optional, for cross-sub
            target_region: str — optional, for cross-region
            overwrite: bool — overwrite if DB exists
            configure_firewall: bool — auto-add firewall rules for restore
        """
        credential = get_arm_credential()
        target_sub = restore_params.get("target_subscription_id", tenant.subscription_id)
        sql = SqlManagementClient(credential, target_sub)

        target_server = restore_params.get("target_server_name")
        target_rg = restore_params.get("target_resource_group")
        target_db = restore_params.get("target_database_name")
        configure_firewall = restore_params.get("configure_firewall", True)

        if not target_server or not target_rg or not target_db:
            raise ValueError("target_server_name, target_resource_group, and target_database_name are required")

        self._log(f"Starting full SQL restore to {target_db} on {target_server}/{target_rg}")

        added_firewall_rule: Optional[str] = None
        try:
            # Step 1: Get BACPAC blob info
            bacpac_blob = snapshot.extra_data.get("bacpac_blob")
            if not bacpac_blob:
                raise ValueError("No BACPAC blob found in snapshot")

            container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql")
            shard = azure_storage_manager.get_default_shard()
            blob_uri = f"https://{shard.account_name}.blob.core.windows.net/{container}/{bacpac_blob}"

            # Step 2: Open Azure-services firewall rule so the BACPAC import control
            # plane can reach the SQL engine. Rule is named per-job and torn down in
            # finally so we don't leave the server open after the restore.
            if configure_firewall:
                added_firewall_rule = await self._configure_firewall_for_restore(sql, target_rg, target_server)

            # Step 3: Import BACPAC
            admin_user, admin_password = self._get_sql_credentials(tenant)

            import_params = {
                "storageKeyType": "StorageAccessKey",
                "storageKey": shard.account_key,
                "storageUri": blob_uri,
                "administratorLogin": admin_user,
                "administratorLoginPassword": admin_password,
                "authenticationType": "SQL",
            }

            poller = await sql.databases.begin_import(
                resource_group_name=target_rg,
                server_name=target_server,
                database_name=target_db,
                parameters=import_params,
            )

            await await_lro(
                poller, f"bacpac_import/{target_db}",
                timeout_seconds=14400,  # 4 hours
                poll_interval=60,
            )

            # Step 4: Verify import
            db = await sql.databases.get(
                resource_group_name=target_rg,
                server_name=target_server,
                database_name=target_db,
            )

            self._log(f"Full SQL restore completed: {target_db} (status={getattr(db, 'status', 'unknown')})")
            return {
                "success": True,
                "database_name": target_db,
                "server_name": target_server,
                "resource_group": target_rg,
                "subscription_id": target_sub,
                "status": getattr(db, 'status', None),
                "firewall_rule": added_firewall_rule,
            }

        except Exception as e:
            self._log(f"Full SQL restore FAILED: {e}", "ERROR")
            return {"success": False, "error": str(e)[:1000]}
        finally:
            if added_firewall_rule:
                await self._remove_firewall_rule(sql, target_rg, target_server, added_firewall_rule)

    async def restore_schema_only(self, tenant: Tenant, snapshot: Snapshot,
                                   restore_params: Dict) -> Dict:
        """Schema-only restore.

        Honest failure path: the SQL backup handler captures schema metadata as
        annotated comments (INFORMATION_SCHEMA rows formatted as `-- Column: ...`),
        not executable DDL. Running those blobs against a target server would
        create zero objects, so we refuse instead of silently reporting success.

        The working alternatives today:
          • `restore_full` — imports the BACPAC (schema + data) into a scratch
            database, then callers can TRUNCATE tables if they want empty structure.
          • `restore_pitr` — creates a new database at a past point in time via
            Azure SQL's native capability (no BACPAC needed).

        A real schema-only restore needs backup-side changes: DACPAC extraction
        or reverse-engineered CREATE DDL — both larger than this path is scoped for.
        """
        schema_blob = snapshot.extra_data.get("schema_blob")
        msg = (
            "Schema-only restore is not supported: TM Vault's SQL backup captures "
            "schema as annotated metadata, not executable DDL. Use restore_full "
            "against a scratch database, or restore_pitr for a point-in-time copy."
        )
        self._log(msg, "WARNING")
        return {
            "success": False,
            "mode": "SCHEMA_ONLY",
            "error": msg,
            "schema_blob": schema_blob,
            "alternatives": ["FULL", "PITR"],
        }

    async def restore_pitr(self, tenant: Tenant, snapshot: Snapshot,
                           restore_params: Dict) -> Dict:
        """
        Point-in-time restore using Azure SQL's native PITR capability.
        
        This creates a new database at a specific point in time.
        """
        credential = get_arm_credential()
        target_sub = restore_params.get("target_subscription_id", tenant.subscription_id)
        sql = SqlManagementClient(credential, target_sub)

        source_server = restore_params.get("source_server_name")
        source_rg = restore_params.get("source_resource_group")
        source_db = restore_params.get("source_database_name")
        target_db = restore_params.get("target_database_name", f"{source_db}_restored")
        target_rg = restore_params.get("target_resource_group", source_rg)
        restore_point = restore_params.get("restore_point")  # ISO datetime

        if not all([source_server, source_rg, source_db, target_db, restore_point]):
            raise ValueError("source_server_name, source_resource_group, source_database_name, target_database_name, and restore_point are required")

        self._log(f"Starting PITR restore: {source_db} → {target_db} at {restore_point}")

        restore_params_obj = {
            "location": restore_params.get("target_region", "eastus"),
            "create_mode": "PointInTimeRestore",
            "source_database_id": f"/subscriptions/{target_sub}/resourceGroups/{source_rg}/providers/Microsoft.Sql/servers/{source_server}/databases/{source_db}",
            "restore_point_in_time": restore_point,
        }

        poller = await sql.databases.begin_create_or_update(
            resource_group_name=target_rg,
            server_name=source_server,
            database_name=target_db,
            parameters=restore_params_obj,
        )

        result = await await_lro(
            poller, f"pitr_restore/{target_db}",
            timeout_seconds=7200,  # 2 hours
            poll_interval=60,
        )

        self._log(f"PITR restore completed: {target_db}")
        return {
            "success": True,
            "mode": "PITR",
            "database_name": target_db,
            "server_name": source_server,
            "resource_group": target_rg,
            "restore_point": restore_point,
            "database_id": getattr(result, 'id', None),
        }

    # ==================== Helper Methods ====================

    async def _configure_firewall_for_restore(self, sql, rg: str, server_name: str) -> Optional[str]:
        """Open Azure-services access on the SQL server for the duration of the restore.

        Creates a rule `tmvault-restore-<epoch>` with range 0.0.0.0-0.0.0.0 (Azure
        SDK convention for "Allow Azure services and resources"). Returns the rule
        name so the caller can tear it down in `finally`.
        """
        rule_name = f"tmvault-restore-{int(datetime.now(timezone.utc).timestamp())}"
        self._log(f"  Opening firewall rule {rule_name} on {server_name} (Allow Azure services)")
        try:
            await sql.firewall_rules.create_or_update(
                resource_group_name=rg,
                server_name=server_name,
                firewall_rule_name=rule_name,
                parameters={
                    "start_ip_address": "0.0.0.0",
                    "end_ip_address": "0.0.0.0",
                },
            )
            return rule_name
        except HttpResponseError as e:
            self._log(f"  Failed to add firewall rule: {e}", "WARNING")
            return None

    async def _remove_firewall_rule(self, sql, rg: str, server_name: str, rule_name: str):
        """Best-effort teardown of a temporary firewall rule added for a restore."""
        try:
            await sql.firewall_rules.delete(
                resource_group_name=rg,
                server_name=server_name,
                firewall_rule_name=rule_name,
            )
            self._log(f"  Removed firewall rule {rule_name}")
        except Exception as e:
            self._log(f"  Could not remove firewall rule {rule_name}: {e}", "WARNING")

    def _get_sql_credentials(self, tenant: Tenant) -> tuple:
        """Retrieve SQL admin credentials from tenant extra_data."""
        from shared.security import decrypt_secret

        if tenant.extra_data:
            admin_user_key = "azure_sql_admin_user_encrypted"
            admin_pass_key = "azure_sql_admin_password_encrypted"
            if admin_user_key in tenant.extra_data and admin_pass_key in tenant.extra_data:
                user = decrypt_secret(tenant.extra_data[admin_user_key])
                password = decrypt_secret(tenant.extra_data[admin_pass_key])
                return user, password

        raise ValueError("SQL admin credentials not found in tenant extra_data")

    def _log(self, message: str, level: str = "INFO"):
        prefix = f"[{self.worker_id}]"
        if level == "ERROR":
            print(f"{prefix} [SQL-RESTORE] ERROR: {message}")
        elif level == "WARNING":
            print(f"{prefix} [SQL-RESTORE] WARNING: {message}")
        else:
            print(f"{prefix} [SQL-RESTORE] {message}")
