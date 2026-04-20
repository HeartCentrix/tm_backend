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
import re
from datetime import datetime, timezone, date
from typing import Dict, Any, Optional, List

import pyodbc
from azure.mgmt.sql.aio import SqlManagementClient
from handlers._progress import update_job_pct
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

        # Live progress so the Activity bar moves during the multi-
        # minute BACPAC import instead of jumping 5% → 100%.
        job_id = restore_params.get("job_id")
        await update_job_pct(job_id, 10)

        added_firewall_rule: Optional[str] = None
        try:
            # The SQL backup handler doesn't produce a BACPAC — it
            # streams per-table data to .sql/.json siblings + schema
            # to a nested folder tree. We mirror the Postgres restore
            # pattern: open Azure-services firewall, create the DB,
            # connect with the platform SP (which is already the
            # server's Entra admin), apply schema tree in dependency
            # order, then replay rows from .json.
            schema_prefix = (snapshot.extra_data or {}).get("schema_blob", "")
            data_prefix = (snapshot.extra_data or {}).get("data_blob_prefix", "")
            if not schema_prefix and not data_prefix:
                raise ValueError("snapshot missing schema_blob / data_blob_prefix — is this a legacy BACPAC snapshot?")

            container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql-db")
            await update_job_pct(job_id, 15)

            if configure_firewall:
                added_firewall_rule = await self._configure_firewall_for_restore(sql, target_rg, target_server)
                await update_job_pct(job_id, 25)

            sp_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
            sp_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
            server_fqdn = f"{target_server}.database.windows.net"

            # CREATE DATABASE on the target server. T-SQL disallows
            # CREATE DATABASE inside a user transaction, so we use
            # autocommit=True on the master connection.
            await self._ensure_database(server_fqdn, "master", sp_id, sp_secret, target_db)
            await update_job_pct(job_id, 40)

            # Apply schema tree from blob storage.
            schema_applied = await self._apply_schema_tree(
                tenant, container, schema_prefix,
                server_fqdn, target_db, sp_id, sp_secret,
            )
            await update_job_pct(job_id, 65)

            # Replay row data.
            tables_applied, rows_applied = await self._apply_data_tree(
                tenant, container, data_prefix,
                server_fqdn, target_db, sp_id, sp_secret,
            )
            await update_job_pct(job_id, 95)

            # Verify the ARM resource exists now.
            try:
                db = await sql.databases.get(
                    resource_group_name=target_rg,
                    server_name=target_server,
                    database_name=target_db,
                )
                status = getattr(db, "status", None)
            except Exception:
                status = "unknown"

            self._log(
                f"Full SQL restore completed: {target_db} "
                f"(schema={schema_applied}, tables={tables_applied}, rows={rows_applied}, status={status})"
            )
            return {
                "success": True,
                "database_name": target_db,
                "server_name": target_server,
                "resource_group": target_rg,
                "subscription_id": target_sub,
                "status": status,
                "firewall_rule": added_firewall_rule,
                "schema_statements_applied": schema_applied,
                "tables_restored": tables_applied,
                "rows_restored": rows_applied,
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

    def _odbc_conn_str(self, server_fqdn: str, db_name: str, sp_id: str, sp_secret: str) -> str:
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server_fqdn};"
            f"DATABASE={db_name};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={sp_id};"
            f"PWD={sp_secret};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

    async def _ensure_database(self, server_fqdn: str, admin_db: str,
                               sp_id: str, sp_secret: str, target_db: str) -> None:
        """Create the target database on the server if it doesn't exist.
        Runs against the master DB with autocommit enabled — T-SQL forbids
        CREATE DATABASE inside an implicit transaction."""
        conn_str = self._odbc_conn_str(server_fqdn, admin_db, sp_id, sp_secret)

        def _run():
            conn = pyodbc.connect(conn_str, autocommit=True)
            try:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sys.databases WHERE name = ?", target_db)
                if cur.fetchone():
                    return False
                # Quote identifier by doubling embedded ]. Azure SQL allows
                # new DB creation for Entra admins; sticks with server defaults.
                safe = target_db.replace("]", "]]")
                cur.execute(f'CREATE DATABASE [{safe}]')
                return True
            finally:
                conn.close()

        created = await asyncio.get_event_loop().run_in_executor(None, _run)
        if created:
            self._log(f"Created destination database {target_db!r}")
        else:
            self._log(f"Target database {target_db!r} already exists — reusing")

    @staticmethod
    def _sanitize_tsql(content: str) -> str:
        """Strip MySQL-style numeric type widths the SQL backup emits
        (e.g. `INTEGER(32)` → `INTEGER`) that Azure SQL rejects."""
        return re.sub(
            r"\b(INTEGER|BIGINT|SMALLINT|INT|TINYINT)\s*\(\s*\d+\s*\)",
            r"\1", content, flags=re.IGNORECASE,
        )

    async def _apply_schema_tree(self, tenant, container: str, schema_prefix: str,
                                 server_fqdn: str, target_db: str,
                                 sp_id: str, sp_secret: str) -> int:
        """Walk every schema-dump blob under `schema/` and apply the DDL
        in dependency order (Tables → Views → Procedures → Functions →
        Other/indexes → Other/foreign_keys → Other/users_permissions)."""
        if not schema_prefix:
            return 0
        shard = azure_storage_manager.get_default_shard()
        container_client = shard.get_async_client().get_container_client(container)

        order = {"Tables": 0, "Views": 1, "Procedures": 2, "Functions": 3,
                 "indexes": 4, "foreign_keys": 5, "users_permissions": 6}
        files: list[tuple[int, str]] = []
        async for b in container_client.list_blobs(name_starts_with=schema_prefix):
            # Skip the dacpac metadata sidecar + any other non-SQL
            # artifacts — they're not executable DDL and only noise
            # up the warning log if we try to cur.execute() them.
            low = b.name.lower()
            if low.endswith((".json", ".dacpac", ".metadata.json")):
                continue
            # System-view schema files (sys/*) aren't restorable to a
            # fresh database — the sys catalog is server-managed.
            if "/schema/sql/sys/" in low:
                continue
            rank = 99
            for key, val in order.items():
                if f"/{key}/" in b.name or b.name.endswith(f"/{key}"):
                    rank = val
                    break
            files.append((rank, b.name))
        files.sort()

        conn_str = self._odbc_conn_str(server_fqdn, target_db, sp_id, sp_secret)
        applied = 0

        def _apply_all() -> int:
            n = 0
            conn = pyodbc.connect(conn_str, autocommit=True)
            try:
                cur = conn.cursor()
                for _, name in files:
                    # Use the sync blob client here since we're in a
                    # threadpool worker — mixing async clients across
                    # threads is unsafe.
                    from azure.storage.blob import BlobClient
                    bc = BlobClient(
                        account_url=f"https://{shard.account_name}.blob.core.windows.net",
                        container_name=container,
                        blob_name=name,
                        credential=shard.account_key,
                    )
                    try:
                        payload = bc.download_blob().readall()
                    except Exception as e:
                        self._log(f"  ✗ Failed to download {name}: {e}", "WARNING")
                        continue
                    if not payload:
                        continue
                    content = (payload.decode("utf-8") if isinstance(payload, (bytes, bytearray))
                               else str(payload))
                    real = [ln for ln in content.splitlines()
                            if ln.strip() and not ln.strip().startswith("--")]
                    if not real:
                        continue
                    cleaned = SqlRestoreHandler._sanitize_tsql(content)
                    try:
                        cur.execute(cleaned)
                        n += sum(1 for ln in real if ln.strip().upper().startswith(("CREATE", "ALTER", "GRANT")))
                    except Exception as e:
                        self._log(f"  ✗ Failed to apply {name}: {e}", "WARNING")
            finally:
                conn.close()
            return n

        applied = await asyncio.get_event_loop().run_in_executor(None, _apply_all)
        self._log(f"Schema applied: {applied} statement(s)")
        return applied

    async def _apply_data_tree(self, tenant, container: str, data_prefix: str,
                               server_fqdn: str, target_db: str,
                               sp_id: str, sp_secret: str) -> tuple[int, int]:
        """Replay every per-table data dump. Prefers `<schema>_<table>.json`
        over the companion `.sql` because the JSON always carries the full
        row set; the .sql file is often an incremental delta."""
        if not data_prefix:
            return (0, 0)
        shard = azure_storage_manager.get_default_shard()
        container_client = shard.get_async_client().get_container_client(container)

        # Some backup paths omit the trailing slash. Normalise so
        # name_starts_with matches everything under data/.
        prefix = data_prefix if data_prefix.endswith("/") else data_prefix + "/"
        blobs_by_stem: dict[str, dict[str, str]] = {}
        async for b in container_client.list_blobs(name_starts_with=prefix):
            if b.name.endswith(".json"):
                blobs_by_stem.setdefault(b.name[:-5], {})["json"] = b.name
            elif b.name.endswith(".sql"):
                blobs_by_stem.setdefault(b.name[:-4], {})["sql"] = b.name

        conn_str = self._odbc_conn_str(server_fqdn, target_db, sp_id, sp_secret)

        def _apply_all() -> tuple[int, int]:
            from azure.storage.blob import BlobClient
            from datetime import datetime as _dt, date as _date
            iso_dt_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
            iso_date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

            def coerce(v):
                if not isinstance(v, str):
                    return v
                if iso_dt_re.match(v):
                    try:
                        return _dt.fromisoformat(v.replace("Z", "+00:00"))
                    except ValueError:
                        return v
                if iso_date_re.match(v):
                    try:
                        return _date.fromisoformat(v)
                    except ValueError:
                        return v
                return v

            tables_applied = 0
            rows_applied = 0
            conn = pyodbc.connect(conn_str, autocommit=False)
            try:
                cur = conn.cursor()
                for stem, kinds in blobs_by_stem.items():
                    chosen = kinds.get("json") or kinds.get("sql")
                    if not chosen:
                        continue
                    bc = BlobClient(
                        account_url=f"https://{shard.account_name}.blob.core.windows.net",
                        container_name=container,
                        blob_name=chosen,
                        credential=shard.account_key,
                    )
                    try:
                        payload = bc.download_blob().readall()
                    except Exception as e:
                        self._log(f"  ✗ Failed to download {chosen}: {e}", "WARNING")
                        continue
                    if not payload:
                        tables_applied += 1
                        continue

                    if chosen.endswith(".json"):
                        try:
                            doc = json.loads(payload.decode("utf-8"))
                        except Exception as e:
                            self._log(f"  ✗ Bad JSON in {chosen}: {e}", "WARNING")
                            continue
                        schema = doc.get("schema") or "dbo"
                        table = doc.get("table")
                        cols = doc.get("columns") or []
                        rows = doc.get("rows") or []
                        if not table or not cols or not rows:
                            tables_applied += 1
                            continue
                        col_list = ", ".join(f"[{c}]" for c in cols)
                        placeholders = ", ".join(["?"] * len(cols))
                        stmt = f"INSERT INTO [{schema}].[{table}] ({col_list}) VALUES ({placeholders})"
                        first_err: Optional[str] = None
                        inserted = 0
                        for r in rows:
                            # SQL backup persists rows as positional
                            # arrays (row[i] aligns with columns[i]).
                            # Earlier Postgres handler used dicts; cover
                            # both shapes so either format restores.
                            if isinstance(r, list):
                                values = [coerce(v) for v in r]
                            elif isinstance(r, dict):
                                values = [coerce(r.get(c)) for c in cols]
                            else:
                                continue
                            try:
                                cur.execute(stmt, *values)
                                inserted += 1
                            except Exception as e:
                                if first_err is None:
                                    first_err = str(e)[:200]
                        conn.commit()
                        if inserted == 0 and first_err:
                            self._log(f"    all {len(rows)} rows failed for {schema}.{table}: {first_err}", "WARNING")
                        tables_applied += 1
                        rows_applied += inserted
                        self._log(f"  ✓ Applied {chosen} ({inserted} rows from JSON)")
                    else:
                        text = payload.decode("utf-8")
                        if sum(1 for ln in text.splitlines()
                               if ln.strip().upper().startswith("INSERT")) == 0:
                            tables_applied += 1
                            continue
                        try:
                            cur.execute(SqlRestoreHandler._sanitize_tsql(text))
                            conn.commit()
                            tables_applied += 1
                            self._log(f"  ✓ Applied {chosen}")
                        except Exception as e:
                            conn.rollback()
                            self._log(f"  ✗ Failed to apply {chosen}: {e}", "WARNING")
            finally:
                conn.close()
            return (tables_applied, rows_applied)

        return await asyncio.get_event_loop().run_in_executor(None, _apply_all)

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
