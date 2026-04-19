"""Azure PostgreSQL Backup Handler (2026).

Supports both:
- Flexible Server (2026 standard): Microsoft.DBforPostgreSQL/flexibleServers
- Single Server (deprecated): Microsoft.DBforPostgreSQL/servers

Both use Azure AD authentication via Service Principal — no stored passwords.
Streaming table exports to plain .sql files (viewable in Azure Portal).

Files stored UNCOMPRESSED (.sql) so they can be viewed directly
in Azure Portal's Blob Storage viewer.
"""
import asyncio
import asyncpg
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from azure.mgmt.rdbms.postgresql_flexibleservers.aio import PostgreSQLManagementClient
from azure.mgmt.rdbms.postgresql.aio import PostgreSQLManagementClient as SingleServerClient
from azure.identity.aio import ClientSecretCredential

from shared.models import Resource, Tenant, Snapshot, SnapshotItem
from shared.database import async_session_factory
import hashlib as _hashlib
import uuid as _uuid
from shared.azure_storage import azure_storage_manager
from shared.config import settings


# Timestamp column names to detect for incremental backup
TIMESTAMP_COLUMNS = [
    "updated_at", "updatedat", "updateddate",
    "modified_at", "modifiedat", "modifieddate",
    "last_modified", "lastmodified",
]

# Azure AD token scope for PostgreSQL
# Flexible Server: https://ossrdbms-aad.database.windows.net/.default
# Single Server: same scope but different connection format
PG_AAD_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


class PostgresBackupHandler:
    """Backup Azure PostgreSQL servers via Afi-style streaming export."""

    def __init__(self, worker_id: str = "azure-pg-worker"):
        self.worker_id = worker_id

    def _log(self, message: str, level: str = "INFO"):
        prefix = f"[{self.worker_id}] [PG]"
        print(f"{prefix} {message}")

    def _get_server_type(self, resource: Resource) -> str:
        """
        Detect PostgreSQL server type from resource type.

        2026 Standard:
        - AZURE_POSTGRESQL = Flexible Server (primary)
        - AZURE_POSTGRESQL_SINGLE = Single Server (deprecated, legacy)
        """
        rtype = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
        if rtype in ("AZURE_POSTGRESQL_SINGLE", "AZURE_PG_SINGLE"):
            return "SINGLE"
        return "FLEXIBLE"

    async def backup(self, resource: Resource, tenant: Tenant,
                     snapshot: Snapshot, msg: Dict) -> Dict:
        """
        Afi-style PostgreSQL backup:
        - Phase 1: Connect and stream tables to .sql files
        - Phase 2: Capture schema (CREATE TABLE statements)
        - Phase 3: Capture server configuration
        """
        server_type = self._get_server_type(resource)
        server_name = resource.extra_data.get("server_name", "")
        db_name = resource.extra_data.get("database_name", resource.external_id)
        rg = resource.azure_resource_group or ""
        sub_id = resource.azure_subscription_id

        if not server_name:
            raise ValueError(f"No server_name found in resource {resource.external_id}")

        self._log(f"Starting PostgreSQL {server_type} backup: {db_name} on server {server_name} (RG={rg})")

        conn = None

        try:
            # Phase 1: Get Azure AD connection and stream tables
            conn = await self._get_azure_ad_connection(server_name, db_name, server_type, resource)

            data_result = await self._stream_tables(
                resource, tenant, snapshot, conn, server_name, db_name
            )

            # Phase 2: Capture schema
            schema_result = await self._capture_schema(
                conn, server_name, db_name, snapshot, resource
            )

            # Phase 3: Capture configuration
            config_result = await self._capture_configuration(
                server_name, sub_id, rg, snapshot, resource, server_type
            )

            # Persist SnapshotItem rows so the Recovery UI (AzureDbView)
            # can render Configuration / Schema / Data tabs. Each phase
            # above wrote blobs; this step surfaces them as indexed rows.
            try:
                await self._persist_snapshot_items(
                    resource=resource, tenant=tenant, snapshot=snapshot,
                    server_name=server_name, db_name=db_name, server_type=server_type,
                    data_result=data_result, schema_result=schema_result,
                    config_result=config_result,
                )
            except Exception as pe:
                self._log(f"WARN snapshot_item persist failed: {pe}", "WARN")

            # Finalize
            total_size = data_result.get("total_bytes", 0) + schema_result.get("size_bytes", 0) + config_result.get("size_bytes", 0)

            # Record last backup time on RESOURCE for incremental
            resource.extra_data = {
                **(resource.extra_data or {}),
                "last_full_backup_at": datetime.now(timezone.utc).isoformat(),
                "last_backup_rows": data_result.get("rows_count", 0),
            }

            # Set blob_path for recovery panel
            snapshot.blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/"

            snapshot.extra_data = {
                **(snapshot.extra_data or {}),
                "mode": "AFI_STREAMING",
                "server_type": server_type,
                "total_size_bytes": total_size,
                "tables_exported": data_result.get("tables_count", 0),
                "rows_exported": data_result.get("rows_count", 0),
                "schema_blob": schema_result.get("blob_path", ""),
                "config_blob": config_result.get("blob_path", ""),
                "server_name": server_name,
                "database_name": db_name,
                "resource_group": rg,
                "subscription_id": sub_id,
                "auth_method": "AzureADServicePrincipal",
                "backup_timestamp": datetime.now(timezone.utc).isoformat(),
                "last_full_backup_at": datetime.now(timezone.utc).isoformat(),
            }

            size_str = self._format_size(total_size)

            self._log(
                f"PostgreSQL {server_type} backup completed: {db_name} — "
                f"{data_result.get('tables_count', 0)} tables, "
                f"{data_result.get('rows_count', 0)} rows, "
                f"{size_str} total"
            )

            return {
                "success": True,
                "mode": "AFI_STREAMING",
                "server_type": server_type,
                "tables_exported": data_result.get("tables_count", 0),
                "rows_exported": data_result.get("rows_count", 0),
                "total_size_bytes": total_size,
                "auth_method": "AzureADServicePrincipal",
            }

        except Exception as e:
            self._log(f"PostgreSQL backup FAILED for {db_name}: {e}", "ERROR")
            raise
        finally:
            try:
                if conn:
                    await conn.close()
            except Exception:
                pass

    async def _get_azure_ad_connection(self, server_name: str, db_name: str, server_type: str, resource: Resource):
        """
        Connect to PostgreSQL using Azure AD authentication (no stored passwords).

        Uses our Service Principal to get an Azure AD access token for PostgreSQL.

        Flexible Server (2026 standard):
        - User: `azure_ad_admin` (special username for Azure AD auth)
        - Password: Azure AD access token

        Single Server (deprecated):
        - User: `username@servername`
        - Password: Azure AD access token
        """
        sp_client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
        sp_client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
        sp_tenant_id = settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID

        if not sp_client_id or not sp_client_secret or not sp_tenant_id:
            raise ValueError(
                f"SP credentials not configured. "
                f"Set MICROSOFT_CLIENT_ID/SECRET/TENANT_ID or AZURE_AD_CLIENT_ID/SECRET/TENANT_ID."
            )

        # Get Azure AD access token for PostgreSQL
        credential = ClientSecretCredential(
            client_id=sp_client_id,
            client_secret=sp_client_secret,
            tenant_id=sp_tenant_id,
        )

        try:
            token_result = await credential.get_token(PG_AAD_SCOPE)
            access_token = token_result.token
        except Exception as e:
            raise RuntimeError(
                f"Failed to get Azure AD token for PostgreSQL: {e}. "
                f"Ensure the SP is registered as Azure AD admin of this PostgreSQL server."
            )
        finally:
            await credential.close()

        # Build connection parameters based on server type
        host = f"{server_name}.postgres.database.azure.com"

        if server_type == "FLEXIBLE":
            # Flexible Server: Azure AD auth uses the SP's client ID as username
            # The SP must be registered as Azure AD admin via ARM API during discovery
            user = sp_client_id
        else:
            # Single Server (deprecated): need original admin user + server suffix
            admin_user = resource.extra_data.get("admin_user", "pgadmin")
            user = f"{admin_user}@{server_name}"

        self._log(f"Connecting to {host}/{db_name} using Azure AD auth ({server_type})")

        conn = await asyncpg.connect(
            host=host,
            port=5432,
            user=user,
            password=access_token,
            database=db_name,
            ssl="require",
            timeout=30.0,
        )

        self._log(f"Connected to PostgreSQL {server_type}: {db_name}")
        return conn

    async def _stream_tables(self, resource: Resource, tenant: Tenant,
                              snapshot: Snapshot, conn, server_name: str,
                              db_name: str) -> Dict:
        """
        Stream data row-by-row from all tables into per-table .sql files.

        Incremental support:
        - Checks for timestamp columns (updated_at, modified_at, etc.)
        - Filters rows WHERE timestamp_col >= last_backup_time
        - Falls back to full export if no timestamp column found
        """
        total_bytes = 0
        total_rows = 0
        tables_count = 0
        # Per-table summary for SnapshotItem persistence (populated below).
        table_details: list = []

        # Get incremental watermark
        last_backup_time = resource.last_backup_at
        if last_backup_time and last_backup_time.tzinfo is None:
            last_backup_time = last_backup_time.replace(tzinfo=timezone.utc)

        # Discover all tables
        tables = await conn.fetch("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_schema !~ '^pg_toast'
            ORDER BY table_schema, table_name
        """)

        self._log(f"  Phase 1: Found {len(tables)} tables in {db_name}")

        if last_backup_time:
            self._log(f"  Incremental backup since {last_backup_time}")
        else:
            self._log(f"  Full backup (no previous backup recorded)")

        # Export each table
        for table_row in tables:
            schema = table_row["table_schema"]
            table = table_row["table_name"]
            full_table = f'"{schema}"."{table}"'

            start = time.monotonic()

            # Check for incremental timestamp column
            timestamp_col = None
            if last_backup_time:
                timestamp_col = await self._find_timestamp_column(conn, schema, table)

            # Build query
            if timestamp_col:
                query = f'SELECT * FROM {full_table} WHERE "{timestamp_col}" >= $1 ORDER BY "{timestamp_col}"'
                rows = await conn.fetch(query, last_backup_time)
                self._log(f"    Incremental export for {full_table} via [{timestamp_col}]")
            else:
                query = f"SELECT * FROM {full_table}"
                rows = await conn.fetch(query)

            # Generate INSERT statements as plain .sql
            sql_lines = [f"-- Table: {full_table}"]
            sql_lines.append(f"-- Exported at: {datetime.now(timezone.utc).isoformat()}")
            sql_lines.append(f"-- Rows: {len(rows)}")
            sql_lines.append("")

            if rows:
                columns = list(rows[0].keys())
                col_list = ", ".join(f'"{c}"' for c in columns)

                for row in rows:
                    values = []
                    for col in columns:
                        val = row[col]
                        if val is None:
                            values.append("NULL")
                        elif isinstance(val, (int, float)):
                            values.append(str(val))
                        elif isinstance(val, datetime):
                            values.append(f"'{val.isoformat()}'")
                        elif isinstance(val, bool):
                            values.append("TRUE" if val else "FALSE")
                        else:
                            escaped = str(val).replace("'", "''")
                            values.append(f"'{escaped}'")

                    sql_lines.append(f"INSERT INTO {full_table} ({col_list}) VALUES ({', '.join(values)});")

            sql_content = "\n".join(sql_lines) + "\n"
            sql_bytes = len(sql_content.encode("utf-8"))

            # Upload to blob
            blob_name = f"{schema}_{table}.sql"
            blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/data/{blob_name}"

            shard = azure_storage_manager.get_default_shard()
            container = azure_storage_manager.get_container_name(str(tenant.id), "azure-postgresql")
            await shard._ensure_container(container)

            blob_client = shard.get_async_client().get_blob_client(container, blob_path)
            await blob_client.upload_blob(sql_content.encode("utf-8"), overwrite=True)

            elapsed = time.monotonic() - start
            tables_count += 1
            total_rows += len(rows)
            total_bytes += sql_bytes
            table_details.append({
                "schema": schema,
                "table": table,
                "blob_path": blob_path,
                "size_bytes": sql_bytes,
                "row_count": len(rows),
                "incremental": bool(timestamp_col),
            })

            size_str = self._format_size(sql_bytes)
            self._log(f"  ✓ Table {full_table}: {len(rows)} rows, {size_str} in {elapsed:.1f}s")

        return {
            "tables_count": tables_count,
            "rows_count": total_rows,
            "total_bytes": total_bytes,
            "tables": table_details,
        }

    async def _find_timestamp_column(self, conn, schema: str, table: str) -> Optional[str]:
        """Find a timestamp column suitable for incremental filtering."""
        cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
              AND data_type IN ('timestamp with time zone', 'timestamp without time zone', 'date')
            ORDER BY
                CASE column_name
                    WHEN 'updated_at' THEN 1
                    WHEN 'updatedat' THEN 2
                    WHEN 'updateddate' THEN 3
                    WHEN 'modified_at' THEN 4
                    WHEN 'modifiedat' THEN 5
                    WHEN 'last_modified' THEN 6
                    WHEN 'lastmodified' THEN 7
                    WHEN 'created_at' THEN 8
                    ELSE 99
                END
        """, schema, table)

        for col in cols:
            col_name = col["column_name"].lower()
            if col_name in [c.lower() for c in TIMESTAMP_COLUMNS]:
                return col["column_name"]

        return None

    async def _capture_schema(self, conn, server_name: str, db_name: str,
                               snapshot: Snapshot, resource: Resource) -> Dict:
        """Capture CREATE TABLE statements for all tables."""
        shard = azure_storage_manager.get_default_shard()
        container = azure_storage_manager.get_container_name(str(resource.tenant_id), "azure-postgresql")
        await shard._ensure_container(container)

        tables = await conn.fetch("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_schema !~ '^pg_toast'
            ORDER BY table_schema, table_name
        """)

        schema_lines = [f"-- Schema export for database: {db_name}"]
        schema_lines.append(f"-- Exported at: {datetime.now(timezone.utc).isoformat()}")
        schema_lines.append("")

        for table_row in tables:
            schema = table_row["table_schema"]
            table = table_row["table_name"]

            columns = await conn.fetch("""
                SELECT column_name, data_type, character_maximum_length,
                       is_nullable, column_default, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """, schema, table)

            schema_lines.append(f"-- Table: {schema}.{table}")
            schema_lines.append(f"CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}\" (")

            col_defs = []
            for col in columns:
                dtype = col["data_type"].upper()
                if col["character_maximum_length"]:
                    dtype += f"({col['character_maximum_length']})"
                elif col["numeric_precision"]:
                    if col["numeric_scale"]:
                        dtype += f"({col['numeric_precision']},{col['numeric_scale']})"
                    else:
                        dtype += f"({col['numeric_precision']})"

                nullable = "" if col["is_nullable"] == "NO" else " NULL"
                default = f" DEFAULT {col['column_default']}" if col["column_default"] else ""

                col_defs.append(f'    "{col["column_name"]}" {dtype}{nullable}{default}')

            schema_lines.append(",\n".join(col_defs))
            schema_lines.append(");\n")

        schema_content = "\n".join(schema_lines) + "\n"
        schema_bytes = len(schema_content.encode("utf-8"))

        schema_blob_name = f"{db_name}-schema.sql"
        schema_blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/schema/{schema_blob_name}"

        blob_client = shard.get_async_client().get_blob_client(container, schema_blob_path)
        await blob_client.upload_blob(schema_content.encode("utf-8"), overwrite=True)

        self._log(f"  Phase 2: Schema captured: {len(tables)} tables, {self._format_size(schema_bytes)}")

        return {"blob_path": schema_blob_path, "size_bytes": schema_bytes}

    async def _capture_configuration(self, server_name: str, sub_id: str,
                                      rg: str, snapshot: Snapshot,
                                      resource: Resource, server_type: str) -> Dict:
        """Capture Azure PostgreSQL server configuration via ARM API."""
        try:
            credential = ClientSecretCredential(
                client_id=settings.EFFECTIVE_ARM_CLIENT_ID,
                client_secret=settings.EFFECTIVE_ARM_CLIENT_SECRET,
                tenant_id=settings.EFFECTIVE_ARM_TENANT_ID,
            )

            if server_type == "FLEXIBLE":
                pg_client = PostgreSQLManagementClient(credential, sub_id)
                server = await pg_client.servers.get(
                    resource_group_name=rg,
                    server_name=server_name,
                )
                storage_mb = getattr(getattr(server, "storage", None), "storage_size_gb", None)
                if storage_mb is None:
                    storage_mb = getattr(getattr(server, "storage", None), "storage_size_mb", 0)

                # Time created — try in order:
                #   1) server.create_date (legacy SDK attribute)
                #   2) server.system_data.created_at (newer SDK versions)
                #   3) as_dict()["systemData"]["createdAt"] — Azure ARM
                #      always returns this field in the raw JSON even when
                #      the SDK doesn't expose it as a typed attribute.
                #      This is the one that actually resolves on Flexible
                #      Server today.
                #   4) tags["createdAt"] — customer-set fallback.
                time_created = ""
                cd = getattr(server, "create_date", None)
                if cd:
                    try:
                        time_created = cd.isoformat()
                    except Exception:
                        time_created = str(cd)
                if not time_created:
                    sys_data = getattr(server, "system_data", None)
                    sd_created = getattr(sys_data, "created_at", None) if sys_data else None
                    if sd_created:
                        try:
                            time_created = sd_created.isoformat()
                        except Exception:
                            time_created = str(sd_created)
                if not time_created:
                    try:
                        raw_dict = server.as_dict() if hasattr(server, "as_dict") else {}
                        sd = (raw_dict.get("systemData") or raw_dict.get("system_data") or {})
                        time_created = sd.get("createdAt") or sd.get("created_at") or ""
                    except Exception:
                        pass
                if not time_created:
                    tags = getattr(server, "tags", None) or {}
                    time_created = tags.get("createdAt") or tags.get("created_at") or ""
                # Final fallback — direct ARM REST call with a recent
                # api-version that always surfaces systemData.createdAt.
                # The typed SDK response strips it on older API versions,
                # so bypass it entirely for this one field.
                if not time_created:
                    try:
                        import httpx as _httpx
                        tok = await credential.get_token("https://management.azure.com/.default")
                        arm_url = (
                            f"https://management.azure.com/subscriptions/{sub_id}"
                            f"/resourceGroups/{rg}/providers/Microsoft.DBforPostgreSQL"
                            f"/flexibleServers/{server_name}?api-version=2024-08-01"
                        )
                        async with _httpx.AsyncClient(timeout=15.0) as _c:
                            _r = await _c.get(arm_url, headers={"Authorization": f"Bearer {tok.token}"})
                            if _r.status_code == 200:
                                sd = (_r.json().get("systemData") or {})
                                time_created = sd.get("createdAt") or sd.get("created_at") or ""
                    except Exception as tce:
                        self._log(f"WARN systemData.createdAt REST fallback failed: {tce}", "WARN")

                # Firewall rules — the Azure screenshots show the count
                # ("1 firewall rules") rather than the full list. One
                # extra ARM call gives us the count without bloating
                # the captured config blob.
                firewall_rule_count = 0
                try:
                    fw_iter = pg_client.firewall_rules.list_by_server(
                        resource_group_name=rg, server_name=server_name,
                    )
                    async for _ in fw_iter:
                        firewall_rule_count += 1
                except Exception as fe:
                    self._log(f"WARN firewall rules fetch failed: {fe}", "WARN")

                config = {
                    "server_type": "FLEXIBLE",
                    "server_name": server_name,
                    "resource_group": rg,
                    "subscription_id": sub_id,
                    "location": getattr(server, "location", "unknown"),
                    "fully_qualified_domain_name": getattr(server, "fully_qualified_domain_name", "") or "",
                    "administrator_login": getattr(server, "administrator_login", "") or "",
                    "availability_zone": getattr(server, "availability_zone", "") or "",
                    "time_created": time_created,
                    "sku": {
                        "name": getattr(getattr(server, "sku", None), "name", ""),
                        "tier": getattr(getattr(server, "sku", None), "tier", ""),
                    },
                    "version": getattr(server, "version", "unknown"),
                    "state": getattr(server, "state", "unknown"),
                    "storage": {
                        "storageSizeGB": storage_mb,
                        "autoGrow": getattr(getattr(server, "storage", None), "auto_grow", "Disabled"),
                    },
                    "backup": {
                        "backupRetentionDays": getattr(getattr(server, "backup", None), "backup_retention_days", 7),
                        "geoRedundantBackup": getattr(getattr(server, "backup", None), "geo_redundant_backup", "Disabled"),
                    },
                    "network": {
                        "publicNetworkAccess": getattr(getattr(server, "network", None), "public_network_access", "Enabled"),
                        "delegatedSubnetResourceId": getattr(getattr(server, "network", None), "delegated_subnet_resource_id", None),
                    },
                    "firewallRuleCount": firewall_rule_count,
                    "highAvailability": {
                        "mode": getattr(getattr(server, "high_availability", None), "mode", "Disabled"),
                    },
                    "replicationRole": getattr(server, "replication_role", "None"),
                }
            else:
                # Single Server (deprecated)
                pg_client = SingleServerClient(credential, sub_id)
                server = await pg_client.servers.get(
                    resource_group_name=rg,
                    server_name=server_name,
                )
                config = {
                    "server_type": "SINGLE (DEPRECATED)",
                    "server_name": server_name,
                    "resource_group": rg,
                    "subscription_id": sub_id,
                    "location": getattr(server, "location", "unknown"),
                    "sku": getattr(getattr(server, "sku", None), "name", "unknown"),
                    "version": getattr(server, "version", "unknown"),
                    "state": getattr(server, "user_visible_state", "unknown"),
                    "storage_mb": getattr(server, "storage_mb", 0),
                    "backup_retention_days": getattr(server, "backup_retention_days", 7),
                    "geo_redundant_backup": getattr(server, "geo_redundant_backup", "Disabled"),
                    "ssl_enforcement": getattr(server, "ssl_enforcement", "Unknown"),
                }

            config["captured_at"] = datetime.now(timezone.utc).isoformat()

            await pg_client.close()
            await credential.close()

        except Exception as e:
            config = {
                "server_type": server_type,
                "server_name": server_name,
                "resource_group": rg,
                "error": str(e),
                "captured_at": datetime.now(timezone.utc).isoformat(),
            }

        config_content = f"-- PostgreSQL Server Configuration ({server_type})\n-- Captured: {config['captured_at']}\n\n"
        config_content += "-- Server Metadata:\n"
        for key, value in config.items():
            config_content += f"--   {key}: {value}\n"

        config_bytes = len(config_content.encode("utf-8"))

        shard = azure_storage_manager.get_default_shard()
        container = azure_storage_manager.get_container_name(str(resource.tenant_id), "azure-postgresql")
        await shard._ensure_container(container)

        db_name = resource.extra_data.get("database_name", resource.external_id)
        config_blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/config/{server_name}-config.json"

        blob_client = shard.get_async_client().get_blob_client(container, config_blob_path)
        await blob_client.upload_blob(config_content.encode("utf-8"), overwrite=True)

        self._log(f"  Phase 3: Configuration captured: {self._format_size(config_bytes)}")

        return {"blob_path": config_blob_path, "size_bytes": config_bytes, "config": config}

    async def _persist_snapshot_items(
        self, *, resource: Resource, tenant: Tenant, snapshot: Snapshot,
        server_name: str, db_name: str, server_type: str,
        data_result: Dict, schema_result: Dict, config_result: Dict,
    ) -> None:
        """Insert SnapshotItem rows for the Recovery UI.

        Shape matches what AzureDbView expects:
          • 1 × AZURE_DB_CONFIG    — server metadata JSON (detail card).
          • 1 × AZURE_DB_DATABASE  — the database itself (left rails).
          • 1 × AZURE_DB_SCHEMA_FILE — the schema.sql dump file.
          • N × AZURE_DB_TABLE     — one per captured table (Data left rail).

        Each row carries blob_path so the existing
        /snapshots/{id}/items/{id}/content endpoint can stream the bytes
        back on download without handler-side special casing.
        """
        now_hash = _hashlib.sha256(f"{snapshot.id}".encode()).hexdigest()
        rows: list = []

        # AZURE_DB_CONFIG — the rich per-row metadata powers the
        # Configuration tab's detail card directly.
        cfg = config_result.get("config") or {}
        rows.append(SnapshotItem(
            id=_uuid.uuid4(),
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=f"{server_name}:{db_name}:config",
            item_type="AZURE_DB_CONFIG",
            name=f"{server_name}-config.json",
            folder_path="",
            content_size=int(config_result.get("size_bytes") or 0),
            content_hash=now_hash,
            content_checksum=None,
            blob_path=config_result.get("blob_path"),
            extra_data={
                "raw": cfg,
                "server_name": server_name,
                "database_name": db_name,
                "server_type": server_type,
            },
        ))

        # AZURE_DB_DATABASE — one per database. Each AZURE_POSTGRESQL
        # resource today maps to a single database (resource.external_id
        # includes `server/db`), so emit just this one. When discovery
        # expands to enumerate every database on the server we'll loop
        # here instead.
        rows.append(SnapshotItem(
            id=_uuid.uuid4(),
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=f"{server_name}:{db_name}",
            item_type="AZURE_DB_DATABASE",
            name=db_name,
            folder_path="",
            content_size=0,
            content_hash=None,
            content_checksum=None,
            blob_path=None,
            extra_data={
                "server_name": server_name,
                "database_name": db_name,
                "server_type": server_type,
            },
        ))

        # AZURE_DB_SCHEMA_FILE — schema dump lives under the db name in
        # the Schema tab's file list.
        if schema_result.get("blob_path"):
            schema_name = schema_result["blob_path"].rsplit("/", 1)[-1]
            rows.append(SnapshotItem(
                id=_uuid.uuid4(),
                snapshot_id=snapshot.id,
                tenant_id=tenant.id,
                external_id=f"{server_name}:{db_name}:schema",
                item_type="AZURE_DB_SCHEMA_FILE",
                name=schema_name or f"{db_name}-schema.sql",
                folder_path=db_name,
                content_size=int(schema_result.get("size_bytes") or 0),
                content_hash=None,
                content_checksum=None,
                blob_path=schema_result.get("blob_path"),
                extra_data={
                    "server_name": server_name,
                    "database_name": db_name,
                    "server_type": server_type,
                },
            ))

        # AZURE_DB_TABLE — one row per exported table. folder_path is
        # `<db_name>/<schema>` so Data tab left rail can nest tables
        # under schema when we have multiple schemas.
        for t in (data_result.get("tables") or []):
            schema = t.get("schema") or "public"
            table = t.get("table") or "(unnamed)"
            rows.append(SnapshotItem(
                id=_uuid.uuid4(),
                snapshot_id=snapshot.id,
                tenant_id=tenant.id,
                external_id=f"{server_name}:{db_name}:{schema}:{table}",
                item_type="AZURE_DB_TABLE",
                name=table,
                folder_path=f"{db_name}/{schema}",
                content_size=int(t.get("size_bytes") or 0),
                content_hash=None,
                content_checksum=None,
                blob_path=t.get("blob_path"),
                extra_data={
                    "server_name": server_name,
                    "database_name": db_name,
                    "schema": schema,
                    "table": table,
                    "row_count": int(t.get("row_count") or 0),
                    "incremental": bool(t.get("incremental")),
                },
            ))

        async with async_session_factory() as sess:
            sess.add_all(rows)
            await sess.commit()
        self._log(f"  Persisted {len(rows)} SnapshotItem rows for Recovery UI")

    def _format_size(self, size_bytes: int) -> str:
        """Human-readable file size."""
        if size_bytes >= 1024 * 1024 * 1024:
            return f"{size_bytes / (1024**3):.2f} GB"
        elif size_bytes >= 1024 * 1024:
            return f"{size_bytes / (1024**2):.2f} MB"
        elif size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes} bytes"
