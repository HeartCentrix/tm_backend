"""
Azure SQL Database Backup Handler — Afi-Style (Passwordless via SP Entra Admin)

Architecture (as per db.md):
1. During discovery, our SP is auto-assigned as Entra admin of SQL server
2. For backup, we connect using our SP's credentials (Active Directory Service Principal)
3. We stream data row-by-row using pyodbc — no stored customer SQL passwords needed

This is exactly how Afi.ai handles Azure SQL backup — the customer never provides
SQL credentials; our own app's SP is the identity.
"""
import asyncio
import json
import tempfile
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import pyodbc
from azure.mgmt.sql.aio import SqlManagementClient
from azure.core.exceptions import HttpResponseError

from shared.models import Resource, Tenant, Snapshot
from shared.azure_storage import azure_storage_manager
from shared.config import settings
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from lro import await_lro
from arm_credentials import get_arm_credential


class SqlBackupHandler:
    """
    Afi-style Azure SQL backup handler.

    Connection flow:
    1. Discovery auto-assigns our SP as SQL Entra admin
    2. Backup connects using our SP via Active Directory Service Principal auth
    3. Data streamed row-by-row → per-table .sql.gz files in blob storage
    4. Schema captured as SQL scripts for restore
    5. Configuration captured as JSON
    """

    def __init__(self, worker_id: str = "azure-sql-worker"):
        self.worker_id = worker_id

    async def backup(self, resource: Resource, tenant: Tenant,
                     snapshot: Snapshot, msg: Dict) -> Dict:
        """
        Full Azure SQL backup pipeline.

        Pipeline:
          Phase 1: Connect to SQL via SP (Active Directory Service Principal)
          Phase 2: Stream data row-by-row → per-table .sql.gz files
          Phase 3: Capture schema as SQL scripts
          Phase 4: Capture SQL server + DB configuration as JSON
          Phase 5: Store all artifacts in organized blob structure
        """
        credential = get_arm_credential()
        sql_mgmt = SqlManagementClient(credential, resource.azure_subscription_id)

        server_name = resource.extra_data.get("server_name", "") if resource.extra_data else ""
        rg = resource.azure_resource_group or ""
        db_name = resource.external_id

        if not server_name or not rg:
            raise ValueError(f"No server_name or azure_resource_group found in resource {resource.external_id}")

        self._log(f"Starting SQL backup: {db_name} on server {server_name} (RG={rg})")

        # Get our SP credentials for SQL data-plane auth
        # Our SP is Entra admin of this SQL server (assigned during discovery)
        sp_client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
        sp_client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET

        if not sp_client_id or not sp_client_secret:
            raise ValueError(
                f"No SP credentials configured. Set MICROSOFT_CLIENT_ID/SECRET or AZURE_AD_CLIENT_ID/SECRET."
            )

        # Phase 1: Connect and stream data
        data_result = await self._stream_tables(
            resource, tenant, snapshot, rg, server_name, db_name,
            sp_client_id, sp_client_secret
        )

        # Phase 2: Capture schema
        schema_result = await self._capture_schema(
            resource, tenant, snapshot, rg, server_name, db_name,
            sp_client_id, sp_client_secret
        )

        # Phase 3: Capture configuration
        config_result = await self._capture_configuration(
            sql_mgmt, resource, tenant, snapshot, rg, server_name, db_name
        )

        # Finalize
        total_size = data_result.get("total_bytes", 0) + schema_result.get("size_bytes", 0) + config_result.get("size_bytes", 0)

        # Record last backup time on RESOURCE for next incremental
        resource.extra_data = {
            **(resource.extra_data or {}),
            "last_full_backup_at": datetime.now(timezone.utc).isoformat(),
            "last_backup_rows": data_result.get("rows_count", 0),
        }

        # Set blob_path on snapshot for recovery panel indexing
        snapshot.blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/"

        snapshot.extra_data = {
            **(snapshot.extra_data or {}),
            "mode": "AFI_STREAMING",
            "total_size_bytes": total_size,
            "tables_exported": data_result.get("tables_count", 0),
            "rows_exported": data_result.get("rows_count", 0),
            "data_blob_prefix": data_result.get("blob_prefix", ""),
            "schema_blob": schema_result.get("blob_path", ""),
            "config_blob": config_result.get("blob_path", ""),
            "server_name": server_name,
            "database_name": db_name,
            "resource_group": rg,
            "subscription_id": resource.azure_subscription_id,
            "auth_method": "ActiveDirectoryServicePrincipal",
            "backup_timestamp": datetime.now(timezone.utc).isoformat(),
            "last_full_backup_at": datetime.now(timezone.utc).isoformat(),
        }

        # Format size in human-readable units
        if total_size >= 1024 * 1024 * 1024:
            size_str = f"{total_size / (1024**3):.2f} GB"
        elif total_size >= 1024 * 1024:
            size_str = f"{total_size / (1024**2):.2f} MB"
        elif total_size >= 1024:
            size_str = f"{total_size / 1024:.1f} KB"
        else:
            size_str = f"{total_size} bytes"

        self._log(
            f"SQL backup completed: {db_name} — "
            f"{data_result.get('tables_count', 0)} tables, "
            f"{data_result.get('rows_count', 0)} rows, "
            f"{size_str} total"
        )

        return {
            "success": True,
            "mode": "AFI_STREAMING",
            "tables_exported": data_result.get("tables_count", 0),
            "rows_exported": data_result.get("rows_count", 0),
            "total_size_bytes": total_size,
            "auth_method": "ActiveDirectoryServicePrincipal",
        }

    # ==================== Phase 1: Stream Tables Row-by-Row ====================

    async def _stream_tables(self, resource: Resource, tenant: Tenant,
                              snapshot: Snapshot, rg: str, server_name: str,
                              db_name: str, sp_id: str, sp_secret: str) -> Dict:
        """
        Stream data row-by-row from all tables into per-table .sql files.

        Incremental support:
        - On first run: full backup of all tables
        - On subsequent runs: incremental via watermark tracking
          Only exports rows modified since last backup (tracked in resource.extra_data)

        Files are stored UNCOMPRESSED (.sql) so they can be viewed directly
        in Azure Portal's Blob Storage viewer.
        """
        server_fqdn = f"{server_name}.database.windows.net"

        conn_str = (
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

        loop = asyncio.get_event_loop()

        def _get_tables():
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            tables = [(row[0], row[1]) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables

        tables = await loop.run_in_executor(None, _get_tables)
        self._log(f"  Phase 1: Found {len(tables)} tables in {db_name}")

        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql")
        shard = azure_storage_manager.get_default_shard()
        await shard._ensure_container(container)

        blob_prefix = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/data"

        total_bytes = 0
        total_rows = 0

        # Check for incremental: get last successful backup time from resource column
        last_backup_time = resource.last_backup_at
        if last_backup_time and last_backup_time.tzinfo is None:
            last_backup_time = last_backup_time.replace(tzinfo=timezone.utc)

        if last_backup_time:
            self._log(f"  Incremental backup since {last_backup_time}")
        else:
            self._log(f"  Full backup (no previous backup recorded)")

        # Export tables in parallel (max 4 concurrent table exports)
        semaphore = asyncio.Semaphore(4)

        async def export_table(schema: str, table: str):
            nonlocal total_bytes, total_rows
            async with semaphore:
                table_start = time.monotonic()
                table_bytes, table_rows = await self._export_single_table(
                    conn_str, container, blob_prefix, schema, table, shard,
                    last_backup_time=last_backup_time,
                )
                total_bytes += table_bytes
                total_rows += table_rows
                table_duration = time.monotonic() - table_start
                self._log(f"  ✓ Table [{schema}].[{table}]: {table_rows} rows, "
                          f"{table_bytes / 1024:.1f} KB in {table_duration:.1f}s")

        tasks = [export_table(schema, table) for schema, table in tables]
        await asyncio.gather(*tasks, return_exceptions=True)

        return {
            "tables_count": len(tables),
            "rows_count": total_rows,
            "total_bytes": total_bytes,
            "blob_prefix": blob_prefix,
        }

    async def _export_single_table(self, conn_str: str, container: str,
                                    blob_prefix: str, schema: str, table: str,
                                    shard, last_backup_time: Optional[datetime] = None) -> tuple:
        """
        Export a single table as INSERT statements (uncompressed .sql).

        Incremental support:
        - If last_backup_time is provided and the table has an UpdatedDate/ModifiedAt column,
          only exports rows where UpdatedDate >= last_backup_time
        - Otherwise exports all rows (full backup)

        Files are plain .sql so they can be viewed directly in Azure Portal.
        """
        loop = asyncio.get_event_loop()

        def _stream_table():
            """Export table rows as SQL INSERT statements."""
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            # Get column info
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                ORDER BY ORDINAL_POSITION
            """)
            columns = [(row[0], row[1]) for row in cursor.fetchall()]
            col_names = [c[0] for c in columns]
            col_names_str = ", ".join(f"[{c}]" for c in col_names)

            # Check for incremental: find a timestamp column (UpdatedDate, ModifiedAt, etc.)
            incremental_col = None
            if last_backup_time:
                ts_candidates = ['UpdatedDate', 'ModifiedAt', 'UpdatedAt', 'LastModified', 'ModifiedDate', 'ModifiedDate', 'Updated']
                for candidate in ts_candidates:
                    if candidate in col_names:
                        incremental_col = candidate
                        break

            # Build WHERE clause for incremental
            where_clause = ""
            row_count_for_log = 0
            if incremental_col and last_backup_time:
                where_clause = f" WHERE [{incremental_col}] >= '{last_backup_time.strftime('%Y-%m-%d %H:%M:%S')}'"
                self._log(f"    Incremental export for [{schema}].[{table}] via [{incremental_col}] >= {last_backup_time}")

            # Stream rows in batches
            batch_size = 1000
            offset = 0
            total_rows = 0

            # Create temp file (plain .sql, uncompressed)
            tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
            tmp_path = tmp_file.name

            with open(tmp_path, 'w', encoding='utf-8') as f:
                f.write(f"-- Table: [{schema}].[{table}]\n")
                f.write(f"-- Exported: {datetime.now(timezone.utc).isoformat()}\n")
                f.write(f"-- Backup type: {'incremental' if incremental_col else 'full'}\n")
                f.write(f"-- Columns: {col_names_str}\n\n")

                while True:
                    query = f"""
                        SELECT * FROM [{schema}].[{table}]{where_clause}
                        ORDER BY {col_names[0]}
                        OFFSET {offset} ROWS
                        FETCH NEXT {batch_size} ROWS ONLY
                    """
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    if not rows:
                        break

                    for row in rows:
                        values = []
                        for val in row:
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            elif isinstance(val, datetime):
                                values.append(f"'{val.isoformat()}'")
                            else:
                                escaped = str(val).replace("'", "''")
                                values.append(f"'{escaped}'")

                        values_str = ", ".join(values)
                        f.write(f"INSERT INTO [{schema}].[{table}] ({col_names_str}) VALUES ({values_str});\n")

                    total_rows += len(rows)
                    offset += batch_size

            cursor.close()
            conn.close()
            return tmp_path, total_rows, len(columns)

        tmp_path, row_count, col_count = await loop.run_in_executor(None, _stream_table)

        try:
            # Upload to blob using parallel chunked upload
            file_size = os.path.getsize(tmp_path)
            blob_name = f"{blob_prefix}/{schema}.{table}.sql"

            result = await shard.upload_blob_from_file(
                container_name=container,
                blob_path=blob_name,
                file_path=tmp_path,
                file_size=file_size,
                metadata={
                    "content_type": "sql-data",
                    "table_schema": schema,
                    "table_name": table,
                    "row_count": str(row_count),
                    "column_count": str(col_count),
                    "compression": "none",
                    "backup_type": "incremental" if row_count > 0 and last_backup_time else "full",
                },
            )

            return file_size, row_count
        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ==================== Phase 2: Capture Schema ====================

    async def _capture_schema(self, resource: Resource, tenant: Tenant,
                               snapshot: Snapshot, rg: str, server_name: str,
                               db_name: str, sp_id: str, sp_secret: str) -> Dict:
        """
        Capture database schema as SQL scripts.
        """
        server_fqdn = f"{server_name}.database.windows.net"

        conn_str = (
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

        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql")
        shard = azure_storage_manager.get_default_shard()

        self._log(f"  Phase 2: Capturing schema for {db_name}...")

        # Schema queries
        schema_queries = [
            {"name": "tables.sql", "query": self._QUERY_TABLES},
            {"name": "table_definitions.sql", "query": self._QUERY_COLUMNS},
            {"name": "indexes.sql", "query": self._QUERY_INDEXES},
            {"name": "foreign_keys.sql", "query": self._QUERY_FK},
            {"name": "stored_procedures.sql", "query": self._QUERY_PROCEDURES},
            {"name": "views.sql", "query": self._QUERY_VIEWS},
            {"name": "users_permissions.sql", "query": self._QUERY_USERS},
        ]

        schema_size = 0
        schema_artifacts = []

        loop = asyncio.get_event_loop()

        for artifact in schema_queries:
            artifact_name = artifact["name"]
            query = artifact["query"]

            try:
                def _run_query():
                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()
                    cursor.execute(query)
                    columns = [col[0] for col in cursor.description]
                    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    cursor.close()
                    conn.close()
                    return rows

                results = await loop.run_in_executor(None, _run_query)
                content = self._format_sql_script(artifact_name, results)
                content_bytes = content.encode("utf-8")

                blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/schema/{artifact_name}"

                result = await shard.upload_blob(
                    container_name=container,
                    blob_path=blob_path,
                    content=content_bytes,
                    metadata={"content_type": "sql-schema", "artifact_type": artifact_name},
                )

                schema_size += len(content_bytes)
                schema_artifacts.append({"name": artifact_name, "size": len(content_bytes)})
            except Exception as e:
                self._log(f"  Warning: Failed to capture {artifact_name}: {e}", "WARNING")

        # .dacpac metadata
        dacpac_metadata = {
            "dacpac_version": "2.0",
            "database_name": db_name,
            "database_platform": "Azure SQL Database",
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "exported_by": "TM Vault",
            "source_server": server_name,
            "source_resource_group": rg,
        }

        dacpac_blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/schema/{db_name}.dacpac.metadata.json"
        dacpac_content = json.dumps(dacpac_metadata, indent=2, default=str).encode()
        await shard.upload_blob(
            container_name=container,
            blob_path=dacpac_blob_path,
            content=dacpac_content,
            metadata={"content_type": "dacpac-metadata"},
        )
        schema_size += len(dacpac_content)

        self._log(f"  ✓ Schema captured: {len(schema_artifacts)} artifacts, {schema_size / 1024:.1f} KB")
        return {"blob_path": f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/schema/", "size_bytes": schema_size}

    # ==================== Phase 3: Capture Configuration ====================

    async def _capture_configuration(self, sql_mgmt, resource: Resource, tenant: Tenant,
                                      snapshot: Snapshot, rg: str, server_name: str,
                                      db_name: str) -> Dict:
        """
        Capture SQL server + database configuration as JSON.
        """
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql")
        shard = azure_storage_manager.get_default_shard()

        self._log(f"  Phase 3: Capturing configuration for {db_name}...")

        config = {
            "server_name": server_name,
            "database_name": db_name,
            "resource_group": rg,
            "subscription_id": resource.azure_subscription_id,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "server_config": {},
            "database_config": {},
            "firewall_rules": [],
            "tde": {},
        }

        try:
            db = await sql_mgmt.databases.get(
                resource_group_name=rg,
                server_name=server_name,
                database_name=db_name,
            )
            config["database_config"] = {
                "edition": getattr(db, 'edition', None),
                "status": getattr(db, 'status', None),
                "collation": getattr(db, 'collation', None),
                "max_size_bytes": getattr(db, 'max_size_bytes', None),
                "service_objective_name": getattr(db, 'current_service_objective_name', None),
                "zone_redundant": getattr(db, 'zone_redundant', None),
            }

            fw_rules = []
            try:
                async for rule in sql_mgmt.firewall_rules.list_by_server(rg, server_name):
                    fw_rules.append({
                        "name": rule.name,
                        "start_ip": rule.start_ip_address,
                        "end_ip": rule.end_ip_address,
                    })
            except Exception:
                pass
            config["firewall_rules"] = fw_rules

            try:
                tde = await sql_mgmt.transparent_data_encryptions.get(rg, server_name, db_name, "current")
                config["tde"] = {"status": getattr(tde, 'status', None)}
            except Exception:
                config["tde"] = {"status": "unknown"}

            try:
                server = await sql_mgmt.servers.get(rg, server_name)
                config["server_config"] = {
                    "location": getattr(server, 'location', None),
                    "version": getattr(server, 'version', None),
                    "state": getattr(server, 'state', None),
                }
            except Exception:
                pass

        except Exception as e:
            self._log(f"  Warning: Failed to capture config: {e}", "WARNING")

        blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/config/{db_name}-config.json"
        content = json.dumps(config, indent=2, default=str).encode()
        await shard.upload_blob(
            container_name=container,
            blob_path=blob_path,
            content=content,
            metadata={"content_type": "sql-config"},
        )

        self._log(f"  ✓ Configuration captured: {len(content) / 1024:.1f} KB")
        return {"blob_path": blob_path, "size_bytes": len(content)}

    # ==================== Helper Methods ====================

    def _format_sql_script(self, artifact_name: str, results: List[Dict]) -> str:
        """Format query results as a SQL script."""
        lines = [
            f"-- =============================================",
            f"-- TM Vault SQL Schema Export",
            f"-- Artifact: {artifact_name}",
            f"-- Generated: {datetime.now(timezone.utc).isoformat()}",
            f"-- =============================================",
            f"",
        ]

        if not results:
            lines.append("-- No results (query returned empty or was not supported)")
            lines.append("")
            return "\n".join(lines)

        for row in results:
            if artifact_name == "tables.sql":
                schema = row.get("TABLE_SCHEMA", "dbo")
                table = row.get("TABLE_NAME", "unknown")
                cols = row.get("column_count", 0)
                lines.append(f"-- Table: [{schema}].[{table}] ({cols} columns)")
            elif artifact_name == "table_definitions.sql":
                schema = row.get("TABLE_SCHEMA", "dbo")
                table = row.get("TABLE_NAME", "unknown")
                col = row.get("COLUMN_NAME", "")
                dtype = row.get("DATA_TYPE", "")
                nullable = "NULL" if row.get("IS_NULLABLE") == "YES" else "NOT NULL"
                lines.append(f"-- Column: [{schema}].[{table}].[{col}] {dtype} {nullable}")

        return "\n".join(lines)

    def _log(self, message: str, level: str = "INFO"):
        prefix = f"[{self.worker_id}]"
        if level == "ERROR":
            print(f"{prefix} [SQL] ERROR: {message}")
        elif level == "WARNING":
            print(f"{prefix} [SQL] WARNING: {message}")
        else:
            print(f"{prefix} [SQL] {message}")

    # ==================== SQL Queries ====================

    _QUERY_TABLES = """
SELECT
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) as column_count
FROM INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_TYPE = 'BASE TABLE'
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
"""

    _QUERY_COLUMNS = """
SELECT
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS c
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
"""

    _QUERY_INDEXES = """
SELECT
    t.name as table_name,
    i.name as index_name,
    i.type_desc as index_type,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
WHERE i.type > 0
ORDER BY t.name, i.name;
"""

    _QUERY_FK = """
SELECT
    fk.name as constraint_name,
    OBJECT_NAME(fk.parent_object_id) as table_name,
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as column_name,
    OBJECT_NAME(fk.referenced_object_id) as referenced_table,
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as referenced_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
ORDER BY fk.name;
"""

    _QUERY_PROCEDURES = """
SELECT
    ROUTINE_SCHEMA,
    ROUTINE_NAME,
    ROUTINE_DEFINITION
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'
ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;
"""

    _QUERY_VIEWS = """
SELECT
    TABLE_SCHEMA,
    TABLE_NAME as view_name,
    VIEW_DEFINITION
FROM INFORMATION_SCHEMA.VIEWS
ORDER BY TABLE_SCHEMA, TABLE_NAME;
"""

    _QUERY_USERS = """
SELECT
    dp.name as principal_name,
    dp.type_desc as principal_type,
    dp.default_schema_name,
    perm.permission_name,
    perm.state_desc
FROM sys.database_principals dp
LEFT JOIN sys.database_permissions perm ON dp.principal_id = perm.grantee_principal_id
WHERE dp.type IN ('S', 'U', 'G')
ORDER BY dp.name;
"""
