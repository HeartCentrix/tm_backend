"""Shared database connection and schema bootstrap helpers."""

import asyncio
import logging
from time import monotonic
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from shared.config import settings

SCHEMA_INIT_LOCK_ID = 1234567890
DDL_LOCK_TIMEOUT = "2000ms"
DDL_STATEMENT_TIMEOUT = "30000ms"
SCHEMA_READY_TIMEOUT_SECONDS = 120
SCHEMA_READY_POLL_INTERVAL_SECONDS = 1.0
SEARCH_PATH = f"{settings.DB_SCHEMA},public"

REQUIRED_TABLES = (
    "organizations",
    "tenants",
    "platform_users",
    "user_roles",
    "sla_policies",
    "resources",
    "jobs",
    "snapshots",
    "snapshot_items",
    "job_logs",
    "alerts",
    "audit_events",
    "admin_consent_tokens",
    "discovery_runs",
    "resource_discovery_staging",
)

REQUIRED_COLUMNS = {
    "sla_policies": (
        "service_type",
        "backup_azure_vm",
        "backup_azure_sql",
        "backup_azure_postgresql",
    ),
    "resources": ("resource_hash",),
    "resource_discovery_staging": (
        "azure_subscription_id",
        "azure_resource_group",
        "azure_region",
    ),
}

logger = logging.getLogger(__name__)


engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_use_lifo=settings.DB_POOL_USE_LIFO,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    connect_args={"server_settings": {"search_path": SEARCH_PATH}},
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def _has_required_tables(conn) -> bool:
    for table_name in REQUIRED_TABLES:
        result = await conn.execute(
            text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
            {"qualified_name": f"{settings.DB_SCHEMA}.{table_name}"},
        )
        if not bool(result.scalar()):
            return False
    return True


async def _has_required_columns(conn) -> bool:
    for table_name, columns in REQUIRED_COLUMNS.items():
        for column_name in columns:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = :schema_name
                          AND table_name = :table_name
                          AND column_name = :column_name
                    )
                    """
                ),
                {
                    "schema_name": settings.DB_SCHEMA,
                    "table_name": table_name,
                    "column_name": column_name,
                },
            )
            if not bool(result.scalar()):
                return False
    return True


async def wait_for_schema_ready(
    timeout_seconds: int = SCHEMA_READY_TIMEOUT_SECONDS,
    poll_interval_seconds: float = SCHEMA_READY_POLL_INTERVAL_SECONDS,
) -> bool:
    deadline = monotonic() + timeout_seconds
    last_error: Exception | None = None

    while monotonic() < deadline:
        try:
            async with engine.connect() as conn:
                if await _has_required_tables(conn) and await _has_required_columns(conn):
                    return True
        except Exception as exc:  # pragma: no cover - defensive startup logging
            last_error = exc

        await asyncio.sleep(poll_interval_seconds)

    if last_error:
        logger.warning("[DB INIT] Timed out waiting for schema readiness: %s", last_error)
    return False


async def _execute_batch(conn, statements: list[str]) -> None:
    for stmt in statements:
        await conn.execute(text(stmt))


async def _ensure_enum_values() -> None:
    statements = [
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'IN_PROGRESS';",
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'COMPLETED';",
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'FAILED';",
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'PARTIAL';",
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'PENDING_DELETION';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_SERVICE_PRINCIPAL';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'AZURE_POSTGRESQL_SINGLE';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_ROLE';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_ADMIN_UNIT';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_AUDIT_LOG';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'INTUNE_MANAGED_DEVICE';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'PENDING_DISCOVERY';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'QUEUED';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'RUNNING';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'COMPLETED';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'FAILED';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'CANCELLED';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'RETRYING';",
        "ALTER TYPE snapshottype ADD VALUE IF NOT EXISTS 'FULL';",
        "ALTER TYPE snapshottype ADD VALUE IF NOT EXISTS 'INCREMENTAL';",
        "ALTER TYPE snapshottype ADD VALUE IF NOT EXISTS 'PREEMPTIVE';",
        "ALTER TYPE snapshottype ADD VALUE IF NOT EXISTS 'MANUAL';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'DISCOVERED';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'ACTIVE';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'ARCHIVED';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'SUSPENDED';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'PENDING_DELETION';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'PENDING';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'ACTIVE';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'DISCONNECTED';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'SUSPENDED';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'PENDING_DELETION';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'DISCOVERING';",
        "ALTER TYPE tenanttype ADD VALUE IF NOT EXISTS 'M365';",
        "ALTER TYPE tenanttype ADD VALUE IF NOT EXISTS 'AZURE';",
        # 'BOTH' removed; legacy deployments may still have it as an orphaned enum
        # value (harmless — no rows reference it after the migration).
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'BACKUP';",
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'RESTORE';",
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'EXPORT';",
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'DISCOVERY';",
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'DELETE';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'SUPER_ADMIN';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'ORG_ADMIN';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'TENANT_ADMIN';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'BACKUP_OPERATOR';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'RESTORE_OPERATOR';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'CONTENT_VIEWER';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'USER';",
    ]

    for stmt in statements:
        try:
            async with engine.begin() as conn:
                await conn.execute(text(f"SET LOCAL lock_timeout = '{DDL_LOCK_TIMEOUT}'"))
                await conn.execute(text(f"SET LOCAL statement_timeout = '{DDL_STATEMENT_TIMEOUT}'"))
                await conn.execute(text(stmt))
        except Exception:
            # Postgres version / transaction semantics differ a bit here; IF NOT EXISTS
            # plus the guarded fallback keeps startup idempotent.
            pass


async def init_db() -> None:
    """Create schema objects on first boot and wait for readiness on contended boot."""

    enum_type_statements = [
        """DO $$ BEGIN
            CREATE TYPE userrole AS ENUM ('SUPER_ADMIN', 'ORG_ADMIN', 'TENANT_ADMIN', 'BACKUP_OPERATOR', 'RESTORE_OPERATOR', 'CONTENT_VIEWER', 'USER');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE tenanttype AS ENUM ('M365', 'AZURE');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE tenantstatus AS ENUM ('PENDING', 'ACTIVE', 'DISCONNECTED', 'SUSPENDED', 'PENDING_DELETION', 'DISCOVERING', 'PENDING_DISCOVERY');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE resourcetype AS ENUM ('MAILBOX', 'SHARED_MAILBOX', 'ROOM_MAILBOX', 'ONEDRIVE', 'SHAREPOINT_SITE', 'TEAMS_CHANNEL', 'TEAMS_CHAT', 'ENTRA_USER', 'ENTRA_GROUP', 'ENTRA_APP', 'ENTRA_SERVICE_PRINCIPAL', 'ENTRA_DEVICE', 'ENTRA_ROLE', 'ENTRA_ADMIN_UNIT', 'ENTRA_AUDIT_LOG', 'INTUNE_MANAGED_DEVICE', 'AZURE_VM', 'AZURE_SQL_DB', 'AZURE_POSTGRESQL', 'AZURE_POSTGRESQL_SINGLE', 'RESOURCE_GROUP', 'DYNAMIC_GROUP', 'POWER_BI', 'POWER_APPS', 'POWER_AUTOMATE', 'POWER_DLP', 'COPILOT', 'PLANNER', 'TODO', 'ONENOTE');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE resourcestatus AS ENUM ('DISCOVERED', 'ACTIVE', 'ARCHIVED', 'SUSPENDED', 'PENDING_DELETION', 'INACCESSIBLE');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE jobtype AS ENUM ('BACKUP', 'RESTORE', 'EXPORT', 'DISCOVERY', 'DELETE');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE jobstatus AS ENUM ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'RETRYING');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE snapshottype AS ENUM ('FULL', 'INCREMENTAL', 'PREEMPTIVE', 'MANUAL');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE snapshotstatus AS ENUM ('IN_PROGRESS', 'COMPLETED', 'FAILED', 'PARTIAL', 'PENDING_DELETION');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
    ]

    table_statements = [
        """
        CREATE TABLE IF NOT EXISTS organizations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR NOT NULL,
            slug VARCHAR UNIQUE NOT NULL,
            storage_region VARCHAR,
            encryption_mode VARCHAR DEFAULT 'TMVAULT_MANAGED',
            storage_quota_bytes BIGINT DEFAULT 536870912000,
            storage_bytes_used BIGINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            type VARCHAR DEFAULT 'M365',
            display_name VARCHAR NOT NULL,
            external_tenant_id VARCHAR UNIQUE,
            customer_id VARCHAR,
            subscription_id VARCHAR,
            client_id VARCHAR,
            client_secret_ref VARCHAR,
            graph_client_id VARCHAR,
            graph_client_secret_encrypted BYTEA,
            status VARCHAR DEFAULT 'PENDING',
            storage_region VARCHAR,
            last_discovery_at TIMESTAMP,
            graph_delta_tokens JSON DEFAULT '{}',
            extra_data JSON DEFAULT '{}',
            dr_region_enabled BOOLEAN DEFAULT FALSE,
            dr_region VARCHAR,
            dr_storage_account_name VARCHAR,
            dr_storage_account_key_encrypted BYTEA,
            dr_last_replicated_at TIMESTAMP,
            azure_refresh_token_encrypted BYTEA,
            azure_refresh_token_updated_at TIMESTAMP,
            azure_subscriptions_cached JSON DEFAULT '[]',
            azure_sql_servers_configured JSON DEFAULT '[]',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS platform_users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR UNIQUE NOT NULL,
            name VARCHAR NOT NULL,
            external_user_id VARCHAR,
            org_id UUID REFERENCES organizations(id),
            tenant_id UUID REFERENCES tenants(id),
            mfa_enabled BOOLEAN DEFAULT FALSE,
            last_login_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_roles (
            user_id UUID REFERENCES platform_users(id),
            role VARCHAR,
            PRIMARY KEY (user_id, role)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sla_policies (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID REFERENCES tenants(id),
            service_type VARCHAR DEFAULT 'm365',
            name VARCHAR NOT NULL,
            frequency VARCHAR DEFAULT 'DAILY',
            backup_days VARCHAR[],
            backup_window_start VARCHAR,
            backup_window_end VARCHAR,
            resource_types VARCHAR[],
            batch_size INTEGER DEFAULT 1000,
            max_concurrent_backups INTEGER DEFAULT 5,
            sla_violation_alert BOOLEAN DEFAULT TRUE,
            retention_days INTEGER DEFAULT 2555,
            retention_versions INTEGER DEFAULT 10,
            backup_exchange BOOLEAN DEFAULT TRUE,
            backup_exchange_archive BOOLEAN DEFAULT FALSE,
            backup_exchange_recoverable BOOLEAN DEFAULT FALSE,
            backup_onedrive BOOLEAN DEFAULT TRUE,
            backup_sharepoint BOOLEAN DEFAULT TRUE,
            backup_teams BOOLEAN DEFAULT TRUE,
            backup_teams_chats BOOLEAN DEFAULT FALSE,
            backup_entra_id BOOLEAN DEFAULT TRUE,
            backup_power_platform BOOLEAN DEFAULT FALSE,
            backup_copilot BOOLEAN DEFAULT FALSE,
            contacts BOOLEAN DEFAULT TRUE,
            calendars BOOLEAN DEFAULT TRUE,
            tasks BOOLEAN DEFAULT FALSE,
            group_mailbox BOOLEAN DEFAULT TRUE,
            planner BOOLEAN DEFAULT FALSE,
            backup_azure_vm BOOLEAN DEFAULT TRUE,
            backup_azure_sql BOOLEAN DEFAULT TRUE,
            backup_azure_postgresql BOOLEAN DEFAULT TRUE,
            retention_type VARCHAR DEFAULT 'INDEFINITE',
            retention_hot_days INTEGER DEFAULT 7,
            retention_cool_days INTEGER DEFAULT 30,
            retention_archive_days INTEGER,
            legal_hold_enabled BOOLEAN DEFAULT FALSE,
            legal_hold_until TIMESTAMP,
            immutability_mode VARCHAR DEFAULT 'None',
            enabled BOOLEAN DEFAULT TRUE,
            is_default BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS resources (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID REFERENCES tenants(id),
            type VARCHAR NOT NULL,
            external_id VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            email VARCHAR,
            metadata JSON DEFAULT '{}',
            sla_policy_id UUID REFERENCES sla_policies(id),
            status VARCHAR DEFAULT 'DISCOVERED',
            last_backup_job_id UUID,
            last_backup_at TIMESTAMP,
            last_backup_status VARCHAR,
            storage_bytes BIGINT DEFAULT 0,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            archived_at TIMESTAMP,
            deletion_queued_at TIMESTAMP,
            azure_subscription_id VARCHAR,
            azure_resource_group VARCHAR,
            azure_region VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            type VARCHAR NOT NULL,
            tenant_id UUID REFERENCES tenants(id),
            resource_id UUID REFERENCES resources(id),
            batch_resource_ids UUID[] DEFAULT '{}',
            snapshot_id UUID,
            status VARCHAR DEFAULT 'QUEUED',
            priority INTEGER DEFAULT 5,
            attempts INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 5,
            error_message TEXT,
            progress_pct INTEGER DEFAULT 0,
            items_processed BIGINT DEFAULT 0,
            bytes_processed BIGINT DEFAULT 0,
            result JSON DEFAULT '{}',
            spec JSON DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            resource_id UUID REFERENCES resources(id),
            job_id UUID REFERENCES jobs(id),
            type VARCHAR DEFAULT 'INCREMENTAL',
            status VARCHAR DEFAULT 'COMPLETED',
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            duration_secs INTEGER,
            item_count INTEGER DEFAULT 0,
            new_item_count INTEGER DEFAULT 0,
            bytes_added BIGINT DEFAULT 0,
            bytes_total BIGINT DEFAULT 0,
            delta_token VARCHAR,
            delta_tokens_json JSON DEFAULT '{}',
            extra_data JSON DEFAULT '{}',
            snapshot_label VARCHAR,
            content_checksum VARCHAR,
            blob_path VARCHAR,
            storage_version INTEGER DEFAULT 1,
            azure_restore_point_id VARCHAR,
            azure_operation_id VARCHAR,
            dr_replication_status VARCHAR DEFAULT 'pending',
            dr_blob_path VARCHAR,
            dr_replicated_at TIMESTAMP,
            dr_error TEXT,
            dr_replication_attempts INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS snapshot_items (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            snapshot_id UUID REFERENCES snapshots(id),
            tenant_id UUID REFERENCES tenants(id),
            external_id VARCHAR NOT NULL,
            item_type VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            folder_path VARCHAR,
            content_hash VARCHAR,
            content_checksum VARCHAR,
            content_size BIGINT DEFAULT 0,
            blob_path VARCHAR,
            encryption_key_id VARCHAR,
            backup_version INTEGER DEFAULT 1,
            metadata JSON DEFAULT '{}',
            is_deleted BOOLEAN DEFAULT FALSE,
            indexed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS job_logs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id UUID REFERENCES jobs(id),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            level VARCHAR DEFAULT 'INFO',
            message TEXT,
            details TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID REFERENCES tenants(id),
            org_id UUID REFERENCES organizations(id),
            type VARCHAR NOT NULL,
            severity VARCHAR DEFAULT 'MEDIUM',
            message TEXT NOT NULL,
            resource_id UUID,
            resource_type VARCHAR,
            resource_name VARCHAR,
            triggered_by VARCHAR,
            resolved BOOLEAN DEFAULT FALSE,
            resolved_at TIMESTAMP,
            resolved_by UUID,
            resolution_note TEXT,
            details JSON DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS audit_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            tenant_id UUID REFERENCES tenants(id),
            actor_id UUID,
            actor_email VARCHAR,
            actor_type VARCHAR DEFAULT 'SYSTEM',
            action VARCHAR NOT NULL,
            resource_id UUID,
            resource_type VARCHAR,
            resource_name VARCHAR,
            outcome VARCHAR DEFAULT 'SUCCESS',
            job_id UUID,
            snapshot_id UUID,
            details JSONB DEFAULT '{}',
            occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS admin_consent_tokens (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            tenant_id UUID REFERENCES tenants(id),
            consent_type VARCHAR NOT NULL,
            access_token_encrypted BYTEA,
            refresh_token_encrypted BYTEA,
            token_type VARCHAR DEFAULT 'Bearer',
            expires_at TIMESTAMP,
            granted_by VARCHAR,
            consented_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_used_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            scope VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS discovery_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID REFERENCES tenants(id) NOT NULL,
            scope JSON DEFAULT '[]',
            status VARCHAR DEFAULT 'RUNNING',
            fetched_count INTEGER DEFAULT 0,
            staged_count INTEGER DEFAULT 0,
            inserted_count INTEGER DEFAULT 0,
            updated_count INTEGER DEFAULT 0,
            unchanged_count INTEGER DEFAULT 0,
            stale_marked_count INTEGER DEFAULT 0,
            error_message TEXT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS resource_discovery_staging (
            id BIGSERIAL PRIMARY KEY,
            run_id UUID REFERENCES discovery_runs(id) ON DELETE CASCADE,
            tenant_id UUID REFERENCES tenants(id) NOT NULL,
            resource_type VARCHAR NOT NULL,
            external_id VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            email VARCHAR,
            metadata JSONB DEFAULT '{}'::jsonb,
            resource_status VARCHAR DEFAULT 'DISCOVERED',
            resource_hash VARCHAR,
            azure_subscription_id VARCHAR,
            azure_resource_group VARCHAR,
            azure_region VARCHAR,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS report_configs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            enabled BOOLEAN DEFAULT FALSE NOT NULL,
            schedule_type VARCHAR DEFAULT 'daily' NOT NULL,
            send_empty_report BOOLEAN DEFAULT TRUE NOT NULL,
            empty_message VARCHAR DEFAULT 'No updates. No backups occurred.',
            send_detailed_report BOOLEAN DEFAULT FALSE NOT NULL,
            email_recipients JSON DEFAULT '[]',
            slack_webhooks JSON DEFAULT '[]',
            teams_webhooks JSON DEFAULT '[]',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS report_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            report_config_id UUID REFERENCES report_configs(id),
            report_type VARCHAR NOT NULL,
            period_start TIMESTAMP,
            period_end TIMESTAMP,
            generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            total_backups INTEGER DEFAULT 0,
            successful_backups INTEGER DEFAULT 0,
            failed_backups INTEGER DEFAULT 0,
            success_rate VARCHAR,
            coverage_rate VARCHAR,
            report_data JSON DEFAULT '{}',
            is_empty BOOLEAN DEFAULT FALSE NOT NULL,
            delivery_status JSON DEFAULT '{}',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    ]

    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_audit_events_tenant ON audit_events(tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_audit_events_action ON audit_events(action)",
        "CREATE INDEX IF NOT EXISTS idx_audit_events_occurred ON audit_events(occurred_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_audit_events_org ON audit_events(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_audit_events_resource ON audit_events(resource_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_consent_org ON admin_consent_tokens(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_consent_tenant ON admin_consent_tokens(tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_consent_type ON admin_consent_tokens(consent_type)",
        "CREATE INDEX IF NOT EXISTS idx_admin_consent_active ON admin_consent_tokens(is_active)",
        "CREATE INDEX IF NOT EXISTS idx_sla_policies_tenant_service ON sla_policies(tenant_id, service_type)",
        "CREATE INDEX IF NOT EXISTS idx_resources_tenant_type_external ON resources(tenant_id, type, external_id)",
        "CREATE INDEX IF NOT EXISTS idx_resources_tenant_status_type ON resources(tenant_id, status, type)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_runs_tenant_started ON discovery_runs(tenant_id, started_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_runs_status ON discovery_runs(status)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_stage_run ON resource_discovery_staging(run_id)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_stage_lookup ON resource_discovery_staging(run_id, tenant_id, resource_type, external_id)",
        "CREATE INDEX IF NOT EXISTS idx_report_configs_org ON report_configs(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_report_history_org ON report_history(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_report_history_generated ON report_history(generated_at DESC)",
    ]

    add_column_statements = [
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS customer_id VARCHAR;",
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS azure_subscription_id VARCHAR;",
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS azure_resource_group VARCHAR;",
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS azure_region VARCHAR;",
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS resource_hash VARCHAR;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS azure_restore_point_id VARCHAR;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS azure_operation_id VARCHAR;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS retention_hot_days INTEGER DEFAULT 7;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS retention_cool_days INTEGER DEFAULT 30;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS retention_archive_days INTEGER;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS legal_hold_enabled BOOLEAN DEFAULT FALSE;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMP;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS immutability_mode VARCHAR DEFAULT 'None';",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS service_type VARCHAR DEFAULT 'm365';",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS backup_azure_vm BOOLEAN DEFAULT TRUE;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS backup_azure_sql BOOLEAN DEFAULT TRUE;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS backup_azure_postgresql BOOLEAN DEFAULT TRUE;",
        "UPDATE sla_policies SET service_type = 'm365' WHERE service_type IS NULL OR service_type = '';",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_region_enabled BOOLEAN DEFAULT FALSE;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_region VARCHAR;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_storage_account_name VARCHAR;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_storage_account_key_encrypted BYTEA;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_last_replicated_at TIMESTAMP;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS extra_data JSON DEFAULT '{}';",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_refresh_token_encrypted BYTEA;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_refresh_token_updated_at TIMESTAMP;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_subscriptions_cached JSON DEFAULT '{}';",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_sql_servers_configured JSON DEFAULT '{}';",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_pg_servers_configured JSON DEFAULT '{}';",
        "ALTER TABLE report_configs ADD COLUMN IF NOT EXISTS send_detailed_report BOOLEAN DEFAULT FALSE NOT NULL;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_replication_status VARCHAR DEFAULT 'pending';",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_blob_path VARCHAR;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_replicated_at TIMESTAMP;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_error TEXT;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_replication_attempts INTEGER DEFAULT 0;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS extra_data JSON DEFAULT '{}';",
        "ALTER TABLE resource_discovery_staging ADD COLUMN IF NOT EXISTS azure_subscription_id VARCHAR;",
        "ALTER TABLE resource_discovery_staging ADD COLUMN IF NOT EXISTS azure_resource_group VARCHAR;",
        "ALTER TABLE resource_discovery_staging ADD COLUMN IF NOT EXISTS azure_region VARCHAR;",
    ]

    alter_statements = [
        """ALTER TABLE tenants ALTER COLUMN type DROP DEFAULT, ALTER COLUMN type TYPE tenanttype USING type::tenanttype, ALTER COLUMN type SET DEFAULT 'M365'::tenanttype;""",
        """ALTER TABLE tenants ALTER COLUMN status DROP DEFAULT, ALTER COLUMN status TYPE tenantstatus USING status::tenantstatus, ALTER COLUMN status SET DEFAULT 'PENDING'::tenantstatus;""",
        """ALTER TABLE resources ALTER COLUMN type DROP DEFAULT, ALTER COLUMN type TYPE resourcetype USING type::resourcetype, ALTER COLUMN type SET DEFAULT 'ENTRA_USER'::resourcetype;""",
        """ALTER TABLE resources ALTER COLUMN status DROP DEFAULT, ALTER COLUMN status TYPE resourcestatus USING status::resourcestatus, ALTER COLUMN status SET DEFAULT 'DISCOVERED'::resourcestatus;""",
        """ALTER TABLE jobs ALTER COLUMN type DROP DEFAULT, ALTER COLUMN type TYPE jobtype USING type::jobtype, ALTER COLUMN type SET DEFAULT 'BACKUP'::jobtype;""",
        """ALTER TABLE jobs ALTER COLUMN status DROP DEFAULT, ALTER COLUMN status TYPE jobstatus USING status::jobstatus, ALTER COLUMN status SET DEFAULT 'QUEUED'::jobstatus;""",
        """ALTER TABLE snapshots ALTER COLUMN type DROP DEFAULT, ALTER COLUMN type TYPE snapshottype USING type::snapshottype, ALTER COLUMN type SET DEFAULT 'FULL'::snapshottype;""",
        """ALTER TABLE snapshots ALTER COLUMN status DROP DEFAULT, ALTER COLUMN status TYPE snapshotstatus USING status::snapshotstatus, ALTER COLUMN status SET DEFAULT 'IN_PROGRESS'::snapshotstatus;""",
        """ALTER TABLE user_roles ALTER COLUMN role TYPE userrole USING role::userrole;""",
    ]

    try:
        async with engine.begin() as conn:
            locked = (
                await conn.execute(text(f"SELECT pg_try_advisory_xact_lock({SCHEMA_INIT_LOCK_ID});"))
            ).scalar()
            if not locked:
                if await wait_for_schema_ready():
                    return
                raise RuntimeError(
                    "Timed out waiting for another service to finish database schema initialization"
                )

            await conn.execute(text(f"SET LOCAL lock_timeout = '{DDL_LOCK_TIMEOUT}'"))
            await conn.execute(text(f"SET LOCAL statement_timeout = '{DDL_STATEMENT_TIMEOUT}'"))
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.DB_SCHEMA}"))
            await conn.execute(text(f"SET search_path TO {SEARCH_PATH}"))

            await _execute_batch(conn, enum_type_statements)
            await _execute_batch(conn, table_statements)

            for stmt in add_column_statements:
                try:
                    await conn.execute(text(stmt))
                except Exception:
                    pass

            await _execute_batch(conn, index_statements)

            for stmt in alter_statements:
                await conn.execute(text("SAVEPOINT alter_sp"))
                try:
                    await conn.execute(text(stmt))
                    await conn.execute(text("RELEASE SAVEPOINT alter_sp"))
                except Exception:
                    await conn.execute(text("ROLLBACK TO SAVEPOINT alter_sp"))  # Already converted

            await conn.run_sync(Base.metadata.create_all)

        await _ensure_enum_values()
    except Exception as exc:
        logger.warning("[DB INIT] Schema sync phase failed: %s", exc)
        if await wait_for_schema_ready(timeout_seconds=30):
            return
        raise


async def close_db():
    await engine.dispose()
