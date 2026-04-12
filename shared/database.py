"""Shared database connection"""
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text

from shared.config import settings

# Append search_path to the connection
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=30,
    max_overflow=40,
    connect_args={"server_settings": {"search_path": settings.DB_SCHEMA}},
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


async def init_db():
    """
    Auto-create schema, enum types, and ALL tables on every service startup.
    Everything is idempotent (IF NOT EXISTS / EXCEPTION duplicate_object).
    No separate migration script or init_db.py needed.
    """
    async with engine.begin() as conn:
        # Create schema if not exists
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.DB_SCHEMA}"))
        await conn.execute(text(f"SET search_path TO {settings.DB_SCHEMA}"))

        # ── Enum Types (idempotent) ──
        enum_statements = [
            """DO $$ BEGIN
                CREATE TYPE userrole AS ENUM ('SUPER_ADMIN', 'ORG_ADMIN', 'TENANT_ADMIN', 'BACKUP_OPERATOR', 'RESTORE_OPERATOR', 'CONTENT_VIEWER', 'USER');
            EXCEPTION WHEN duplicate_object THEN null; END $$;""",
            """DO $$ BEGIN
                CREATE TYPE tenanttype AS ENUM ('M365', 'AZURE', 'BOTH');
            EXCEPTION WHEN duplicate_object THEN null; END $$;""",
            """DO $$ BEGIN
                CREATE TYPE tenantstatus AS ENUM ('PENDING', 'ACTIVE', 'DISCONNECTED', 'SUSPENDED', 'PENDING_DELETION', 'DISCOVERING');
            EXCEPTION WHEN duplicate_object THEN null; END $$;""",
            """DO $$ BEGIN
                CREATE TYPE resourcetype AS ENUM ('MAILBOX', 'SHARED_MAILBOX', 'ROOM_MAILBOX', 'ONEDRIVE', 'SHAREPOINT_SITE', 'TEAMS_CHANNEL', 'TEAMS_CHAT', 'ENTRA_USER', 'ENTRA_GROUP', 'ENTRA_APP', 'ENTRA_SERVICE_PRINCIPAL', 'ENTRA_DEVICE', 'AZURE_VM', 'AZURE_SQL_DB', 'AZURE_POSTGRESQL', 'RESOURCE_GROUP', 'DYNAMIC_GROUP', 'POWER_BI', 'POWER_APPS', 'POWER_AUTOMATE', 'POWER_DLP', 'COPILOT', 'PLANNER', 'TODO', 'ONENOTE');
            EXCEPTION WHEN duplicate_object THEN null; END $$;""",
            """DO $$ BEGIN
                CREATE TYPE resourcestatus AS ENUM ('DISCOVERED', 'ACTIVE', 'ARCHIVED', 'SUSPENDED', 'PENDING_DELETION');
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
        for stmt in enum_statements:
            await conn.execute(text(stmt))

        # New enum values (idempotent)
        await conn.execute(text("ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_SERVICE_PRINCIPAL';"))
        await conn.execute(text("ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'PARTIAL';"))

        # ── CREATE TABLE IF NOT EXISTS (13 tables + indexes) ──
        await conn.execute(text("""
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
        """))

        # 2. Tenants
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tenants (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id UUID REFERENCES organizations(id),
                type VARCHAR DEFAULT 'M365',
                display_name VARCHAR NOT NULL,
                external_tenant_id VARCHAR UNIQUE,
                subscription_id VARCHAR,
                client_id VARCHAR,
                client_secret_ref VARCHAR,
                graph_client_id VARCHAR,
                graph_client_secret_encrypted BYTEA,
                status VARCHAR DEFAULT 'PENDING',
                storage_region VARCHAR,
                last_discovery_at TIMESTAMP,
                graph_delta_tokens JSON DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 3. Platform Users
        await conn.execute(text("""
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
        """))

        # 4. User Roles
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_roles (
                user_id UUID REFERENCES platform_users(id),
                role VARCHAR,
                PRIMARY KEY (user_id, role)
            )
        """))

        # 5. SLA Policies
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sla_policies (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tenant_id UUID REFERENCES tenants(id),
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
                retention_type VARCHAR DEFAULT 'INDEFINITE',
                enabled BOOLEAN DEFAULT TRUE,
                is_default BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 6. Resources
        await conn.execute(text("""
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 7. Jobs
        await conn.execute(text("""
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
        """))

        # 8. Snapshots
        await conn.execute(text("""
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
                snapshot_label VARCHAR,
                content_checksum VARCHAR,
                blob_path VARCHAR,
                storage_version INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 9. Snapshot Items
        await conn.execute(text("""
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
        """))

        # 10. Job Logs
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS job_logs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                job_id UUID REFERENCES jobs(id),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level VARCHAR DEFAULT 'INFO',
                message TEXT,
                details TEXT
            )
        """))

        # 11. Alerts
        await conn.execute(text("""
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
        """))

        # 12. Access Groups
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS access_groups (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id UUID REFERENCES organizations(id),
                tenant_id UUID REFERENCES tenants(id),
                name VARCHAR NOT NULL,
                description VARCHAR,
                scope VARCHAR DEFAULT 'TENANT',
                resource_ids UUID[] DEFAULT '{}',
                permissions JSON DEFAULT '{}',
                member_ids UUID[] DEFAULT '{}',
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 13. Audit Events
        await conn.execute(text("""
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
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_events_tenant ON audit_events(tenant_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_events_action ON audit_events(action)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_events_occurred ON audit_events(occurred_at DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_events_org ON audit_events(org_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_events_resource ON audit_events(resource_id)"))

        # ── Convert VARCHAR columns to enum types (each in its own top-level transaction) ──
        # DROP DEFAULT first for columns that have defaults, otherwise PG can't auto-cast
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
        for stmt in alter_statements:
            try:
                async with engine.begin() as ac:
                    await ac.execute(text(f"SET search_path TO {settings.DB_SCHEMA};"))
                    await ac.execute(text(stmt))
            except Exception:
                pass  # Already converted or column doesn't exist

        # Sync ORM models (for any remaining ORM-specific configurations)
        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    await engine.dispose()
