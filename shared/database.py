"""Shared database connection"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import event, text

from shared.config import settings

# Append search_path to the connection
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    connect_args={"server_settings": {"search_path": settings.DB_SCHEMA}},
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
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
    async with engine.begin() as conn:
        # Create schema if not exists
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.DB_SCHEMA}"))
        await conn.execute(text(f"SET search_path TO {settings.DB_SCHEMA}"))
        
        # Create tables if they don't exist
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
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tenants (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id UUID REFERENCES organizations(id),
                type VARCHAR DEFAULT 'M365',
                display_name VARCHAR NOT NULL,
                external_tenant_id VARCHAR,
                subscription_id VARCHAR,
                client_id VARCHAR,
                client_secret_ref VARCHAR,
                status VARCHAR DEFAULT 'PENDING',
                storage_region VARCHAR,
                last_discovery_at TIMESTAMP,
                graph_delta_tokens JSON DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
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
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_roles (
                user_id UUID REFERENCES platform_users(id),
                role VARCHAR,
                PRIMARY KEY (user_id, role)
            )
        """))
        
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
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS jobs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                type VARCHAR NOT NULL,
                tenant_id UUID REFERENCES tenants(id),
                resource_id UUID REFERENCES resources(id),
                snapshot_id UUID,
                status VARCHAR DEFAULT 'QUEUED',
                priority INTEGER DEFAULT 5,
                progress_pct INTEGER DEFAULT 0,
                result JSON DEFAULT '{}',
                spec JSON DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                resource_id UUID REFERENCES resources(id),
                job_id UUID REFERENCES jobs(id),
                type VARCHAR DEFAULT 'INCREMENTAL',
                status VARCHAR DEFAULT 'COMPLETED',
                item_count INTEGER DEFAULT 0,
                bytes_total BIGINT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS snapshot_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                snapshot_id UUID REFERENCES snapshots(id),
                tenant_id UUID REFERENCES tenants(id),
                external_id VARCHAR NOT NULL,
                item_type VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                folder_path VARCHAR,
                content_size BIGINT DEFAULT 0,
                metadata JSON DEFAULT '{}',
                is_deleted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS alerts (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tenant_id UUID REFERENCES tenants(id),
                org_id UUID REFERENCES organizations(id),
                type VARCHAR NOT NULL,
                severity VARCHAR DEFAULT 'MEDIUM',
                message TEXT NOT NULL,
                resolved BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS access_groups (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id UUID REFERENCES organizations(id),
                tenant_id UUID REFERENCES tenants(id),
                name VARCHAR NOT NULL,
                description VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

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

        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    await engine.dispose()
