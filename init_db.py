"""
Database initialization script for Railway deployment.
Creates enum types and tables if they don't exist.
Run: python init_db.py
"""
import asyncio
import sys
sys.path.insert(0, ".")

from sqlalchemy import text
from shared.database import engine, async_session_factory


async def init_db():
    async with engine.begin() as conn:
        print("[INIT_DB] Creating enum types...")

        # Create enum types (idempotent)
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
                CREATE TYPE resourcetype AS ENUM ('MAILBOX', 'SHARED_MAILBOX', 'ROOM_MAILBOX', 'ONEDRIVE', 'SHAREPOINT_SITE', 'TEAMS_CHANNEL', 'TEAMS_CHAT', 'ENTRA_USER', 'ENTRA_GROUP', 'ENTRA_APP', 'ENTRA_DEVICE', 'AZURE_VM', 'AZURE_SQL_DB', 'AZURE_POSTGRESQL', 'RESOURCE_GROUP', 'DYNAMIC_GROUP', 'POWER_BI', 'POWER_APPS', 'POWER_AUTOMATE', 'POWER_DLP', 'COPILOT', 'PLANNER', 'TODO', 'ONENOTE');
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
                CREATE TYPE snapshotstatus AS ENUM ('IN_PROGRESS', 'COMPLETED', 'FAILED', 'PENDING_DELETION');
            EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        ]

        for stmt in enum_statements:
            await conn.execute(text(stmt))

        print("[INIT_DB] Creating tables...")

        # Import models to trigger Base.metadata.create_all
        from shared.models import (
            Organization, Tenant, PlatformUser, UserRoleMapping,
            Resource, SlaPolicy, Job, Snapshot, SnapshotItem,
            JobLog, Alert, AccessGroup, AuditEvent
        )
        from shared.database import Base

        await conn.run_sync(Base.metadata.create_all)

        # Convert existing VARCHAR columns to enum types (idempotent)
        print("[INIT_DB] Converting VARCHAR columns to enum types...")
        alter_statements = [
            """ALTER TABLE tenants ALTER COLUMN type TYPE tenanttype USING type::tenanttype;""",
            """ALTER TABLE tenants ALTER COLUMN status TYPE tenantstatus USING status::tenantstatus;""",
            """ALTER TABLE resources ALTER COLUMN type TYPE resourcetype USING type::resourcetype;""",
            """ALTER TABLE resources ALTER COLUMN status TYPE resourcestatus USING status::resourcestatus;""",
            """ALTER TABLE jobs ALTER COLUMN type TYPE jobtype USING type::jobtype;""",
            """ALTER TABLE jobs ALTER COLUMN status TYPE jobstatus USING status::jobstatus;""",
            """ALTER TABLE snapshots ALTER COLUMN type TYPE snapshottype USING type::snapshottype;""",
            """ALTER TABLE snapshots ALTER COLUMN status TYPE snapshotstatus USING status::snapshotstatus;""",
            """ALTER TABLE user_roles ALTER COLUMN role TYPE userrole USING role::userrole;""",
        ]

        for stmt in alter_statements:
            try:
                await conn.execute(text(stmt))
            except Exception as e:
                # Column might already be enum type, skip
                print(f"[INIT_DB]   SKIP: {e}")

        print("[INIT_DB] Database initialization complete!")


if __name__ == "__main__":
    asyncio.run(init_db())
