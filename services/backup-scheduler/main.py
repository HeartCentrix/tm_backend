"""
Backup Scheduler Service - SLA Policy Frequency-Based Scheduling
Port: 8008

Responsibilities:
- Scan active resources grouped by their assigned SLA policy
- Schedule backup jobs dynamically based on each policy's frequency field
- Filter resources by SLA policy backup flags (backup_exchange, backup_onedrive, etc.)
- Group resources by type and tenant for batch processing
- Dispatch mass backup jobs to RabbitMQ queues
- Track SLA compliance and violations
"""
import asyncio
import os
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
from fastapi import FastAPI, BackgroundTasks
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from shared.database import async_session_factory
from shared.models import (
    Resource, SlaPolicy, Tenant, Job, Organization, Snapshot,
    ResourceType, ResourceStatus, JobType, JobStatus, SnapshotType, TenantStatus, TenantType
)
from shared.message_bus import (
    message_bus,
    create_mass_backup_message,
    create_backup_message,
    create_audit_event_message,
)
from shared.config import settings
from shared.power_bi_client import PowerBIClient

app = FastAPI(title="Backup Scheduler Service", version="3.0.0")

# APScheduler for dynamic SLA policy scheduling
scheduler = AsyncIOScheduler()

# Resource type to SLA flag mapping — determines which workloads a policy backs up
RESOURCE_TYPE_TO_SLA_FLAG: Dict[str, str] = {
    "MAILBOX": "backup_exchange",
    "SHARED_MAILBOX": "backup_exchange",
    "ROOM_MAILBOX": "backup_exchange",
    "ONEDRIVE": "backup_onedrive",
    "SHAREPOINT_SITE": "backup_sharepoint",
    "TEAMS_CHANNEL": "backup_teams",
    # TEAMS_CHAT rows stay in the catalog as the user-facing entity (UI,
    # restore-by-chat), but are excluded from job dispatch — see
    # SCHEDULER_IGNORED_TYPES below. Actual chat backup runs against the
    # per-user TEAMS_CHAT_EXPORT shard emitted by discovery.
    "TEAMS_CHAT": "backup_teams_chats",
    "TEAMS_CHAT_EXPORT": "backup_teams_chats",
    "ENTRA_USER": "backup_entra_id",
    "ENTRA_GROUP": "backup_entra_id",
    "ENTRA_APP": "backup_entra_id",
    "ENTRA_SERVICE_PRINCIPAL": "backup_entra_id",
    "ENTRA_DEVICE": "backup_entra_id",
    "ENTRA_ROLE": "backup_entra_id",
    "ENTRA_ADMIN_UNIT": "backup_entra_id",
    "ENTRA_AUDIT_LOG": "backup_entra_id",
    "INTUNE_MANAGED_DEVICE": "backup_entra_id",
    "POWER_BI": "backup_power_platform",
    "POWER_APPS": "backup_power_platform",
    "POWER_AUTOMATE": "backup_power_platform",
    "POWER_DLP": "backup_power_platform",
    "COPILOT": "backup_copilot",
    "PLANNER": "planner",
    "TODO": "tasks",
    "ONENOTE": "backup_onedrive",
    "AZURE_VM": "backup_azure_vm",
    "AZURE_SQL_DB": "backup_azure_sql",
    "AZURE_POSTGRESQL": "backup_azure_postgresql",
    "AZURE_POSTGRESQL_SINGLE": "backup_azure_postgresql",
}

RESOURCE_TYPE_DISPLAY_NAMES: Dict[str, str] = {
    "MAILBOX": "Exchange mailboxes",
    "SHARED_MAILBOX": "shared mailboxes",
    "ROOM_MAILBOX": "room mailboxes",
    "ONEDRIVE": "OneDrive",
    "SHAREPOINT_SITE": "SharePoint",
    "TEAMS_CHANNEL": "Teams channel data",
    "TEAMS_CHAT": "Teams chats",
    "TEAMS_CHAT_EXPORT": "Teams chat exports",
    "ENTRA_USER": "Entra user data",
    "ENTRA_GROUP": "Entra group data",
    "ENTRA_APP": "Entra app data",
    "ENTRA_SERVICE_PRINCIPAL": "Entra service principal data",
    "ENTRA_DEVICE": "Entra device data",
    "ENTRA_ROLE": "Entra role data",
    "ENTRA_ADMIN_UNIT": "Entra administrative unit data",
    "ENTRA_AUDIT_LOG": "Entra audit data",
    "INTUNE_MANAGED_DEVICE": "Intune managed devices",
    "POWER_BI": "Power BI",
    "POWER_APPS": "Power Apps",
    "POWER_AUTOMATE": "Power Automate",
    "POWER_DLP": "Power DLP",
    "COPILOT": "Copilot",
    "PLANNER": "Planner",
    "TODO": "Microsoft To Do",
    "ONENOTE": "OneNote",
    "AZURE_VM": "Azure virtual machines",
    "AZURE_SQL_DB": "Azure SQL databases",
    "AZURE_POSTGRESQL": "Azure PostgreSQL",
    "AZURE_POSTGRESQL_SINGLE": "Azure PostgreSQL",
}

SLA_FLAG_DISPLAY_NAMES: Dict[str, str] = {
    "backup_exchange": "Exchange",
    "backup_onedrive": "OneDrive and OneNote",
    "backup_sharepoint": "SharePoint",
    "backup_teams": "Teams channels",
    "backup_teams_chats": "Teams chats",
    "backup_entra_id": "Entra ID",
    "backup_power_platform": "Power Platform",
    "backup_copilot": "Copilot",
    "planner": "Planner",
    "tasks": "Tasks",
    "group_mailbox": "group mailbox",
    "backup_azure_vm": "Virtual machines",
    "backup_azure_sql": "Azure SQL databases",
    "backup_azure_postgresql": "Azure PostgreSQL servers",
}

# AZ-4: Azure workload queue routing
AZURE_WORKLOAD_QUEUES = {
    ResourceType.AZURE_VM: "azure.vm",
    ResourceType.AZURE_SQL_DB: "azure.sql",
    ResourceType.AZURE_POSTGRESQL: "azure.postgres",
    ResourceType.AZURE_POSTGRESQL_SINGLE: "azure.postgres",
}


# Valid APScheduler day-of-week abbreviations
DAY_MAP = {
    "MON": "mon", "TUE": "tue", "WED": "wed", "THU": "thu",
    "FRI": "fri", "SAT": "sat", "SUN": "sun",
}


def _parse_window_start(window_start: str | None) -> tuple[int, int]:
    """Parse 'HH:MM' (or 'HHMM') from policy.backup_window_start. Returns
    (hour, minute) UTC. Falls back to (2, 0) — afi default 02:00 — on any
    parse failure so a malformed value never breaks the scheduler."""
    if not window_start:
        return (2, 0)
    try:
        s = window_start.strip()
        if ":" in s:
            h, m = s.split(":", 1)
            return (int(h) % 24, int(m) % 60)
        if len(s) >= 3 and s.isdigit():  # "0830"
            return (int(s[:-2]) % 24, int(s[-2:]) % 60)
    except Exception:
        pass
    return (2, 0)


def _policy_minute_jitter(policy_id: str | None) -> int:
    """Deterministic minute offset (0–54) for a given policy ID.

    R3.1 — without jitter, every DAILY policy fires at HH:00:00 and a
    100-tenant deployment thundering-herds the worker pool + Graph API at
    the same moment. A stable per-policy offset spreads load across the
    hour while keeping the schedule predictable for the same policy across
    deploys (deterministic from the ID, not random)."""
    if not policy_id:
        return 0
    import hashlib
    h = hashlib.md5(str(policy_id).encode()).digest()
    return h[0] % 55  # 0..54 — leaves the last 5 minutes of the hour clear


def frequency_to_cron_params(
    frequency: str,
    backup_days: list[str] | None = None,
    window_start: str | None = None,
    policy_id: str | None = None,
):
    """Convert SLA policy frequency + window into APScheduler cron parameters.

    afi-parity behaviour:
      THREE_DAILY — 3x/day, fires at window_start + 0h/+8h/+16h
      DAILY       — 1x/day at window_start (default 02:00 UTC)
      MANUAL      — returns None; caller must skip schedule registration

    Per-policy minute jitter (R3.1) is added to base_minute so concurrent
    policies stagger naturally without operator intervention.

    backup_days restricts to a day-of-week subset (only applied when fewer
    than 7 days are selected — selecting all 7 is equivalent to no restriction).
    """
    if frequency == "MANUAL":
        return None

    base_hour, base_minute = _parse_window_start(window_start)
    jitter = _policy_minute_jitter(policy_id)
    final_minute = (base_minute + jitter) % 60
    # Only roll forward an hour if jitter pushes us past 60min
    hour_carry = (base_minute + jitter) // 60

    # Day-of-week restriction
    dow_clause: dict = {}
    if backup_days:
        aps_days = [DAY_MAP.get(d.upper(), d.lower()) for d in backup_days if DAY_MAP.get(d.upper())]
        if aps_days and len(aps_days) < 7:
            dow_clause["day_of_week"] = ",".join(aps_days)

    if frequency == "THREE_DAILY":
        # Three windows starting at base_hour, base_hour+8, base_hour+16 (mod 24)
        hours = ",".join(str((base_hour + hour_carry + offset) % 24) for offset in (0, 8, 16))
        return {"trigger": "cron", "hour": hours, "minute": final_minute, **dow_clause}

    # DAILY (and any legacy / unrecognized value)
    return {"trigger": "cron", "hour": (base_hour + hour_carry) % 24, "minute": final_minute, **dow_clause}


# Resource types intentionally excluded from dispatch even when an SLA policy
# would otherwise cover them. TEAMS_CHAT is here because actual chat-message
# backup runs through TEAMS_CHAT_EXPORT (one delta pull per user, not per chat);
# the TEAMS_CHAT rows remain as the user-facing catalog entity for restore.
SCHEDULER_IGNORED_TYPES: set[str] = {"TEAMS_CHAT"}


def resource_type_enabled(resource_type: str, policy: SlaPolicy) -> bool:
    """Check if a resource type is enabled in the SLA policy's backup flags."""
    if resource_type in SCHEDULER_IGNORED_TYPES:
        return False

    if resource_type == "ENTRA_USER":
        return bool(
            getattr(policy, "backup_entra_id", False)
            or getattr(policy, "contacts", False)
            or getattr(policy, "calendars", False)
        )

    if resource_type in {"ENTRA_GROUP", "DYNAMIC_GROUP"}:
        return bool(
            getattr(policy, "backup_entra_id", False)
            or getattr(policy, "group_mailbox", False)
        )

    flag_name = RESOURCE_TYPE_TO_SLA_FLAG.get(resource_type)
    if not flag_name:
        # Unknown or unsupported resource types should not be scheduled automatically.
        return False
    return getattr(policy, flag_name, True)


def build_sla_skip_message(resource_type: str, policy: SlaPolicy) -> tuple[str, str | None, str | None]:
    workload_name = RESOURCE_TYPE_DISPLAY_NAMES.get(resource_type, resource_type.replace("_", " ").title())
    flag_name = RESOURCE_TYPE_TO_SLA_FLAG.get(resource_type)
    flag_label = SLA_FLAG_DISPLAY_NAMES.get(flag_name or "", flag_name)

    if flag_label:
        message = (
            f"Scheduled backup skipped because SLA '{policy.name}' does not cover {workload_name}. "
            f"Enable '{flag_label}' in the policy to include this resource."
        )
    else:
        message = (
            f"Scheduled backup skipped because SLA '{policy.name}' has no workload mapping for {workload_name}."
        )
    return message, flag_name, flag_label


async def publish_sla_skip_audit_events(policy: SlaPolicy, skipped_resources: List[Resource]):
    """Emit warning audit events for resources skipped by SLA coverage filters."""
    if not skipped_resources:
        return

    for resource in skipped_resources:
        resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
        message, flag_name, flag_label = build_sla_skip_message(resource_type, policy)
        audit_message = create_audit_event_message(
            action="BACKUP_SKIPPED_SLA_SCOPE",
            tenant_id=str(resource.tenant_id),
            actor_type="SYSTEM",
            resource_id=str(resource.id),
            resource_type=resource_type,
            resource_name=resource.display_name,
            outcome="PARTIAL",
            details={
                "message": message,
                "skip_reason": "sla_scope_mismatch",
                "policy_id": str(policy.id),
                "policy_name": policy.name,
                "required_flag": flag_name,
                "required_flag_label": flag_label,
                "resource_status": resource.status.value if hasattr(resource.status, "value") else str(resource.status),
                "source": "backup_scheduler",
            },
        )
        await message_bus.publish("audit.events", audit_message, priority=3)


@app.on_event("startup")
async def startup():
    """Initialize services on startup and schedule jobs per SLA policy"""
    # Auto-create schema and tables if they don't exist
    from shared.database import init_db as db_init_db
    await db_init_db()

    await message_bus.connect()

    # Dynamically schedule backup jobs for each active SLA policy
    await schedule_all_policies()

    # Schedule SLA violation check every 30 minutes
    scheduler.add_job(check_sla_violations, "interval", minutes=30)

    # Schedule pre-emptive backup check every 15 minutes (ransomware/anomaly detection)
    scheduler.add_job(check_preemptive_backup_triggers, "interval", minutes=15)

    # Schedule M365 audit log ingestion every hour
    scheduler.add_job(ingest_m365_audit_logs, "interval", hours=1)

    # AZ-0: Schedule lifecycle policy reconciler (daily)
    scheduler.add_job(reconcile_lifecycle_policies, "interval", hours=24)

    # AZ-4: Schedule DR setup reconciler (every 6 hours)
    scheduler.add_job(reconcile_dr_setup, "interval", hours=6)

    # Phase 2: Retention cleanup — FLAT/GFS snapshot pruning per SLA policy (daily)
    scheduler.add_job(run_retention_cleanup, "interval", hours=24)

    # Task 26: Delete orphaned export ZIPs older than 1 day (3am UTC daily).
    # Primary mechanism is Azure lifecycle rule (ops/azure-lifecycle-exports.json);
    # this is a fallback for envs without lifecycle API access (local Azurite, restricted tenants).
    async def _exports_cleanup_daily():
        try:
            from services.exports_cleanup import cleanup_exports
            from shared.azure_storage import azure_storage_manager
            shard = azure_storage_manager.get_default_shard()
            deleted = await cleanup_exports(shard=shard, container="exports")
            print(f"[backup-scheduler] exports_cleanup: deleted {len(deleted)} blobs")
        except Exception as exc:
            print(f"[backup-scheduler] exports_cleanup failed: {exc}")

    scheduler.add_job(_exports_cleanup_daily, "cron", hour=3, minute=0, timezone="UTC", id="exports_cleanup_daily")

    # Round 1.5 — daily retry of FAILED snapshots (one shot, throttled).
    scheduler.add_job(retry_failed_snapshots, "interval", hours=24)

    # R3.2 — daily backup integrity sample (random snapshots, hash check).
    scheduler.add_job(run_backup_verification, "interval", hours=24)

    # Round 1.5 — DLQ consumer (poison-message alerting). Runs as a background
    # task, NOT an APScheduler job; consumes continuously from backup.*.dlq.
    asyncio.create_task(consume_backup_dlq())

    # Schedule daily backup report (email/Slack/Teams)
    scheduler.add_job(send_daily_backup_report, "cron", hour=8, minute=0, timezone="UTC")

    # Schedule weekly summary report (Monday 9am UTC)
    scheduler.add_job(send_weekly_summary_report, "cron", hour=9, minute=0, day_of_week="mon", timezone="UTC")

    scheduler.start()

    # Start discovery reconciler (runs every 5 min, independent of APScheduler)
    await start_reconciler_loop()


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown"""
    scheduler.shutdown()
    await message_bus.disconnect()


async def schedule_all_policies():
    """Scan all active SLA policies and schedule backup jobs for each one"""
    async with async_session_factory() as session:
        policies_result = await session.execute(
            select(SlaPolicy).where(SlaPolicy.enabled == True)
        )
        policies = policies_result.scalars().all()

    scheduled_count = 0
    for policy in policies:
        try:
            await schedule_policy_job(policy)
            scheduled_count += 1
        except Exception as e:
            print(f"[SCHEDULER] Failed to schedule job for policy {policy.name} ({policy.id}): {e}")

    print(f"[SCHEDULER] Scheduled backup jobs for {scheduled_count} SLA policies")


async def schedule_policy_job(policy: SlaPolicy):
    """Schedule or reschedule a backup job for a specific SLA policy.

    MANUAL policies are intentionally never scheduled — they only run when
    explicitly triggered via the /trigger endpoint. afi behaves identically."""
    job_id = f"policy_backup_{policy.id}"

    # Remove existing job if it exists (covers reschedule + frequency change to MANUAL)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    cron_params = frequency_to_cron_params(
        policy.frequency, policy.backup_days, policy.backup_window_start,
        policy_id=str(policy.id),
    )
    if cron_params is None:
        print(f"[SCHEDULER] Skipping '{policy.name}' (MANUAL policy — admin-triggered only)")
        return

    scheduler.add_job(
        dispatch_policy_backups,
        args=[str(policy.id)],
        id=job_id,
        name=f"Backup: {policy.name}",
        replace_existing=True,
        timezone="UTC",
        **cron_params,
    )

    days_info = f" days={policy.backup_days}" if policy.backup_days and len(policy.backup_days) < 7 else ""
    print(f"[SCHEDULER] Scheduled '{policy.name}' ({policy.frequency}{days_info}) -> job_id={job_id}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "backup-scheduler"}


@app.post("/scheduler/policy/{policy_id}/trigger")
async def trigger_policy_backup(policy_id: str, background_tasks: BackgroundTasks):
    """Manually trigger backup dispatch for a specific SLA policy"""
    background_tasks.add_task(dispatch_policy_backups, policy_id)
    return {"status": "scheduled", "policy_id": policy_id}


@app.post("/scheduler/reschedule-all")
async def reschedule_all_policies(background_tasks: BackgroundTasks):
    """Re-scan all SLA policies and rebuild the scheduler"""
    background_tasks.add_task(schedule_all_policies)
    return {"status": "rescheduling"}


@app.post("/scheduler/resource/{resource_id}")
async def trigger_single_backup(resource_id: str, full_backup: bool = False):
    """Trigger immediate backup for a single resource"""
    async with async_session_factory() as session:
        resource = await session.get(Resource, uuid.UUID(resource_id))
        if not resource:
            return {"error": "Resource not found"}, 404

        # Prevent manual backup on inaccessible resources
        status_val = resource.status.value if hasattr(resource.status, 'value') else str(resource.status)
        if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
            return {
                "error": f"Resource is {status_val} and cannot be backed up. "
                         f"Run discovery first to restore access or remove the resource.",
                "resource_status": status_val,
            }, 422

        job_id = uuid.uuid4()

        # Create job record
        job = Job(
            id=job_id,
            type=JobType.BACKUP,
            tenant_id=resource.tenant_id,
            resource_id=resource.id,
            status=JobStatus.QUEUED,
            priority=1,  # Urgent
            spec={
                "full_backup": full_backup,
                "triggered_by": "MANUAL",
                "snapshot_label": "manual",
            }
        )
        session.add(job)
        await session.commit()

        # Send to urgent queue
        await message_bus.publish(
            "backup.urgent",
            {
                "jobId": str(job_id),
                "resourceId": resource_id,
                "tenantId": str(resource.tenant_id),
                "type": "FULL" if full_backup else "INCREMENTAL",
                "priority": 1,
                "triggeredBy": "MANUAL",
                "snapshotLabel": "manual",
                "forceFullBackup": full_backup,
            },
            priority=1
        )

        return {"status": "queued", "job_id": str(job_id)}


async def dispatch_policy_backups(policy_id: str):
    """
    Dispatch backups for a specific SLA policy, filtering resources by the policy's backup flags.

    The policy_id comes from the resource's sla_policy_id assignment.

    Strategy:
    1. Fetch the SLA policy
    2. Fetch all active resources assigned to this policy
    3. Filter resources to only include types enabled in the policy's backup flags
    4. Group by resource_type + tenant_id
    5. Split into batches of 1000 resources
    6. Dispatch batches to RabbitMQ
    """
    print(f"[SCHEDULER] Starting backup dispatch for policy {policy_id}")

    async with async_session_factory() as session:
        # Fetch the SLA policy
        policy = await session.get(SlaPolicy, uuid.UUID(policy_id))
        if not policy:
            print(f"[SCHEDULER] Policy {policy_id} not found")
            return

        if not policy.enabled:
            print(f"[SCHEDULER] Policy {policy.name} is disabled, skipping")
            return

        print(f"[SCHEDULER] Processing policy '{policy.name}' (frequency={policy.frequency})")

        # Fetch all active resources assigned to this policy
        resources_result = await session.execute(
            select(Resource).where(
                and_(
                    Resource.sla_policy_id == policy.id,
                    Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
                )
            ).options(selectinload(Resource.tenant))
        )
        all_resources = resources_result.scalars().all()

        if not all_resources:
            print(f"[SCHEDULER] No active resources for policy '{policy.name}'")
            return

        # Filter resources by the policy's backup flags
        enabled_resources = []
        skipped_resources = []
        for resource in all_resources:
            resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
            if resource_type_enabled(resource_type, policy):
                enabled_resources.append(resource)
            else:
                skipped_resources.append(resource)

        skipped = len(skipped_resources)
        if skipped > 0:
            print(f"[SCHEDULER] Filtered out {skipped} resources (disabled by policy flags)")
            await publish_sla_skip_audit_events(policy, skipped_resources)

        if not enabled_resources:
            print(f"[SCHEDULER] No enabled resources for policy '{policy.name}' after flag filtering")
            return

        # R2.2 — honor max_concurrent_backups. Count Jobs already in flight for
        # this policy (QUEUED + RUNNING) and cap how many new resources we
        # publish this tick. The remainder will be picked up by the next
        # scheduled trigger — we'd rather spread load across cycles than
        # overload Graph and induce 429 throttling.
        max_concurrent = getattr(policy, "max_concurrent_backups", None) or 0
        if max_concurrent > 0:
            inflight_stmt = (
                select(func.count(Job.id)).where(and_(
                    Job.spec["sla_policy_id"].astext == str(policy.id),
                    Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]),
                ))
            )
            inflight = (await session.execute(inflight_stmt)).scalar() or 0
            budget = max(0, max_concurrent - inflight)
            if budget == 0:
                print(f"[SCHEDULER] Policy '{policy.name}' at concurrency cap "
                      f"({inflight}/{max_concurrent} in-flight) — deferring this tick")
                return
            if budget < len(enabled_resources):
                print(f"[SCHEDULER] Policy '{policy.name}' concurrency cap "
                      f"({inflight}/{max_concurrent} in-flight) — dispatching {budget} of {len(enabled_resources)} resources")
                # Prefer the resources with the OLDEST last_backup — they're the
                # most behind on SLA. None means never backed up → highest priority.
                enabled_resources.sort(
                    key=lambda r: (r.last_backup_at or datetime.min)
                )
                enabled_resources = enabled_resources[:budget]

        power_bi_resources = [resource for resource in enabled_resources if resource.type == ResourceType.POWER_BI]
        if power_bi_resources:
            tenant_result = await session.execute(
                select(Tenant).where(Tenant.id.in_({resource.tenant_id for resource in power_bi_resources}))
            )
            tenants_map = {tenant.id: tenant for tenant in tenant_result.scalars().all()}

            filtered_power_bi_ids = set()
            resources_by_tenant: Dict[str, List[Resource]] = {}
            for resource in power_bi_resources:
                resources_by_tenant.setdefault(str(resource.tenant_id), []).append(resource)

            for tenant_id, tenant_resources in resources_by_tenant.items():
                tenant = tenants_map.get(uuid.UUID(tenant_id))
                if not tenant:
                    filtered_power_bi_ids.update(str(resource.id) for resource in tenant_resources)
                    continue

                resources_without_backup = [resource for resource in tenant_resources if resource.last_backup_at is None]
                filtered_power_bi_ids.update(str(resource.id) for resource in resources_without_backup)

                resources_with_backup = [resource for resource in tenant_resources if resource.last_backup_at is not None]
                if not resources_with_backup:
                    continue

                min_last_backup = min(resource.last_backup_at for resource in resources_with_backup if resource.last_backup_at)
                modified_since = max(min_last_backup, datetime.utcnow() - timedelta(days=30))
                if modified_since > datetime.utcnow() - timedelta(minutes=31):
                    modified_since = datetime.utcnow() - timedelta(minutes=31)

                try:
                    client = PowerBIClient(
                        tenant.external_tenant_id or settings.EFFECTIVE_POWER_BI_TENANT_ID,
                        refresh_token=PowerBIClient.get_refresh_token_from_tenant(tenant),
                    )
                    modified_workspace_ids = set(await client.list_modified_workspace_ids(modified_since))
                    for resource in resources_with_backup:
                        workspace_id = (resource.extra_data or {}).get("workspace_id")
                        if not workspace_id and resource.external_id and resource.external_id.startswith("pbi_ws_"):
                            workspace_id = resource.external_id.replace("pbi_ws_", "", 1)
                        if workspace_id in modified_workspace_ids:
                            filtered_power_bi_ids.add(str(resource.id))
                except Exception as exc:
                    print(f"[SCHEDULER] Power BI modified-workspace prefilter unavailable for tenant {tenant.display_name}: {exc}")
                    filtered_power_bi_ids.update(str(resource.id) for resource in tenant_resources)

            enabled_resources = [
                resource for resource in enabled_resources
                if resource.type != ResourceType.POWER_BI or str(resource.id) in filtered_power_bi_ids
            ]

        print(f"[SCHEDULER] Found {len(enabled_resources)} resources to backup for policy '{policy.name}'")

        # Group by resource type + tenant
        groups: Dict[str, List[Resource]] = {}
        for resource in enabled_resources:
            group_key = f"{resource.type.value}:{resource.tenant_id}"
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(resource)

        print(f"[SCHEDULER] Grouped into {len(groups)} resource type + tenant combinations")

        # Determine queue based on frequency (frequent = higher priority queue)
        queue = "backup.high" if policy.frequency == "THREE_DAILY" else "backup.normal"

        # Dispatch batches
        total_dispatched = 0
        BATCH_SIZE = 1000

        for group_key, group_resources in groups.items():
            resource_type, tenant_id = group_key.split(":")

            # AZ-4: Route Azure workload resources to their dedicated queues
            azure_queue = None
            try:
                rt = ResourceType(resource_type)
                azure_queue = AZURE_WORKLOAD_QUEUES.get(rt)
            except ValueError:
                pass

            group_queue = azure_queue or ("backup.high" if policy.frequency == "THREE_DAILY" else "backup.normal")
            if azure_queue:
                print(f"[SCHEDULER] Azure workload {resource_type} → queue {azure_queue} (not backup.*)")

            # Split into batches
            for i in range(0, len(group_resources), BATCH_SIZE):
                batch = group_resources[i:i + BATCH_SIZE]
                resource_ids = [str(r.id) for r in batch]

                # Determine fullBackup: True only if NONE of the resources have been backed up before
                has_previous_backup = any(r.last_backup_at is not None for r in batch)
                effective_full_backup = not has_previous_backup

                # Create job record for tracking
                job_id = uuid.uuid4()
                job = Job(
                    id=job_id,
                    type=JobType.BACKUP,
                    tenant_id=uuid.UUID(tenant_id),
                    batch_resource_ids=[uuid.UUID(rid) for rid in resource_ids],
                    status=JobStatus.QUEUED,
                    priority=5,
                    spec={
                        "sla_policy_id": str(policy.id),
                        "sla_policy_name": policy.name,
                        "resource_type": resource_type,
                        "batch_size": len(resource_ids),
                        "triggered_by": "SCHEDULED",
                        "snapshot_label": "scheduled",
                        "fullBackup": effective_full_backup,
                    }
                )
                session.add(job)

                # Create mass backup message
                message = create_mass_backup_message(
                    job_id=str(job_id),
                    tenant_id=tenant_id,
                    resource_type=resource_type,
                    resource_ids=resource_ids,
                    sla_policy_id=str(policy.id),
                    full_backup=effective_full_backup,
                )

                # Send to queue
                await message_bus.publish(
                    group_queue,
                    message,
                    priority=message["priority"],
                )

                total_dispatched += len(resource_ids)

                # Stagger dispatch to prevent overwhelming Graph API
                await asyncio.sleep(0.1)

        await session.commit()

        print(f"[SCHEDULER] Dispatched {total_dispatched} resources to {queue} for policy '{policy.name}'")


async def check_sla_violations():
    """
    Check for SLA violations - resources that missed their backup window
    """
    print("[SLA] Checking for SLA violations...")
    
    async with async_session_factory() as session:
        # Find resources with last_backup_at older than their SLA window
        now = datetime.utcnow()
        
        # Get all active resources with SLA policies
        resources_result = await session.execute(
            select(Resource, SlaPolicy).join(
                SlaPolicy, Resource.sla_policy_id == SlaPolicy.id, isouter=True
            ).where(
                and_(
                    Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
                    Resource.sla_policy_id.isnot(None),
                    SlaPolicy.enabled == True,
                    SlaPolicy.sla_violation_alert == True
                )
            )
        )
        
        violations = []
        for resource, policy in resources_result.all():
            if not policy:
                continue
            
            # Calculate expected backup frequency
            frequency_hours = {
                "THREE_DAILY": 8,  # Every 8 hours
                "DAILY": 24,
                "WEEKLY": 168,
            }.get(policy.frequency, 24)
            
            # Check if backup is overdue
            if resource.last_backup_at:
                hours_since_backup = (now - resource.last_backup_at).total_seconds() / 3600
                
                if hours_since_backup > frequency_hours:
                    severity = "CRITICAL" if hours_since_backup > frequency_hours * 2 else "WARNING"
                    
                    violations.append({
                        "resource_id": str(resource.id),
                        "tenant_id": str(resource.tenant_id),
                        "resource_type": resource.type.value,
                        "sla_policy_id": str(policy.id),
                        "sla_policy_name": policy.name,
                        "last_backup_at": resource.last_backup_at.isoformat(),
                        "hours_overdue": hours_since_backup - frequency_hours,
                        "severity": severity,
                    })
                    
                    # Log violation (in production: send to alert service)
                    print(f"[SLA VIOLATION] {severity}: Resource {resource.id} ({resource.type.value}) "
                          f"last backed up {hours_since_backup:.1f}h ago (SLA: {frequency_hours}h)")
        
        if violations:
            print(f"[SLA] Found {len(violations)} violations")
            # Send violations to alert-service via HTTP
            await send_violations_to_alert_service(violations)
        else:
            print("[SLA] No violations detected")


# ==================== Pre-emptive Backup Triggers ====================

async def check_preemptive_backup_triggers():
    """Consume unresolved BACKUP_ANOMALY alerts emitted by backup-worker's
    per-resource anomaly detector and fire a per-resource preemptive backup
    for each one. R2.3 — single signal source, no duplicate scoring.

    Flow:
      1. backup-worker._check_snapshot_anomaly raises BACKUP_ANOMALY Alert
         after each completed snapshot (compares item_count vs rolling avg).
      2. THIS job (every 15min) picks up unresolved alerts from the last
         hour and triggers an urgent backup of just that resource.
      3. Marks each alert resolved with a note pointing to the trigger job.

    Window of 1h prevents repeatedly re-acting on the same alert if the
    preemptive backup itself takes longer than one tick to start."""
    from shared.models import Alert
    print("[PREEMPTIVE] Checking unresolved BACKUP_ANOMALY alerts...")

    async with async_session_factory() as session:
        cutoff = datetime.utcnow() - timedelta(hours=1)
        stmt = (
            select(Alert).where(and_(
                Alert.type == "BACKUP_ANOMALY",
                Alert.resolved.is_(False),
                Alert.created_at >= cutoff,
            )).order_by(Alert.created_at.asc())
        )
        alerts = (await session.execute(stmt)).scalars().all()
        if not alerts:
            print("[PREEMPTIVE] No unresolved anomaly alerts in last hour")
            return

        triggered = 0
        skipped = 0
        for alert in alerts:
            if not alert.resource_id:
                # Tenant-level alerts don't have a single resource to back up.
                # We could expand to all tenant resources but that's wasteful;
                # log + resolve so it doesn't keep getting picked up.
                alert.resolved = True
                alert.resolution_note = "no resource_id attached — skipped"
                alert.resolved_at = datetime.utcnow()
                skipped += 1
                continue

            resource = await session.get(Resource, alert.resource_id)
            if not resource:
                alert.resolved = True
                alert.resolution_note = "resource gone"
                alert.resolved_at = datetime.utcnow()
                skipped += 1
                continue

            try:
                preemptive_job_id = await trigger_preemptive_backup_for_resource(
                    session, resource, reason=f"anomaly_alert={alert.id}",
                )
                alert.resolved = True
                alert.resolution_note = f"triggered preemptive backup job {preemptive_job_id}"
                alert.resolved_at = datetime.utcnow()
                triggered += 1
            except Exception as e:
                print(f"[PREEMPTIVE] failed to trigger backup for resource {resource.id}: {e}")

        await session.commit()
        print(f"[PREEMPTIVE] triggered={triggered}, skipped={skipped}, total_alerts={len(alerts)}")


async def trigger_preemptive_backup_for_resource(
    session: AsyncSession, resource: Resource, reason: str,
) -> str:
    """Per-resource preemptive backup. Publishes a single-resource job to
    backup.urgent at priority 1 — jumps the queue ahead of scheduled work.
    Returns the job ID for audit linkage."""
    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        type=JobType.BACKUP,
        tenant_id=resource.tenant_id,
        batch_resource_ids=[resource.id],
        status=JobStatus.QUEUED,
        priority=1,
        spec={
            "triggered_by": "PREEMPTIVE",
            "reason": reason,
            "resource_type": resource.type.value if hasattr(resource.type, "value") else str(resource.type),
            "preemptive": True,
        },
    )
    session.add(job)
    payload = {
        "jobId": str(job_id),
        "resourceId": str(resource.id),
        "workload": resource.type.value if hasattr(resource.type, "value") else str(resource.type),
        "triggeredBy": "PREEMPTIVE",
        "reason": reason,
    }
    await message_bus.publish("backup.urgent", payload, priority=1)
    print(f"[PREEMPTIVE] queued backup for resource={resource.display_name} ({resource.id}), job={job_id}, reason={reason}")
    return str(job_id)


async def trigger_preemptive_backup(session: AsyncSession, tenant: Tenant, reason: str):
    """Trigger immediate backup for all active resources in a tenant"""
    # Get all active resources for this tenant
    resources_result = await session.execute(
        select(Resource).where(
            and_(
                Resource.tenant_id == tenant.id,
                Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
                Resource.sla_policy_id.isnot(None)
            )
        )
    )
    resources = resources_result.scalars().all()

    if not resources:
        return

    print(f"[PREEMPTIVE] Triggering backup for {len(resources)} resources in tenant {tenant.display_name}")

    # Group by resource type
    groups: Dict[str, List[Resource]] = {}
    for resource in resources:
        rtype = resource.type.value
        if rtype not in groups:
            groups[rtype] = []
        groups[rtype].append(resource)

    # Dispatch backup jobs
    for rtype, rlist in groups.items():
        resource_ids = [str(r.id) for r in rlist]

        # Create job record
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            type=JobType.BACKUP,
            tenant_id=tenant.id,
            batch_resource_ids=[uuid.UUID(rid) for rid in resource_ids],
            status=JobStatus.QUEUED,
            priority=1,  # Urgent
            spec={
                "triggered_by": "PREEMPTIVE",
                "reason": reason,
                "resource_type": rtype,
            }
        )
        session.add(job)

        # Publish to urgent queue
        message = create_mass_backup_message(
            job_id=str(job_id),
            tenant_id=str(tenant.external_tenant_id),
            resource_type=rtype,
            resource_ids=resource_ids,
            sla_policy_id=None,
            full_backup=True,  # Force full backup for preemptive
        )
        message["triggeredBy"] = "PREEMPTIVE"
        message["reason"] = reason

        await message_bus.publish("backup.urgent", message, priority=1)

    await session.commit()


# ==================== M365 Audit Log Ingestion ====================

async def ingest_m365_audit_logs():
    """
    Periodically pull Microsoft 365 audit logs from Graph API
    and store them in the audit_events table.
    Pulls both directory audit logs and sign-in logs for all active tenants.
    """
    print("[AUDIT_INGEST] Starting M365 audit log ingestion...")

    async with async_session_factory() as session:
        # Get all active M365 tenants
        tenants_result = await session.execute(
            select(Tenant).where(
                and_(
                    Tenant.status == TenantStatus.ACTIVE,
                    Tenant.type == TenantType.M365,
                    Tenant.client_id.isnot(None),
                    Tenant.external_tenant_id.isnot(None),
                )
            )
        )
        tenants = tenants_result.scalars().all()

    ingested_total = 0
    audit_service_url = settings.AUDIT_SERVICE_URL

    async with httpx.AsyncClient(timeout=60.0) as client:
        for tenant in tenants:
            for log_type in ["directory", "signin"]:
                try:
                    # Pull last 1 day of logs (running hourly, so this catches any gaps)
                    resp = await client.post(
                        f"{audit_service_url}/api/v1/audit/ingest/graph/{tenant.id}",
                        params={"days": 1, "log_type": log_type},
                    )
                    if resp.status_code == 200:
                        result = resp.json()
                        count = result.get("ingested", 0)
                        ingested_total += count
                        if count > 0:
                            print(f"[AUDIT_INGEST] Ingested {count} {log_type} logs for tenant {tenant.display_name}")
                    else:
                        print(f"[AUDIT_INGEST] Failed to ingest {log_type} for tenant {tenant.display_name}: {resp.status_code}")
                except Exception as e:
                    print(f"[AUDIT_INGEST] Error ingesting {log_type} for tenant {tenant.display_name}: {e}")

    print(f"[AUDIT_INGEST] Completed. Total ingested: {ingested_total} events")


# ==================== Scheduled Reporting ====================

async def send_daily_backup_report():
    """Trigger daily backup report generation via report-service"""
    print("[REPORT] Triggering daily backup report...")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{settings.REPORT_SERVICE_URL}/api/v1/reports/generate",
                json={"report_type": "DAILY"}
                )
            if response.status_code == 200:
                print(f"[REPORT] Daily report triggered successfully")
            else:
                print(f"[REPORT] Failed to trigger daily report: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[REPORT] Error triggering daily report: {e}")


async def send_weekly_summary_report():
    """Trigger weekly backup report generation via report-service"""
    print("[REPORT] Triggering weekly summary report...")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{settings.REPORT_SERVICE_URL}/api/v1/reports/generate",
                json={"report_type": "WEEKLY"}
            )
            if response.status_code == 200:
                print(f"[REPORT] Weekly report triggered successfully")
            else:
                print(f"[REPORT] Failed to trigger weekly report: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[REPORT] Error triggering weekly report: {e}")


async def send_violations_to_alert_service(violations: List[Dict]):
    """Send SLA violations to alert-service"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            for violation in violations:
                await client.post(f"{settings.ALERT_SERVICE_URL}/api/v1/alerts", json={
                    "type": "SLA_VIOLATION",
                    "severity": violation.get("severity", "WARNING"),
                    "message": f"SLA violation: {violation.get('resource_type')} resource overdue by "
                               f"{violation.get('hours_overdue', 0):.1f} hours",
                    "tenant_id": violation.get("tenant_id"),
                    "resource_id": violation.get("resource_id"),
                    "resource_type": violation.get("resource_type"),
                    "details": violation,
                })
    except Exception as e:
        print(f"[SLA] Failed to send violations to alert-service: {e}")


async def reconcile_pending_discovery():
    """
    Reconciler job: runs every 5 minutes.
    Finds tenants stuck in PENDING_DISCOVERY for >10 minutes and re-enqueues discovery.
    """
    from shared.message_bus import message_bus
    from shared.models import TenantStatus

    try:
        cutoff = datetime.utcnow() - timedelta(minutes=10)
        async with async_session_factory() as session:
            stmt = select(Tenant).where(
                Tenant.status == TenantStatus.PENDING_DISCOVERY,
                Tenant.updated_at < cutoff,
            )
            result = await session.execute(stmt)
            pending_tenants = result.scalars().all()

            if not pending_tenants:
                return

            for tenant in pending_tenants:
                try:
                    print(f"[RECONCILER] Re-enqueueing discovery for tenant {tenant.id} ({tenant.display_name})")
                    if not message_bus.connection:
                        await message_bus.connect()
                    await message_bus.publish("discovery.m365", {
                        "jobId": str(uuid.uuid4()),
                        "tenantId": str(tenant.id),
                        "externalTenantId": tenant.external_tenant_id,
                        "discoveryScope": ["users", "groups", "mailboxes", "shared_mailboxes",
                                           "onedrive", "sharepoint", "teams"],
                        "triggeredBy": "RECONCILER",
                        "triggeredAt": datetime.utcnow().isoformat(),
                    }, priority=5)
                    tenant.status = TenantStatus.DISCOVERING
                    await session.commit()
                    print(f"[RECONCILER] Discovery re-enqueued for tenant {tenant.id}")
                except Exception as e:
                    print(f"[RECONCILER] Failed to re-enqueue discovery for tenant {tenant.id}: {e}")
                    await session.rollback()
    except Exception as e:
        print(f"[RECONCILER] Reconciler error: {e}")


async def start_reconciler_loop():
    """Start reconciler background task (runs every 5 minutes)."""
    async def _reconciler():
        while True:
            await asyncio.sleep(300)  # 5 minutes
            try:
                await reconcile_pending_discovery()
            except Exception as e:
                print(f"[RECONCILER] Reconciler loop error: {e}")

    asyncio.create_task(_reconciler())


# ── R3.2: Backup verification cron ──

# Default sample size — small enough that a daily run is cheap on storage
# (each verify = one blob download), large enough that a corrupt backup gets
# noticed within ~weeks even if it lives in a low-priority tenant.
BACKUP_VERIFY_SAMPLE_SIZE = int(os.environ.get("BACKUP_VERIFY_SAMPLE_SIZE", "50"))
BACKUP_VERIFY_LOOKBACK_HOURS = int(os.environ.get("BACKUP_VERIFY_LOOKBACK_HOURS", "24"))


async def run_backup_verification():
    """Sample-and-verify recent snapshots: download a random SnapshotItem's
    blob and compare its SHA256 against the captured `content_checksum`.
    Mismatches raise a `BACKUP_VERIFICATION_FAILED` Alert.

    afi claims data is "fingerprinted for integrity verification" but doesn't
    document when it's actually checked. We do it daily on a random sample —
    cheap enough that we can run it on every deploy, large enough that
    silent corruption gets flagged within weeks of occurring."""
    import hashlib as _hashlib
    import random
    from shared.models import Snapshot, SnapshotStatus, SnapshotItem, Resource, Alert
    from shared.azure_storage import azure_storage_manager, workload_candidates_for_resource_type

    print("[VERIFY] === START: backup integrity sample ===")
    cutoff = datetime.utcnow() - timedelta(hours=BACKUP_VERIFY_LOOKBACK_HOURS)
    checked = 0
    passed = 0
    failed = 0
    no_blob = 0
    no_checksum = 0

    try:
        async with async_session_factory() as session:
            snap_stmt = (
                select(Snapshot.id, Snapshot.resource_id)
                .where(and_(
                    Snapshot.status == SnapshotStatus.COMPLETED,
                    Snapshot.completed_at >= cutoff,
                    Snapshot.item_count > 0,
                ))
                .order_by(func.random())
                .limit(BACKUP_VERIFY_SAMPLE_SIZE)
            )
            snap_rows = (await session.execute(snap_stmt)).all()
            if not snap_rows:
                print("[VERIFY] No completed snapshots in lookback window — nothing to sample")
                return

            for snap_id, resource_id in snap_rows:
                # Pick one random item from this snapshot to verify
                item_stmt = (
                    select(SnapshotItem)
                    .where(and_(
                        SnapshotItem.snapshot_id == snap_id,
                        SnapshotItem.blob_path.isnot(None),
                    ))
                    .order_by(func.random())
                    .limit(1)
                )
                item = (await session.execute(item_stmt)).scalars().first()
                if not item:
                    no_blob += 1
                    continue
                if not item.content_checksum:
                    no_checksum += 1
                    continue

                resource = await session.get(Resource, resource_id)
                if not resource:
                    continue

                # Find the right container for this item — same workload mapping
                # the backup worker uses on write.
                rtype = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
                workloads = workload_candidates_for_resource_type(rtype) or ("entra",)
                shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(resource.tenant_id))

                content: bytes | None = None
                used_workload: str | None = None
                for wl in workloads:
                    container = azure_storage_manager.get_container_name(str(resource.tenant_id), wl)
                    try:
                        # shard.download_blob returns None on 404 (ResourceNotFound)
                        # and only raises on auth/network errors — try the next
                        # candidate workload either way.
                        content = await shard.download_blob(container, item.blob_path)
                        if content is not None:
                            used_workload = wl
                            break
                    except Exception:
                        continue

                checked += 1
                if content is None:
                    print(f"[VERIFY] FAIL blob unreachable: snapshot={snap_id} item={item.id} blob={item.blob_path}")
                    failed += 1
                    await _raise_verification_alert(
                        session, resource, snap_id, item, reason="blob_unreachable",
                        expected=item.content_checksum, actual=None,
                    )
                    continue

                actual = _hashlib.sha256(content).hexdigest()
                if actual == item.content_checksum:
                    passed += 1
                    continue

                print(f"[VERIFY] FAIL checksum mismatch: snapshot={snap_id} item={item.id} expected={item.content_checksum[:12]}.. actual={actual[:12]}.. workload={used_workload}")
                failed += 1
                await _raise_verification_alert(
                    session, resource, snap_id, item, reason="checksum_mismatch",
                    expected=item.content_checksum, actual=actual,
                )

            await session.commit()
        print(
            f"[VERIFY] === COMPLETE: sampled={len(snap_rows)} checked={checked} "
            f"passed={passed} failed={failed} no_blob={no_blob} no_checksum={no_checksum} ==="
        )
    except Exception as e:
        import traceback as _tb
        print(f"[VERIFY] FATAL: {e}\n{_tb.format_exc()}")


async def _raise_verification_alert(
    session, resource, snapshot_id, item, reason: str,
    expected: str | None, actual: str | None,
) -> None:
    """Persist a BACKUP_VERIFICATION_FAILED alert. Severity HIGH because a
    silent corruption in storage means the next restore will produce wrong data."""
    from shared.models import Alert
    session.add(Alert(
        tenant_id=resource.tenant_id if resource else None,
        type="BACKUP_VERIFICATION_FAILED",
        severity="HIGH",
        message=(
            f"Snapshot integrity check failed ({reason}) for resource "
            f"{resource.display_name if resource else snapshot_id}. "
            f"Stored blob no longer matches the SHA256 captured at backup time — "
            f"restore from this snapshot may produce corrupted data."
        ),
        resource_id=resource.id if resource else None,
        resource_type=resource.type.value if resource and hasattr(resource.type, "value") else None,
        resource_name=resource.display_name if resource else None,
        triggered_by="verification-cron",
        details={
            "snapshot_id": str(snapshot_id),
            "snapshot_item_id": str(item.id),
            "blob_path": item.blob_path,
            "reason": reason,
            "expected_sha256": expected,
            "actual_sha256": actual,
        },
    ))


# ── Round 1.5: DLQ consumer + failed-snapshot retry ──

async def consume_backup_dlq():
    """Long-running consumer for backup.*.dlq queues — every poison message
    becomes an Alert so ops can investigate. Without this, dead-lettered
    messages sit in RabbitMQ forever with no signal to operators."""
    from shared.models import Alert
    dlq_queues = ["backup.urgent.dlq", "backup.high.dlq", "backup.normal.dlq", "backup.low.dlq"]

    # Wait for message_bus to be ready (startup ordering)
    for _ in range(30):
        if message_bus.channel:
            break
        await asyncio.sleep(1)
    if not message_bus.channel:
        print("[DLQ] message bus not connected — DLQ consumer giving up")
        return

    async def consume_one(queue_name: str):
        try:
            queue = await message_bus.channel.get_queue(queue_name)
        except Exception as exc:
            print(f"[DLQ] cannot bind to {queue_name}: {exc}")
            return
        print(f"[DLQ] consuming from {queue_name}")
        async with queue.iterator() as it:
            async for msg in it:
                try:
                    body = json.loads(msg.body.decode())
                    job_id = body.get("jobId")
                    resource_id = body.get("resourceId") or (body.get("resourceIds") or [None])[0]
                    workload = body.get("workload")
                    delivery_count = (msg.headers or {}).get("x-delivery-count", "?")
                    print(f"[DLQ] poison message in {queue_name}: job={job_id} resource={resource_id} workload={workload} delivery_count={delivery_count}")
                    async with async_session_factory() as session:
                        # Look up tenant via resource if available
                        tenant_id = None
                        if resource_id:
                            try:
                                from shared.models import Resource
                                r = await session.get(Resource, uuid.UUID(resource_id))
                                tenant_id = r.tenant_id if r else None
                            except Exception:
                                pass
                        session.add(Alert(
                            tenant_id=tenant_id,
                            type="BACKUP_DLQ",
                            severity="HIGH",
                            message=(
                                f"Backup message dead-lettered after {delivery_count} delivery attempts on {queue_name}. "
                                f"Job {job_id} for resource {resource_id} ({workload}) is stuck — investigate."
                            ),
                            resource_id=uuid.UUID(resource_id) if resource_id else None,
                            triggered_by="dlq-consumer",
                            details={
                                "queue": queue_name,
                                "delivery_count": delivery_count,
                                "job_id": job_id,
                                "resource_id": resource_id,
                                "workload": workload,
                            },
                        ))
                        await session.commit()
                    await msg.ack()
                except Exception as e:
                    # Ack anyway — re-rejecting would just re-queue to DLQ infinitely
                    print(f"[DLQ] failed to alert on poison message: {type(e).__name__}: {e}")
                    try:
                        await msg.ack()
                    except Exception:
                        pass

    await asyncio.gather(*[consume_one(q) for q in dlq_queues], return_exceptions=True)


async def retry_failed_snapshots():
    """Daily: find FAILED snapshots between 1h and 25h old and re-queue ONE
    backup attempt. Marker on snapshot.extra_data prevents an infinite retry
    loop on the same failed snapshot. Skips resources whose next scheduled
    backup will likely cover them anyway."""
    from shared.models import Snapshot, SnapshotStatus, Resource
    from sqlalchemy import select, and_

    print("[RETRY] === START: failed-snapshot retry sweep ===")
    requeued = 0
    skipped_marked = 0
    skipped_recent_success = 0

    cutoff_old = datetime.utcnow() - timedelta(hours=25)
    cutoff_recent = datetime.utcnow() - timedelta(hours=1)

    try:
        async with async_session_factory() as session:
            stmt = (
                select(Snapshot)
                .where(and_(
                    Snapshot.status == SnapshotStatus.FAILED,
                    Snapshot.completed_at >= cutoff_old,
                    Snapshot.completed_at < cutoff_recent,
                ))
            )
            failed = (await session.execute(stmt)).scalars().all()

            for snap in failed:
                if (snap.extra_data or {}).get("retry_attempted"):
                    skipped_marked += 1
                    continue

                # If a newer COMPLETED snapshot for the same resource exists,
                # the failure has already been "healed" by the next scheduled
                # run — skip the retry to avoid wasted work.
                newer_stmt = select(Snapshot.id).where(and_(
                    Snapshot.resource_id == snap.resource_id,
                    Snapshot.status == SnapshotStatus.COMPLETED,
                    Snapshot.completed_at > snap.completed_at,
                )).limit(1)
                if (await session.execute(newer_stmt)).first():
                    snap.extra_data = (snap.extra_data or {}) | {"retry_attempted": True, "retry_skipped_reason": "newer_success_exists"}
                    await session.merge(snap)
                    skipped_recent_success += 1
                    continue

                resource = await session.get(Resource, snap.resource_id)
                if not resource:
                    continue

                # Pick the queue based on workload — Azure goes to its own queue.
                resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
                queue = AZURE_WORKLOAD_QUEUES.get(resource.type, "backup.normal")
                payload = {
                    "jobId": str(uuid.uuid4()),
                    "resourceId": str(resource.id),
                    "workload": resource_type,
                    "retry_of_snapshot": str(snap.id),
                }
                try:
                    await message_bus.publish(queue, payload, priority=4)
                    snap.extra_data = (snap.extra_data or {}) | {"retry_attempted": True, "retry_at": datetime.utcnow().isoformat()}
                    await session.merge(snap)
                    requeued += 1
                except Exception as e:
                    print(f"[RETRY] failed to re-publish for snapshot {snap.id}: {e}")

            await session.commit()
        print(f"[RETRY] === COMPLETE: requeued={requeued}, skipped_marked={skipped_marked}, skipped_already_healed={skipped_recent_success} ===")
    except Exception as e:
        import traceback as _tb
        print(f"[RETRY] FATAL: {e}\n{_tb.format_exc()}")


# ── Phase 2: Retention Cleanup ──

async def run_retention_cleanup():
    """Daily: apply FLAT/GFS retention rules, delete snapshots outside the window.
    Azure lifecycle policies still handle blob tier transitions + TTL; this function
    reclaims DB rows + guarantees GFS behavior that storage lifecycle alone can't express."""
    import traceback as _tb
    print("[RETENTION] === START: Snapshot retention cleanup ===")
    try:
        from shared.retention_cleanup import enforce_retention_all_tenants
        results = await enforce_retention_all_tenants(async_session_factory)
        total_deleted = sum(r.get("deleted_snapshots", 0) for r in results.values() if isinstance(r, dict))
        total_kept = sum(r.get("kept_snapshots", 0) for r in results.values() if isinstance(r, dict))
        total_held = sum(r.get("held", 0) for r in results.values() if isinstance(r, dict))
        errors = [tid for tid, r in results.items() if isinstance(r, dict) and "error" in r]
        print(f"[RETENTION] === COMPLETE: deleted={total_deleted}, kept={total_kept}, on_hold={total_held}, errors={len(errors)} ===")
        for tid in errors:
            print(f"[RETENTION]   tenant {tid}: {results[tid].get('error')}")
    except Exception as e:
        print(f"[RETENTION] FATAL ERROR: {e}\n{_tb.format_exc()}")


# ── AZ-0: Lifecycle Policy Reconciler ──

async def reconcile_lifecycle_policies():
    """
    Daily: ensure every tenant's blob containers have current lifecycle policy
    based on their SLA retention settings.
    """
    import traceback as _tb
    print("[LIFECYCLE] === START: Daily lifecycle policy reconciliation ===")

    try:
        from shared.azure_storage import apply_lifecycle_policy
    except ImportError:
        print("[LIFECYCLE] ERROR: azure_storage.apply_lifecycle_policy not available — skipping")
        return

    try:
        async with async_session_factory() as session:
            tenants_result = await session.execute(select(Tenant))
            tenants = tenants_result.scalars().all()

            if not tenants:
                print("[LIFECYCLE] No tenants found — nothing to reconcile")
                return

            print(f"[LIFECYCLE] Found {len(tenants)} tenant(s) to reconcile")

            success_count = 0
            fail_count = 0
            skip_count = 0

            for tenant in tenants:
                try:
                    # Get tenant's default SLA policy
                    sla_result = await session.execute(
                        select(SlaPolicy).where(
                            SlaPolicy.tenant_id == tenant.id,
                            SlaPolicy.enabled == True
                        ).limit(1)
                    )
                    sla = sla_result.scalar_one_or_none()
                    if not sla:
                        skip_count += 1
                        print(f"[LIFECYCLE] Tenant {tenant.id} ({tenant.display_name}): No active SLA — skipping")
                        continue

                    hot = sla.retention_hot_days or 7
                    cool = sla.retention_cool_days or 30
                    archive = sla.retention_archive_days  # None = unlimited

                    print(
                        f"[LIFECYCLE] Tenant {tenant.id} ({tenant.display_name}): hot={hot}d, cool={cool}d, "
                        f"archive={'unlimited' if archive is None else f'{archive}d'}, "
                        f"immutability={sla.immutability_mode}, legal_hold={sla.legal_hold_enabled}"
                    )

                    for workload in ["files", "azure-vm", "azure-sql", "azure-postgres"]:
                        shard = azure_storage_manager.get_default_shard()
                        container = azure_storage_manager.get_container_name(str(tenant.id), workload)
                        try:
                            result = await apply_lifecycle_policy(container, hot, cool, archive, shard)
                            if result.get("success"):
                                print(f"[LIFECYCLE]   ✓ {container}: {result.get('rules_count', 0)} rules applied")
                                success_count += 1
                            else:
                                print(f"[LIFECYCLE]   ✗ {container}: {result.get('error', 'unknown error')}")
                                fail_count += 1
                        except Exception as e:
                            print(f"[LIFECYCLE]   ✗ {container}: {e}")
                            fail_count += 1

                except Exception as tenant_exc:
                    print(f"[LIFECYCLE] Tenant {tenant.id} reconciliation error: {tenant_exc}")
                    fail_count += 1

            print(
                f"[LIFECYCLE] === COMPLETE: success={success_count}, failed={fail_count}, skipped={skip_count} ==="
            )

    except Exception as e:
        print(f"[LIFECYCLE] FATAL ERROR: {e}\n{_tb.format_exc()}")


# ── AZ-4: DR Setup Reconciler ──

async def reconcile_dr_setup():
    """
    Every 6 hours: ensure DR containers exist and have matching lifecycle policies.
    Only runs for tenants with dr_region_enabled=True.
    """
    import traceback as _tb
    print("[DR-RECONCILER] === START: DR setup reconciliation (every 6h) ===")

    try:
        from shared.azure_storage import apply_lifecycle_policy, AzureStorageShard
        from shared.security import decrypt_secret
    except ImportError as ie:
        print(f"[DR-RECONCILER] ERROR: Required imports not available — skipping: {ie}")
        return

    try:
        async with async_session_factory() as session:
            tenants_result = await session.execute(
                select(Tenant).where(Tenant.dr_region_enabled == True)
            )
            tenants = tenants_result.scalars().all()

            if not tenants:
                print("[DR-RECONCILER] No tenants with DR enabled — skipping")
                return

            print(f"[DR-RECONCILER] Found {len(tenants)} tenant(s) with DR enabled")

            success_count = 0
            fail_count = 0

            for tenant in tenants:
                try:
                    if not tenant.dr_storage_account_name:
                        print(f"[DR-RECONCILER] Tenant {tenant.id}: DR storage account name not configured — skipping")
                        fail_count += 1
                        continue

                    print(
                        f"[DR-RECONCILER] Tenant {tenant.id} ({tenant.display_name}): "
                        f"DR region={tenant.dr_region}, DR account={tenant.dr_storage_account_name}"
                    )

                    try:
                        dr_key = decrypt_secret(tenant.dr_storage_account_key_encrypted)
                    except Exception as dec_exc:
                        print(f"[DR-RECONCILER] Tenant {tenant.id}: Cannot decrypt DR storage key: {dec_exc}")
                        fail_count += 1
                        continue

                    dr_shard = AzureStorageShard(
                        account_name=tenant.dr_storage_account_name,
                        account_key=dr_key,
                    )

                    # Get tenant's SLA
                    sla_result = await session.execute(
                        select(SlaPolicy).where(
                            SlaPolicy.tenant_id == tenant.id,
                            SlaPolicy.enabled == True
                        ).limit(1)
                    )
                    sla = sla_result.scalar_one_or_none()
                    hot = sla.retention_hot_days if sla else 7
                    cool = sla.retention_cool_days if sla else 30
                    archive = sla.retention_archive_days if sla else None

                    print(
                        f"[DR-RECONCILER]   SLA: hot={hot}d, cool={cool}d, "
                        f"archive={'unlimited' if archive is None else f'{archive}d'}"
                    )

                    for workload in ["files", "azure-vm", "azure-sql", "azure-postgres"]:
                        container = f"{azure_storage_manager.get_container_name(str(tenant.id), workload)}-dr"
                        try:
                            result = await apply_lifecycle_policy(container, hot, cool, archive, dr_shard)
                            if result.get("success"):
                                print(f"[DR-RECONCILER]   ✓ DR {container}: {result.get('rules_count', 0)} rules applied")
                                success_count += 1
                            else:
                                print(f"[DR-RECONCILER]   ✗ DR {container}: {result.get('error', 'unknown')}")
                                fail_count += 1
                        except Exception as container_exc:
                            print(f"[DR-RECONCILER]   ✗ DR {container}: {container_exc}")
                            fail_count += 1

                except Exception as tenant_exc:
                    print(f"[DR-RECONCILER] Tenant {tenant.id} DR reconciliation error: {tenant_exc}\n{_tb.format_exc()}")
                    fail_count += 1

            print(
                f"[DR-RECONCILER] === COMPLETE: success={success_count}, failed={fail_count} ==="
            )

    except Exception as e:
        print(f"[DR-RECONCILER] FATAL ERROR: {e}\n{_tb.format_exc()}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8008)
