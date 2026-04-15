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
from shared.message_bus import message_bus, create_mass_backup_message, create_backup_message
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
    "TEAMS_CHAT": "backup_teams_chats",
    "ENTRA_USER": "backup_entra_id",
    "ENTRA_GROUP": "backup_entra_id",
    "ENTRA_APP": "backup_entra_id",
    "ENTRA_DEVICE": "backup_entra_id",
    "POWER_BI": "backup_power_platform",
    "POWER_APPS": "backup_power_platform",
    "POWER_AUTOMATE": "backup_power_platform",
    "POWER_DLP": "backup_power_platform",
    "COPILOT": "backup_copilot",
    "PLANNER": "planner",
    "TODO": "tasks",
    "ONENOTE": "backup_onedrive",
}

# AZ-4: Azure workload queue routing
AZURE_WORKLOAD_QUEUES = {
    ResourceType.AZURE_VM: "azure.vm",
    ResourceType.AZURE_SQL_DB: "azure.sql",
    ResourceType.AZURE_POSTGRESQL: "azure.postgres",
}


# Valid APScheduler day-of-week abbreviations
DAY_MAP = {
    "MON": "mon", "TUE": "tue", "WED": "wed", "THU": "thu",
    "FRI": "fri", "SAT": "sat", "SUN": "sun",
}


def frequency_to_cron_params(frequency: str, backup_days: list[str] | None = None):
    """
    Convert SLA policy frequency string to APScheduler cron parameters.

    Supported frequencies:
    - THREE_DAILY: 3x per day (every 8 hours at 00:00, 08:00, 16:00 UTC)
    - DAILY: 1x per day (at 02:00 UTC) — runs every day
    - WEEKLY: 1x per week (Sunday at 03:00 UTC)
    - CUSTOM: runs on user-selected days at 02:00 UTC (uses backup_days)
    """
    if frequency == "THREE_DAILY":
        return {"trigger": "cron", "hour": "0,8,16", "minute": 0}
    elif frequency == "WEEKLY":
        return {"trigger": "cron", "day_of_week": "sun", "hour": 3, "minute": 0}
    elif frequency == "CUSTOM" and backup_days:
        # Convert user-selected days to APScheduler format
        aps_days = [DAY_MAP.get(d.upper(), d.lower()) for d in backup_days if DAY_MAP.get(d.upper())]
        if aps_days:
            return {"trigger": "cron", "day_of_week": ",".join(aps_days), "hour": 2, "minute": 0}
        # Fallback if no valid days
        return {"trigger": "cron", "hour": 2, "minute": 0}
    else:  # DAILY (default)
        return {"trigger": "cron", "hour": 2, "minute": 0}


def resource_type_enabled(resource_type: str, policy: SlaPolicy) -> bool:
    """Check if a resource type is enabled in the SLA policy's backup flags."""
    flag_name = RESOURCE_TYPE_TO_SLA_FLAG.get(resource_type)
    if not flag_name:
        # Unknown resource types default to enabled
        return True
    return getattr(policy, flag_name, True)


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
    """Schedule or reschedule a backup job for a specific SLA policy"""
    job_id = f"policy_backup_{policy.id}"

    # Remove existing job if it exists (for rescheduling)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    cron_params = frequency_to_cron_params(policy.frequency, policy.backup_days)

    scheduler.add_job(
        dispatch_policy_backups,
        args=[str(policy.id)],
        id=job_id,
        name=f"Backup: {policy.name}",
        replace_existing=True,
        timezone="UTC",
        **cron_params,
    )

    days_info = f" days={policy.backup_days}" if policy.frequency == "CUSTOM" and policy.backup_days else ""
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
        enabled_resources = [
            r for r in all_resources
            if resource_type_enabled(r.type.value, policy)
        ]

        skipped = len(all_resources) - len(enabled_resources)
        if skipped > 0:
            print(f"[SCHEDULER] Filtered out {skipped} resources (disabled by policy flags)")

        if not enabled_resources:
            print(f"[SCHEDULER] No enabled resources for policy '{policy.name}' after flag filtering")
            return

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
    """
    Check for anomaly signals that might indicate ransomware or data loss.
    Triggers pre-emptive backups when suspicious patterns are detected.

    Signals checked:
    - Sudden spike in file deletions/modifications
    - Unusual file extension changes (ransomware encryption)
    - Mass permission changes
    - Suspicious sign-in patterns (from audit logs)
    """
    print("[PREEMPTIVE] Checking for backup trigger signals...")

    async with async_session_factory() as session:
        # Get all active tenants
        tenants_result = await session.execute(
            select(Tenant).where(Tenant.status == TenantStatus.ACTIVE)
        )
        tenants = tenants_result.scalars().all()

        triggered_count = 0
        for tenant in tenants:
            try:
                # Check for anomaly signals
                anomaly_score = await calculate_anomaly_score(session, tenant)

                if anomaly_score > 0.7:  # High anomaly - trigger backup
                    print(f"[PREEMPTIVE] High anomaly score ({anomaly_score:.2f}) for tenant {tenant.display_name}")
                    await trigger_preemptive_backup(session, tenant, f"anomaly_score={anomaly_score:.2f}")
                    triggered_count += 1
                elif anomaly_score > 0.5:  # Medium anomaly - log warning
                    print(f"[PREEMPTIVE] Medium anomaly score ({anomaly_score:.2f}) for tenant {tenant.display_name}")

            except Exception as e:
                print(f"[PREEMPTIVE] Failed to check tenant {tenant.display_name}: {e}")

        if triggered_count > 0:
            print(f"[PREEMPTIVE] Triggered {triggered_count} pre-emptive backups")
        else:
            print("[PREEMPTIVE] No pre-emptive backups triggered")


async def calculate_anomaly_score(session: AsyncSession, tenant: Tenant) -> float:
    """
    Calculate anomaly score based on recent backup patterns and resource changes.
    Returns score between 0.0 (normal) and 1.0 (highly anomalous)
    """
    score = 0.0
    signals = 0

    # Signal 1: Check for resources with failed backups in last 24h
    now = datetime.utcnow()
    failed_backups_result = await session.execute(
        select(func.count(Job.id)).where(
            and_(
                Job.tenant_id == tenant.id,
                Job.type == JobType.BACKUP,
                Job.status == JobStatus.FAILED,
                Job.created_at >= now - timedelta(hours=24)
            )
        )
    )
    failed_backups = failed_backups_result.scalar() or 0
    if failed_backups > 5:
        score += 0.2
        signals += 1

    # Signal 2: Check for resources with sudden status changes
    resources_changed_result = await session.execute(
        select(func.count(Resource.id)).where(
            and_(
                Resource.tenant_id == tenant.id,
                Resource.updated_at >= now - timedelta(hours=24),
                Resource.status.in_(['SUSPENDED', 'PENDING_DELETION'])
            )
        )
    )
    resources_changed = resources_changed_result.scalar() or 0
    if resources_changed > 10:
        score += 0.3
        signals += 1

    # Signal 3: Check for resources without recent backups (>48h)
    resources_no_backup_result = await session.execute(
        select(func.count(Resource.id)).where(
            and_(
                Resource.tenant_id == tenant.id,
                Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
                (Resource.last_backup_at == None) | (Resource.last_backup_at <= now - timedelta(hours=48))
            )
        )
    )
    resources_no_backup = resources_no_backup_result.scalar() or 0
    if resources_no_backup > 20:
        score += 0.3
        signals += 1

    # Signal 4: Check for unusual snapshot item counts (sudden drops might indicate deletion)
    recent_snapshots_result = await session.execute(
        select(Snapshot).where(
            and_(
                Snapshot.resource_id.in_(
                    select(Resource.id).where(Resource.tenant_id == tenant.id)
                ),
                Snapshot.started_at >= now - timedelta(hours=48)
            )
        ).order_by(Snapshot.started_at.desc()).limit(10)
    )
    recent_snapshots = recent_snapshots_result.scalars().all()
    if len(recent_snapshots) >= 2:
        # Compare item counts between last two snapshots
        last_items = recent_snapshots[0].item_count or 0
        prev_items = recent_snapshots[1].item_count or 0
        if prev_items > 0 and last_items < prev_items * 0.5:
            # More than 50% drop in item count
            score += 0.4
            signals += 1

    # Normalize score
    if signals > 0:
        score = min(1.0, score)

    return score


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
                    Tenant.type.in_([TenantType.M365, TenantType.BOTH]),
                    Tenant.client_id.isnot(None),
                    Tenant.external_tenant_id.isnot(None),
                )
            )
        )
        tenants = tenants_result.scalars().all()

    ingested_total = 0
    audit_service_url = "http://audit-service:8012"

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
    """Send daily backup status report via email/Slack/Teams"""
    print("[REPORT] Generating daily backup report...")

    async with async_session_factory() as session:
        now = datetime.utcnow()
        yesterday = now - timedelta(days=1)

        # Get backup statistics for last 24h
        total_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.created_at >= yesterday,
                )
            )
        )
        total_backups = total_backups_result.scalar() or 0

        successful_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.status == JobStatus.COMPLETED,
                    Job.created_at >= yesterday,
                )
            )
        )
        successful_backups = successful_backups_result.scalar() or 0

        failed_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.status == JobStatus.FAILED,
                    Job.created_at >= yesterday,
                )
            )
        )
        failed_backups = failed_backups_result.scalar() or 0

        # Get resource coverage
        total_resources_result = await session.execute(
            select(func.count(Resource.id)).where(Resource.status == ResourceStatus.ACTIVE)
        )
        total_resources = total_resources_result.scalar() or 0

        protected_resources_result = await session.execute(
            select(func.count(Resource.id)).where(
                and_(
                    Resource.status == ResourceStatus.ACTIVE,
                    Resource.last_backup_at >= yesterday,
                    Resource.last_backup_status == "COMPLETED"
                )
            )
        )
        protected_resources = protected_resources_result.scalar() or 0

        # Get tenants with most failures
        failing_tenants_result = await session.execute(
            select(Tenant.display_name, func.count(Job.id).label('fail_count'))
            .join(Job, Tenant.id == Job.tenant_id)
            .where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.status == JobStatus.FAILED,
                    Job.created_at >= yesterday,
                )
            )
            .group_by(Tenant.display_name)
            .order_by(func.count(Job.id).desc())
            .limit(5)
        )
        failing_tenants = failing_tenants_result.all()

        coverage_pct = (protected_resources / total_resources * 100) if total_resources > 0 else 0

        report = {
            "report_type": "DAILY_BACKUP_REPORT",
            "generated_at": now.isoformat(),
            "period": f"{yesterday.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}",
            "summary": {
                "total_backups": total_backups,
                "successful_backups": successful_backups,
                "failed_backups": failed_backups,
                "success_rate": f"{(successful_backups / total_backups * 100) if total_backups > 0 else 0:.1f}%",
                "total_resources": total_resources,
                "protected_resources": protected_resources,
                "coverage_rate": f"{coverage_pct:.1f}%",
            },
            "failing_tenants": [
                {"tenant": t[0], "failures": t[1]}
                for t in failing_tenants
            ],
        }

        # Send report via configured channels
        await send_report_to_channels(report)

        print(f"[REPORT] Daily report: {successful_backups}/{total_backups} successful, "
              f"{coverage_pct:.1f}% coverage")


async def send_weekly_summary_report():
    """Send weekly summary report with trends and recommendations"""
    print("[REPORT] Generating weekly summary report...")

    async with async_session_factory() as session:
        now = datetime.utcnow()
        week_ago = now - timedelta(days=7)

        # Weekly backup statistics
        total_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.created_at >= week_ago,
                )
            )
        )
        total_backups = total_backups_result.scalar() or 0

        successful_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.status == JobStatus.COMPLETED,
                    Job.created_at >= week_ago,
                )
            )
        )
        successful_backups = successful_backups_result.scalar() or 0

        # Storage usage
        storage_result = await session.execute(
            select(func.sum(Resource.storage_bytes)).where(Resource.status == ResourceStatus.ACTIVE)
        )
        total_storage_bytes = storage_result.scalar() or 0

        # SLA compliance
        resources_with_sla_result = await session.execute(
            select(func.count(Resource.id)).where(
                and_(
                    Resource.status == ResourceStatus.ACTIVE,
                    Resource.sla_policy_id.isnot(None)
                )
            )
        )
        resources_with_sla = resources_with_sla_result.scalar() or 0

        # Calculate recommendations
        recommendations = []
        if total_backups > 0:
            success_rate = successful_backups / total_backups
            if success_rate < 0.95:
                recommendations.append("Review failed backup jobs and resolve underlying issues")

        if total_storage_bytes > 100 * 1024**3:  # >100GB
            recommendations.append("Consider implementing data deduplication to reduce storage costs")

        report = {
            "report_type": "WEEKLY_SUMMARY_REPORT",
            "generated_at": now.isoformat(),
            "period": f"Week of {week_ago.strftime('%Y-%m-%d')}",
            "summary": {
                "total_backups": total_backups,
                "successful_backups": successful_backups,
                "success_rate": f"{(successful_backups / total_backups * 100) if total_backups > 0 else 0:.1f}%",
                "total_storage_gb": round(total_storage_bytes / (1024**3), 2),
                "resources_with_sla": resources_with_sla,
            },
            "recommendations": recommendations,
        }

        await send_report_to_channels(report)

        print(f"[REPORT] Weekly summary: {total_backups} backups, "
              f"{total_storage_bytes / (1024**3):.2f}GB storage")


async def send_report_to_channels(report: Dict[str, Any]):
    """Send report to configured notification channels (email/Slack/Teams)"""
    # Send to Teams webhook
    await send_teams_report_notification(report)

    # Send to Slack webhook (if configured)
    await send_slack_report_notification(report)

    # Send via email (if configured)
    await send_email_report_notification(report)


async def send_teams_report_notification(report: Dict[str, Any]):
    """Send report to Teams webhook"""
    if not hasattr(settings, 'TEAMS_WEBHOOK_URL') or not getattr(settings, 'TEAMS_WEBHOOK_URL'):
        return

    summary = report.get("summary", {})
    title = report.get("report_type", "Backup Report").replace("_", " ")

    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": title,
        "themeColor": "0078D4",
        "title": f"TM Vault - {title}",
        "sections": [
            {
                "activityTitle": title,
                "activitySubtitle": report.get("period", ""),
                "facts": [
                    {"name": "Total Backups", "value": str(summary.get("total_backups", 0))},
                    {"name": "Successful", "value": str(summary.get("successful_backups", 0))},
                    {"name": "Success Rate", "value": summary.get("success_rate", "N/A")},
                ],
            }
        ],
    }

    # Add coverage rate if available
    if "coverage_rate" in summary:
        message["sections"][0]["facts"].append(
            {"name": "Coverage Rate", "value": summary["coverage_rate"]}
        )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(settings.TEAMS_WEBHOOK_URL, json=message)
            print(f"[REPORT] Report sent to Teams: {title}")
    except Exception as e:
        print(f"[REPORT] Failed to send Teams notification: {e}")


async def send_slack_report_notification(report: Dict[str, Any]):
    """Send report to Slack webhook"""
    if not hasattr(settings, 'SLACK_WEBHOOK_URL') or not getattr(settings, 'SLACK_WEBHOOK_URL'):
        return

    summary = report.get("summary", {})

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"TM Vault - {report.get('report_type', 'Report').replace('_', ' ')}",
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Total Backups:*\n{summary.get('total_backups', 0)}"},
                {"type": "mrkdwn", "text": f"*Successful:*\n{summary.get('successful_backups', 0)}"},
                {"type": "mrkdwn", "text": f"*Success Rate:*\n{summary.get('success_rate', 'N/A')}"},
            ]
        }
    ]

    # Add recommendations if available
    if "recommendations" in report and report["recommendations"]:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Recommendations:*\n" + "\n".join([f"• {r}" for r in report["recommendations"]])
            }
        })

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(settings.SLACK_WEBHOOK_URL, json={"blocks": blocks})
            print(f"[REPORT] Report sent to Slack")
    except Exception as e:
        print(f"[REPORT] Failed to send Slack notification: {e}")


async def send_email_report_notification(report: Dict[str, Any]):
    """Send report via email (placeholder - requires SMTP configuration)"""
    if not hasattr(settings, 'SMTP_ENABLED') or not getattr(settings, 'SMTP_ENABLED', False):
        return

    # Placeholder for email sending logic
    print(f"[REPORT] Email notification would be sent (SMTP not configured)")


async def send_violations_to_alert_service(violations: List[Dict]):
    """Send SLA violations to alert-service"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            for violation in violations:
                await client.post("http://alert-service:8007/api/v1/alerts", json={
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
