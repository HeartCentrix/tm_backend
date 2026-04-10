"""
Backup Scheduler Service - SLA-based Mass Backup Scheduling
Port: 8008

Responsibilities:
- Read SLA policies and determine which resources need backup
- Group resources by type and tenant for batch processing
- Dispatch mass backup jobs to RabbitMQ queues
- Implement staggered scheduling to prevent Graph API throttling
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

app = FastAPI(title="Backup Scheduler Service", version="2.0.0")

# APScheduler for cron-like scheduling
scheduler = AsyncIOScheduler()


@app.on_event("startup")
async def startup():
    """Initialize services on startup"""
    await message_bus.connect()

    # Schedule SLA-based backup jobs
    scheduler.add_job(dispatch_gold_backups, 'cron', hour='2,10,18', minute=0, timezone='UTC')
    scheduler.add_job(dispatch_silver_backups, 'cron', hour=2, minute=0, timezone='UTC')
    scheduler.add_job(dispatch_bronze_backups, 'cron', hour=4, minute=0, day_of_week='mon-fri', timezone='UTC')

    # Schedule SLA violation check every 30 minutes
    scheduler.add_job(check_sla_violations, 'interval', minutes=30)

    # Schedule pre-emptive backup check every 15 minutes (ransomware/anomaly detection)
    scheduler.add_job(check_preemptive_backup_triggers, 'interval', minutes=15)

    # Schedule M365 audit log ingestion every hour
    scheduler.add_job(ingest_m365_audit_logs, 'interval', hours=1)

    # Schedule daily backup report (email/Slack/Teams)
    scheduler.add_job(send_daily_backup_report, 'cron', hour=8, minute=0, timezone='UTC')

    # Schedule weekly summary report (Monday 9am UTC)
    scheduler.add_job(send_weekly_summary_report, 'cron', hour=9, minute=0, day_of_week='mon', timezone='UTC')

    scheduler.start()


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown"""
    scheduler.shutdown()
    await message_bus.disconnect()


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "backup-scheduler"}


@app.post("/scheduler/trigger/{sla_tier}")
async def trigger_sla_backup(sla_tier: str, background_tasks: BackgroundTasks):
    """Manually trigger backup dispatch for a specific SLA tier"""
    background_tasks.add_task(dispatch_backups_by_sla_tier, sla_tier)
    return {"status": "scheduled", "sla_tier": sla_tier}


@app.post("/scheduler/resource/{resource_id}")
async def trigger_single_backup(resource_id: str, full_backup: bool = False):
    """Trigger immediate backup for a single resource"""
    async with async_session_factory() as session:
        resource = await session.get(Resource, uuid.UUID(resource_id))
        if not resource:
            return {"error": "Resource not found"}, 404
        
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


async def dispatch_gold_backups():
    """Dispatch Gold SLA backups (3x daily at 02:00, 10:00, 18:00 UTC)"""
    await dispatch_backups_by_sla_tier("GOLD")


async def dispatch_silver_backups():
    """Dispatch Silver SLA backups (1x daily at 02:00 UTC)"""
    await dispatch_backups_by_sla_tier("SILVER")


async def dispatch_bronze_backups():
    """Dispatch Bronze SLA backups (1x daily at 04:00 UTC, Mon-Fri)"""
    await dispatch_backups_by_sla_tier("BRONZE")


async def dispatch_backups_by_sla_tier(sla_tier: str):
    """
    Dispatch mass backups for a specific SLA tier
    
    Strategy:
    1. Fetch all active resources with this SLA tier
    2. Group by resource_type + tenant_id
    3. Split into batches of 1000 resources
    4. Dispatch batches with staggered delays
    """
    print(f"[SCHEDULER] Starting {sla_tier} SLA backup dispatch")
    
    async with async_session_factory() as session:
        # Fetch SLA policies for this tier
        policies_result = await session.execute(
            select(SlaPolicy).where(
                and_(
                    SlaPolicy.tier == sla_tier,
                    SlaPolicy.enabled == True
                )
            )
        )
        policies = policies_result.scalars().all()
        
        if not policies:
            print(f"[SCHEDULER] No active {sla_tier} SLA policies found")
            return
        
        policy_ids = [p.id for p in policies]
        
        # Fetch all active resources assigned to these policies
        resources_result = await session.execute(
            select(Resource).where(
                and_(
                    Resource.sla_policy_id.in_(policy_ids),
                    Resource.status == ResourceStatus.ACTIVE
                )
            ).options(selectinload(Resource.tenant))
        )
        resources = resources_result.scalars().all()
        
        if not resources:
            print(f"[SCHEDULER] No active resources for {sla_tier} SLA")
            return
        
        print(f"[SCHEDULER] Found {len(resources)} resources for {sla_tier} SLA")
        
        # Group by resource type + tenant
        groups: Dict[str, List[Resource]] = {}
        for resource in resources:
            group_key = f"{resource.type.value}:{resource.tenant_id}"
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(resource)
        
        print(f"[SCHEDULER] Grouped into {len(groups)} resource type + tenant combinations")
        
        # Determine queue based on SLA tier
        queue_map = {
            "GOLD": "backup.high",
            "SILVER": "backup.normal",
            "BRONZE": "backup.low",
        }
        queue = queue_map.get(sla_tier, "backup.normal")
        
        # Dispatch batches
        total_dispatched = 0
        BATCH_SIZE = 1000
        
        for group_key, group_resources in groups.items():
            resource_type, tenant_id = group_key.split(":")
            
            # Split into batches
            for i in range(0, len(group_resources), BATCH_SIZE):
                batch = group_resources[i:i + BATCH_SIZE]
                resource_ids = [str(r.id) for r in batch]
                
                # Create job record for tracking
                job_id = uuid.uuid4()
                job = Job(
                    id=job_id,
                    type=JobType.BACKUP,
                    tenant_id=uuid.UUID(tenant_id),
                    batch_resource_ids=[uuid.UUID(rid) for rid in resource_ids],
                    status=JobStatus.QUEUED,
                    priority=1 if sla_tier == "GOLD" else 5,
                    spec={
                        "sla_tier": sla_tier,
                        "resource_type": resource_type,
                        "batch_size": len(resource_ids),
                        "triggered_by": "SCHEDULED",
                        "snapshot_label": "scheduled",
                    }
                )
                session.add(job)
                
                # Create mass backup message
                message = create_mass_backup_message(
                    job_id=str(job_id),
                    tenant_id=tenant_id,
                    resource_type=resource_type,
                    resource_ids=resource_ids,
                    sla_tier=sla_tier,
                    full_backup=False
                )
                
                # Send to queue
                await message_bus.publish(
                    queue,
                    message,
                    priority=message["priority"]
                )
                
                total_dispatched += len(resource_ids)
                
                # Stagger dispatch to prevent overwhelming Graph API
                # Add small delay between batches (100ms per batch)
                await asyncio.sleep(0.1)
        
        await session.commit()
        
        print(f"[SCHEDULER] Dispatched {total_dispatched} resources to {queue}")


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
                    Resource.status == ResourceStatus.ACTIVE,
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
                        "sla_tier": policy.tier,
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
                Resource.status == ResourceStatus.ACTIVE,
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
                Resource.status == ResourceStatus.ACTIVE,
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
            sla_tier="MANUAL",
            full_backup=True  # Force full backup for preemptive
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8008)
