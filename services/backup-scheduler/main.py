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
    Resource, SlaPolicy, Tenant, Job, Organization,
    ResourceType, ResourceStatus, JobType, JobStatus
)
from shared.message_bus import message_bus, create_mass_backup_message
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
            # TODO: Send violations to alert-service via HTTP or message bus
        else:
            print("[SLA] No violations detected")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8008)
