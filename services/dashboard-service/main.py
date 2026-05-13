"""Dashboard Service - Aggregated metrics and statistics"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import select, func, text, and_, or_

from shared.database import get_db, close_db, AsyncSession, engine
from shared.models import (
    Resource, Job, JobType, JobStatus, Snapshot, SnapshotItem, SnapshotStatus,
    ResourceType, ResourceStatus, Tenant, TenantType, UI_HIDDEN_TYPES,
)

log = logging.getLogger("dashboard-service")


async def _wait_for_db(timeout_total_s: int = 120) -> None:
    """Ping the DB with exponential backoff so the lifespan startup can
    survive Railway's internal-DNS / Postgres cold-start race.

    Observed Railway 2026-05-13: dashboard-service crashed in a restart
    loop because asyncpg's default connect timeout (10s) raced PG coming
    online, the lifespan ``SELECT 1`` ping raised TimeoutError, and
    Uvicorn exited. Without a retry the only recovery was a manual
    redeploy. Other services in this repo (tenant_service, job_service,
    audit_service, etc.) all retry their startup checks for the same
    reason; dashboard was the outlier.

    Retries 1s → 2s → 4s → 8s → 8s … up to ``timeout_total_s`` wall.
    Each individual attempt is capped at 10s by asyncpg's default, so
    a "stuck DB" scenario can't hold a single attempt forever. Logs
    every failed attempt for visibility into how flaky the dep is.
    """
    deadline = asyncio.get_event_loop().time() + timeout_total_s
    delay = 1.0
    attempt = 0
    while True:
        attempt += 1
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            if attempt > 1:
                log.warning(
                    "[startup] DB reachable after %d attempt(s)", attempt,
                )
            return
        except Exception as exc:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                log.error(
                    "[startup] DB unreachable after %ds (%d attempts): %s — "
                    "exiting so Railway can restart the container",
                    timeout_total_s, attempt, exc,
                )
                raise
            log.warning(
                "[startup] DB ping attempt %d failed (%s: %s); "
                "retrying in %.1fs (deadline in %.0fs)",
                attempt, type(exc).__name__, exc, delay, remaining,
            )
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 2, 8.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Dashboard is read-only and should not run heavyweight schema migration logic
    # during startup. That path can block on application traffic from other services
    # and leave the container stuck in "Waiting for application startup".
    await _wait_for_db()
    yield
    await close_db()


app = FastAPI(title="Dashboard Service", version="1.0.0", lifespan=lifespan)


# Per-user content types — kept for M365_RESOURCE_TYPES below (service-level
# rollups need to know which row types carry per-user backup bytes). The
# Protection Status "Users" card no longer counts these; it counts ENTRA_USER
# rows directly so the card's total equals the Users tab list exactly.
USER_CONTENT_TYPES = {
    ResourceType.MAILBOX,
    ResourceType.ONEDRIVE,
    ResourceType.USER_MAIL,
    ResourceType.USER_ONEDRIVE,
    ResourceType.USER_CONTACTS,
    ResourceType.USER_CALENDAR,
    ResourceType.USER_CHATS,
}

# Each bucket's type set MUST exactly mirror the corresponding tab in
# tm_vault/src/services/resource.ts:M365_TAB_TYPE_MAP. The denominator of
# every Protection Status card has to equal the count shown when the operator
# clicks into that tab — anything else is a lie. SharePoint additionally
# applies the same group-name-collision exclusion that
# /api/v1/resources/by-type?type=SHAREPOINT_SITE applies (handled below).
PROTECTION_BUCKETS = {
    "users": {ResourceType.ENTRA_USER},
    "sharedMailboxes": {ResourceType.SHARED_MAILBOX},
    "rooms": {ResourceType.ROOM_MAILBOX},
    "sharepointSites": {ResourceType.SHAREPOINT_SITE},
    "groupsAndTeams": {ResourceType.ENTRA_GROUP, ResourceType.M365_GROUP, ResourceType.TEAMS_CHANNEL},
    "entraId": {ResourceType.ENTRA_DIRECTORY},
    "powerPlatform": {ResourceType.POWER_BI, ResourceType.POWER_APPS, ResourceType.POWER_AUTOMATE},
}

AZURE_PROTECTION_BUCKETS = {
    "virtualMachines": {ResourceType.AZURE_VM},
    "sqlDatabases": {ResourceType.AZURE_SQL_DB},
    "postgresqlDatabases": {ResourceType.AZURE_POSTGRESQL, ResourceType.AZURE_POSTGRESQL_SINGLE},
}

M365_RESOURCE_TYPES = {
    ResourceType.MAILBOX,
    ResourceType.SHARED_MAILBOX,
    ResourceType.ROOM_MAILBOX,
    ResourceType.ONEDRIVE,
    ResourceType.SHAREPOINT_SITE,
    ResourceType.TEAMS_CHANNEL,
    ResourceType.TEAMS_CHAT,
    ResourceType.ENTRA_USER,
    ResourceType.ENTRA_GROUP,
    ResourceType.ENTRA_APP,
    ResourceType.ENTRA_DEVICE,
    ResourceType.ENTRA_SERVICE_PRINCIPAL,
    ResourceType.POWER_BI,
    ResourceType.POWER_APPS,
    ResourceType.POWER_AUTOMATE,
    ResourceType.POWER_DLP,
    ResourceType.COPILOT,
    ResourceType.PLANNER,
    ResourceType.TODO,
    ResourceType.ONENOTE,
    ResourceType.DYNAMIC_GROUP,
    # Tier 2 per-user content types — these hold the actual backup bytes
    # post-refactor, so they MUST be in this set. Leaving them out
    # silently zeroed out the M365 filter on the dashboard (backup-size
    # dropped from 2.5 GB → 18 KB, 24h / 7d job counts dropped to 0).
    ResourceType.USER_MAIL,
    ResourceType.USER_ONEDRIVE,
    ResourceType.USER_CONTACTS,
    ResourceType.USER_CALENDAR,
    ResourceType.USER_CHATS,
    ResourceType.TEAMS_CHAT_EXPORT,
}

AZURE_RESOURCE_TYPES = {
    ResourceType.AZURE_VM,
    ResourceType.AZURE_SQL_DB,
    ResourceType.AZURE_POSTGRESQL,
    ResourceType.AZURE_POSTGRESQL_SINGLE,
    ResourceType.RESOURCE_GROUP,
}


def parse_service_type(service_type: Optional[str]) -> Optional[str]:
    if not service_type:
        return None
    normalized = service_type.lower()
    if normalized not in ("m365", "azure"):
        raise HTTPException(status_code=400, detail="Unsupported serviceType. Expected 'm365' or 'azure'.")
    return normalized


def resource_types_for_service(service_type: Optional[str]):
    if service_type == "m365":
        return M365_RESOURCE_TYPES
    if service_type == "azure":
        return AZURE_RESOURCE_TYPES
    return None


def datasource_batch_trigger_label(service_type: str) -> str:
    return f"MANUAL_DATASOURCE_{service_type.upper()}"


def format_bytes(bytes_val: int) -> str:
    if bytes_val < 1024**3:
        return f"{bytes_val / 1024**2:.1f} MB"
    return f"{bytes_val / 1024**3:.1f} GB"


@app.get("/health")
async def health():
    return {"status": "ok", "service": "dashboard"}


@app.get("/api/v1/dashboard/overview")
async def get_overview(
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    filters = []
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    
    total = (await db.execute(select(func.count(Resource.id)).where(*filters))).scalar() or 0
    protected = (await db.execute(select(func.count(Resource.id)).where(Resource.sla_policy_id.isnot(None), *filters))).scalar() or 0
    
    # Use naive datetime to match Job.created_at declared as
    # TIMESTAMP WITHOUT TIME ZONE. asyncpg raises DataError on a
    # tz-aware >= naive-column compare. Sibling queries in this file
    # (status/24hour, status/7day, …) already follow this pattern.
    yesterday = datetime.utcnow() - timedelta(hours=24)
    failed = (await db.execute(select(func.count(Job.id)).where(Job.status == JobStatus.FAILED, Job.type == JobType.BACKUP, Job.created_at >= yesterday, *filters))).scalar() or 0
    pending = (await db.execute(select(func.count(Job.id)).where(Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]), Job.type == JobType.BACKUP, *filters))).scalar() or 0
    
    storage = (await db.execute(select(func.sum(Resource.storage_bytes)).where(*filters))).scalar() or 0
    last_backup = (await db.execute(select(func.max(Resource.last_backup_at)).where(*filters))).scalar()
    
    return {
        "totalResources": total,
        "protectedResources": protected,
        "failedBackups": failed,
        "pendingBackups": pending,
        "storageUsed": format_bytes(storage),
        "lastBackupTime": last_backup.isoformat() if last_backup else None,
    }


@app.get("/api/v1/dashboard/status/24hour")
async def get_24hour_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    # Use naive datetime to match TIMESTAMP WITHOUT TIME ZONE
    yesterday = datetime.utcnow() - timedelta(hours=24)
    filters = [Job.type == JobType.BACKUP, Job.created_at >= yesterday]
    if tenantId:
        filters.append(Job.tenant_id == UUID(tenantId))

    service_clause = None
    if service_key and service_resource_types:
        # Two shapes of backup job exist:
        #   1. Per-resource: Job.resource_id points at a single Resource.
        #      Match by that resource's type.
        #   2. Batch (MANUAL_BATCH / USER_ORCHESTRATION): Job.resource_id is
        #      NULL and batch_resource_ids holds the fan-out. We can't join
        #      array → resources cheaply, so we match by Tenant.type
        #      instead — a tenant is either M365 or AZURE, not both, so the
        #      tenant's type tells us which service bucket the batch belongs
        #      to. Replaces the old MANUAL_DATASOURCE_{service} label check
        #      which didn't match the MANUAL_BATCH / USER_ORCHESTRATION
        #      labels our batch jobs actually carry.
        service_tenant_type = TenantType.M365 if service_key == "m365" else TenantType.AZURE
        service_clause = or_(
            and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
            and_(Job.resource_id.is_(None), Tenant.type == service_tenant_type),
        )

    success_stmt = (select(func.count()).select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id))
    warning_stmt = (select(func.count()).select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id))
    failure_stmt = (select(func.count()).select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id))

    if service_clause is not None:
        success_stmt = success_stmt.where(service_clause)
        warning_stmt = warning_stmt.where(service_clause)
        failure_stmt = failure_stmt.where(service_clause)

    # Outcome categories (do NOT count RUNNING/QUEUED — those are
    # in-flight, not outcomes):
    #   success  — COMPLETED with zero failed AND zero skipped items
    #   warnings — COMPLETED with failed_count>0 OR skipped_count>0
    #              (partial success, typical AFI/Veeam dashboard signal),
    #              plus RETRYING jobs (transient failures mid-execution)
    #   failures — FAILED
    # Runtime, queued, cancelled are intentionally excluded.
    _partial_json = text(
        "COALESCE((jobs.result->>'failed_count')::int, 0) > 0 "
        "OR COALESCE((jobs.result->>'skipped_count')::int, 0) > 0"
    )
    success = (await db.execute(
        success_stmt.where(
            Job.status == JobStatus.COMPLETED,
            text("NOT (" + _partial_json.text + ")"),
            *filters,
        )
    )).scalar() or 0
    warnings = (await db.execute(
        warning_stmt.where(
            or_(
                and_(Job.status == JobStatus.COMPLETED, _partial_json),
                Job.status == JobStatus.RETRYING,
            ),
            *filters,
        )
    )).scalar() or 0
    failures = (await db.execute(failure_stmt.where(Job.status == JobStatus.FAILED, *filters))).scalar() or 0

    return {"success": success, "warnings": warnings, "failures": failures}


@app.get("/api/v1/dashboard/status/7day")
async def get_7day_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    # Use naive datetime to match TIMESTAMP WITHOUT TIME ZONE
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    filters = [Job.type == JobType.BACKUP, Job.created_at >= seven_days_ago]
    if tenantId:
        filters.append(Job.tenant_id == UUID(tenantId))
    
    # 7-day outcome buckets match the 24h definitions above. RUNNING /
    # QUEUED jobs are in-flight and don't belong to any outcome bucket;
    # previously they were mis-counted as "warnings" so every active
    # backup inflated the Warnings number on the Overview chart.
    _partial_json_7d = text(
        "COALESCE((jobs.result->>'failed_count')::int, 0) > 0 "
        "OR COALESCE((jobs.result->>'skipped_count')::int, 0) > 0"
    )
    stmt = (
        select(
            func.date(Job.created_at).label("date"),
            func.count().filter(
                and_(
                    Job.status == JobStatus.COMPLETED,
                    text("NOT (" + _partial_json_7d.text + ")"),
                )
            ).label("success"),
            func.count().filter(
                or_(
                    and_(Job.status == JobStatus.COMPLETED, _partial_json_7d),
                    Job.status == JobStatus.RETRYING,
                )
            ).label("warnings"),
            func.count().filter(Job.status == JobStatus.FAILED).label("failures"),
        )
        .select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id)
        .where(*filters)
        .group_by(func.date(Job.created_at))
        .order_by(func.date(Job.created_at))
    )
    if service_key and service_resource_types:
        service_tenant_type = TenantType.M365 if service_key == "m365" else TenantType.AZURE
        stmt = stmt.where(
            or_(
                and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
                and_(Job.resource_id.is_(None), Tenant.type == service_tenant_type),
            )
        )
    result = await db.execute(stmt)
    rows = result.all()

    # Build a dict of existing data
    data_by_date = {str(r.date): {"success": r.success or 0, "warnings": r.warnings or 0, "failures": r.failures or 0} for r in rows}
    
    # Fill all 7 days, padding missing days with zeros
    daily_status = []
    for i in range(7):
        date = (datetime.utcnow() - timedelta(days=6-i)).date()
        date_str = date.isoformat()
        daily_status.append({
            "date": date_str,
            "success": data_by_date.get(date_str, {}).get("success", 0),
            "warnings": data_by_date.get(date_str, {}).get("warnings", 0),
            "failures": data_by_date.get(date_str, {}).get("failures", 0),
        })
    
    total_backups = sum(d["success"] + d["warnings"] + d["failures"] for d in daily_status)
    total_success = sum(d["success"] for d in daily_status)
    
    return {
        "dailyStatus": daily_status,
        "summary": {
            "totalBackups": total_backups,
            "successRate": round(total_success / total_backups * 100, 2) if total_backups > 0 else 0,
            "avgDuration": "N/A",
        },
    }


@app.get("/api/v1/dashboard/protection/status")
async def get_protection_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    filters = [Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE])]
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    if service_resource_types:
        filters.append(Resource.type.in_(service_resource_types))

    stmt = (
        select(
            Resource.type,
            func.count(Resource.id).label("total"),
            func.count(Resource.id).filter(Resource.sla_policy_id.isnot(None)).label("protected"),
        )
        .where(*filters)
        .group_by(Resource.type)
    )
    rows = (await db.execute(stmt)).all()

    totals_by_type = {
        row.type: {"total": row.total or 0, "protected": row.protected or 0}
        for row in rows
    }

    def bucket_item(bucket_name: str, buckets: dict):
        total = 0
        protected = 0
        for resource_type in buckets[bucket_name]:
            values = totals_by_type.get(resource_type, {"total": 0, "protected": 0})
            total += values["total"]
            protected += values["protected"]
        return {"protectedCount": protected, "total": total}
    if service_key == "azure":
        bucket_values = {name: bucket_item(name, AZURE_PROTECTION_BUCKETS) for name in AZURE_PROTECTION_BUCKETS}
        total = sum(item["total"] for item in bucket_values.values())
        protected = sum(item["protectedCount"] for item in bucket_values.values())
        percentage = round(protected / total * 100, 2) if total > 0 else 0
        return {
            "virtualMachines": bucket_values["virtualMachines"],
            "sqlDatabases": bucket_values["sqlDatabases"],
            "postgresqlDatabases": bucket_values["postgresqlDatabases"],
            "percentage": percentage,
        }

    bucket_values = {name: bucket_item(name, PROTECTION_BUCKETS) for name in PROTECTION_BUCKETS}

    # SharePoint sites: the Sites tab hides sites whose name collides with an
    # M365 group / Entra group / Teams channel (the admin API surfaces the
    # group's display name as the site title, so a single Team appears as
    # both a SP site row AND a Groups & Teams row otherwise). The Overview
    # card must hide the same rows or the denominator inflates above what
    # the operator can actually click into. The exclusion below MUST stay
    # in sync with resource-service /api/v1/resources/by-type's
    # sp_exclude_clause — a future change to the rule has to touch both.
    sp_params = {}
    sp_tenant_clause = ""
    if tenantId:
        sp_tenant_clause = "AND r.tenant_id = :tenant_id"
        sp_params["tenant_id"] = str(UUID(tenantId))
    sp_total_row = (await db.execute(text(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.sla_policy_id IS NOT NULL) AS protected_count
        FROM resources r
        WHERE r.type = 'SHAREPOINT_SITE'
          AND r.status IN ('DISCOVERED', 'ACTIVE')
          {sp_tenant_clause}
          AND NOT EXISTS (
            SELECT 1 FROM resources g
            WHERE g.tenant_id = r.tenant_id
              AND g.type IN ('M365_GROUP', 'ENTRA_GROUP', 'TEAMS_CHANNEL')
              AND LOWER(g.display_name) = LOWER(r.display_name)
              AND (
                    COALESCE(r.email, '') = ''
                 OR LOWER(COALESCE(g.email, '')) = LOWER(COALESCE(r.email, ''))
              )
          )
    """), sp_params)).first()
    bucket_values["sharepointSites"] = {
        "total": int(sp_total_row.total or 0),
        "protectedCount": int(sp_total_row.protected_count or 0),
    }

    total = sum(item["total"] for item in bucket_values.values())
    protected = sum(item["protectedCount"] for item in bucket_values.values())
    percentage = round(protected / total * 100, 2) if total > 0 else 0

    # TEMP DEBUG — verify GROUP BY is returning the types/counts we expect.
    # Remove once Overview vs Tab parity is confirmed. The keys come back as
    # enum members; str() them so the JSON is human-readable.
    _debug = {
        "service_key": service_key,
        "tenantId": tenantId,
        "totals_by_type": {str(getattr(k, "value", k)): v for k, v in totals_by_type.items()},
        "bucket_groupsAndTeams_members": [str(getattr(m, "value", m)) for m in PROTECTION_BUCKETS["groupsAndTeams"]],
        "bucket_entraId_members": [str(getattr(m, "value", m)) for m in PROTECTION_BUCKETS["entraId"]],
    }
    return {
        "users": bucket_values["users"],
        "sharedMailboxes": bucket_values["sharedMailboxes"],
        "rooms": bucket_values["rooms"],
        "sharepointSites": bucket_values["sharepointSites"],
        "groupsAndTeams": bucket_values["groupsAndTeams"],
        "entraId": bucket_values["entraId"],
        "powerPlatform": bucket_values["powerPlatform"],
        "percentage": percentage,
        "_debug": _debug,
    }


@app.get("/api/v1/dashboard/backup/size")
async def get_backup_size(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    filters = []
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    if service_resource_types:
        filters.append(Resource.type.in_(service_resource_types))

    # Single source of truth: Resource.storage_bytes. The snapshots table holds
    # many legacy FAILED rows and bytes_added is unreliable per snapshot, so using
    # it produced a dashboard where the headline said 7.9 MB but the chart/pills
    # read 0. All four fields below are derived from resources only.
    total = int((await db.execute(select(func.sum(Resource.storage_bytes)).where(*filters))).scalar() or 0)

    # Per-day growth based on when each resource was last backed up.
    # Resources last backed up BEFORE the 30-day window seed day 0 as a baseline.
    today = datetime.utcnow().date()
    window_start = today - timedelta(days=29)
    window_start_ts = datetime.combine(window_start, datetime.min.time())

    day_bucket = func.date_trunc("day", Resource.last_backup_at).label("day")
    per_day_rows = (await db.execute(
        select(day_bucket, func.sum(Resource.storage_bytes))
        .where(
            Resource.last_backup_at.isnot(None),
            Resource.last_backup_at >= window_start_ts,
            *filters,
        )
        .group_by(day_bucket)
    )).all()
    per_day_map: dict = {}
    for row in per_day_rows:
        day_val = row[0]
        if day_val is None:
            continue
        day_key = day_val.date() if hasattr(day_val, "date") else day_val
        per_day_map[day_key] = int(row[1] or 0)

    baseline = int((await db.execute(
        select(func.sum(Resource.storage_bytes)).where(
            Resource.last_backup_at.isnot(None),
            Resource.last_backup_at < window_start_ts,
            *filters,
        )
    )).scalar() or 0)

    daily_data = []
    running_total = baseline
    for i in range(30):
        date = window_start + timedelta(days=i)
        running_total += per_day_map.get(date, 0)
        daily_data.append({"date": date.isoformat(), "bytes": running_total})

    seven_day_change = daily_data[-1]["bytes"] - (daily_data[-8]["bytes"] if len(daily_data) > 7 else 0)
    one_day_change = daily_data[-1]["bytes"] - (daily_data[-2]["bytes"] if len(daily_data) > 1 else 0)

    return {
        "total": format_bytes(total),
        "oneDayChange": format_bytes(abs(one_day_change)) + (" ↑" if one_day_change > 0 else " ↓"),
        "oneMonthChange": format_bytes(abs(seven_day_change)) + (" ↑" if seven_day_change > 0 else " ↓"),
        "allTimeTotal": format_bytes(total),
        "dailyData": daily_data,
    }
