"""API routes router"""
from fastapi import APIRouter

from app.api.auth import router as auth_router
from app.api.dashboard import router as dashboard_router
from app.api.tenants import router as tenants_router
from app.api.resources import router as resources_router
from app.api.jobs import router as jobs_router
from app.api.snapshots import router as snapshots_router
from app.api.restore import router as restore_router
from app.api.alerts import router as alerts_router
from app.api.policies import router as policies_router
from app.api.access_control import router as access_control_router

router = APIRouter()

# Auth
router.include_router(auth_router, prefix="/auth", tags=["auth"])

# Dashboard
router.include_router(dashboard_router, prefix="/dashboard", tags=["dashboard"])

# Tenants
router.include_router(tenants_router, prefix="/tenants", tags=["tenants"])
router.include_router(tenants_router, prefix="/organizations", tags=["organizations"])

# Resources
router.include_router(resources_router, prefix="/resources", tags=["resources"])

# Jobs
router.include_router(jobs_router, prefix="/jobs", tags=["jobs"])
router.include_router(jobs_router, prefix="/backups", tags=["backups"])
router.include_router(jobs_router, prefix="/dlq", tags=["dlq"])

# Snapshots
router.include_router(snapshots_router, tags=["snapshots"])

# Restore
router.include_router(restore_router, prefix="/jobs", tags=["restore"])

# Alerts
router.include_router(alerts_router, prefix="/alerts", tags=["alerts"])

# SLA Policies
router.include_router(policies_router, prefix="/policies", tags=["policies"])

# Access Control
router.include_router(access_control_router, prefix="/access-groups", tags=["access-control"])
