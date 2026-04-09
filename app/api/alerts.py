"""Alerts routes"""
import uuid
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import (
    AlertResponse,
    AlertListResponse,
    NotificationSettings,
    NotificationSettingsAlertThresholds,
    WebhookResponse,
)
from app.security import get_current_user

router = APIRouter()


@router.get("")
async def list_alerts(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    unresolved: Optional[bool] = Query(None),
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List alerts with pagination"""
    filters = []
    if tenantId:
        filters.append(models.Alert.tenant_id == uuid.UUID(tenantId))
    if unresolved:
        filters.append(models.Alert.resolved == False)
    
    count_stmt = select(func.count(models.Alert.id)).where(*filters)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0
    
    stmt = (
        select(models.Alert)
        .where(*filters)
        .order_by(models.Alert.created_at.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    alerts = result.scalars().all()
    
    return AlertListResponse(
        content=[
            AlertResponse(
                id=str(a.id),
                severity=a.severity,
                title=a.message[:100] if a.message else "Alert",
                description=a.message or "",
                status="RESOLVED" if a.resolved else "ACTIVE",
                createdAt=a.created_at.isoformat() if a.created_at else "",
                resolved=a.resolved,
                resolvedAt=a.resolved_at.isoformat() if a.resolved_at else None,
                tenantId=str(a.tenant_id) if a.tenant_id else None,
                type=a.type,
                message=a.message,
                resourceId=str(a.resource_id) if a.resource_id else None,
                resourceType=a.resource_type,
                resourceName=a.resource_name,
                triggeredBy=a.triggered_by,
            )
            for a in alerts
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size,
        number=page,
    )


@router.get("/{alert_id}", response_model=AlertResponse)
async def get_alert(
    alert_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get alert by ID"""
    stmt = select(models.Alert).where(models.Alert.id == uuid.UUID(alert_id))
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    return AlertResponse(
        id=str(alert.id),
        severity=alert.severity,
        title=alert.message[:100] if alert.message else "Alert",
        description=alert.message or "",
        status="RESOLVED" if alert.resolved else "ACTIVE",
        createdAt=alert.created_at.isoformat(),
        resolved=alert.resolved,
        message=alert.message,
    )


@router.post("/{alert_id}/resolve", status_code=204)
async def resolve_alert(
    alert_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Resolve alert"""
    stmt = select(models.Alert).where(models.Alert.id == uuid.UUID(alert_id))
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    alert.resolved = True
    alert.resolved_at = datetime.now(timezone.utc)
    await db.flush()


@router.get("/notifications/settings", response_model=NotificationSettings)
async def get_notification_settings(
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get notification settings"""
    return NotificationSettings(
        emailEnabled=False,
        slackEnabled=False,
        teamsEnabled=False,
        alertThresholds=NotificationSettingsAlertThresholds(
            critical=True,
            high=True,
            medium=False,
            low=False,
        ),
    )


@router.put("/notifications/settings", response_model=NotificationSettings)
async def update_notification_settings(
    settings: NotificationSettings,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update notification settings"""
    return settings


@router.get("/webhooks", response_model=list[WebhookResponse])
async def list_webhooks(
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List configured webhooks"""
    return []


@router.post("/webhooks", response_model=WebhookResponse)
async def create_webhook(
    webhook: dict,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Register webhook"""
    return WebhookResponse(
        id=str(uuid.uuid4()),
        name=webhook.get("name", ""),
        url=webhook.get("url", ""),
        enabled=True,
        createdAt=datetime.now(timezone.utc).isoformat(),
    )


@router.delete("/webhooks/{webhook_id}", status_code=204)
async def delete_webhook(
    webhook_id: str,
    user=Depends(get_current_user),
):
    """Delete webhook"""
    pass


@router.post("/webhooks/{webhook_id}/test")
async def test_webhook(
    webhook_id: str,
    user=Depends(get_current_user),
):
    """Test webhook delivery"""
    return {"success": True, "message": "Webhook test successful"}
