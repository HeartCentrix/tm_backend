"""
Report Configuration Endpoints

API endpoints for managing scheduled report configuration and viewing report history.
These endpoints are added to the backup-scheduler service (port 8008).
"""
import uuid
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import async_session_factory
from shared.models import ReportConfig, ReportHistory
from shared.security import get_current_user_from_token

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])


# ==================== Pydantic Schemas ====================

class WebhookConfig(BaseModel):
    name: str
    url: str
    enabled: bool = True


class ReportConfigCreate(BaseModel):
    enabled: bool = False
    schedule_type: str = Field(default="daily")  # daily, weekly, monthly
    send_empty_report: bool = True
    empty_message: Optional[str] = "No updates. No backups occurred."
    send_detailed_report: bool = False
    email_recipients: List[str] = []
    slack_webhooks: List[WebhookConfig] = []
    teams_webhooks: List[WebhookConfig] = []


class ReportConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    schedule_type: Optional[str] = None
    send_empty_report: Optional[bool] = None
    empty_message: Optional[str] = None
    send_detailed_report: Optional[bool] = None
    email_recipients: Optional[List[str]] = None
    slack_webhooks: Optional[List[WebhookConfig]] = None
    teams_webhooks: Optional[List[WebhookConfig]] = None


class ReportConfigResponse(BaseModel):
    id: str
    org_id: str
    enabled: bool
    schedule_type: str
    send_empty_report: bool
    empty_message: Optional[str]
    send_detailed_report: bool
    email_recipients: List[str]
    slack_webhooks: List[WebhookConfig]
    teams_webhooks: List[WebhookConfig]
    created_at: str
    updated_at: str

    class Config:
        from_attributes = True


class ReportHistoryResponse(BaseModel):
    id: str
    org_id: Optional[str]
    report_config_id: Optional[str]
    report_type: str
    period_start: Optional[str]
    period_end: Optional[str]
    generated_at: str
    total_backups: int
    successful_backups: int
    failed_backups: int
    success_rate: Optional[str]
    coverage_rate: Optional[str]
    is_empty: bool
    delivery_status: Optional[dict]
    error_message: Optional[str]
    created_at: str

    class Config:
        from_attributes = True


# ==================== Helper Functions ====================


def _require_org_id(current_user: dict) -> str:
    org_id = current_user.get("org_id")
    if not org_id:
        raise HTTPException(status_code=403, detail="User is not bound to an organization")
    return org_id


# ==================== Report Configuration Endpoints ====================

@router.get("/config", response_model=ReportConfigResponse)
async def get_report_config(current_user: dict = Depends(get_current_user_from_token)):
    """Get the current report configuration for the organization"""
    org_id = _require_org_id(current_user)

    async with async_session_factory() as session:
        result = await session.execute(
            select(ReportConfig).where(ReportConfig.org_id == org_id)
        )
        config = result.scalar_one_or_none()
        
        if not config:
            # Create default config
            config = ReportConfig(org_id=org_id)
            session.add(config)
            await session.commit()
            await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id),
            enabled=config.enabled,
            schedule_type=config.schedule_type,
            send_empty_report=config.send_empty_report,
            empty_message=config.empty_message,
            send_detailed_report=config.send_detailed_report,
            email_recipients=config.email_recipients or [],
            slack_webhooks=[WebhookConfig(**w) for w in (config.slack_webhooks or [])],
            teams_webhooks=[WebhookConfig(**w) for w in (config.teams_webhooks or [])],
            created_at=config.created_at.isoformat() if config.created_at else "",
            updated_at=config.updated_at.isoformat() if config.updated_at else "",
        )


@router.post("/config", response_model=ReportConfigResponse)
async def create_report_config(
    config_data: ReportConfigCreate,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Create a new report configuration"""
    org_id = _require_org_id(current_user)

    async with async_session_factory() as session:
        # Check if config already exists
        result = await session.execute(
            select(ReportConfig).where(ReportConfig.org_id == org_id)
        )
        existing = result.scalar_one_or_none()
        
        if existing:
            raise HTTPException(status_code=400, detail="Configuration already exists. Use PUT to update.")

        config = ReportConfig(
            org_id=org_id,
            enabled=config_data.enabled,
            schedule_type=config_data.schedule_type,
            send_empty_report=config_data.send_empty_report,
            empty_message=config_data.empty_message,
            send_detailed_report=config_data.send_detailed_report,
            email_recipients=config_data.email_recipients,
            slack_webhooks=[w.dict() for w in config_data.slack_webhooks],
            teams_webhooks=[w.dict() for w in config_data.teams_webhooks],
        )
        session.add(config)
        await session.commit()
        await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id),
            enabled=config.enabled,
            schedule_type=config.schedule_type,
            send_empty_report=config.send_empty_report,
            empty_message=config.empty_message,
            send_detailed_report=config.send_detailed_report,
            email_recipients=config.email_recipients or [],
            slack_webhooks=[WebhookConfig(**w) for w in (config.slack_webhooks or [])],
            teams_webhooks=[WebhookConfig(**w) for w in (config.teams_webhooks or [])],
            created_at=config.created_at.isoformat() if config.created_at else "",
            updated_at=config.updated_at.isoformat() if config.updated_at else "",
        )


@router.put("/config", response_model=ReportConfigResponse)
async def update_report_config(
    config_data: ReportConfigUpdate,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Update the report configuration"""
    org_id = _require_org_id(current_user)

    async with async_session_factory() as session:
        result = await session.execute(
            select(ReportConfig).where(ReportConfig.org_id == org_id)
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(status_code=404, detail="Configuration not found")

        # Update fields if provided
        if config_data.enabled is not None:
            config.enabled = config_data.enabled
        if config_data.schedule_type is not None:
            config.schedule_type = config_data.schedule_type
        if config_data.send_empty_report is not None:
            config.send_empty_report = config_data.send_empty_report
        if config_data.empty_message is not None:
            config.empty_message = config_data.empty_message
        if config_data.send_detailed_report is not None:
            config.send_detailed_report = config_data.send_detailed_report
        if config_data.email_recipients is not None:
            config.email_recipients = config_data.email_recipients
        if config_data.slack_webhooks is not None:
            config.slack_webhooks = [w.dict() for w in config_data.slack_webhooks]
        if config_data.teams_webhooks is not None:
            config.teams_webhooks = [w.dict() for w in config_data.teams_webhooks]

        await session.commit()
        await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id),
            enabled=config.enabled,
            schedule_type=config.schedule_type,
            send_empty_report=config.send_empty_report,
            empty_message=config.empty_message,
            send_detailed_report=config.send_detailed_report,
            email_recipients=config.email_recipients or [],
            slack_webhooks=[WebhookConfig(**w) for w in (config.slack_webhooks or [])],
            teams_webhooks=[WebhookConfig(**w) for w in (config.teams_webhooks or [])],
            created_at=config.created_at.isoformat() if config.created_at else "",
            updated_at=config.updated_at.isoformat() if config.updated_at else "",
        )


# ==================== Report History Endpoints ====================

@router.get("/history", response_model=List[ReportHistoryResponse])
async def get_report_history(
    limit: int = 50,
    offset: int = 0,
    report_type: Optional[str] = None,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Get report sending history"""
    org_id = _require_org_id(current_user)

    async with async_session_factory() as session:
        query = select(ReportHistory).where(ReportHistory.org_id == org_id)
        
        if report_type:
            query = query.where(ReportHistory.report_type == report_type.upper())
        
        query = query.order_by(desc(ReportHistory.generated_at)).limit(limit).offset(offset)
        
        result = await session.execute(query)
        reports = result.scalars().all()

        return [
            ReportHistoryResponse(
                id=str(r.id),
                org_id=str(r.org_id) if r.org_id else None,
                report_config_id=str(r.report_config_id) if r.report_config_id else None,
                report_type=r.report_type,
                period_start=r.period_start.isoformat() if r.period_start else None,
                period_end=r.period_end.isoformat() if r.period_end else None,
                generated_at=r.generated_at.isoformat() if r.generated_at else "",
                total_backups=r.total_backups,
                successful_backups=r.successful_backups,
                failed_backups=r.failed_backups,
                success_rate=r.success_rate,
                coverage_rate=r.coverage_rate,
                is_empty=r.is_empty,
                delivery_status=r.delivery_status,
                error_message=r.error_message,
                created_at=r.created_at.isoformat() if r.created_at else "",
            )
            for r in reports
        ]


@router.get("/history/{report_id}", response_model=ReportHistoryResponse)
async def get_report_history_detail(
    report_id: str,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Get detailed information about a specific report"""
    org_id = _require_org_id(current_user)

    async with async_session_factory() as session:
        result = await session.execute(
            select(ReportHistory).where(
                ReportHistory.id == report_id,
                ReportHistory.org_id == org_id
            )
        )
        report = result.scalar_one_or_none()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        return ReportHistoryResponse(
            id=str(report.id),
            org_id=str(report.org_id) if report.org_id else None,
            report_config_id=str(report.report_config_id) if report.report_config_id else None,
            report_type=report.report_type,
            period_start=report.period_start.isoformat() if report.period_start else None,
            period_end=report.period_end.isoformat() if report.period_end else None,
            generated_at=report.generated_at.isoformat() if report.generated_at else "",
            total_backups=report.total_backups,
            successful_backups=report.successful_backups,
            failed_backups=report.failed_backups,
            success_rate=report.success_rate,
            coverage_rate=report.coverage_rate,
            is_empty=report.is_empty,
            delivery_status=report.delivery_status,
            error_message=report.error_message,
            created_at=report.created_at.isoformat() if report.created_at else "",
        )
