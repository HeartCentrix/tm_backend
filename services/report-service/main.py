"""
Report Service - Scheduled Report Configuration & History
Port: 8014

Responsibilities:
- Manage report configuration (CRUD)
- Track report sending history
- Generate and send scheduled reports (called by scheduler)
- Support multiple notification endpoints (email, Slack, Teams, Google Chat)
"""
import uuid
import httpx
import smtplib
import asyncio
import csv
import io
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, desc, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import async_session_factory, init_db
from shared.models import (
    ReportConfig, ReportHistory, Organization,
    Job, Resource, Tenant, Snapshot,
    JobType, JobStatus, ResourceStatus, ResourceType
)
from shared.config import settings

app = FastAPI(title="Report Service", version="1.0.0")

# SMTP config from environment
SMTP_ENABLED = os.getenv("NOTIFICATION_EMAIL_ENABLED", "false").lower() in ("true", "1", "yes")
SMTP_HOST = os.getenv("NOTIFICATION_EMAIL_SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("NOTIFICATION_EMAIL_SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("NOTIFICATION_EMAIL_SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("NOTIFICATION_EMAIL_SMTP_PASSWORD", "")
SMTP_FROM = os.getenv("NOTIFICATION_EMAIL_FROM", "noreply@tm-vault.io")


# ==================== Pydantic Schemas ====================

class WebhookConfig(BaseModel):
    name: str
    url: str
    enabled: bool = True


class ReportConfigCreate(BaseModel):
    enabled: bool = False
    schedule_type: str = Field(default="daily")
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

async def get_org_id_from_token(token: str) -> Optional[str]:
    """Extract org_id from JWT token (simplified - use proper auth in production)"""
    async with async_session_factory() as session:
        result = await session.execute(select(Organization.id).limit(1))
        org = result.scalar_one_or_none()
        return str(org) if org else None


async def save_report_history(session, config, report_type, period_start, period_end, 
                              total_backups, successful_backups, failed_backups,
                              is_empty=False, report_data=None, delivery_status=None):
    """Save report to history table"""
    success_rate = f"{(successful_backups / total_backups * 100) if total_backups > 0 else 0:.1f}%"
    
    org_id = config.org_id if hasattr(config, 'org_id') else None
    
    history_record = ReportHistory(
        org_id=org_id,
        report_config_id=config.id,
        report_type=report_type,
        period_start=period_start,
        period_end=period_end,
        total_backups=total_backups,
        successful_backups=successful_backups,
        failed_backups=failed_backups,
        success_rate=success_rate,
        is_empty=is_empty,
        report_data=report_data,
        delivery_status=delivery_status,
    )
    session.add(history_record)
    await session.commit()
    print(f"[REPORT] Saved {report_type} report to history (empty={is_empty})")


# ==================== Report Generation ====================

async def send_report_to_channels(report: Dict[str, Any], config: ReportConfig, is_empty: bool = False, csv_bytes: Optional[bytes] = None):
    """Send report to configured notification channels"""
    delivery_status = {"email": "skipped", "slack": "skipped", "teams": "skipped"}

    if config.teams_webhooks:
        for webhook in config.teams_webhooks:
            if webhook.get("enabled"):
                result = await send_teams_webhook_notification(report, webhook["url"], is_empty, config.empty_message)
                delivery_status["teams"] = "sent" if result else "failed"

    if config.slack_webhooks:
        for webhook in config.slack_webhooks:
            if webhook.get("enabled"):
                result = await send_slack_webhook_notification(report, webhook["url"], is_empty, config.empty_message)
                delivery_status["slack"] = "sent" if result else "failed"

    if config.email_recipients:
        result = await send_email_notification(report, config.email_recipients, is_empty, config.empty_message, csv_bytes)
        delivery_status["email"] = "sent" if result else "failed"

    return delivery_status


async def send_teams_webhook_notification(report: Dict[str, Any], webhook_url: str, is_empty: bool, empty_message: str):
    """Send report to Teams webhook"""
    try:
        period = report.get("period", "")
        if is_empty:
            message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": "Backup Report",
                "themeColor": "FFA500",
                "title": f"Backup report ({period}): No backups occurred",
                "sections": [{"text": empty_message or "No updates. No backups occurred."}],
            }
        else:
            summary = report.get("summary", {})
            breakdown = report.get("resource_breakdown", {})
            domain = report.get("tenant_domain", "")

            breakdown_facts = [{"name": label, "value": f"protected {v['protected']} out of {v['total']}"} for label, v in breakdown.items()]
            breakdown_facts.append({"name": "Total", "value": f"{summary.get('protected_resources', 0)} out of {summary.get('total_resources', 0)} resources protected"})
            breakdown_facts.append({"name": "Backup storage size", "value": f"{summary.get('storage_gb', 0)} GB"})

            message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": "Backup Report",
                "themeColor": "0078D4",
                "title": f"Backup report ({period}): Everything looks good",
                "sections": [
                    {
                        "activityTitle": "Overview",
                        "facts": [{"name": "Microsoft 365 domain", "value": domain}] + breakdown_facts,
                    },
                    {
                        "activityTitle": "Backup and recovery details",
                        "facts": [
                            {"name": "Backup", "value": f"{summary.get('successful_backups', 0)} succeeded"},
                            {"name": "Discover", "value": f"{summary.get('successful_discoveries', 0)} succeeded"},
                        ],
                    },
                ],
            }

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(webhook_url, json=message)
            return True
    except Exception as e:
        print(f"[REPORT] Failed to send Teams notification: {e}")
        return False


async def send_slack_webhook_notification(report: Dict[str, Any], webhook_url: str, is_empty: bool, empty_message: str):
    """Send report to Slack webhook"""
    try:
        period = report.get("period", "")
        if is_empty:
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"Backup report ({period}): No backups occurred"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": empty_message or "No updates. No backups occurred."}},
            ]
        else:
            summary = report.get("summary", {})
            breakdown = report.get("resource_breakdown", {})
            domain = report.get("tenant_domain", "")

            breakdown_lines = "\n".join([f"• {label}: protected {v['protected']} out of {v['total']}" for label, v in breakdown.items()])
            breakdown_lines += f"\n• Total: {summary.get('protected_resources', 0)} out of {summary.get('total_resources', 0)} resources protected"
            breakdown_lines += f"\n• Backup storage size: {summary.get('storage_gb', 0)} GB"

            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"Backup report ({period}): Everything looks good"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Overview*\n*Microsoft 365 domain:* {domain}\n{breakdown_lines}"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Backup and recovery details*\n• Backup: {summary.get('successful_backups', 0)} succeeded\n• Discover: {summary.get('successful_discoveries', 0)} succeeded"}},
            ]

        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(webhook_url, json={"blocks": blocks})
            return True
    except Exception as e:
        print(f"[REPORT] Failed to send Slack notification: {e}")
        return False


async def send_email_notification(report: Dict[str, Any], recipients: List[str], is_empty: bool, empty_message: str, csv_bytes: Optional[bytes] = None):
    """Send report via email using SMTP"""
    if not recipients:
        return False

    if not SMTP_ENABLED:
        print(f"[REPORT] Email notification skipped (NOTIFICATION_EMAIL_ENABLED=false)")
        return False

    if not SMTP_USERNAME or not SMTP_PASSWORD:
        print(f"[REPORT] Email notification skipped (SMTP credentials not configured)")
        return False

    def _send():
        period = report.get("period", "")
        if is_empty:
            subject = f"Backup report ({period}): No backups occurred"
            body_html = f"<p style='font-family:sans-serif'>{empty_message or 'No updates. No backups occurred.'}</p>"
        else:
            summary = report.get("summary", {})
            breakdown = report.get("resource_breakdown", {})
            domain = report.get("tenant_domain", "")
            subject = f"Backup report ({period}): Everything looks good"

            breakdown_rows = "".join([
                f"<tr><td style='padding:4px 12px;color:#555'>{label}</td>"
                f"<td style='padding:4px 12px'>protected {v['protected']} out of {v['total']}</td></tr>"
                for label, v in breakdown.items()
            ])
            breakdown_rows += (
                f"<tr><td style='padding:4px 12px;font-weight:bold'>Total</td>"
                f"<td style='padding:4px 12px;font-weight:bold'>{summary.get('protected_resources', 0)} out of {summary.get('total_resources', 0)} resources protected</td></tr>"
                f"<tr><td style='padding:4px 12px;color:#555'>Backup storage size</td>"
                f"<td style='padding:4px 12px'>{summary.get('storage_gb', 0)} GB</td></tr>"
            )

            body_html = f"""
<div style='font-family:sans-serif;font-size:14px;color:#1a1a1a;max-width:600px'>
  <h2 style='color:#0d9488'>Backup report ({period}): Everything looks good</h2>

  <h3 style='margin-top:24px;border-bottom:1px solid #e2e8f0;padding-bottom:6px'>Overview</h3>
  <table style='border-collapse:collapse;width:100%'>
    <tr><td style='padding:4px 12px;color:#555'>Microsoft 365 domain</td><td style='padding:4px 12px'>{domain}</td></tr>
    {breakdown_rows}
  </table>

  <h3 style='margin-top:24px;border-bottom:1px solid #e2e8f0;padding-bottom:6px'>Backup and recovery details</h3>
  <table style='border-collapse:collapse;width:100%'>
    <tr><td style='padding:4px 12px;color:#555'>Backup</td><td style='padding:4px 12px'>{summary.get('successful_backups', 0)} succeeded</td></tr>
    <tr><td style='padding:4px 12px;color:#555'>Discover</td><td style='padding:4px 12px'>{summary.get('successful_discoveries', 0)} succeeded</td></tr>
  </table>

  <p style='margin-top:32px;color:#555'>Sincerely,<br><strong>The TM Vault Team</strong></p>
</div>
"""

        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = SMTP_FROM
        msg["To"] = ", ".join(recipients)
        msg.attach(MIMEText(body_html, "html"))

        if csv_bytes:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(csv_bytes)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="backup_details.csv"')
            msg.attach(part)

        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as server:
                server.ehlo()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.sendmail(SMTP_FROM, recipients, msg.as_string())
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.sendmail(SMTP_FROM, recipients, msg.as_string())

    try:
        await asyncio.to_thread(_send)
        print(f"[REPORT] Email sent to {len(recipients)} recipients")
        return True
    except Exception as e:
        print(f"[REPORT] Failed to send email: {e}")
        return False


# ==================== API Endpoints ====================

@app.get("/health")
async def health():
    return {"status": "ok", "service": "report-service"}


@app.get("/api/v1/reports/config", response_model=ReportConfigResponse)
async def get_report_config():
    """Get the current report configuration"""
    async with async_session_factory() as session:
        result = await session.execute(select(ReportConfig).limit(1))
        config = result.scalar_one_or_none()
        
        if not config:
            config = ReportConfig(org_id=None)
            session.add(config)
            await session.commit()
            await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id) if config.org_id else "",
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


@app.post("/api/v1/reports/config", response_model=ReportConfigResponse)
async def create_report_config(config_data: ReportConfigCreate):
    """Create a new report configuration"""
    async with async_session_factory() as session:
        result = await session.execute(select(ReportConfig).limit(1))
        existing = result.scalar_one_or_none()
        
        if existing:
            raise HTTPException(status_code=400, detail="Configuration already exists. Use PUT to update.")

        config = ReportConfig(
            org_id=None,
            enabled=config_data.enabled,
            schedule_type=config_data.schedule_type,
            send_empty_report=config_data.send_empty_report,
            empty_message=config_data.empty_message,
            send_detailed_report=config_data.send_detailed_report,
            email_recipients=config_data.email_recipients,
            slack_webhooks=[w.model_dump() for w in config_data.slack_webhooks],
            teams_webhooks=[w.model_dump() for w in config_data.teams_webhooks],
        )
        session.add(config)
        await session.commit()
        await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id) if config.org_id else "",
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


@app.put("/api/v1/reports/config", response_model=ReportConfigResponse)
async def update_report_config(config_data: ReportConfigUpdate):
    """Update the report configuration"""
    async with async_session_factory() as session:
        result = await session.execute(select(ReportConfig).limit(1))
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(status_code=404, detail="Configuration not found")

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
            config.slack_webhooks = [w.model_dump() for w in config_data.slack_webhooks]
        if config_data.teams_webhooks is not None:
            config.teams_webhooks = [w.model_dump() for w in config_data.teams_webhooks]

        await session.commit()
        await session.refresh(config)

        return ReportConfigResponse(
            id=str(config.id),
            org_id=str(config.org_id) if config.org_id else "",
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


@app.get("/api/v1/reports/history", response_model=List[ReportHistoryResponse])
async def get_report_history(
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    report_type: Optional[str] = None
):
    """Get report sending history"""
    async with async_session_factory() as session:
        query = select(ReportHistory).order_by(desc(ReportHistory.generated_at))
        
        if report_type:
            query = query.where(ReportHistory.report_type == report_type.upper())
        
        query = query.limit(limit).offset(offset)
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


@app.get("/api/v1/reports/history/{report_id}", response_model=ReportHistoryResponse)
async def get_report_history_detail(report_id: str):
    """Get detailed information about a specific report"""
    async with async_session_factory() as session:
        result = await session.execute(select(ReportHistory).where(ReportHistory.id == report_id))
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


# ==================== Report Generation Endpoints (called by scheduler) ====================

class GenerateReportRequest(BaseModel):
    report_type: str  # DAILY, WEEKLY, MONTHLY


class GenerateReportResponse(BaseModel):
    success: bool
    report_id: Optional[str]
    message: str


@app.post("/api/v1/reports/generate", response_model=GenerateReportResponse)
async def generate_report(request: GenerateReportRequest):
    """Generate and send a report (called by scheduler on schedule)"""
    print(f"[REPORT] Generating {request.report_type} report...")

    async with async_session_factory() as session:
        config_result = await session.execute(select(ReportConfig).limit(1))
        config = config_result.scalar_one_or_none()
        
        if not config or not config.enabled:
            print("[REPORT] Reports are not enabled. Skipping.")
            return GenerateReportResponse(success=False, report_id=None, message="Reports not enabled")

        now = datetime.utcnow()
        
        if request.report_type == "DAILY":
            period_start = now - timedelta(days=1)
            period_end = now
        elif request.report_type == "WEEKLY":
            period_start = now - timedelta(days=7)
            period_end = now
        elif request.report_type == "MONTHLY":
            period_start = now - timedelta(days=30)
            period_end = now
        else:
            return GenerateReportResponse(success=False, report_id=None, message=f"Unknown report type: {request.report_type}")

        total_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.created_at >= period_start,
                    Job.created_at <= period_end,
                )
            )
        )
        total_backups = total_backups_result.scalar() or 0

        successful_backups_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.BACKUP,
                    Job.status == JobStatus.COMPLETED,
                    Job.created_at >= period_start,
                    Job.created_at <= period_end,
                )
            )
        )
        successful_backups = successful_backups_result.scalar() or 0

        failed_backups = total_backups - successful_backups
        is_empty = total_backups == 0
        
        if is_empty and not config.send_empty_report:
            await save_report_history(
                session, config, request.report_type, period_start, period_end,
                total_backups, successful_backups, failed_backups,
                is_empty=True, report_data={"message": config.empty_message}
            )
            return GenerateReportResponse(success=True, report_id=None, message="Empty report skipped")

        # Per-resource-type breakdown
        resource_type_groups = {
            "Users": [ResourceType.MAILBOX, ResourceType.SHARED_MAILBOX],
            "SharePoint Sites": [ResourceType.SHAREPOINT_SITE],
            "Entra ID": [ResourceType.ENTRA_USER, ResourceType.ENTRA_GROUP, ResourceType.ENTRA_APP, ResourceType.ENTRA_SERVICE_PRINCIPAL, ResourceType.ENTRA_DEVICE],
            "Microsoft 365 Groups & Teams": [ResourceType.TEAMS_CHANNEL, ResourceType.TEAMS_CHAT],
        }

        resource_breakdown = {}
        total_resources = 0
        protected_resources = 0
        for label, types in resource_type_groups.items():
            total_q = await session.execute(
                select(func.count(Resource.id)).where(
                    and_(Resource.status == ResourceStatus.ACTIVE, Resource.type.in_(types))
                )
            )
            protected_q = await session.execute(
                select(func.count(Resource.id)).where(
                    and_(
                        Resource.status == ResourceStatus.ACTIVE,
                        Resource.type.in_(types),
                        Resource.last_backup_at >= period_start,
                        Resource.last_backup_status == "COMPLETED",
                    )
                )
            )
            t = total_q.scalar() or 0
            p = protected_q.scalar() or 0
            resource_breakdown[label] = {"protected": p, "total": t}
            total_resources += t
            protected_resources += p

        coverage_pct = (protected_resources / total_resources * 100) if total_resources > 0 else 0

        # Total backup storage
        storage_result = await session.execute(select(func.sum(Resource.storage_bytes)))
        total_storage_bytes = storage_result.scalar() or 0
        storage_gb = round(total_storage_bytes / (1024 ** 3), 2)

        # Discovery job count
        discovery_result = await session.execute(
            select(func.count(Job.id)).where(
                and_(
                    Job.type == JobType.DISCOVERY,
                    Job.status == JobStatus.COMPLETED,
                    Job.created_at >= period_start,
                    Job.created_at <= period_end,
                )
            )
        )
        successful_discoveries = discovery_result.scalar() or 0

        # Tenant domain from resource email
        domain_result = await session.execute(
            select(Resource.email).where(Resource.email.isnot(None), Resource.email.contains("@")).limit(1)
        )
        email = domain_result.scalar() or ""
        tenant_domain = email.split("@")[1] if "@" in email else ""

        period_label = f"{period_start.strftime('%b %-d')} - {period_end.strftime('%b %-d, %Y')}"

        report = {
            "report_type": f"{request.report_type}_BACKUP_REPORT",
            "generated_at": now.isoformat(),
            "period": period_label,
            "tenant_domain": tenant_domain,
            "summary": {
                "total_backups": total_backups,
                "successful_backups": successful_backups,
                "failed_backups": failed_backups,
                "successful_discoveries": successful_discoveries,
                "total_resources": total_resources,
                "protected_resources": protected_resources,
                "coverage_rate": f"{coverage_pct:.1f}%",
                "storage_gb": storage_gb,
            },
            "resource_breakdown": resource_breakdown,
        }

        if not is_empty and config.send_detailed_report:
            detail_result = await session.execute(
                select(Resource.display_name, Job.completed_at, Snapshot.bytes_total)
                .join(Resource, Job.resource_id == Resource.id)
                .outerjoin(Snapshot, Job.snapshot_id == Snapshot.id)
                .where(
                    and_(
                        Job.type == JobType.BACKUP,
                        Job.status == JobStatus.COMPLETED,
                        Job.created_at >= period_start,
                        Job.created_at <= period_end,
                    )
                )
                .order_by(Job.completed_at)
            )
            detail_rows = detail_result.all()
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["Resource Name", "Backup Time", "Size (bytes)"])
            for row in detail_rows:
                writer.writerow([row[0], row[1].isoformat() if row[1] else "", row[2] or 0])
            csv_bytes = buf.getvalue().encode("utf-8")
            delivery_status = await send_report_to_channels(report, config, is_empty, csv_bytes)
        else:
            delivery_status = await send_report_to_channels(report, config, is_empty)

        history_record = ReportHistory(
            org_id=config.org_id,
            report_config_id=config.id,
            report_type=request.report_type,
            period_start=period_start,
            period_end=period_end,
            total_backups=total_backups,
            successful_backups=successful_backups,
            failed_backups=failed_backups,
            success_rate=f"{(successful_backups / total_backups * 100) if total_backups > 0 else 0:.1f}%",
            coverage_rate=f"{coverage_pct:.1f}%",
            is_empty=is_empty,
            report_data=report,
            delivery_status=delivery_status,
        )
        session.add(history_record)
        await session.commit()

        print(f"[REPORT] {request.report_type} report generated: {history_record.id}")
        return GenerateReportResponse(success=True, report_id=str(history_record.id), message="Report generated successfully")


@app.on_event("startup")
async def startup():
    await init_db()
    print("[REPORT] Report service started")
