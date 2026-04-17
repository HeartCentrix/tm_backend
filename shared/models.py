"""Shared database models"""
import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, DateTime, Boolean, Integer, BigInteger,
    Text, ForeignKey, Enum as SAEnum, JSON, ARRAY, func, LargeBinary
)
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import UUID
import enum

from shared.database import Base


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class UserRole(str, enum.Enum):
    SUPER_ADMIN = "SUPER_ADMIN"
    ORG_ADMIN = "ORG_ADMIN"
    TENANT_ADMIN = "TENANT_ADMIN"
    BACKUP_OPERATOR = "BACKUP_OPERATOR"
    RESTORE_OPERATOR = "RESTORE_OPERATOR"
    CONTENT_VIEWER = "CONTENT_VIEWER"
    USER = "USER"


class TenantType(str, enum.Enum):
    M365 = "M365"
    AZURE = "AZURE"
    # Legacy 'BOTH' removed. A tenant is now exactly one workload type; to back up
    # M365 + Azure for the same Microsoft tenant, create two tenant rows.


class TenantStatus(str, enum.Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    DISCONNECTED = "DISCONNECTED"
    SUSPENDED = "SUSPENDED"
    PENDING_DELETION = "PENDING_DELETION"
    DISCOVERING = "DISCOVERING"
    PENDING_DISCOVERY = "PENDING_DISCOVERY"  # Tenant saved but discovery not yet enqueued


class ResourceType(str, enum.Enum):
    MAILBOX = "MAILBOX"
    SHARED_MAILBOX = "SHARED_MAILBOX"
    ROOM_MAILBOX = "ROOM_MAILBOX"
    ONEDRIVE = "ONEDRIVE"
    SHAREPOINT_SITE = "SHAREPOINT_SITE"
    TEAMS_CHANNEL = "TEAMS_CHANNEL"
    TEAMS_CHAT = "TEAMS_CHAT"
    ENTRA_USER = "ENTRA_USER"
    ENTRA_GROUP = "ENTRA_GROUP"
    ENTRA_APP = "ENTRA_APP"
    ENTRA_SERVICE_PRINCIPAL = "ENTRA_SERVICE_PRINCIPAL"
    ENTRA_DEVICE = "ENTRA_DEVICE"
    ENTRA_ROLE = "ENTRA_ROLE"
    ENTRA_ADMIN_UNIT = "ENTRA_ADMIN_UNIT"
    ENTRA_AUDIT_LOG = "ENTRA_AUDIT_LOG"
    INTUNE_MANAGED_DEVICE = "INTUNE_MANAGED_DEVICE"
    AZURE_VM = "AZURE_VM"
    AZURE_SQL_DB = "AZURE_SQL_DB"
    AZURE_POSTGRESQL = "AZURE_POSTGRESQL"
    AZURE_POSTGRESQL_SINGLE = "AZURE_POSTGRESQL_SINGLE"
    RESOURCE_GROUP = "RESOURCE_GROUP"
    DYNAMIC_GROUP = "DYNAMIC_GROUP"
    POWER_BI = "POWER_BI"
    POWER_APPS = "POWER_APPS"
    POWER_AUTOMATE = "POWER_AUTOMATE"
    POWER_DLP = "POWER_DLP"
    COPILOT = "COPILOT"
    PLANNER = "PLANNER"
    TODO = "TODO"
    ONENOTE = "ONENOTE"


class ResourceStatus(str, enum.Enum):
    DISCOVERED = "DISCOVERED"
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"
    SUSPENDED = "SUSPENDED"
    PENDING_DELETION = "PENDING_DELETION"
    INACCESSIBLE = "INACCESSIBLE"  # Resource not found (404) or locked (423) in source system


class JobType(str, enum.Enum):
    BACKUP = "BACKUP"
    RESTORE = "RESTORE"
    EXPORT = "EXPORT"
    DISCOVERY = "DISCOVERY"
    DELETE = "DELETE"


class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RETRYING = "RETRYING"


class SnapshotType(str, enum.Enum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    PREEMPTIVE = "PREEMPTIVE"
    MANUAL = "MANUAL"


class SnapshotStatus(str, enum.Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    PENDING_DELETION = "PENDING_DELETION"


class Organization(Base):
    __tablename__ = "organizations"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    slug = Column(String, unique=True, nullable=False)
    storage_region = Column(String)
    encryption_mode = Column(String, default="TMVAULT_MANAGED")
    storage_quota_bytes = Column(BigInteger, default=500 * 1024**3)
    storage_bytes_used = Column(BigInteger, default=0)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class Tenant(Base):
    __tablename__ = "tenants"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    type = Column(SAEnum(TenantType), default=TenantType.M365)
    display_name = Column(String, nullable=False)
    external_tenant_id = Column(String, unique=True, index=True)

    customer_id = Column(String)
    subscription_id = Column(String)
    client_id = Column(String)
    client_secret_ref = Column(String)
    # Graph API app-only credentials (encrypted)
    graph_client_id = Column(String, nullable=True)
    graph_client_secret_encrypted = Column(LargeBinary, nullable=True)
    status = Column(SAEnum(TenantStatus), default=TenantStatus.PENDING)
    storage_region = Column(String)
    last_discovery_at = Column(DateTime)
    graph_delta_tokens = Column(MutableDict.as_mutable(JSON), default=dict)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # AZ-4: Cross-region DR replication fields
    dr_region_enabled = Column(Boolean, default=False, nullable=False)
    dr_region = Column(String, nullable=True)  # e.g., "westeurope"
    dr_storage_account_name = Column(String, nullable=True)
    dr_storage_account_key_encrypted = Column(LargeBinary, nullable=True)
    dr_last_replicated_at = Column(DateTime, nullable=True)

    # Afi.ai-style Azure onboarding fields
    azure_refresh_token_encrypted = Column(LargeBinary, nullable=True)
    azure_refresh_token_updated_at = Column(DateTime(timezone=True), nullable=True)
    azure_subscriptions_cached = Column(JSON, default=dict, nullable=False)
    azure_sql_servers_configured = Column(JSON, default=dict, nullable=False)
    azure_pg_servers_configured = Column(JSON, default=dict, nullable=False)
    extra_data = Column(MutableDict.as_mutable(JSON), default=dict, nullable=True)


class PlatformUser(Base):
    __tablename__ = "platform_users"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    external_user_id = Column(String)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"))
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"))
    mfa_enabled = Column(Boolean, default=False)
    last_login_at = Column(DateTime)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class UserRoleMapping(Base):
    __tablename__ = "user_roles"
    user_id = Column(UUID(as_uuid=True), ForeignKey("platform_users.id"), primary_key=True)
    role = Column(SAEnum(UserRole), primary_key=True)


class Resource(Base):
    __tablename__ = "resources"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    type = Column(SAEnum(ResourceType), nullable=False)
    external_id = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    email = Column(String)
    extra_data = Column("metadata", MutableDict.as_mutable(JSON), default=dict)
    resource_hash = Column(String, nullable=True)
    sla_policy_id = Column(UUID(as_uuid=True), ForeignKey("sla_policies.id"))
    status = Column(SAEnum(ResourceStatus), default=ResourceStatus.DISCOVERED)
    last_backup_job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))
    last_backup_at = Column(DateTime)
    last_backup_status = Column(String)
    storage_bytes = Column(BigInteger, default=0)
    discovered_at = Column(DateTime, default=utcnow)
    archived_at = Column(DateTime)
    deletion_queued_at = Column(DateTime)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # Azure workload metadata (for VM, SQL, PostgreSQL)
    azure_subscription_id = Column(String, nullable=True)
    azure_resource_group = Column(String, nullable=True)
    azure_region = Column(String, nullable=True)


class SlaPolicy(Base):
    __tablename__ = "sla_policies"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    service_type = Column(String, default="m365", nullable=False, index=True)
    name = Column(String, nullable=False)
    frequency = Column(String, default="DAILY")
    backup_days = Column(ARRAY(String), default=["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"])
    backup_window_start = Column(String)
    backup_window_end = Column(String)
    backup_exchange = Column(Boolean, default=True)
    backup_exchange_archive = Column(Boolean, default=False)
    backup_exchange_recoverable = Column(Boolean, default=False)
    backup_onedrive = Column(Boolean, default=True)
    backup_sharepoint = Column(Boolean, default=True)
    backup_teams = Column(Boolean, default=True)
    backup_teams_chats = Column(Boolean, default=False)
    backup_entra_id = Column(Boolean, default=True)
    backup_power_platform = Column(Boolean, default=False)
    backup_copilot = Column(Boolean, default=False)
    contacts = Column(Boolean, default=True)
    calendars = Column(Boolean, default=True)
    tasks = Column(Boolean, default=False)
    group_mailbox = Column(Boolean, default=True)
    planner = Column(Boolean, default=False)
    backup_azure_vm = Column(Boolean, default=True)
    backup_azure_sql = Column(Boolean, default=True)
    backup_azure_postgresql = Column(Boolean, default=True)
    resource_types = Column(ARRAY(String), default=[])
    batch_size = Column(Integer, default=20)
    max_concurrent_backups = Column(Integer, default=50)
    sla_violation_alert = Column(Boolean, default=True)
    retention_type = Column(String, default="INDEFINITE")
    retention_days = Column(Integer)
    retention_versions = Column(Integer)
    # AZ-0: Tiered retention policy (Hot → Cool → Archive → Delete)
    retention_hot_days = Column(Integer, default=7, nullable=False)
    retention_cool_days = Column(Integer, default=30, nullable=False)
    retention_archive_days = Column(Integer, nullable=True)  # NULL = unlimited (no delete rule)
    legal_hold_enabled = Column(Boolean, default=False, nullable=False)
    legal_hold_until = Column(DateTime, nullable=True)
    immutability_mode = Column(String, default="None", nullable=False)  # "None", "Unlocked", "Locked"
    enabled = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class Job(Base):
    __tablename__ = "jobs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(SAEnum(JobType), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id"))
    batch_resource_ids = Column(ARRAY(UUID(as_uuid=True)), default=[])  # NEW: for mass backup
    snapshot_id = Column(UUID(as_uuid=True), ForeignKey("snapshots.id"))
    status = Column(SAEnum(JobStatus), default=JobStatus.QUEUED)
    priority = Column(Integer, default=5)
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=5)
    error_message = Column(Text)
    progress_pct = Column(Integer, default=0)
    items_processed = Column(BigInteger, default=0)
    bytes_processed = Column(BigInteger, default=0)
    result = Column(JSON, default=dict)
    spec = Column(JSON, default=dict)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)
    completed_at = Column(DateTime)


class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id"), nullable=False, index=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))
    type = Column(SAEnum(SnapshotType), default=SnapshotType.INCREMENTAL)
    status = Column(SAEnum(SnapshotStatus), default=SnapshotStatus.IN_PROGRESS)
    started_at = Column(DateTime, default=utcnow)
    completed_at = Column(DateTime)
    duration_secs = Column(Integer)
    item_count = Column(Integer, default=0)
    new_item_count = Column(Integer, default=0)
    bytes_added = Column(BigInteger, default=0)
    bytes_total = Column(BigInteger, default=0)
    delta_token = Column(String)
    delta_tokens_json = Column(MutableDict.as_mutable(JSON), default=dict)  # per-folder/resource delta tokens
    extra_data = Column(MutableDict.as_mutable(JSON), default=dict)  # VM backup metadata (config blobs, disk info, etc.)
    snapshot_label = Column(String)
    content_checksum = Column(String)  # NEW: SHA-256 of stored blob
    blob_path = Column(String)  # NEW: full Azure Blob path
    storage_version = Column(Integer, default=1)  # NEW: storage schema version
    azure_restore_point_id = Column(String, nullable=True)  # VM restore point ID for restore
    azure_operation_id = Column(String, nullable=True)  # Track in-flight Azure LROs for resume
    # AZ-4: Cross-region DR replication fields
    dr_replication_status = Column(String, default="pending", nullable=False)  # "pending", "in_progress", "replicated", "failed", "skipped"
    dr_blob_path = Column(String, nullable=True)
    dr_replicated_at = Column(DateTime, nullable=True)
    dr_error = Column(Text, nullable=True)
    dr_replication_attempts = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=utcnow)


class SnapshotItem(Base):
    __tablename__ = "snapshot_items"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_id = Column(UUID(as_uuid=True), ForeignKey("snapshots.id"), nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    external_id = Column(String, nullable=False)
    item_type = Column(String, nullable=False)
    name = Column(String, nullable=False)
    folder_path = Column(String)
    content_hash = Column(String, index=True)
    content_checksum = Column(String)  # NEW: SHA-256 integrity checksum
    content_size = Column(BigInteger, default=0)
    blob_path = Column(String)  # NEW: Azure Blob path for this item
    encryption_key_id = Column(String)  # NEW: DEK version used
    backup_version = Column(Integer, default=1)  # NEW: backup schema version
    extra_data = Column("metadata", JSON, default=dict)
    is_deleted = Column(Boolean, default=False)
    indexed_at = Column(DateTime)
    created_at = Column(DateTime, default=utcnow)


class JobLog(Base):
    __tablename__ = "job_logs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False, index=True)
    timestamp = Column(DateTime, default=utcnow)
    level = Column(String, default="INFO")
    message = Column(Text, nullable=False)
    details = Column(Text)


class Alert(Base):
    __tablename__ = "alerts"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"))
    type = Column(String, nullable=False)
    severity = Column(String, nullable=False, default="MEDIUM")
    message = Column(Text, nullable=False)
    resource_id = Column(UUID(as_uuid=True))
    resource_type = Column(String)
    resource_name = Column(String)
    triggered_by = Column(String)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime)
    resolved_by = Column(UUID(as_uuid=True))
    resolution_note = Column(Text)
    details = Column(JSON, default=dict)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class AccessGroup(Base):
    __tablename__ = "access_groups"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"))
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"))
    name = Column(String, nullable=False)
    description = Column(String)
    scope = Column(String, default="TENANT")
    resource_ids = Column(ARRAY(UUID(as_uuid=True)), default=list)
    permissions = Column(JSON, default=dict)
    member_ids = Column(ARRAY(UUID(as_uuid=True)), default=list)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class AuditEvent(Base):
    __tablename__ = "audit_events"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)

    # Actor who triggered the action
    actor_id = Column(UUID(as_uuid=True))
    actor_email = Column(String)
    actor_type = Column(String, default="SYSTEM")  # USER | SYSTEM | WORKER

    # Action details
    action = Column(String, nullable=False, index=True)  # BACKUP_COMPLETED, etc.
    resource_id = Column(UUID(as_uuid=True))
    resource_type = Column(String)  # MAILBOX, ONEDRIVE, etc.
    resource_name = Column(String)
    outcome = Column(String, default="SUCCESS")  # SUCCESS | FAILURE | PARTIAL

    # Job/snapshot references
    job_id = Column(UUID(as_uuid=True))
    snapshot_id = Column(UUID(as_uuid=True))

    # Extended details (JSONB for flexibility)
    details = Column(JSON, default=dict)

    occurred_at = Column(DateTime, default=utcnow, index=True, nullable=False)


class AdminConsentToken(Base):
    __tablename__ = "admin_consent_tokens"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    
    # Consent type: M365 or AZURE
    consent_type = Column(String, nullable=False, index=True)
    
    # Encrypted tokens
    access_token_encrypted = Column(LargeBinary, nullable=True)
    refresh_token_encrypted = Column(LargeBinary, nullable=True)
    token_type = Column(String, default="Bearer")
    expires_at = Column(DateTime, nullable=True)
    
    # Metadata
    granted_by = Column(String, nullable=True)  # Email of user who granted consent
    consented_at = Column(DateTime, default=utcnow)
    last_used_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True, index=True)
    scope = Column(String, nullable=True)  # Space-separated list of scopes
    
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class DiscoveryRun(Base):
    __tablename__ = "discovery_runs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    scope = Column(JSON, default=list, nullable=False)
    status = Column(String, default="RUNNING", nullable=False, index=True)
    fetched_count = Column(Integer, default=0, nullable=False)
    staged_count = Column(Integer, default=0, nullable=False)
    inserted_count = Column(Integer, default=0, nullable=False)
    updated_count = Column(Integer, default=0, nullable=False)
    unchanged_count = Column(Integer, default=0, nullable=False)
    stale_marked_count = Column(Integer, default=0, nullable=False)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=utcnow, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class ResourceDiscoveryStaging(Base):
    __tablename__ = "resource_discovery_staging"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(UUID(as_uuid=True), ForeignKey("discovery_runs.id"), nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    resource_type = Column(String, nullable=False)
    external_id = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    email = Column(String, nullable=True)
    extra_data = Column("metadata", MutableDict.as_mutable(JSON), default=dict)
    resource_status = Column(String, default="DISCOVERED", nullable=False)
    resource_hash = Column(String, nullable=True)
    azure_subscription_id = Column(String, nullable=True)
    azure_resource_group = Column(String, nullable=True)
    azure_region = Column(String, nullable=True)
    discovered_at = Column(DateTime, default=utcnow, nullable=False)
    created_at = Column(DateTime, default=utcnow)


class ReportConfig(Base):
    __tablename__ = "report_configs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), index=True, nullable=True)

    # Schedule settings
    enabled = Column(Boolean, default=False, nullable=False)
    schedule_type = Column(String, default="daily", nullable=False)  # daily, weekly, monthly

    # Empty report handling
    send_empty_report = Column(Boolean, default=True, nullable=False)
    empty_message = Column(String, default="No updates. No backups occurred.", nullable=True)
    send_detailed_report = Column(Boolean, default=False, nullable=False)

    # Notification endpoints (stored as JSON arrays)
    email_recipients = Column(JSON, default=list, nullable=True)
    slack_webhooks = Column(JSON, default=list, nullable=True)
    teams_webhooks = Column(JSON, default=list, nullable=True)

    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class ReportHistory(Base):
    __tablename__ = "report_history"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), index=True)
    report_config_id = Column(UUID(as_uuid=True), ForeignKey("report_configs.id"), nullable=True)

    # Report metadata
    report_type = Column(String, nullable=False)  # DAILY, WEEKLY, MONTHLY
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=True)
    generated_at = Column(DateTime, default=utcnow, nullable=False)

    # Report content summary
    total_backups = Column(Integer, default=0)
    successful_backups = Column(Integer, default=0)
    failed_backups = Column(Integer, default=0)
    success_rate = Column(String, nullable=True)
    coverage_rate = Column(String, nullable=True)

    # Full report data (JSON)
    report_data = Column(JSON, default=dict, nullable=True)
    is_empty = Column(Boolean, default=False, nullable=False)

    # Delivery status
    delivery_status = Column(JSON, default=dict, nullable=True)

    # Error tracking
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=utcnow)
