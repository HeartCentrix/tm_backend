"""Database models"""
import uuid
from datetime import datetime
from sqlalchemy import (
    Column, String, DateTime, Boolean, Integer, BigInteger, 
    Text, ForeignKey, Enum as SAEnum, JSON, ARRAY
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import enum

from app.db.database import Base


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
    BOTH = "BOTH"


class TenantStatus(str, enum.Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    DISCONNECTED = "DISCONNECTED"
    SUSPENDED = "SUSPENDED"
    PENDING_DELETION = "PENDING_DELETION"
    DISCOVERING = "DISCOVERING"


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
    ENTRA_DEVICE = "ENTRA_DEVICE"
    AZURE_VM = "AZURE_VM"
    AZURE_SQL_DB = "AZURE_SQL_DB"
    AZURE_POSTGRESQL = "AZURE_POSTGRESQL"
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


class JobType(str, enum.Enum):
    BACKUP_SYNC = "BACKUP"
    RESTORE = "RESTORE"
    EXPORT = "EXPORT"
    DISCOVERY = "DISCOVERY"
    M365_DISCOVERY = "DISCOVERY"
    AZURE_DISCOVERY = "DISCOVERY"
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
    RUNNING = "IN_PROGRESS"
    COMPLETE = "COMPLETED"
    FAILED = "FAILED"
    PARTIAL = "COMPLETED"
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
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    tenants = relationship("Tenant", back_populates="organization")
    users = relationship("PlatformUser", back_populates="organization")


class Tenant(Base):
    __tablename__ = "tenants"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    type = Column(SAEnum(TenantType), default=TenantType.M365)
    display_name = Column(String, nullable=False)
    external_tenant_id = Column(String)
    subscription_id = Column(String)
    client_id = Column(String)
    client_secret_ref = Column(String)
    status = Column(SAEnum(TenantStatus), default=TenantStatus.PENDING)
    storage_region = Column(String)
    last_discovery_at = Column(DateTime)
    graph_delta_tokens = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization", back_populates="tenants")
    resources = relationship("Resource", back_populates="tenant")
    jobs = relationship("Job", back_populates="tenant")
    sla_policies = relationship("SlaPolicy", back_populates="tenant")


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
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization", back_populates="users")
    roles = relationship("UserRoleMapping", back_populates="user")


class UserRoleMapping(Base):
    __tablename__ = "user_roles"
    
    user_id = Column(UUID(as_uuid=True), ForeignKey("platform_users.id"), primary_key=True)
    role = Column(SAEnum(UserRole), primary_key=True)
    
    user = relationship("PlatformUser", back_populates="roles")


class Resource(Base):
    __tablename__ = "resources"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    type = Column(SAEnum(ResourceType), nullable=False)
    external_id = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    email = Column(String)
    metadata = Column(JSON, default={})
    sla_policy_id = Column(UUID(as_uuid=True), ForeignKey("sla_policies.id"))
    status = Column(SAEnum(ResourceStatus), default=ResourceStatus.DISCOVERED)
    last_backup_job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))
    last_backup_at = Column(DateTime)
    last_backup_status = Column(String)
    storage_bytes = Column(BigInteger, default=0)
    discovered_at = Column(DateTime, default=datetime.utcnow)
    archived_at = Column(DateTime)
    deletion_queued_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    tenant = relationship("Tenant", back_populates="resources")
    sla_policy = relationship("SlaPolicy", back_populates="resources")
    jobs = relationship("Job", back_populates="resource")
    snapshots = relationship("Snapshot", back_populates="resource")


class SlaPolicy(Base):
    __tablename__ = "sla_policies"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    name = Column(String, nullable=False)
    tier = Column(String, nullable=False, default="BRONZE")
    frequency = Column(String, default="DAILY")
    backup_window_start = Column(String)
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
    backup_planner = Column(Boolean, default=False)
    retention_type = Column(String, default="INDEFINITE")
    retention_days = Column(Integer)
    retention_versions = Column(Integer)
    gfs_daily = Column(Integer)
    gfs_weekly = Column(Integer)
    gfs_monthly = Column(Integer)
    gfs_yearly = Column(Integer)
    exclude_exchange_labels = Column(ARRAY(String))
    exclude_file_extensions = Column(ARRAY(String))
    encryption_mode = Column(String)
    byok_key_id = Column(String)
    enabled = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    tenant = relationship("Tenant", back_populates="sla_policies")
    resources = relationship("Resource", back_populates="sla_policy")


class Job(Base):
    __tablename__ = "jobs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(SAEnum(JobType), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id"))
    snapshot_id = Column(UUID(as_uuid=True), ForeignKey("snapshots.id"))
    status = Column(SAEnum(JobStatus), default=JobStatus.QUEUED)
    priority = Column(Integer, default=5)
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=5)
    queue_job_id = Column(String)
    worker_id = Column(String)
    error_message = Column(Text)
    progress_pct = Column(Integer, default=0)
    items_processed = Column(BigInteger, default=0)
    bytes_processed = Column(BigInteger, default=0)
    result = Column(JSON, default={})
    spec = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime)
    
    tenant = relationship("Tenant", back_populates="jobs")
    resource = relationship("Resource", back_populates="jobs")
    snapshot = relationship("Snapshot", back_populates="job")
    logs = relationship("JobLog", back_populates="job")


class Snapshot(Base):
    __tablename__ = "snapshots"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id"), nullable=False, index=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))
    type = Column(SAEnum(SnapshotType), default=SnapshotType.INCREMENTAL)
    status = Column(SAEnum(SnapshotStatus), default=SnapshotStatus.RUNNING)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    duration_secs = Column(Integer)
    item_count = Column(Integer, default=0)
    new_item_count = Column(Integer, default=0)
    bytes_added = Column(BigInteger, default=0)
    bytes_total = Column(BigInteger, default=0)
    delta_token = Column(String)
    snapshot_label = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    resource = relationship("Resource", back_populates="snapshots")
    job = relationship("Job", back_populates="snapshot")
    items = relationship("SnapshotItem", back_populates="snapshot")


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
    content_size = Column(BigInteger, default=0)
    metadata = Column(JSON, default={})
    is_deleted = Column(Boolean, default=False)
    indexed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    snapshot = relationship("Snapshot", back_populates="items")


class JobLog(Base):
    __tablename__ = "job_logs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    level = Column(String, default="INFO")
    message = Column(Text, nullable=False)
    details = Column(Text)
    
    job = relationship("Job", back_populates="logs")


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
    details = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AccessGroup(Base):
    __tablename__ = "access_groups"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"))
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"))
    name = Column(String, nullable=False)
    description = Column(String)
    scope = Column(String, default="TENANT")
    resource_ids = Column(ARRAY(UUID(as_uuid=True)), default=[])
    permissions = Column(JSON, default={})
    member_ids = Column(ARRAY(UUID(as_uuid=True)), default=[])
    entra_group_ids = Column(ARRAY(String), default=[])
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
