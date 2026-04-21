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
    TEAMS_CHAT_EXPORT = "TEAMS_CHAT_EXPORT"
    # Singleton per-tenant "Azure Active Directory" resource — matches
    # AFI's `office_directory` kind. Stores the 8 Entra-wide content
    # categories (users, groups, roles, security, audit, applications,
    # intune, admin units) as snapshot items under this one resource.
    ENTRA_DIRECTORY = "ENTRA_DIRECTORY"
    ENTRA_USER = "ENTRA_USER"
    ENTRA_GROUP = "ENTRA_GROUP"
    M365_GROUP = "M365_GROUP"  # Unified (modern) group — links group mailbox + SP site + (optional) Team
    ENTRA_CONDITIONAL_ACCESS = "ENTRA_CONDITIONAL_ACCESS"  # CA policy (full JSON definition incl. conditions + grants)
    ENTRA_BITLOCKER_KEY = "ENTRA_BITLOCKER_KEY"  # Per-device recovery key (no key bytes — just the metadata Graph exposes)
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
    # Tier 2 per-user content categories — children of an ENTRA_USER row,
    # linked via parent_resource_id. Distinct from the Tier 1 MAILBOX /
    # ONEDRIVE / TEAMS_CHAT types so stale-marking on a Tier 1 run doesn't
    # touch Tier 2 children.
    USER_MAIL = "USER_MAIL"
    USER_ONEDRIVE = "USER_ONEDRIVE"
    USER_CONTACTS = "USER_CONTACTS"
    USER_CALENDAR = "USER_CALENDAR"
    USER_CHATS = "USER_CHATS"


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
    PENDING = "PENDING"  # T0 migration: chat-export PENDING (queued-but-idempotency-safe)
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    CANCELLING = "CANCELLING"  # T0 migration: transient state while worker wraps up
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

    # Two-tier discovery: Tier 1 creates parent rows (ENTRA_USER, etc.) with
    # parent_resource_id NULL; Tier 2 creates child rows (MAILBOX, ONEDRIVE,
    # USER_CONTACTS, USER_CALENDAR, TEAMS_CHAT) pointing at the parent user
    # via parent_resource_id. Lets the UI group all of a user's backup-ables
    # under one card.
    parent_resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id", ondelete="CASCADE"), nullable=True, index=True)


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

    # Retention scheme (afi.ai parity): how the worker decides which snapshots/items to prune.
    # FLAT       = simple days-since cutoff (uses retention_days / retention_hot/cool/archive_days)
    # GFS        = Grandfather-Father-Son: keep N daily, N weekly, N monthly, N yearly
    # ITEM_LEVEL = per-item cutoff based on the item's own date (email receivedDate, file mTime)
    # HYBRID     = FLAT for snapshots + ITEM_LEVEL for items within retained snapshots
    retention_mode = Column(String, default="FLAT", nullable=False)
    gfs_daily_count = Column(Integer, nullable=True)
    gfs_weekly_count = Column(Integer, nullable=True)
    gfs_monthly_count = Column(Integer, nullable=True)
    gfs_yearly_count = Column(Integer, nullable=True)
    item_retention_days = Column(Integer, nullable=True)
    item_retention_basis = Column(String, default="SNAPSHOT", nullable=False)  # "SNAPSHOT" | "ITEM_DATE"

    # Separate retention for resources marked ARCHIVED (afi dropdown):
    # SAME       = reuse the live retention rules (default)
    # KEEP_ALL   = never prune
    # KEEP_LAST  = keep only the most recent snapshot
    # CUSTOM     = use archived_retention_days for a flat cutoff
    archived_retention_mode = Column(String, default="SAME", nullable=False)
    archived_retention_days = Column(Integer, nullable=True)

    # Per-policy overrides (fall back to tenant/org defaults when NULL)
    storage_region = Column(String, nullable=True)
    encryption_mode = Column(String, default="VAULT_MANAGED", nullable=False)  # VAULT_MANAGED | CUSTOMER_KEY
    key_vault_uri = Column(String, nullable=True)  # for BYOK
    key_name = Column(String, nullable=True)
    key_version = Column(String, nullable=True)

    # Auto-apply hook — when true, the discovery-worker will assign this policy to
    # any newly-discovered resource that matches one of its resource-group rules.
    auto_apply_to_matching = Column(Boolean, default=False, nullable=False)

    enabled = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class SlaExclusion(Base):
    """Exclusion rule attached to an SLA policy.

    Backup handlers consult the parent policy's exclusions before staging each
    item. apply_to_historical means an offline job should also purge matching
    items from prior snapshots (retroactive compliance / space reclaim)."""
    __tablename__ = "sla_exclusions"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    policy_id = Column(UUID(as_uuid=True), ForeignKey("sla_policies.id", ondelete="CASCADE"), nullable=False, index=True)
    # FOLDER_PATH | FILE_EXTENSION | SUBJECT_REGEX | MIME_TYPE | EMAIL_ADDRESS | FILENAME_GLOB
    exclusion_type = Column(String, nullable=False)
    pattern = Column(String, nullable=False)
    # Optional scope to a single workload family. NULL = applies everywhere relevant.
    # Values: EMAIL | FILE | CALENDAR | CONTACT | TEAMS_MESSAGE | CHAT_MESSAGE | ALL
    workload = Column(String, nullable=True)
    apply_to_historical = Column(Boolean, default=False, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class ResourceGroup(Base):
    """Named group of resources. Two kinds:
    - DYNAMIC: rules[] evaluated against resource attributes (name, email, dept, etc.)
    - STATIC:  explicit list of resource IDs stored via resource.metadata or join table
    rules is a list of {field, operator, value} dicts; combinator picks AND vs OR.
    Priority affects tie-breaking when a resource matches multiple groups
    (lower = higher priority — matches afi's Dynamic > Provider > Default ordering)."""
    __tablename__ = "resource_groups"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    group_type = Column(String, default="DYNAMIC", nullable=False)  # STATIC | DYNAMIC | PROVIDER_NATIVE
    rules = Column(JSON, default=list, nullable=False)
    combinator = Column(String, default="AND", nullable=False)  # AND | OR
    priority = Column(Integer, default=100, nullable=False)
    auto_protect_new = Column(Boolean, default=False, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class GroupPolicyAssignment(Base):
    """Link table: which SLA policies are attached to which resource groups."""
    __tablename__ = "group_policy_assignments"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    group_id = Column(UUID(as_uuid=True), ForeignKey("resource_groups.id", ondelete="CASCADE"), nullable=False, index=True)
    policy_id = Column(UUID(as_uuid=True), ForeignKey("sla_policies.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime, default=utcnow)


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
    # Storage toggle retry plumbing (2026-04-21)
    retry_reason = Column(Text)
    pre_toggle_job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))


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
    # Storage backend that holds this snapshot's blobs (2026-04-21).
    # NOT NULL enforced after backfill migration.
    backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    created_at = Column(DateTime, default=utcnow)


class SnapshotItem(Base):
    __tablename__ = "snapshot_items"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_id = Column(UUID(as_uuid=True), ForeignKey("snapshots.id"), nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    external_id = Column(String, nullable=False)
    parent_external_id = Column(String, index=True)
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
    # Storage backend that holds this item's blob. Permanent — wins over
    # system_config.active_backend_id during passthrough restores.
    backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
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


class TenantSecret(Base):
    """Credentials + KMS-key references the user stores once and reuses
    across restore + other operations. Known `type` values:
      • SQL_SERVER_LOGIN / POSTGRESQL_LOGIN — login + password used
        during Azure DB out-of-place restore.
      • AES_256_KEY — external-KMS key material (AWS/GCP/Azure KV).

    Passwords / key material live in `encrypted_payload` (opaque base64
    of shared.security.encrypt_secret), never returned to the frontend.
    `metadata_hints` carries non-sensitive fields safe to render in
    lists (login username, KMS provider name, etc)."""
    __tablename__ = "tenant_secrets"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), index=True, nullable=False)
    type = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    metadata_hints = Column(JSON, default=dict, nullable=True)
    encrypted_payload = Column(Text, nullable=True)
    is_default = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=utcnow, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)


# ==================== Storage backend abstraction (2026-04-21) ====================


class StorageBackendKind(str, enum.Enum):
    azure_blob = "azure_blob"
    seaweedfs = "seaweedfs"


class TransitionState(str, enum.Enum):
    stable = "stable"
    draining = "draining"
    flipping = "flipping"


class ToggleStatus(str, enum.Enum):
    started = "started"
    drain_started = "drain_started"
    drain_completed = "drain_completed"
    db_promoted = "db_promoted"
    dns_flipped = "dns_flipped"
    workers_restarted = "workers_restarted"
    smoke_passed = "smoke_passed"
    completed = "completed"
    aborted = "aborted"
    failed = "failed"


# Import extras used only by these new classes; placed here instead of at the
# top so unrelated code doesn't pay the import tax until storage is imported.
from sqlalchemy import SmallInteger
from sqlalchemy.dialects.postgresql import INET, JSONB


class StorageBackend(Base):
    __tablename__ = "storage_backends"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    kind = Column(String, nullable=False)
    name = Column(String, unique=True, nullable=False)
    endpoint = Column(String, nullable=False)
    config = Column(JSONB, nullable=False, default=dict)
    secret_ref = Column(String, nullable=False)
    is_enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class SystemConfig(Base):
    __tablename__ = "system_config"
    id = Column(SmallInteger, primary_key=True)
    active_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    transition_state = Column(String, nullable=False, default="stable")
    last_toggle_at = Column(DateTime(timezone=True))
    cooldown_until = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class StorageToggleEvent(Base):
    __tablename__ = "storage_toggle_events"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    actor_id = Column(UUID(as_uuid=True), nullable=False)
    actor_ip = Column(INET)
    from_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    to_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    reason = Column(String)
    status = Column(String, nullable=False, default="started")
    started_at = Column(DateTime(timezone=True), default=utcnow)
    drain_completed_at = Column(DateTime(timezone=True))
    flip_completed_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    pre_flight_checks = Column(JSONB)
    drained_job_count = Column(Integer)
    retried_job_count = Column(Integer)
