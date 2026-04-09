"""Pydantic schemas for API request/response validation"""
from datetime import datetime
from typing import Optional, List, Any
from uuid import UUID
from pydantic import BaseModel, Field


# ============ Auth Schemas ============

class MicrosoftAuthUrlResponse(BaseModel):
    url: str
    state: str


class OAuthCallbackRequest(BaseModel):
    code: str
    state: Optional[str] = None


class UserResponse(BaseModel):
    id: str
    email: str
    name: str
    roles: List[str]
    organizationId: str
    tenantId: Optional[str] = None


class LoginResponse(BaseModel):
    accessToken: str
    refreshToken: str
    expiresIn: int
    user: UserResponse


class RefreshTokenRequest(BaseModel):
    refreshToken: str


class RefreshTokenResponse(BaseModel):
    accessToken: str
    refreshToken: str
    expiresIn: int


# ============ Dashboard Schemas ============

class DashboardOverview(BaseModel):
    totalResources: int
    protectedResources: int
    failedBackups: int
    pendingBackups: int
    storageUsed: str
    lastBackupTime: Optional[str] = None


class BackupStatus24Hour(BaseModel):
    success: int
    warnings: int
    failures: int


class DailyStatus(BaseModel):
    date: str
    success: int
    warnings: int
    failures: int


class ProtectionStatusItem(BaseModel):
    protectedCount: int
    total: int


class ProtectionStatus(BaseModel):
    users: ProtectionStatusItem
    sharedMailboxes: ProtectionStatusItem
    rooms: ProtectionStatusItem
    sharepointSites: ProtectionStatusItem
    groupsAndTeams: ProtectionStatusItem
    entraId: ProtectionStatusItem
    powerPlatform: ProtectionStatusItem
    percentage: float


class BackupSizeDailyData(BaseModel):
    date: str
    bytes: int


class BackupSize(BaseModel):
    total: str
    oneDayChange: str
    oneMonthChange: str
    oneYearChange: str
    dailyData: List[BackupSizeDailyData]


# ============ Tenant Schemas ============

class TenantResponse(BaseModel):
    id: str
    displayName: str
    orgId: Optional[str] = None
    type: Optional[str] = None
    externalTenantId: Optional[str] = None
    status: str
    storageRegion: Optional[str] = None
    lastDiscoveryAt: Optional[str] = None
    createdAt: Optional[str] = None


class TenantCreateRequest(BaseModel):
    name: str
    organizationId: str
    microsoftTenantId: str
    connectionDetails: Optional[dict] = None


class TenantUpdateRequest(BaseModel):
    name: Optional[str] = None
    status: Optional[str] = None
    connectionDetails: Optional[dict] = None


class DiscoveryStatus(BaseModel):
    tenantId: str
    status: str
    progress: int
    resourcesDiscovered: int
    startedAt: str
    completedAt: Optional[str] = None
    errorMessage: Optional[str] = None


class StorageSummaryItem(BaseModel):
    workload: str
    size: int
    resourceCount: int


class OrganizationResponse(BaseModel):
    id: str
    name: str
    status: str
    tenantCount: int
    createdAt: str


# ============ Resource Schemas ============

class ResourceResponse(BaseModel):
    id: str
    name: str
    email: Optional[str] = None
    type: str
    sla: Optional[str] = None
    totalSize: str
    lastBackup: Optional[str] = None
    nextBackup: Optional[str] = None
    status: Optional[str] = None
    licenseStatus: Optional[str] = None
    showSlaDropdown: bool = False
    tenantId: Optional[str] = None
    archived: bool = False
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    resourceGroup: Optional[str] = None
    location: Optional[str] = None
    subscriptionId: Optional[str] = None


class ResourceListResponse(BaseModel):
    content: List[ResourceResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int
    first: bool
    last: bool


class UserResourceResponse(BaseModel):
    id: str
    tenantId: str
    email: str
    displayName: str
    hasMailbox: Optional[bool] = None
    mailboxStatus: Optional[str] = None
    mailboxLastBackup: Optional[str] = None
    mailboxStorageBytes: Optional[int] = None
    hasOneDrive: Optional[bool] = None
    oneDriveStatus: Optional[str] = None
    oneDriveLastBackup: Optional[str] = None
    oneDriveStorageBytes: Optional[int] = None
    hasTeamsChat: Optional[bool] = None
    teamsChatStatus: Optional[str] = None
    teamsChatLastBackup: Optional[str] = None
    hasContacts: Optional[bool] = None
    contactsStatus: Optional[str] = None
    contactsLastBackup: Optional[str] = None
    hasCalendar: Optional[bool] = None
    calendarStatus: Optional[str] = None
    calendarLastBackup: Optional[str] = None
    sla: Optional[str] = None
    slaPolicyId: Optional[str] = None
    status: Optional[str] = None
    lastBackupAt: Optional[str] = None
    totalStorageBytes: Optional[int] = None
    discoveredAt: Optional[str] = None
    showSlaDropdown: bool = False


class StorageHistoryItem(BaseModel):
    date: str
    size: int


class AssignPolicyRequest(BaseModel):
    policyId: str


class BulkOperationRequest(BaseModel):
    resourceIds: List[str]


# ============ Job Schemas ============

class JobResponse(BaseModel):
    id: str
    type: str
    status: str
    progress: int
    resourceId: Optional[str] = None
    tenantId: Optional[str] = None
    createdAt: str
    updatedAt: str
    completedAt: Optional[str] = None
    errorMessage: Optional[str] = None
    createdBy: Optional[str] = None


class JobListResponse(BaseModel):
    content: List[JobResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int
    first: bool
    last: bool


class JobProgress(BaseModel):
    jobId: str
    status: str
    progress: int
    message: str
    itemsProcessed: int
    totalItems: int


class JobLog(BaseModel):
    id: str
    jobId: str
    timestamp: str
    level: str
    message: str
    details: Optional[str] = None


class TriggerBackupRequest(BaseModel):
    resourceId: str
    fullBackup: Optional[bool] = False
    priority: Optional[int] = 1
    note: Optional[str] = None


class TriggerBulkBackupRequest(BaseModel):
    resourceIds: List[str]
    fullBackup: Optional[bool] = False
    priority: Optional[int] = 1
    note: Optional[str] = None


class DLQStats(BaseModel):
    dlqName: str
    messageCount: int
    oldestMessageDate: Optional[str] = None


# ============ Snapshot Schemas ============

class SnapshotResponse(BaseModel):
    id: str
    resourceId: str
    createdAt: str
    size: int
    status: str
    type: str
    itemCount: int


class SnapshotItemResponse(BaseModel):
    id: str
    snapshotId: str
    externalId: str
    itemType: str
    name: str
    folderPath: Optional[str] = None
    contentSize: int
    metadata: dict
    isDeleted: bool
    createdAt: str


class SnapshotListResponse(BaseModel):
    content: List[SnapshotResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


class SnapshotItemListResponse(BaseModel):
    content: List[SnapshotItemResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


class SnapshotCalendarItem(BaseModel):
    __root__: dict[str, int]


class SnapshotDiff(BaseModel):
    added: List[SnapshotItemResponse]
    removed: List[SnapshotItemResponse]
    modified: List[SnapshotItemResponse]


# ============ Restore Schemas ============

class RestoreRequest(BaseModel):
    snapshotId: str
    snapshotItemIds: Optional[List[str]] = None
    targetResourceId: Optional[str] = None
    inPlaceRestore: Optional[bool] = True
    targetPath: Optional[str] = None
    restoreMetadata: Optional[dict] = None


class RestoreJobStatus(BaseModel):
    jobId: str
    status: str
    progress: int
    message: Optional[str] = None
    completedAt: Optional[str] = None


class RestoreHistoryItem(BaseModel):
    id: str
    type: str
    status: str
    targetResource: Optional[str] = None
    createdAt: str
    completedAt: Optional[str] = None


class RestoreHistoryResponse(BaseModel):
    content: List[RestoreHistoryItem]
    totalPages: int
    totalElements: int
    size: int
    number: int


class ExportRequest(BaseModel):
    snapshotItemIds: List[str]
    format: Optional[str] = "ZIP"


class ExportJobStatus(BaseModel):
    jobId: str
    status: str
    progress: int
    downloadUrl: Optional[str] = None


class ExportDownloadResponse(BaseModel):
    url: str


# ============ Alert Schemas ============

class AlertResponse(BaseModel):
    id: str
    severity: str
    title: str
    description: str
    status: str
    createdAt: str
    resolvedAt: Optional[str] = None
    resolvedBy: Optional[str] = None
    tenantId: Optional[str] = None
    type: Optional[str] = None
    message: Optional[str] = None
    resourceId: Optional[str] = None
    resourceType: Optional[str] = None
    resourceName: Optional[str] = None
    triggeredBy: Optional[str] = None
    resolved: Optional[bool] = None
    resolutionNote: Optional[str] = None
    details: Optional[dict] = None


class AlertListResponse(BaseModel):
    content: List[AlertResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


class NotificationSettingsAlertThresholds(BaseModel):
    critical: bool
    high: bool
    medium: bool
    low: bool


class NotificationSettings(BaseModel):
    emailEnabled: bool
    slackEnabled: bool
    teamsEnabled: bool
    webhookUrl: Optional[str] = None
    alertThresholds: NotificationSettingsAlertThresholds


class WebhookResponse(BaseModel):
    id: str
    name: str
    url: str
    enabled: bool
    createdAt: str
    lastTriggeredAt: Optional[str] = None


# ============ SLA Policy Schemas ============

class SlaPolicyResponse(BaseModel):
    id: str
    tenantId: str
    name: str
    tier: str
    frequency: str
    backupWindowStart: Optional[str] = None
    backupExchange: Optional[bool] = True
    backupExchangeArchive: Optional[bool] = False
    backupExchangeRecoverable: Optional[bool] = False
    backupOneDrive: Optional[bool] = True
    backupSharepoint: Optional[bool] = True
    backupTeams: Optional[bool] = True
    backupTeamsChats: Optional[bool] = False
    backupEntraId: Optional[bool] = True
    backupPowerPlatform: Optional[bool] = False
    backupCopilot: Optional[bool] = False
    retentionType: str
    retentionDays: Optional[int] = None
    retentionVersions: Optional[int] = None
    gfsDaily: Optional[int] = None
    gfsWeekly: Optional[int] = None
    gfsMonthly: Optional[int] = None
    gfsYearly: Optional[int] = None
    archiveRetentionDays: Optional[int] = None
    excludeExchangeLabels: Optional[List[str]] = None
    excludeFileExtensions: Optional[List[str]] = None
    encryptionMode: Optional[str] = None
    byokKeyId: Optional[str] = None
    enabled: Optional[bool] = True
    isDefault: Optional[bool] = False
    createdAt: str


class SlaPolicyCreateRequest(BaseModel):
    tenantId: str
    name: str
    tier: str
    frequency: str
    backupWindowStart: Optional[str] = None
    backupExchange: Optional[bool] = True
    backupExchangeArchive: Optional[bool] = False
    backupExchangeRecoverable: Optional[bool] = False
    backupOneDrive: Optional[bool] = True
    backupSharepoint: Optional[bool] = True
    backupTeams: Optional[bool] = True
    backupTeamsChats: Optional[bool] = False
    backupEntraId: Optional[bool] = True
    backupPowerPlatform: Optional[bool] = False
    backupCopilot: Optional[bool] = False
    retentionType: str
    retentionDays: Optional[int] = None
    retentionVersions: Optional[int] = None
    gfsDaily: Optional[int] = None
    gfsWeekly: Optional[int] = None
    gfsMonthly: Optional[int] = None
    gfsYearly: Optional[int] = None
    archiveRetentionDays: Optional[int] = None
    excludeExchangeLabels: Optional[List[str]] = None
    excludeFileExtensions: Optional[List[str]] = None
    encryptionMode: Optional[str] = None
    byokKeyId: Optional[str] = None
    enabled: Optional[bool] = True
    isDefault: Optional[bool] = False


class PolicyResourceItem(BaseModel):
    id: str
    name: str
    type: str
    assignedAt: str


class PolicyResourcesResponse(BaseModel):
    content: List[PolicyResourceItem]
    totalPages: int
    totalElements: int


# ============ Access Group Schemas ============

class AccessGroupResponse(BaseModel):
    id: str
    orgId: Optional[str] = None
    tenantId: Optional[str] = None
    name: str
    description: Optional[str] = None
    scope: Optional[str] = None
    resourceIds: Optional[List[str]] = None
    permissions: Optional[dict] = None
    memberIds: Optional[List[str]] = None
    entraGroupIds: Optional[List[str]] = None
    active: Optional[bool] = True
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    memberCount: Optional[int] = None


class AccessGroupListResponse(BaseModel):
    content: List[AccessGroupResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


class AccessGroupMemberResponse(BaseModel):
    id: str
    groupId: str
    userId: str
    userName: str
    userEmail: str
    role: str
    addedAt: str


# ============ Deletion Schemas ============

class DeletionResponse(BaseModel):
    deletionId: str
    gracePeriodEnd: str
