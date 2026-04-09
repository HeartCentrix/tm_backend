"""Shared Pydantic schemas for all microservices"""
from datetime import datetime
from typing import Optional, List
from uuid import UUID
from pydantic import BaseModel


# ============ Auth ============

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


class MicrosoftAuthUrlResponse(BaseModel):
    url: str
    state: str


class OAuthCallbackRequest(BaseModel):
    code: str
    state: Optional[str] = None


# ============ Dashboard ============

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


# ============ Tenant ============

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


# ============ Resource ============

class ResourceResponse(BaseModel):
    id: str
    name: str
    email: Optional[str] = None
    type: str
    sla: Optional[str] = None
    totalSize: str
    lastBackup: Optional[str] = None
    status: Optional[str] = None
    tenantId: Optional[str] = None
    archived: bool = False


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
    hasOneDrive: Optional[bool] = None
    oneDriveStatus: Optional[str] = None
    hasTeamsChat: Optional[bool] = None
    teamsChatStatus: Optional[str] = None
    sla: Optional[str] = None


class AssignPolicyRequest(BaseModel):
    policyId: str


class BulkOperationRequest(BaseModel):
    resourceIds: List[str]


# ============ Job ============

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


class JobListResponse(BaseModel):
    content: List[JobResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int
    first: bool
    last: bool


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


# ============ Snapshot ============

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


# ============ SLA Policy ============

class SlaPolicyResponse(BaseModel):
    id: str
    tenantId: str
    name: str
    tier: str
    frequency: str
    backupWindowStart: Optional[str] = None
    backupExchange: Optional[bool] = True
    backupOneDrive: Optional[bool] = True
    backupSharepoint: Optional[bool] = True
    backupTeams: Optional[bool] = True
    backupEntraId: Optional[bool] = True
    retentionType: str
    retentionDays: Optional[int] = None
    enabled: Optional[bool] = True
    createdAt: str


class SlaPolicyCreateRequest(BaseModel):
    tenantId: str
    name: str
    tier: str
    frequency: str
    backupWindowStart: Optional[str] = None
    backupExchange: Optional[bool] = True
    backupOneDrive: Optional[bool] = True
    backupSharepoint: Optional[bool] = True
    backupTeams: Optional[bool] = True
    backupEntraId: Optional[bool] = True
    retentionType: str
    retentionDays: Optional[int] = None
    enabled: Optional[bool] = True


# ============ Alert ============

class AlertResponse(BaseModel):
    id: str
    severity: str
    title: str
    description: str
    status: str
    createdAt: str
    resolved: Optional[bool] = None
    tenantId: Optional[str] = None
    type: Optional[str] = None
    message: Optional[str] = None


class AlertListResponse(BaseModel):
    content: List[AlertResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


# ============ Access Group ============

class AccessGroupResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    memberCount: Optional[int] = None
    createdAt: Optional[str] = None


class AccessGroupListResponse(BaseModel):
    content: List[AccessGroupResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int
