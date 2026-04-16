"""Shared Pydantic schemas for all microservices"""
from datetime import datetime
from typing import Optional, List
from uuid import UUID
from pydantic import BaseModel, Field, field_validator, ConfigDict


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


class DatasourceConsentRequest(BaseModel):
    """Admin consent callback request for M365 datasource onboarding."""
    external_tenant_id: str  # from ?tenant=... query param
    admin_consent: bool
    state: str               # CSRF token issued during initiation


class DatasourceCallbackResponse(BaseModel):
    tenantId: str
    tenantName: str
    discoveryStatus: str


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
    customerId: Optional[str] = None
    status: str
    storageRegion: Optional[str] = None
    lastDiscoveryAt: Optional[str] = None
    createdAt: Optional[str] = None


class TenantCreateRequest(BaseModel):
    name: str
    organizationId: str
    microsoftTenantId: str
    connectionDetails: Optional[dict] = None


class TenantInfoResponse(BaseModel):
    """Response for tenant info page (Customer ID, Tenant ID, Region)"""
    customerId: str
    tenantId: str
    region: str


class UsageReportEntry(BaseModel):
    """Single entry in the usage report"""
    resourceId: str
    resourceName: str
    resourceKind: str
    sla: str
    isActive: str
    backupSizeGB: float
    dailySizes: dict  # date -> size in GB


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


class BulkAssignRequest(BaseModel):
    """Request to assign an SLA policy to multiple resources"""
    resourceIds: List[str]
    policyId: str


class BulkUnassignRequest(BaseModel):
    """Request to remove SLA policy from multiple resources"""
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
    fullBackup: Optional[bool] = True
    priority: Optional[int] = 1
    note: Optional[str] = None


class TriggerBulkBackupRequest(BaseModel):
    resourceIds: List[str]
    fullBackup: Optional[bool] = True
    priority: Optional[int] = 1
    note: Optional[str] = None


class TriggerDatasourceBackupRequest(BaseModel):
    tenantId: str
    serviceType: str
    fullBackup: Optional[bool] = True
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


class SnapshotDiff(BaseModel):
    added: List[SnapshotItemResponse]
    removed: List[SnapshotItemResponse]
    modified: List[SnapshotItemResponse]


# ============ SLA Policy ============

class SlaPolicyResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)
    
    id: str
    tenantId: str = Field(alias='tenant_id')
    serviceType: str = Field(default='m365', alias='service_type')
    name: str
    frequency: str
    backupDays: Optional[List[str]] = Field(default=None, alias='backup_days')
    backupWindowStart: Optional[str] = Field(default=None, alias='backup_window_start')
    backupExchange: Optional[bool] = Field(default=True, alias='backup_exchange')
    backupExchangeArchive: Optional[bool] = Field(default=False, alias='backup_exchange_archive')
    backupExchangeRecoverable: Optional[bool] = Field(default=False, alias='backup_exchange_recoverable')
    backupOneDrive: Optional[bool] = Field(default=True, alias='backup_onedrive')
    backupSharepoint: Optional[bool] = Field(default=True, alias='backup_sharepoint')
    backupTeams: Optional[bool] = Field(default=True, alias='backup_teams')
    backupTeamsChats: Optional[bool] = Field(default=False, alias='backup_teams_chats')
    backupEntraId: Optional[bool] = Field(default=True, alias='backup_entra_id')
    backupPowerPlatform: Optional[bool] = Field(default=False, alias='backup_power_platform')
    backupCopilot: Optional[bool] = Field(default=False, alias='backup_copilot')
    contacts: Optional[bool] = True
    calendars: Optional[bool] = True
    tasks: Optional[bool] = False
    groupMailbox: Optional[bool] = Field(default=True, alias='group_mailbox')
    planner: Optional[bool] = False
    backupAzureVm: Optional[bool] = Field(default=True, alias='backup_azure_vm')
    backupAzureSql: Optional[bool] = Field(default=True, alias='backup_azure_sql')
    backupAzurePostgresql: Optional[bool] = Field(default=True, alias='backup_azure_postgresql')
    retentionType: str = Field(alias='retention_type')
    retentionDays: Optional[int] = Field(default=None, alias='retention_days')
    enabled: Optional[bool] = True
    createdAt: str = Field(alias='created_at')
    
    @field_validator('id', 'tenantId', mode='before')
    @classmethod
    def uuid_to_str(cls, v):
        return str(v) if v else v
    
    @field_validator('createdAt', mode='before')
    @classmethod
    def datetime_to_str(cls, v):
        return v.isoformat() if v else v


class SlaPolicyCreateRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    tenantId: str = Field(alias='tenant_id')
    serviceType: str = Field(default='m365', alias='service_type')
    name: str
    frequency: str
    backupDays: Optional[List[str]] = Field(default=None, alias='backup_days')
    backupWindowStart: Optional[str] = Field(default=None, alias='backup_window_start')
    backupExchange: Optional[bool] = Field(default=True, alias='backup_exchange')
    backupExchangeArchive: Optional[bool] = Field(default=False, alias='backup_exchange_archive')
    backupExchangeRecoverable: Optional[bool] = Field(default=False, alias='backup_exchange_recoverable')
    backupOneDrive: Optional[bool] = Field(default=True, alias='backup_onedrive')
    backupSharepoint: Optional[bool] = Field(default=True, alias='backup_sharepoint')
    backupTeams: Optional[bool] = Field(default=True, alias='backup_teams')
    backupTeamsChats: Optional[bool] = Field(default=False, alias='backup_teams_chats')
    backupEntraId: Optional[bool] = Field(default=True, alias='backup_entra_id')
    backupPowerPlatform: Optional[bool] = Field(default=False, alias='backup_power_platform')
    backupCopilot: Optional[bool] = Field(default=False, alias='backup_copilot')
    contacts: Optional[bool] = True
    calendars: Optional[bool] = True
    tasks: Optional[bool] = False
    groupMailbox: Optional[bool] = Field(default=True, alias='group_mailbox')
    planner: Optional[bool] = False
    backupAzureVm: Optional[bool] = Field(default=True, alias='backup_azure_vm')
    backupAzureSql: Optional[bool] = Field(default=True, alias='backup_azure_sql')
    backupAzurePostgresql: Optional[bool] = Field(default=True, alias='backup_azure_postgresql')
    retentionType: str = Field(alias='retention_type')
    retentionDays: Optional[int] = Field(default=None, alias='retention_days')
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


# ============ Admin Consent ============

class AdminConsentResponse(BaseModel):
    """Response for admin consent status"""
    id: str
    consentType: str = Field(alias='consent_type')
    grantedBy: Optional[str] = Field(default=None, alias='granted_by')
    consentedAt: Optional[str] = Field(default=None, alias='consented_at')
    lastUsedAt: Optional[str] = Field(default=None, alias='last_used_at')
    isActive: bool = Field(default=True, alias='is_active')
    scope: Optional[str] = None
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    @field_validator('id', mode='before')
    @classmethod
    def uuid_to_str(cls, v):
        return str(v) if v else v

    @field_validator('consentedAt', 'lastUsedAt', mode='before')
    @classmethod
    def datetime_to_str(cls, v):
        return v.isoformat() if v else v


class AdminConsentTokenResponse(BaseModel):
    """Response when granting admin consent"""
    tenantId: str
    consentType: str
    message: str
    consentedAt: str


class PowerBIOAuthCallbackRequest(BaseModel):
    tenantId: str
    code: str
    state: Optional[str] = None


class PowerBIReadinessCheckResponse(BaseModel):
    key: str
    label: str
    status: str
    detail: str


class PowerBIReadinessResponse(BaseModel):
    tenantId: str
    status: str
    summary: str
    authMode: str
    usesDedicatedApp: bool
    accessibleWorkspaceCount: int
    discoveredWorkspaceCount: int
    checks: List[PowerBIReadinessCheckResponse]
    recommendedActions: List[str]
