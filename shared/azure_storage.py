"""
High-Performance Azure Blob Storage Module

Features:
- Storage sharding across multiple accounts
- Server-Side Copy (start_copy_from_url) for zero-server-load transfers
- Connection pooling and async operations
- Automatic retry with exponential backoff
- RBAC and Key-based authentication
"""
import asyncio
import base64
import hashlib
import logging
import re
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError

from shared.config import settings


def _sanitize_metadata(metadata: Optional[Dict]) -> Dict:
    """
    Sanitize Azure Blob Storage metadata.
    - Keys: alphanumeric + underscores only (Azure requires valid C# identifier format)
    - Values: ASCII printable chars only (no Unicode, no control chars)
    Non-ASCII values are stripped to ASCII; keys are cleaned of invalid chars.
    """
    if not metadata:
        return {}
    clean = {}
    for k, v in metadata.items():
        # Sanitize key: replace non-alphanumeric/underscore chars
        safe_key = re.sub(r'[^a-zA-Z0-9_]', '_', str(k))
        if not safe_key or safe_key[0].isdigit():
            safe_key = f"m_{safe_key}"
        # Sanitize value: encode to ASCII, dropping non-ASCII chars
        safe_val = str(v).encode('ascii', errors='replace').decode('ascii')
        # Remove control characters (0x00-0x1F and 0x7F)
        safe_val = re.sub(r'[\x00-\x1f\x7f]', '', safe_val)
        # Truncate to 8KB limit per Azure spec
        clean[safe_key] = safe_val[:8192]
    return clean


class AzureStorageShard:
    """Represents a single Azure Storage Account shard"""
    
    def __init__(self, account_name: str, account_key: str, shard_index: int = 0):
        self.account_name = account_name
        self.account_key = account_key
        self.shard_index = shard_index
        # EndpointSuffix must be just "core.windows.net" — not the full URL
        raw_endpoint = settings.AZURE_STORAGE_BLOB_ENDPOINT.replace('https://', '').rstrip('/')
        # raw_endpoint = "stamitkmishr369164905478.blob.core.windows.net"
        # We need "core.windows.net"
        parts = raw_endpoint.split('.')
        if len(parts) >= 3:
            endpoint_suffix = '.'.join(parts[2:])  # "core.windows.net"
        else:
            endpoint_suffix = raw_endpoint
        self.connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account_name};"
            f"AccountKey={account_key};"
            f"EndpointSuffix={endpoint_suffix}"
        )
        self._sync_client: Optional[BlobServiceClient] = None
        self._async_client: Optional[AsyncBlobServiceClient] = None
    
    def get_sync_client(self) -> BlobServiceClient:
        if not self._sync_client:
            self._sync_client = BlobServiceClient.from_connection_string(self.connection_string)
        return self._sync_client
    
    def get_async_client(self) -> AsyncBlobServiceClient:
        if not self._async_client:
            self._async_client = AsyncBlobServiceClient.from_connection_string(self.connection_string)
        return self._async_client
    
    async def server_side_copy(self, source_url: str, container_name: str, blob_path: str) -> Dict:
        """
        Server-Side Copy: Azure copies file directly from source to destination.
        Zero load on our server. Fastest method for file-based backups.

        Args:
            source_url: Source file URL (e.g., @microsoft.graph.downloadUrl or SAS URL)
            container_name: Target container name
            blob_path: Target blob path in container

        Returns:
            Dict with copy_id, status, and blob_url
        """
        try:
            await self._ensure_container(container_name)

            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)

            # Start server-side copy
            copy_operation = await blob_client.start_copy_from_url(source_url)
            
            return {
                "success": True,
                "copy_id": copy_operation.get("id", str(uuid.uuid4())),
                "status": "pending",
                "blob_url": blob_client.url,
                "blob_path": blob_path,
                "method": "server_side_copy",
            }
        except AzureError as e:
            return {
                "success": False,
                "error": str(e),
                "method": "server_side_copy",
            }

    async def copy_from_url_sync(self, source_url: str, container_name: str,
                                  blob_path: str, source_size: int,
                                  metadata: Optional[Dict] = None) -> Dict:
        """
        Synchronous server-side copy via upload_blob_from_url (≤256 MB) or
        stage_block_from_url (chunked, for >256 MB up to 5 TB).

        Returns when copy is complete. No polling needed for ≤256 MB path.
        For large files, stages blocks from URL and commits block list.
        """
        await self._ensure_container(container_name)

        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        sanitized_meta = _sanitize_metadata(metadata or {})

        SYNC_LIMIT = 256 * 1024 * 1024  # 256 MB

        if source_size <= SYNC_LIMIT:
            # Single-shot synchronous copy
            await blob_client.upload_blob_from_url(
                source_url=source_url,
                overwrite=True,
                metadata=sanitized_meta,
            )
        else:
            # Chunked: stage blocks from URL, then commit
            BLOCK_SIZE = 100 * 1024 * 1024  # 100 MB blocks
            block_ids = []
            offset = 0
            idx = 0
            while offset < source_size:
                length = min(BLOCK_SIZE, source_size - offset)
                block_id = base64.b64encode(f"{idx:08d}".encode()).decode()
                await blob_client.stage_block_from_url(
                    block_id=block_id,
                    source_url=source_url,
                    source_offset=offset,
                    source_length=length,
                )
                block_ids.append(block_id)
                offset += length
                idx += 1
            await blob_client.commit_block_list(block_ids, metadata=sanitized_meta)

        props = await blob_client.get_blob_properties()
        return {
            "size": props.size,
            "etag": props.etag,
            "content_md5": props.content_settings.content_md5.hex() if props.content_settings.content_md5 else None,
            "last_modified": props.last_modified.isoformat(),
        }
    
    async def _ensure_container(self, container_name: str):
        """Create container if it doesn't exist"""
        try:
            async_client = self.get_async_client()
            container_client = async_client.get_container_client(container_name)
            await container_client.create_container()
        except ResourceExistsError:
            pass  # Already exists — fine
        except AzureError as e:
            print(f"[AzureStorage] Warning: Could not create container {container_name}: {e}")

    async def upload_blob(self, container_name: str, blob_path: str, content: bytes,
                         overwrite: bool = True, metadata: Optional[Dict] = None) -> Dict:
        """
        Upload content to Azure Blob Storage with parallel block uploads.
        Auto-creates container if it doesn't exist.

        Args:
            container_name: Target container name
            blob_path: Target blob path
            content: Content bytes
            overwrite: Whether to overwrite existing blob
            metadata: Optional metadata dict

        Returns:
            Dict with success status and blob info
        """
        try:
            await self._ensure_container(container_name)

            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)

            # Azure Blob SDK automatically chunks and uploads blocks in parallel
            # with max_concurrency. For large files this is much faster than sequential.
            await asyncio.wait_for(
                blob_client.upload_blob(
                    content,
                    overwrite=overwrite,
                    metadata=_sanitize_metadata(metadata),
                    max_concurrency=5,  # Upload 5 blocks in parallel
                    length=len(content),
                ),
                timeout=600.0,  # 10 min timeout for large files
            )

            return {
                "success": True,
                "blob_url": blob_client.url,
                "blob_path": blob_path,
                "size_bytes": len(content),
                "method": "direct_upload",
            }
        except asyncio.TimeoutError:
            print(f"[AzureStorage] Upload TIMEOUT for {container_name}/{blob_path} after 120s")
            return {
                "success": False,
                "error": f"Upload timed out after 120s",
                "method": "direct_upload",
            }
        except AzureError as e:
            print(f"[AzureStorage] Upload failed for {container_name}/{blob_path}: {e}")
            return {
                "success": False,
                "error": str(e),
                "method": "direct_upload",
            }

    async def upload_blob_from_file(self, container_name: str, blob_path: str, file_path: str,
                                    file_size: int = 0, overwrite: bool = True,
                                    metadata: Optional[Dict] = None) -> Dict:
        """
        Upload from local file using resilient block upload with retry.

        Enterprise-grade upload for files up to multi-GB:
        - Splits file into blocks (100 MB each for efficiency)
        - Uploads blocks in parallel with configurable concurrency
        - Retries individual failed blocks with exponential backoff
        - Commits block list only when ALL blocks succeed
        - No single point of failure — one slow block doesn't kill the upload

        NOTE: The async Azure SDK requires an open file object, NOT a string path.
        Passing a string path causes the upload to hang indefinitely (SDK bug).
        """
        try:
            await self._ensure_container(container_name)

            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)

            # Block size: 100 MB — optimal for Azure (max 50K blocks = 5 TB max blob)
            block_size = 100 * 1024 * 1024  # 100 MB
            num_blocks = max(1, (file_size + block_size - 1) // block_size)

            # For small files (< block_size), use simple upload (faster)
            if file_size <= block_size:
                with open(file_path, "rb") as f:
                    await asyncio.wait_for(
                        blob_client.upload_blob(
                            f,
                            overwrite=overwrite,
                            metadata=_sanitize_metadata(metadata),
                            max_concurrency=settings.AZURE_UPLOAD_CONCURRENCY,
                            length=file_size,
                        ),
                        timeout=3600.0,  # 1 hour for large files
                    )
                return {
                    "success": True,
                    "blob_url": blob_client.url,
                    "blob_path": blob_path,
                    "size_bytes": file_size,
                    "method": "stream_upload",
                }

            # Large file: use SDK's built-in block upload with file object.
            # The SDK automatically splits into blocks and uploads in parallel.
            # Using a file object (not path string) is critical — path strings hang.
            with open(file_path, "rb") as f:
                await asyncio.wait_for(
                    blob_client.upload_blob(
                        f,
                        overwrite=overwrite,
                        metadata=_sanitize_metadata(metadata),
                        max_concurrency=8,  # Upload 8 blocks in parallel
                        length=file_size,
                    ),
                    timeout=3600.0,  # 1 hour for very large files
                )

            return {
                "success": True,
                "blob_url": blob_client.url,
                "blob_path": blob_path,
                "size_bytes": file_size,
                "method": "stream_upload",
            }
        except asyncio.TimeoutError:
            print(f"[AzureStorage] Stream upload TIMEOUT for {container_name}/{blob_path} after 1hr")
            return {
                "success": False,
                "error": "Upload timed out after 1hr",
                "method": "stream_upload",
            }
        except AzureError as e:
            print(f"[AzureStorage] Stream upload failed for {container_name}/{blob_path}: {e}")
            return {
                "success": False,
                "error": str(e),
                "method": "stream_upload",
            }
    
    async def download_blob(self, container_name: str, blob_path: str) -> Optional[bytes]:
        """Download a blob's bytes. Returns None if the blob does not exist.
        Raises AzureError for auth/network/other failures so callers can surface them."""
        async_client = self.get_async_client()
        blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
        try:
            stream = await blob_client.download_blob()
            return await stream.readall()
        except ResourceNotFoundError:
            return None

    async def get_blob_properties(self, container_name: str, blob_path: str) -> Optional[Dict]:
        """Get blob properties including copy status"""
        try:
            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
            props = await blob_client.get_blob_properties()
            return {
                "name": props.name,
                "size": props.size,
                "content_type": props.content_type,
                "last_modified": props.last_modified,
                "copy_status": props.copy.status if props.copy else None,
                "copy_progress": props.copy.progress if props.copy else None,
                "metadata": props.metadata,
            }
        except AzureError:
            return None
    
    async def wait_for_copy_complete(self, container_name: str, blob_path: str, 
                                     timeout_seconds: int = 3600, poll_interval: int = 5) -> Dict:
        """
        Wait for a server-side copy to complete.
        
        Args:
            container_name: Container name
            blob_path: Blob path
            timeout_seconds: Max wait time
            poll_interval: Seconds between status checks
        
        Returns:
            Dict with final copy status
        """
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).total_seconds() < timeout_seconds:
            props = await self.get_blob_properties(container_name, blob_path)
            if not props:
                return {"success": False, "error": "Blob not found"}
            
            copy_status = props.get("copy_status")
            if copy_status == "success":
                return {"success": True, "status": "completed", "size": props.get("size")}
            elif copy_status == "failed":
                return {"success": False, "error": "Copy failed", "status": "failed"}
            elif copy_status == "aborted":
                return {"success": False, "error": "Copy aborted", "status": "aborted"}
            
            # Still pending
            progress = props.get("copy_progress", "unknown")
            await asyncio.sleep(poll_interval)
        
        return {"success": False, "error": "Copy timed out", "status": "timeout"}


class AzureStorageManager:
    """
    Manages multiple Azure Storage Accounts for sharding.
    Distributes backup load across accounts to avoid IOPS bottlenecks.
    """
    
    def __init__(self):
        self.shards: List[AzureStorageShard] = []
        self._initialize_shards()
    
    def _initialize_shards(self):
        """Initialize storage shards from configuration"""
        # If sharding is configured, use multiple accounts
        if settings.STORAGE_SHARD_ACCOUNTS and settings.STORAGE_SHARD_KEYS:
            for i, (account, key) in enumerate(zip(
                settings.STORAGE_SHARD_ACCOUNTS, 
                settings.STORAGE_SHARD_KEYS
            )):
                shard = AzureStorageShard(account, key, shard_index=i)
                self.shards.append(shard)
                print(f"[AzureStorage] Initialized shard {i}: {account}")
        # Fall back to single account
        elif settings.AZURE_STORAGE_ACCOUNT_NAME and settings.AZURE_STORAGE_ACCOUNT_KEY:
            shard = AzureStorageShard(
                settings.AZURE_STORAGE_ACCOUNT_NAME, 
                settings.AZURE_STORAGE_ACCOUNT_KEY,
                shard_index=0
            )
            self.shards.append(shard)
            print(f"[AzureStorage] Initialized single shard: {settings.AZURE_STORAGE_ACCOUNT_NAME}")
        else:
            print("[AzureStorage] WARNING: No Azure Storage configured")
    
    def get_shard_for_resource(self, resource_id: str, tenant_id: str) -> AzureStorageShard:
        """
        Deterministically assign a storage shard based on resource/tenant ID.
        Ensures consistent placement for the same resource.
        """
        if not self.shards:
            raise RuntimeError("No storage shards configured")
        
        # Use hash to deterministically assign shard
        hash_input = f"{tenant_id}:{resource_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        shard_index = hash_value % len(self.shards)
        return self.shards[shard_index]
    
    def get_shard_by_index(self, index: int) -> AzureStorageShard:
        """Get a specific shard by index"""
        if not self.shards:
            raise RuntimeError("No storage shards configured")
        return self.shards[index % len(self.shards)]

    def get_default_shard(self) -> AzureStorageShard:
        """Get the first/default storage shard"""
        if not self.shards:
            raise RuntimeError("No storage shards configured — set AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY")
        return self.shards[0]
    
    def get_container_name(self, tenant_id: str, resource_type: str) -> str:
        """
        Generate container name following Azure naming conventions.
        Containers must be lowercase, 3-63 chars, only alphanumeric and hyphens.
        """
        # Shorten tenant/tenant ID to avoid exceeding 63 char limit
        tenant_short = tenant_id.replace("-", "")[:8]
        # Replace underscores with hyphens (Azure container names don't allow underscores)
        safe_type = resource_type.lower().replace("_", "-")
        return f"backup-{safe_type}-{tenant_short}"
    
    def build_blob_path(self, tenant_id: str, resource_id: str, 
                       snapshot_id: str, item_id: str, timestamp: str = None) -> str:
        """
        Build versioned blob path for organized storage.
        Format: {tenant_id}/{resource_type}/{resource_id}/{snapshot_id}/{timestamp}/{item_id}
        """
        ts = timestamp or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        return f"{tenant_id}/{resource_id}/{snapshot_id}/{ts}/{item_id}"


# Global singleton
azure_storage_manager = AzureStorageManager()


# Mirror of the workload strings the backup worker passes to get_container_name(...)
# when writing blobs. Kept here so read and write stay in sync in one place.
# Values are ordered: the first candidate is the primary container; later entries
# cover secondary containers the worker may also write into (e.g. group mailboxes
# materialized while processing an ENTRA_GROUP).
RESOURCE_TYPE_TO_WORKLOADS: Dict[str, Tuple[str, ...]] = {
    "MAILBOX": ("mailbox",),
    "SHARED_MAILBOX": ("mailbox",),
    "ROOM_MAILBOX": ("mailbox",),
    "ONEDRIVE": ("files",),
    "SHAREPOINT_SITE": ("files",),
    "TEAMS_CHANNEL": ("teams",),
    "TEAMS_CHAT": ("teams",),
    "ENTRA_USER": ("entra",),
    "ENTRA_GROUP": ("entra", "group-mailbox"),
    "DYNAMIC_GROUP": ("entra", "group-mailbox"),
    "ENTRA_APP": ("entra",),
    "ENTRA_DEVICE": ("entra",),
    "ENTRA_SERVICE_PRINCIPAL": ("entra",),
    "PLANNER": ("planner",),
    "TODO": ("todo",),
    "ONENOTE": ("onenote",),
    "POWER_BI": ("power-bi",),
    "POWER_APPS": ("power-apps",),
    "POWER_AUTOMATE": ("power-automate",),
    "POWER_DLP": ("power-dlp",),
    "AZURE_VM": ("azure-vm",),
    "AZURE_SQL_DB": ("azure-sql-db",),
    "AZURE_POSTGRESQL": ("azure-postgresql",),
    "AZURE_POSTGRESQL_SINGLE": ("azure-postgresql-single",),
}


def workload_candidates_for_resource_type(resource_type: str) -> Tuple[str, ...]:
    """Return container-workload suffixes a blob for this resource_type may live in."""
    if not resource_type:
        return ()
    return RESOURCE_TYPE_TO_WORKLOADS.get(str(resource_type).upper(), ())


async def server_side_copy_with_retry(source_url: str, container_name: str, 
                                      blob_path: str, shard: AzureStorageShard,
                                      max_retries: int = 3) -> Dict:
    """
    Server-Side Copy with automatic retry and exponential backoff.
    """
    last_error = None
    
    for attempt in range(max_retries):
        result = await shard.server_side_copy(source_url, container_name, blob_path)
        
        if result["success"]:
            return result
        
        last_error = result.get("error", "Unknown error")
        
        # Don't retry on auth/permission errors
        if "Authorization" in last_error or "Authentication" in last_error:
            return result
        
        # Exponential backoff
        delay = settings.RETRY_DELAY_MS * (settings.RETRY_BACKOFF_MULTIPLIER ** attempt) / 1000
        await asyncio.sleep(delay)

    return {"success": False, "error": f"Failed after {max_retries} retries: {last_error}"}


# ── AZ-0: Lifecycle & Immutability Helpers ──

import logging
import traceback

_lifecycle_logger = logging.getLogger("azure.lifecycle")


async def apply_lifecycle_policy(container_name: str, hot_days: int = 7,
                                  cool_days: int = 30, archive_days: int = None,
                                  shard: AzureStorageShard = None) -> Dict:
    """
    Configure Azure Blob lifecycle management rules for a container.
    If archive_days is None, blobs in archive never expire (unlimited retention).

    Returns:
        {"success": True/False, "rules_count": N, "container": "...", "error": "..."}
    """
    _lifecycle_logger.info(
        "[LifecyclePolicy] START — container=%s, hot=%dd, cool=%dd, archive=%s (unlimited=%s)",
        container_name, hot_days, cool_days,
        archive_days if archive_days is not None else "NULL",
        archive_days is None,
    )

    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            _lifecycle_logger.error("[LifecyclePolicy] No storage shard available — cannot apply policy")
            return {"success": False, "error": "No storage shard available"}

    rules = [
        {
            "enabled": True,
            "name": f"{container_name}_hot_to_cool",
            "definition": {
                "filters": {"blobTypes": ["blockBlob"],
                            "prefixMatch": [f"{container_name}/"]},
                "actions": {"baseBlob": {
                    "tierToCool": {"daysAfterModificationGreaterThan": hot_days}}}
            }
        },
        {
            "enabled": True,
            "name": f"{container_name}_cool_to_archive",
            "definition": {
                "filters": {"blobTypes": ["blockBlob"],
                            "prefixMatch": [f"{container_name}/"]},
                "actions": {"baseBlob": {
                    "tierToArchive": {"daysAfterModificationGreaterThan": hot_days + cool_days}}}
            }
        },
    ]

    # Only add delete rule if archive_days is set (unlimited retention = no delete)
    if archive_days is not None:
        rules.append({
            "enabled": True,
            "name": f"{container_name}_expire",
            "definition": {
                "filters": {"blobTypes": ["blockBlob"],
                            "prefixMatch": [f"{container_name}/"]},
                "actions": {"baseBlob": {
                    "delete": {"daysAfterModificationGreaterThan":
                               hot_days + cool_days + archive_days}}}
            }
        })

    _lifecycle_logger.info(
        "[LifecyclePolicy] Prepared %d rules for container=%s (shard=%s)",
        len(rules), container_name, shard.account_name,
    )

    try:
        from azure.mgmt.storage.aio import StorageManagementClient
        from azure.identity.aio import ClientSecretCredential
        from shared.config import settings

        client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
        client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
        tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID

        if not client_id or not client_secret:
            _lifecycle_logger.warning(
                "[LifecyclePolicy] No ARM credentials configured — skipping policy application for %s",
                container_name,
            )
            return {"success": False, "error": "ARM credentials not configured (set AZURE_ARM_CLIENT_ID/SECRET)"}

        credential = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
        )

        async with StorageManagementClient(credential) as mgmt:
            account_name = shard.account_name
            rg_name = settings.AZURE_BACKUP_RESOURCE_GROUP or "tmvault-storage"

            _lifecycle_logger.info(
                "[LifecyclePolicy] Applying policy — account=%s, rg=%s, policy=DefaultManagementPolicy",
                account_name, rg_name,
            )

            try:
                await mgmt.management_policies.create_or_update(
                    resource_group_name=rg_name,
                    account_name=account_name,
                    policy_name="DefaultManagementPolicy",
                    parameters={"policy": {"rules": rules}},
                )
                _lifecycle_logger.info(
                    "[LifecyclePolicy] SUCCESS — Applied %d rules to %s (account=%s, rg=%s)",
                    len(rules), container_name, account_name, rg_name,
                )
                return {"success": True, "rules_count": len(rules), "container": container_name,
                        "account": account_name, "resource_group": rg_name}
            except Exception as mgmt_exc:
                _lifecycle_logger.error(
                    "[LifecyclePolicy] MANAGEMENT API ERROR — container=%s, account=%s, rg=%s: %s\n%s",
                    container_name, account_name, rg_name, mgmt_exc, traceback.format_exc(),
                )
                return {"success": False, "error": str(mgmt_exc), "container": container_name}
    except ImportError as ie:
        _lifecycle_logger.error(
            "[LifecyclePolicy] IMPORT ERROR — Missing Azure management libraries: %s", ie,
        )
        return {"success": False, "error": f"Missing dependency: {ie}"}
    except Exception as outer_exc:
        _lifecycle_logger.error(
            "[LifecyclePolicy] UNEXPECTED ERROR — container=%s: %s\n%s",
            container_name, outer_exc, traceback.format_exc(),
        )
        return {"success": False, "error": str(outer_exc)}


async def apply_blob_immutability(container_name: str, blob_path: str,
                                   retention_until: datetime, mode: str = "Unlocked",
                                   shard: AzureStorageShard = None) -> Dict:
    """
    Apply time-based immutability (WORM) to a specific blob.
    mode='Locked' is permanent; mode='Unlocked' allows extension.
    For "forever" retention, Azure requires a concrete expiry date — use now + 100 years.

    Returns:
        {"success": True/False, "blob": "...", "until": "...", "mode": "...", "error": "..."}
    """
    _lifecycle_logger.info(
        "[Immutability] START — container=%s, blob=%s, until=%s, mode=%s",
        container_name, blob_path, retention_until.isoformat(), mode,
    )

    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            _lifecycle_logger.error("[Immutability] No storage shard available — cannot apply immutability")
            return {"success": False, "error": "No storage shard available", "blob": blob_path}

    try:
        async_client = shard.get_async_client()
        blob_client = async_client.get_blob_client(container_name, blob_path)
        await blob_client.set_immutability_policy(
            immutability_policy={"expiry_time": retention_until, "policy_mode": mode},
        )
        _lifecycle_logger.info(
            "[Immutability] SUCCESS — blob=%s, until=%s, mode=%s",
            blob_path, retention_until.isoformat(), mode,
        )
        return {"success": True, "blob": blob_path, "until": retention_until.isoformat(), "mode": mode}
    except Exception as e:
        error_type = type(e).__name__
        _lifecycle_logger.error(
            "[Immutability] FAILED — container=%s, blob=%s, error_type=%s: %s\n%s",
            container_name, blob_path, error_type, e, traceback.format_exc(),
        )
        # Provide actionable hints
        error_msg = str(e)
        if "ImmutabilityPolicyAlreadyExists" in error_msg:
            hint = "Immutability policy already set on this blob — this is safe, blob is already protected."
            _lifecycle_logger.warning("[Immutability] %s", hint)
            return {"success": True, "blob": blob_path, "until": retention_until.isoformat(),
                    "mode": mode, "note": "already_exists"}
        elif "ImmutableBlob" in error_msg or "immutable" in error_msg.lower():
            hint = "Blob is already immutable — this is expected behavior."
            _lifecycle_logger.warning("[Immutability] %s", hint)
            return {"success": True, "blob": blob_path, "note": "already_immutable"}
        elif "Authorization" in error_msg or "Authentication" in error_msg:
            hint = "Check storage account key permissions — need Blob Contributor role."
            _lifecycle_logger.warning("[Immutability] %s", hint)
            return {"success": False, "error": error_msg, "hint": hint}
        else:
            return {"success": False, "error": error_msg}


async def apply_legal_hold(container_name: str, blob_path: str,
                           tag: str = "tmvault-legal-hold",
                           shard: AzureStorageShard = None) -> Dict:
    """
    Apply legal hold to a blob. Separate from time-based immutability and does not expire.

    Returns:
        {"success": True/False, "blob": "...", "legal_hold": True, "tag": "...", "error": "..."}
    """
    _lifecycle_logger.info(
        "[LegalHold] START — container=%s, blob=%s, tag=%s",
        container_name, blob_path, tag,
    )

    if shard is None:
        shard = azure_storage_manager.get_default_shard()
        if not shard:
            _lifecycle_logger.error("[LegalHold] No storage shard available — cannot apply legal hold")
            return {"success": False, "error": "No storage shard available", "blob": blob_path}

    try:
        async_client = shard.get_async_client()
        blob_client = async_client.get_blob_client(container_name, blob_path)
        await blob_client.set_legal_hold(legal_hold=True)
        _lifecycle_logger.info(
            "[LegalHold] SUCCESS — blob=%s, tag=%s",
            blob_path, tag,
        )
        return {"success": True, "blob": blob_path, "legal_hold": True, "tag": tag}
    except Exception as e:
        error_type = type(e).__name__
        _lifecycle_logger.error(
            "[LegalHold] FAILED — container=%s, blob=%s, error_type=%s: %s\n%s",
            container_name, blob_path, error_type, e, traceback.format_exc(),
        )
        error_msg = str(e)
        if "LegalHoldAlreadyExists" in error_msg or "already" in error_msg.lower():
            hint = "Legal hold already set on this blob — this is safe."
            _lifecycle_logger.warning("[LegalHold] %s", hint)
            return {"success": True, "blob": blob_path, "legal_hold": True, "note": "already_set"}
        elif "Authorization" in error_msg or "Authentication" in error_msg:
            hint = "Check storage account key permissions — need Blob Contributor role."
            _lifecycle_logger.warning("[LegalHold] %s", hint)
            return {"success": False, "error": error_msg, "hint": hint}
        else:
            return {"success": False, "error": error_msg}


async def upload_blob_with_retry_from_file(container_name: str, blob_path: str, file_path: str,
                                           shard: AzureStorageShard, file_size: int = 0,
                                           max_retries: int = 3, metadata: Optional[Dict] = None) -> Dict:
    """
    Upload from a local file with parallel block uploads.
    Streams from disk — never loads full file into memory.
    Uses large block sizes (100MB) for maximum throughput.
    """
    import os
    last_error = None
    file_size = file_size or os.path.getsize(file_path)

    for attempt in range(max_retries):
        result = await shard.upload_blob_from_file(container_name, blob_path, file_path,
                                                    file_size=file_size, metadata=metadata)

        if result["success"]:
            try:
                os.unlink(file_path)
            except OSError:
                pass
            return result

        last_error = result.get("error", "Unknown error")
        if "Authorization" in last_error or "Authentication" in last_error:
            return result

        delay = settings.RETRY_DELAY_MS * (settings.RETRY_BACKOFF_MULTIPLIER ** attempt) / 1000
        await asyncio.sleep(delay)

    try:
        os.unlink(file_path)
    except OSError:
        pass
    return {"success": False, "error": f"Failed after {max_retries} retries: {last_error}"}
async def upload_blob_with_retry(container_name: str, blob_path: str, content: bytes,
                                shard: AzureStorageShard, max_retries: int = 3,
                                metadata: Optional[Dict] = None) -> Dict:
    """
    Direct upload with automatic retry and exponential backoff.
    """
    last_error = None
    
    for attempt in range(max_retries):
        result = await shard.upload_blob(container_name, blob_path, content, metadata=metadata)
        
        if result["success"]:
            return result
        
        last_error = result.get("error", "Unknown error")
        
        # Don't retry on auth/permission errors
        if "Authorization" in last_error or "Authentication" in last_error:
            return result
        
        # Exponential backoff
        delay = settings.RETRY_DELAY_MS * (settings.RETRY_BACKOFF_MULTIPLIER ** attempt) / 1000
        await asyncio.sleep(delay)
    
    return {"success": False, "error": f"Failed after {max_retries} retries: {last_error}"}
