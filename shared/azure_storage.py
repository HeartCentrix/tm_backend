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
import hashlib
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.core.exceptions import AzureError, ResourceExistsError

from shared.config import settings


class AzureStorageShard:
    """Represents a single Azure Storage Account shard"""
    
    def __init__(self, account_name: str, account_key: str, shard_index: int = 0):
        self.account_name = account_name
        self.account_key = account_key
        self.shard_index = shard_index
        self.connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account_name};"
            f"AccountKey={account_key};"
            f"EndpointSuffix={settings.AZURE_STORAGE_BLOB_ENDPOINT.replace('https://', '')}"
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
    
    async def upload_blob(self, container_name: str, blob_path: str, content: bytes, 
                         overwrite: bool = True, metadata: Optional[Dict] = None) -> Dict:
        """
        Upload content to Azure Blob Storage.
        Used for small items (emails, metadata, configs).
        
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
            async_client = self.get_async_client()
            blob_client = async_client.get_blob_client(container=container_name, blob=blob_path)
            
            await blob_client.upload_blob(
                content,
                overwrite=overwrite,
                metadata=metadata or {},
            )
            
            return {
                "success": True,
                "blob_url": blob_client.url,
                "blob_path": blob_path,
                "size_bytes": len(content),
                "method": "direct_upload",
            }
        except AzureError as e:
            return {
                "success": False,
                "error": str(e),
                "method": "direct_upload",
            }
    
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
    
    def get_container_name(self, tenant_id: str, resource_type: str) -> str:
        """
        Generate container name following Azure naming conventions.
        Containers must be lowercase, 3-63 chars, no special chars except hyphens.
        """
        # Shorten tenant/tenant ID to avoid exceeding 63 char limit
        tenant_short = tenant_id.replace("-", "")[:8]
        return f"backup-{resource_type.lower()}-{tenant_short}"
    
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
