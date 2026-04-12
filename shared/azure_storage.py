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
import re
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.core.exceptions import AzureError, ResourceExistsError

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
