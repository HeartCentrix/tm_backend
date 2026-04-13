"""Azure blob copy helper — wraps shared azure_storage for server-side copy."""
from shared.azure_storage import azure_storage_manager


async def copy_from_url_to_blob(source_url: str, container_name: str,
                                 blob_path: str, source_size: int,
                                 metadata: dict = None) -> dict:
    """
    Server-side copy from a SAS URL to Azure blob storage.

    Uses the default shard from azure_storage_manager.
    The source_url must be a valid Azure Storage SAS URL (or any HTTPS URL
    that Azure Blob Storage can fetch from).
    """
    shard = azure_storage_manager.get_default_shard()
    return await shard.copy_from_url_sync(
        source_url=source_url,
        container_name=container_name,
        blob_path=blob_path,
        source_size=source_size,
        metadata=metadata or {},
    )
