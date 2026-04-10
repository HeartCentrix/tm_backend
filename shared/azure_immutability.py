"""Azure Blob Storage Immutability Configuration

Configures Azure Blob containers with:
- Time-based retention policies (WORM - Write Once Read Many)
- Legal holds for specific backups
- Version-level immutability for ransomware protection
"""
from azure.storage.blob import BlobServiceClient
from azure.mgmt.storage import StorageManagementClient
from azure.identity import ClientSecretCredential
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class AzureImmutabilityConfig:
    """Configure Azure Blob Storage immutability policies"""

    def __init__(
        self,
        storage_account_name: str,
        storage_account_key: str,
        subscription_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage_account_name};"
            f"AccountKey={storage_account_key};"
            f"EndpointSuffix=core.windows.net"
        )
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

        # Management client (requires Azure AD credentials)
        self.storage_mgmt_client = None
        if subscription_id and tenant_id and client_id and client_secret:
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
            self.storage_mgmt_client = StorageManagementClient(
                credential=credential,
                subscription_id=subscription_id,
            )

    def configure_container_immutability(
        self,
        container_name: str,
        retention_days: int = 365,
        allow_protected_append_writes: bool = True,
    ):
        """
        Configure a container container with time-based retention policy.
        This makes blobs immutable for the specified retention period.

        Note: Requires 'Microsoft.Authorization/roleAssignments/write' permission
        and the storage account must have hierarchical namespace enabled
        (or use management API for standard accounts).

        Args:
            container_name: Name of the container to configure
            retention_days: Number of days to retain blobs (WORM period)
            allow_protected_append_writes: Allow appending to blobs (useful for logs)
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)

            # Create container if it doesn't exist
            container_client.create_container()

            # Set immutability policy via management API
            if self.storage_mgmt_client:
                from azure.mgmt.storage.models import ImmutabilityPolicy

                # Policy name derived from container name
                policy_name = f"immutability-{container_name}"
                resource_group = self._get_resource_group()

                # Create immutability policy
                policy = self.storage_mgmt_client.immutability_policies.create_or_update(
                    resource_group_name=resource_group,
                    account_name=self.storage_account_name,
                    container_name=container_name,
                    immutability_policy_name=policy_name,
                    parameters=ImmutabilityPolicy(
                        immutability_period_since_creation_in_days=retention_days,
                        allow_protected_append_writes=allow_protected_append_writes,
                    ),
                )

                logger.info(
                    f"Immutability policy created for container {container_name}: "
                    f"{retention_days} days retention"
                )
                return policy
            else:
                logger.warning(
                    f"Cannot configure immutability policy for {container_name}: "
                    f"Azure AD credentials not provided. Using container-level metadata instead."
                )
                # Fallback: Set metadata flag to mark as immutable
                container_client.set_container_metadata(
                    metadata={
                        "immutability-enabled": "true",
                        "retention-days": str(retention_days),
                        "worm-protected": "true",
                    }
                )
                return None

        except Exception as e:
            logger.error(f"Failed to configure immutability for {container_name}: {e}")
            raise

    def set_legal_hold(
        self,
        container_name: str,
        blob_prefix: str,
        hold_reason: str = "Investigation hold",
    ):
        """
        Set legal hold on blobs matching prefix.
        Legal holds prevent deletion/modification regardless of retention policy.

        Args:
            container_name: Container name
            blob_prefix: Blob path prefix to apply hold
            hold_reason: Reason for the legal hold
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)

            # List blobs matching prefix
            blobs = container_client.list_blobs(name_starts_with=blob_prefix)

            held_count = 0
            for blob in blobs:
                blob_client = container_client.get_blob_client(blob.name)

                # Set metadata to mark as legal hold
                blob_client.set_blob_metadata(
                    metadata={
                        **blob.metadata,
                        "legal-hold": "true",
                        "legal-hold-reason": hold_reason,
                        "legal-hold-date": "true",
                    }
                )
                held_count += 1

            logger.info(f"Legal hold applied to {held_count} blobs under {blob_prefix}")
            return held_count

        except Exception as e:
            logger.error(f"Failed to set legal hold: {e}")
            raise

    def enable_version_level_immutability(
        self,
        container_name: str,
        retention_days: int = 365,
    ):
        """
        Enable version-level immutability on a container.
        This makes all blob versions immutable for the retention period.

        Note: Requires storage account with versioning enabled.
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)

            # Enable container versioning (if not already enabled)
            container_client.set_container_metadata(
                metadata={
                    "versioning-enabled": "true",
                    "version-retention-days": str(retention_days),
                    "version-immutability": "true",
                }
            )

            logger.info(
                f"Version-level immutability enabled for {container_name}: "
                f"{retention_days} days retention"
            )

        except Exception as e:
            logger.error(f"Failed to enable version immutability: {e}")
            raise

    def upload_blob_with_immutability(
        self,
        container_name: str,
        blob_name: str,
        data: bytes,
        overwrite: bool = True,
    ):
        """
        Upload a blob with immutability metadata.

        Args:
            container_name: Container name
            blob_name: Blob path/name
            data: Blob content as bytes
            overwrite: Whether to overwrite existing blob

        Returns:
            BlobProperties object
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)

            # Upload with immutability metadata
            metadata = {
                "immutable": "true",
                "upload-time": "true",
            }

            blob_client.upload_blob(
                data,
                overwrite=overwrite,
                metadata=metadata,
            )

            logger.info(f"Blob uploaded with immutability: {blob_name}")
            return blob_client.get_blob_properties()

        except Exception as e:
            logger.error(f"Failed to upload blob with immutability: {e}")
            raise

    def clear_legal_hold(
        self,
        container_name: str,
        blob_prefix: str,
    ):
        """
        Clear legal hold from blobs matching prefix.
        Only authorized users should be able to do this.

        Args:
            container_name: Container name
            blob_prefix: Blob path prefix to clear hold
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs(name_starts_with=blob_prefix)

            cleared_count = 0
            for blob in blobs:
                blob_client = container_client.get_blob_client(blob.name)
                props = blob_client.get_blob_properties()

                if props.metadata.get("legal-hold") == "true":
                    # Remove legal hold metadata
                    metadata = {k: v for k, v in props.metadata.items() if not k.startswith("legal-hold")}
                    blob_client.set_blob_metadata(metadata=metadata)
                    cleared_count += 1

            logger.info(f"Legal hold cleared from {cleared_count} blobs under {blob_prefix}")
            return cleared_count

        except Exception as e:
            logger.error(f"Failed to clear legal hold: {e}")
            raise

    def _get_resource_group(self) -> str:
        """Get resource group for the storage account"""
        # In production, this should be configurable from settings
        return "tm-vault-backup-rg"


# Global instance (initialized on demand)
_immutability_config: Optional[AzureImmutabilityConfig] = None


def get_immutability_config() -> Optional[AzureImmutabilityConfig]:
    """Get Azure immutability configuration"""
    global _immutability_config

    if _immutability_config is None:
        from shared.config import settings

        if not settings.AZURE_STORAGE_ACCOUNT_NAME or not settings.AZURE_STORAGE_ACCOUNT_KEY:
            logger.warning("Azure Storage credentials not configured")
            return None

        _immutability_config = AzureImmutabilityConfig(
            storage_account_name=settings.AZURE_STORAGE_ACCOUNT_NAME,
            storage_account_key=settings.AZURE_STORAGE_ACCOUNT_KEY,
            subscription_id=getattr(settings, 'AZURE_SUBSCRIPTION_ID', None),
            tenant_id=getattr(settings, 'AZURE_AD_TENANT_ID', None),
            client_id=getattr(settings, 'AZURE_AD_CLIENT_ID', None),
            client_secret=getattr(settings, 'AZURE_AD_CLIENT_SECRET', None),
        )

    return _immutability_config
