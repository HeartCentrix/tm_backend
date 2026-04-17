"""Lightweight Azure region lookup helpers.

Split out from shared/azure_storage.py so services that don't need the full
azure-storage SDK (e.g. tenant-service) can resolve an account's region
without importing BlobServiceClient at module load."""
import logging
import time
from typing import Dict, Optional, Tuple

from shared.config import settings


# Azure region code → display name. Unknown codes fall back to the raw code.
_AZURE_REGION_DISPLAY: Dict[str, str] = {
    "eastus": "East US", "eastus2": "East US 2",
    "westus": "West US", "westus2": "West US 2", "westus3": "West US 3",
    "centralus": "Central US", "northcentralus": "North Central US",
    "southcentralus": "South Central US", "westcentralus": "West Central US",
    "canadacentral": "Canada Central", "canadaeast": "Canada East",
    "brazilsouth": "Brazil South", "brazilsoutheast": "Brazil Southeast",
    "northeurope": "North Europe", "westeurope": "West Europe",
    "uksouth": "UK South", "ukwest": "UK West",
    "francecentral": "France Central", "francesouth": "France South",
    "germanywestcentral": "Germany West Central", "germanynorth": "Germany North",
    "switzerlandnorth": "Switzerland North", "switzerlandwest": "Switzerland West",
    "norwayeast": "Norway East", "norwaywest": "Norway West",
    "swedencentral": "Sweden Central", "swedensouth": "Sweden South",
    "polandcentral": "Poland Central", "italynorth": "Italy North",
    "spaincentral": "Spain Central",
    "eastasia": "East Asia", "southeastasia": "Southeast Asia",
    "japaneast": "Japan East", "japanwest": "Japan West",
    "koreacentral": "Korea Central", "koreasouth": "Korea South",
    "australiaeast": "Australia East", "australiasoutheast": "Australia Southeast",
    "australiacentral": "Australia Central", "australiacentral2": "Australia Central 2",
    "centralindia": "Central India", "southindia": "South India", "westindia": "West India",
    "jioindiawest": "Jio India West", "jioindiacentral": "Jio India Central",
    "uaenorth": "UAE North", "uaecentral": "UAE Central",
    "southafricanorth": "South Africa North", "southafricawest": "South Africa West",
    "qatarcentral": "Qatar Central", "israelcentral": "Israel Central",
}

# Per-process cache: account_name -> (region_code, expiry_unix_ts).
# ARM calls aren't free and the account's region never changes, so an hour
# keeps restarts cheap without ever going stale.
_STORAGE_REGION_CACHE: Dict[str, Tuple[str, float]] = {}
_STORAGE_REGION_TTL_SECONDS = 3600

_logger = logging.getLogger("storage.region")


async def get_storage_account_region(account_name: str) -> Optional[str]:
    """Return the Azure region code (e.g. "centralus") for a storage account,
    or None if ARM credentials aren't configured or the lookup fails."""
    if not account_name:
        return None
    cached = _STORAGE_REGION_CACHE.get(account_name)
    if cached and cached[1] > time.time():
        return cached[0]

    try:
        from azure.mgmt.storage.aio import StorageManagementClient
        from azure.mgmt.subscription.aio import SubscriptionClient
        from azure.identity.aio import ClientSecretCredential
    except ImportError as ie:
        _logger.warning("[StorageRegion] Azure mgmt libs missing: %s", ie)
        return None

    client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    arm_tenant_id = settings.EFFECTIVE_ARM_TENANT_ID or settings.MICROSOFT_TENANT_ID
    if not client_id or not client_secret or not arm_tenant_id:
        _logger.info("[StorageRegion] ARM credentials not configured — skipping lookup")
        return None

    configured_sub = (settings.AZURE_SUBSCRIPTION_ID or "").strip()
    credential = ClientSecretCredential(
        client_id=client_id, client_secret=client_secret, tenant_id=arm_tenant_id,
    )

    async def _region_from_sub(subscription_id: str) -> Optional[str]:
        async with StorageManagementClient(credential, subscription_id) as mgmt:
            async for acct in mgmt.storage_accounts.list():
                if acct.name == account_name:
                    return (acct.location or "").lower() or None
        return None

    region: Optional[str] = None
    try:
        if configured_sub:
            region = await _region_from_sub(configured_sub)
        else:
            # No subscription configured — enumerate subs the credential can see
            # and search each for this storage account. Usually there's exactly
            # one sub in scope, so this is cheap.
            async with SubscriptionClient(credential) as sub_client:
                async for sub in sub_client.subscriptions.list():
                    found = await _region_from_sub(sub.subscription_id)
                    if found:
                        region = found
                        break
        if region:
            _STORAGE_REGION_CACHE[account_name] = (region, time.time() + _STORAGE_REGION_TTL_SECONDS)
        else:
            _logger.info(
                "[StorageRegion] account=%s not found in accessible subscriptions",
                account_name,
            )
        return region
    except Exception as exc:
        _logger.warning(
            "[StorageRegion] lookup failed for account=%s: %s", account_name, exc,
        )
        return None
    finally:
        await credential.close()


def format_azure_region(region_code: Optional[str]) -> Optional[str]:
    """Pretty-print an Azure region code. Returns None if input is empty."""
    if not region_code:
        return None
    return _AZURE_REGION_DISPLAY.get(region_code.lower(), region_code)
