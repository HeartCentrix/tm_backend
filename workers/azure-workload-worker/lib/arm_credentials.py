"""Azure ARM credential acquisition for workload backup operations.

Uses Azure SDK's DefaultAzureCredential which tries:
1. Environment credentials (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
2. Managed Identity (when running on Azure)
3. Azure CLI credentials (for local development)

For Railway deployment, we set environment variables from our config.
"""
import os
from azure.identity.aio import DefaultAzureCredential, ClientSecretCredential
from shared.config import settings


def get_arm_credential():
    """
    Get an Azure credential for ARM API access.

    Prefer ClientSecretCredential with our service principal credentials
    (since we run on Railway, not Azure, Managed Identity is not available).
    Falls back to DefaultAzureCredential for local development.
    """
    client_id = settings.EFFECTIVE_ARM_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET
    tenant_id = settings.EFFECTIVE_ARM_TENANT_ID

    if client_id and client_secret and tenant_id:
        return ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
        )

    # Fall back to DefaultAzureCredential (for local dev with az login)
    return DefaultAzureCredential()


def get_arm_credential_for_tenant(tenant_subscription_id: str = None):
    """
    Get credential, optionally scoped to a specific subscription.

    Note: The credential itself isn't subscription-scoped; the management
    clients are. This is a convenience wrapper for future role-based scoping.
    """
    return get_arm_credential()
