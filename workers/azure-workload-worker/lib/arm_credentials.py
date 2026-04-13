"""Azure ARM credential acquisition for workload backup operations.

Uses a singleton credential with proper lifecycle management.
Credentials create aiohttp sessions internally — these must be
closed on shutdown to avoid "unclosed client session" warnings
and resource leaks at scale.
"""
import asyncio
import atexit
from azure.identity.aio import DefaultAzureCredential, ClientSecretCredential
from shared.config import settings


class CredentialManager:
    """
    Manages Azure credentials with proper connection lifecycle.

    - Singleton: reuses the same credential across all backup operations
    - Proper cleanup: closes aiohttp sessions on shutdown
    - Thread-safe: uses asyncio.Lock for async safety
    """

    def __init__(self):
        self._credential = None
        self._lock = asyncio.Lock()
        self._closed = False

    async def get_credential(self):
        """Get or create the singleton credential."""
        async with self._lock:
            if self._credential is None:
                client_id = settings.EFFECTIVE_ARM_CLIENT_ID
                client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET
                tenant_id = settings.EFFECTIVE_ARM_TENANT_ID

                if client_id and client_secret and tenant_id:
                    self._credential = ClientSecretCredential(
                        client_id=client_id,
                        client_secret=client_secret,
                        tenant_id=tenant_id,
                    )
                else:
                    self._credential = DefaultAzureCredential()

            return self._credential

    async def close(self):
        """Close all internal aiohttp connections."""
        async with self._lock:
            if self._credential and not self._closed:
                try:
                    await self._credential.close()
                    self._closed = True
                except Exception:
                    pass  # Best effort cleanup

    def __del__(self):
        """Sync fallback for cleanup."""
        if self._credential and hasattr(self._credential, '_closed') and not self._credential._closed:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.close())
            except Exception:
                pass


# Global singleton
credential_manager = CredentialManager()


def get_arm_credential():
    """
    Synchronous getter for backward compatibility.
    For new code, use `await credential_manager.get_credential()` instead.
    """
    # Return the credential without awaiting — Azure SDK clients accept it
    client_id = settings.EFFECTIVE_ARM_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET
    tenant_id = settings.EFFECTIVE_ARM_TENANT_ID

    if client_id and client_secret and tenant_id:
        return ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
        )

    return DefaultAzureCredential()


def get_arm_credential_for_tenant(tenant_subscription_id: str = None):
    """Convenience wrapper."""
    return get_arm_credential()
