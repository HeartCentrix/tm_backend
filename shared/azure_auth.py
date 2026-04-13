"""
Afi-style Azure Token Management

Manages Azure access using admin's refresh token (delegated ARM access)
and application credentials (Graph Directory.ReadWrite.All).

The admin-consented refresh token has typical lifetime of ~90 days.
We refresh the access token before each ARM operation (it's short-lived, ~1 hour).
If the refresh token expires, we mark the tenant 'needs_reconsent'.
"""
import httpx
import json
from datetime import datetime, timezone
from typing import Optional, Tuple

from shared.config import settings
from shared.security import decrypt_secret, encrypt_secret
from shared.database import async_session_factory
from shared.models import Tenant, TenantStatus
from sqlalchemy import select


class AzureAuthError(Exception):
    """Raised when Azure auth operations fail."""
    pass


class AzureTokenManager:
    """
    Manages Afi-style Azure access using admin's refresh token.
    """

    async def get_arm_access_token(self, tenant: Tenant) -> str:
        """
        Fetch a fresh ARM access token using the admin's refresh token.
        """
        if not tenant.azure_refresh_token_encrypted:
            raise AzureAuthError(
                f"No Azure refresh token for tenant {tenant.id}. "
                f"Admin must complete Azure onboarding consent flow."
            )

        refresh_token = decrypt_secret(tenant.azure_refresh_token_encrypted)

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://login.microsoftonline.com/{tenant.external_tenant_id}/oauth2/v2.0/token",
                data={
                    "client_id": settings.MICROSOFT_CLIENT_ID,
                    "client_secret": settings.MICROSOFT_CLIENT_SECRET,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                    "scope": "https://management.azure.com/user_impersonation offline_access",
                },
            )
            if resp.status_code != 200:
                await self._mark_reconsent_needed(tenant, resp.text)
                raise AzureAuthError(f"Refresh token invalid/expired; admin must reconsent: {resp.text[:200]}")

            data = resp.json()

            # Rotate refresh token if new one returned
            if "refresh_token" in data and data["refresh_token"] != refresh_token:
                async with async_session_factory() as session:
                    t = await session.get(Tenant, tenant.id)
                    if t:
                        t.azure_refresh_token_encrypted = encrypt_secret(data["refresh_token"])
                        t.azure_refresh_token_updated_at = datetime.now(timezone.utc)
                        await session.commit()

            return data["access_token"]

    async def get_graph_app_token(self, tenant: Tenant) -> str:
        """
        App-only Graph token for Directory.ReadWrite.All operations.
        Uses client credentials (our app's secret), no user context.
        """
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://login.microsoftonline.com/{tenant.external_tenant_id}/oauth2/v2.0/token",
                data={
                    "client_id": settings.MICROSOFT_CLIENT_ID,
                    "client_secret": settings.MICROSOFT_CLIENT_SECRET,
                    "scope": "https://graph.microsoft.com/.default",
                    "grant_type": "client_credentials",
                },
            )
            resp.raise_for_status()
            return resp.json()["access_token"]

    async def get_sql_sp_credential(self) -> Tuple[str, str]:
        """
        For SQL data-plane auth: use our app's own client credentials.
        Works because we're assigned as Entra admin of the SQL server.
        
        Returns (client_id, client_secret) for ActiveDirectoryServicePrincipal auth.
        """
        return settings.MICROSOFT_CLIENT_ID, settings.MICROSOFT_CLIENT_SECRET

    async def _mark_reconsent_needed(self, tenant: Tenant, error: str):
        """Mark tenant as needing reconsent."""
        async with async_session_factory() as session:
            t = await session.get(Tenant, tenant.id)
            if t:
                t.status = TenantStatus.NEEDS_RECONSENT
                t.extra_data = t.extra_data or {}
                t.extra_data["reconsent_error"] = error[:500]
                t.extra_data["reconsent_at"] = datetime.now(timezone.utc).isoformat()
                await session.commit()


# Global singleton
azure_token_manager = AzureTokenManager()
