"""
Azure Resource Discovery — Afi-Style (Delegated ARM Access)

Discovers VMs, SQL databases, and PostgreSQL servers via ARM API.
During discovery, automatically assigns our SP as Entra admin of SQL servers
so backup can use our SP's credentials (no customer SQL passwords needed).
"""
import asyncio
import httpx
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from shared.config import settings
from shared.models import Tenant, TenantStatus
from shared.database import async_session_factory


async def discover_azure_tenant(tenant: Tenant) -> Dict[str, Any]:
    """
    Enumerate all subscriptions, VMs, SQL servers, Postgres servers
    accessible via the admin's delegated ARM access.

    As we discover SQL servers, we auto-assign our SP as Entra admin
    (requires Directory.ReadWrite.All).
    """
    arm_token = await _get_arm_access_token(tenant)

    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {arm_token}"}

        # List subscriptions
        subs_resp = await client.get(
            "https://management.azure.com/subscriptions?api-version=2022-12-01",
            headers=headers,
        )
        subscriptions = subs_resp.json().get("value", [])

        discovered = {"subscriptions": [], "vms": [], "sql_dbs": [], "postgres_servers": []}

        for sub in subscriptions:
            sub_id = sub["subscriptionId"]
            discovered["subscriptions"].append({
                "id": sub_id, "display_name": sub.get("displayName", ""),
            })

            # VMs
            vms_resp = await client.get(
                f"https://management.azure.com/subscriptions/{sub_id}"
                f"/providers/Microsoft.Compute/virtualMachines?api-version=2023-09-01",
                headers=headers,
            )
            for vm in vms_resp.json().get("value", []):
                discovered["vms"].append({
                    "id": vm.get("id", ""),
                    "name": vm.get("name", ""),
                    "location": vm.get("location", ""),
                    "resource_group": vm.get("id", "").split("/")[4],
                    "subscription_id": sub_id,
                    "type": "Microsoft.Compute/virtualMachines",
                })

            # SQL servers
            sql_resp = await client.get(
                f"https://management.azure.com/subscriptions/{sub_id}"
                f"/providers/Microsoft.Sql/servers?api-version=2022-05-01-preview",
                headers=headers,
            )
            sql_servers = sql_resp.json().get("value", [])

            for server in sql_servers:
                # Auto-assign our SP as Entra admin — the magic step
                try:
                    # Dynamic import — directory has hyphen, Python converts to underscore
                    from services.tenant_service.azure_provisioning import ensure_sql_server_backup_ready
                    await ensure_sql_server_backup_ready(tenant, server, arm_token)
                except Exception as e:
                    print(f"[AzureDiscovery] Warning: Failed to provision SQL server {server.get('name')}: {e}")

                # Enumerate DBs on this server
                server_id = server.get("id", "")
                server_fqdn = server.get("properties", {}).get("fullyQualifiedDomainName", "")
                server_rg = server_id.split("/")[4] if server_id else ""

                dbs_resp = await client.get(
                    f"https://management.azure.com{server_id}"
                    f"/databases?api-version=2022-05-01-preview",
                    headers=headers,
                )
                for db in dbs_resp.json().get("value", []):
                    db_name = db.get("name", "")
                    if db_name != "master":
                        discovered["sql_dbs"].append({
                            "server_id": server_id,
                            "server_fqdn": server_fqdn,
                            "server_name": server.get("name", ""),
                            "database_name": db_name,
                            "database_id": db.get("id", ""),
                            "resource_group": server_rg,
                            "subscription_id": sub_id,
                            "type": "Microsoft.Sql/servers/databases",
                            "location": server.get("location", ""),
                        })

            # PostgreSQL Flexible Servers (2026 standard)
            pg_resp = await client.get(
                f"https://management.azure.com/subscriptions/{sub_id}"
                f"/providers/Microsoft.DBforPostgreSQL/flexibleServers"
                f"?api-version=2023-03-01-preview",
                headers=headers,
            )
            for pg in pg_resp.json().get("value", []):
                pg_name = pg.get("name", "")
                pg_id = pg.get("id", "")
                pg_rg = pg_id.split("/")[4] if pg_id else ""

                # Auto-assign our SP as Azure AD admin — the magic step
                try:
                    from services.tenant_service.azure_provisioning import ensure_pg_server_backup_ready
                    await ensure_pg_server_backup_ready(tenant, pg, arm_token)
                except Exception as e:
                    print(f"[AzureDiscovery] Warning: Failed to provision PostgreSQL server {pg_name}: {e}")

                # Enumerate databases on this server
                dbs_resp = await client.get(
                    f"https://management.azure.com{pg_id}"
                    f"/databases?api-version=2023-03-01-preview",
                    headers=headers,
                )
                for db in dbs_resp.json().get("value", []):
                    db_name = db.get("name", "")
                    if db_name not in ("postgres", "azure_maintenance", "azure_sys"):
                        discovered["postgres_servers"].append({
                            "id": pg_id,
                            "name": pg_name,
                            "location": pg.get("location", ""),
                            "resource_group": pg_rg,
                            "subscription_id": sub_id,
                            "type": "Microsoft.DBforPostgreSQL/flexibleServers",
                            "database_name": db_name,
                        })

                # If no databases found, add the server itself with default database
                if not discovered["postgres_servers"] or all(p.get("server_id") != pg_id for p in discovered["postgres_servers"]):
                    discovered["postgres_servers"].append({
                        "id": pg_id,
                        "name": pg_name,
                        "location": pg.get("location", ""),
                        "resource_group": pg_rg,
                        "subscription_id": sub_id,
                        "type": "Microsoft.DBforPostgreSQL/flexibleServers",
                        "database_name": "postgres",  # Default database
                    })

            # PostgreSQL Single Servers (deprecated, but still discover for legacy customers)
            # Note: Microsoft is retiring Single Server, customers should migrate to Flexible
            single_pg_resp = await client.get(
                f"https://management.azure.com/subscriptions/{sub_id}"
                f"/providers/Microsoft.DBforPostgreSQL/servers"
                f"?api-version=2017-12-01",
                headers=headers,
            )
            for pg in single_pg_resp.json().get("value", []):
                pg_name = pg.get("name", "")
                pg_id = pg.get("id", "")
                pg_rg = pg_id.split("/")[4] if pg_id else ""

                discovered["postgres_servers"].append({
                    "id": pg_id,
                    "name": pg_name,
                    "location": pg.get("location", ""),
                    "resource_group": pg_rg,
                    "subscription_id": sub_id,
                    "type": "Microsoft.DBforPostgreSQL/servers",  # Single Server (deprecated)
                    "database_name": pg.get("properties", {}).get("fullyQualifiedDomainName", pg_name),
                })

    return discovered


async def _get_arm_access_token(tenant: Tenant) -> str:
    """Get ARM access token using admin's refresh token or SP credentials."""
    # If tenant has a refresh token from admin consent, use it
    if tenant.azure_refresh_token_encrypted:
        from shared.security import decrypt_secret
        from shared.azure_auth import AzureTokenManager

        token_manager = AzureTokenManager()
        return await token_manager.get_arm_access_token(tenant)

    # Fallback: use our own SP credentials
    client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
    client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
    tenant_id = tenant.external_tenant_id or "common"

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://management.azure.com/.default",
                "grant_type": "client_credentials",
            },
        )
        resp.raise_for_status()
        return resp.json()["access_token"]
