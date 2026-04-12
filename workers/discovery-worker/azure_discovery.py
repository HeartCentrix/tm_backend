"""Azure Resource Discovery — discovers VMs, SQL DBs, and PostgreSQL servers via ARM API."""
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any
from uuid import UUID

from azure.identity.aio import ClientSecretCredential
from azure.mgmt.resource.subscriptions.aio import SubscriptionClient
from azure.core.exceptions import HttpResponseError

from shared.config import settings


async def _get_credential() -> ClientSecretCredential:
    """Get Azure credential for ARM API access."""
    return ClientSecretCredential(
        client_id=settings.EFFECTIVE_ARM_CLIENT_ID,
        client_secret=settings.EFFECTIVE_ARM_CLIENT_SECRET,
        tenant_id=settings.EFFECTIVE_ARM_TENANT_ID,
    )


async def _list_subscriptions(credential) -> List[Dict]:
    """List all Azure subscriptions the credential has access to."""
    async with SubscriptionClient(credential) as sub_client:
        subs = []
        async for sub in sub_client.subscriptions.list():
            subs.append({
                "subscription_id": sub.subscription_id,
                "display_name": sub.display_name,
                "state": sub.state,
            })
    return subs


async def discover_azure_vms(credential, subscription_id: str) -> List[Dict[str, Any]]:
    """Discover all VMs in a subscription."""
    from azure.mgmt.compute.aio import ComputeManagementClient
    vms = []
    try:
        async with ComputeManagementClient(credential, subscription_id) as compute:
            async for vm in compute.virtual_machines.list_all():
                vms.append({
                    "external_id": vm.name,
                    "display_name": vm.name,
                    "type": "AZURE_VM",
                    "azure_subscription_id": subscription_id,
                    "azure_resource_group": vm.id.split("/")[4] if vm.id else "",
                    "azure_region": vm.location,
                    "metadata": {
                        "vm_size": getattr(vm.hardware_profile, 'vm_size', None),
                        "os_type": getattr(getattr(vm.storage_profile, 'os_disk', None), 'os_type', None),
                        "power_state": "unknown",  # Would need instance view
                    },
                })
    except HttpResponseError as e:
        print(f"[AzureDiscovery] Failed to discover VMs in {subscription_id}: {e}")
    return vms


async def discover_azure_sql_databases(credential, subscription_id: str) -> List[Dict[str, Any]]:
    """Discover all SQL databases across all servers in a subscription."""
    from azure.mgmt.sql.aio import SqlManagementClient
    from azure.mgmt.resource import ResourceManagementClient
    databases = []
    try:
        async with SqlManagementClient(credential, subscription_id) as sql_client:
            # Try listing servers across the subscription first
            servers_list = []
            try:
                async for server in sql_client.servers.list():
                    servers_list.append(server)
            except (HttpResponseError, NotImplementedError):
                # Fallback: enumerate resource groups, then list servers per RG
                async with ResourceManagementClient(credential, subscription_id) as rg_client:
                    async for rg in rg_client.resource_groups.list():
                        try:
                            async for server in sql_client.servers.list_by_resource_group(rg.name):
                                servers_list.append(server)
                        except HttpResponseError:
                            pass

            for server in servers_list:
                server_name = server.name
                rg = server.id.split("/")[4] if server.id else ""
                try:
                    async for db in sql_client.databases.list_by_server(rg, server_name):
                        # Skip system databases
                        if db.name in ("master",):
                            continue
                        databases.append({
                            "external_id": db.name,
                            "display_name": f"{server_name}/{db.name}",
                            "type": "AZURE_SQL_DB",
                            "azure_subscription_id": subscription_id,
                            "azure_resource_group": rg,
                            "azure_region": server.location,
                            "metadata": {
                                "server_name": server_name,
                                "sku": getattr(db.sku, 'name', None),
                                "collation": getattr(db, 'collation', None),
                                "max_size_bytes": getattr(db, 'max_size_bytes', None),
                            },
                        })
                except HttpResponseError:
                    pass  # Server may not have accessible databases
    except HttpResponseError as e:
        print(f"[AzureDiscovery] Failed to discover SQL DBs in {subscription_id}: {e}")
    return databases


async def discover_azure_postgresql(credential, subscription_id: str) -> List[Dict[str, Any]]:
    """Discover all PostgreSQL flexible servers in a subscription."""
    from azure.mgmt.rdbms.postgresql_flexibleservers.aio import PostgreSQLManagementClient
    from azure.mgmt.resource import ResourceManagementClient
    servers = []
    try:
        async with PostgreSQLManagementClient(credential, subscription_id) as pg_client:
            # Try listing across subscription first
            servers_list = []
            try:
                async for server in pg_client.servers.list():
                    servers_list.append(server)
            except (HttpResponseError, NotImplementedError):
                # Fallback: enumerate resource groups
                async with ResourceManagementClient(credential, subscription_id) as rg_client:
                    async for rg in rg_client.resource_groups.list():
                        try:
                            async for server in pg_client.servers.list_by_resource_group(rg.name):
                                servers_list.append(server)
                        except HttpResponseError:
                            pass

            for server in servers_list:
                # Extract resource group from server ID
                rg = server.id.split("/")[4] if server.id else ""
                servers.append({
                    "external_id": server.name,
                    "display_name": server.name,
                    "type": "AZURE_POSTGRESQL",
                    "azure_subscription_id": subscription_id,
                    "azure_resource_group": rg,
                    "azure_region": server.location,
                    "metadata": {
                        "server_name": server.name,
                        "version": getattr(server, 'version', None),
                        "sku": getattr(getattr(server, 'sku', None), 'name', None),
                        "storage_mb": getattr(getattr(server, 'storage', None), 'storage_size_mb', None),
                    },
                })
    except HttpResponseError as e:
        print(f"[AzureDiscovery] Failed to discover PostgreSQL in {subscription_id}: {e}")
    return servers


async def discover_all_azure_resources() -> List[Dict[str, Any]]:
    """
    Discover all Azure resources (VMs, SQL DBs, PostgreSQL) across all accessible subscriptions.
    Returns a list of resource-shaped dicts ready for upsert into the Resource table.
    """
    if not settings.EFFECTIVE_ARM_CLIENT_ID or not settings.EFFECTIVE_ARM_CLIENT_SECRET:
        print("[AzureDiscovery] No Azure ARM credentials configured, skipping Azure discovery")
        return []

    credential = _get_credential()
    all_resources = []

    # Get subscriptions
    try:
        subscriptions = await _list_subscriptions(credential)
        print(f"[AzureDiscovery] Found {len(subscriptions)} accessible subscription(s)")
    except Exception as e:
        print(f"[AzureDiscovery] Failed to list subscriptions: {e}")
        return []

    # For each subscription, discover resources in parallel
    tasks = []
    for sub in subscriptions:
        sub_id = sub["subscription_id"]
        tasks.append(discover_azure_vms(credential, sub_id))
        tasks.append(discover_azure_sql_databases(credential, sub_id))
        tasks.append(discover_azure_postgresql(credential, sub_id))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, list):
            all_resources.extend(result)
        elif isinstance(result, Exception):
            print(f"[AzureDiscovery] Discovery task failed: {result}")

    print(f"[AzureDiscovery] Total Azure resources discovered: {len(all_resources)}")
    return all_resources
