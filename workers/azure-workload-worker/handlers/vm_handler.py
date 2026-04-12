"""Azure VM Backup Handler — uses VM Restore Points for atomic multi-disk snapshots."""
import asyncio
import time
from typing import Dict, Any
from datetime import datetime, timezone

from azure.core.exceptions import HttpResponseError
from azure.mgmt.compute.aio import ComputeManagementClient

from shared.models import Resource, Tenant, Snapshot, SnapshotStatus
from shared.azure_storage import azure_storage_manager
from shared.azure_storage import upload_blob_with_retry
from shared.config import settings
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from lro import await_lro
from arm_credentials import get_arm_credential
from azure_copy import copy_from_url_to_blob


class VmBackupHandler:
    """Backup Azure VMs via Restore Point Collections (atomic multi-disk capture)."""

    def __init__(self, worker_id: str = "azure-vm-worker"):
        self.worker_id = worker_id

    async def backup(self, resource: Resource, tenant: Tenant,
                     snapshot: Snapshot, msg: Dict) -> Dict:
        """
        Three-phase backup:
        1. Create/ensure VM Restore Point Collection
        2. Create Restore Point (LRO, atomic multi-disk snapshot)
        3. Server-side copy each disk to our backup blob container
        """
        credential = get_arm_credential()
        compute_client = ComputeManagementClient(credential, resource.azure_subscription_id)

        # === Phase 1: Ensure Restore Point Collection ===
        rpc_name = f"rpc-{str(resource.external_id)[:40]}"
        backup_rg = settings.AZURE_BACKUP_RESOURCE_GROUP

        await self._ensure_restore_point_collection(
            compute_client, backup_rg, rpc_name, resource)

        # === Phase 2: Create Restore Point (LRO) ===
        rp_name = f"rp-{snapshot.id.hex[:16]}-{int(time.time())}"

        # Determine consistency mode (ApplicationConsistent needs VM Agent)
        consistency_mode = tenant.extra_data.get("vm_consistency_mode", "CrashConsistent")

        # For incremental: link to previous restore point
        previous_rp_id = await self._get_previous_restore_point_id(
            resource, compute_client, backup_rg, rpc_name)

        rp_params = {
            "properties": {
                "excludeDisks": [],
                "consistencyMode": consistency_mode,
            }
        }
        if previous_rp_id:
            rp_params["properties"]["sourceRestorePoint"] = {"id": previous_rp_id}

        try:
            poller = await compute_client.restore_points.begin_create(
                resource_group_name=backup_rg,
                restore_point_collection_name=rpc_name,
                restore_point_name=rp_name,
                parameters=rp_params,
            )
        except HttpResponseError as e:
            if "RestorePointCollectionNotFound" in str(e):
                raise RuntimeError(f"RPC {rpc_name} was deleted externally")
            raise

        restore_point: RestorePoint = await await_lro(
            poller, f"create_restore_point/{rp_name}",
            timeout_seconds=3600, poll_interval=15)

        # Persist RP id on snapshot for restore
        snapshot.azure_restore_point_id = restore_point.id

        # Detect consistency downgrade
        actual_consistency = getattr(restore_point, 'consistency_mode', consistency_mode)
        if actual_consistency != consistency_mode:
            print(f"[{self.worker_id}] Consistency downgraded "
                  f"{consistency_mode} -> {actual_consistency} for {resource.external_id}")
            snapshot.extra_data = snapshot.extra_data or {}
            snapshot.extra_data["consistency_downgrade"] = {
                "requested": consistency_mode, "actual": actual_consistency}

        # === Phase 3: Copy each disk to our blob storage ===
        copy_results = []
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-vm")

        # Get disk info from restore point source metadata
        try:
            storage_profile = restore_point.source_metadata.storage_profile
            disks = list(storage_profile.data_disks or [])
            if storage_profile.os_disk:
                disks.insert(0, storage_profile.os_disk)
        except Exception:
            # Fallback: enumerate disks from the restore point collection
            disks = await self._enumerate_disks_from_rp(
                compute_client, backup_rg, rpc_name, rp_name)

        for drp in disks:
            drp_name = self._extract_drp_name(drp)
            sas_url = await self._grant_disk_access(
                compute_client, backup_rg, rpc_name, rp_name, drp_name)

            try:
                disk_size_gb = self._extract_disk_size(drp)
                blob_path = f"{resource.external_id}/{snapshot.id.hex[:12]}/{drp_name}.vhd"

                # Server-side copy — Azure Storage SAS IS valid source
                result = await copy_fromURL_to_blob(
                    source_url=sas_url,
                    container_name=container,
                    blob_path=blob_path,
                    source_size=disk_size_gb * 1024**3,
                    metadata={
                        "source_vm": resource.external_id,
                        "source_disk": drp_name,
                        "restore_point_id": restore_point.id,
                        "consistency_mode": str(actual_consistency),
                    },
                )
                copy_results.append({
                    "disk": drp_name,
                    "size_gb": disk_size_gb,
                    "blob_path": blob_path,
                    "status": "success",
                })
            except Exception as e:
                print(f"[{self.worker_id}] Disk copy FAILED for {drp_name}: {e}")
                copy_results.append({
                    "disk": drp_name,
                    "size_gb": 0,
                    "status": f"failed: {e}",
                })
            finally:
                # Always revoke SAS even if copy failed
                try:
                    await self._revoke_disk_access(
                        compute_client, backup_rg, rpc_name, rp_name, drp_name)
                except Exception:
                    pass  # SAS will expire on its own

        # === Phase 4: Chain management ===
        chain_depth = self._get_chain_depth(resource)
        if chain_depth >= 450:
            resource.extra_data = resource.extra_data or {}
            resource.extra_data["force_new_chain"] = True
            print(f"[{self.worker_id}] Chain depth {chain_depth} approaching 500 limit; "
                  f"next backup will start new chain")

        # Increment chain depth
        resource.extra_data = resource.extra_data or {}
        resource.extra_data["chain_depth"] = chain_depth + 1

        success = all(r["status"] == "success" for r in copy_results)
        return {
            "success": success,
            "restore_point_id": restore_point.id,
            "disks_copied": sum(1 for r in copy_results if r["status"] == "success"),
            "total_size_gb": sum(r.get("size_gb", 0) for r in copy_results if r["status"] == "success"),
            "consistency_mode": str(actual_consistency),
            "chain_depth": chain_depth + 1,
        }

    async def _ensure_restore_point_collection(self, compute_client, rg_name: str,
                                                rpc_name: str, resource: Resource):
        """Create Restore Point Collection if it doesn't exist (one per VM)."""
        try:
            await compute_client.restore_point_collections.get(rg_name, rpc_name)
        except HttpResponseError:
            # Doesn't exist, create it
            rp_source = {"id": f"/subscriptions/{resource.azure_subscription_id}/resourceGroups/{resource.azure_resource_group}/providers/Microsoft.Compute/virtualMachines/{resource.external_id}"}
            await compute_client.restore_point_collections.begin_create_or_update(
                resource_group_name=rg_name,
                restore_point_collection_name=rpc_name,
                parameters={
                    "location": resource.azure_region or settings.AZURE_BACKUP_REGION,
                    "source": {"id": rp_source["id"]},
                },
            )

    async def _get_previous_restore_point_id(self, resource, compute_client,
                                              rg_name: str, rpc_name: str) -> str:
        """Get the most recent restore point for incremental chaining."""
        # Don't chain if force_new_chain is set
        if resource.extra_data and resource.extra_data.get("force_new_chain"):
            resource.extra_data["force_new_chain"] = False
            return None

        try:
            rps = await compute_client.restore_points.list(rg_name, rpc_name)
            rp_list = list(rps) if hasattr(rps, '__iter__') else []
            if rp_list:
                # Sort by creation time, get most recent
                rp_list.sort(key=lambda x: getattr(x, 'created_time', ''), reverse=True)
                return rp_list[0].id
        except Exception:
            pass
        return None

    async def _grant_disk_access(self, compute_client, rg_name: str,
                                  rpc_name: str, rp_name: str, drp_name: str) -> str:
        """Grant read access to a disk restore point, return SAS URL."""
        grant_params = {
            "access": "Read",
            "durationInSeconds": 3600,  # 1 hour should be enough for copy
        }
        result = await compute_client.disk_restore_point_pairs.begin_grant_access(
            resource_group_name=rg_name,
            restore_point_collection_name=rpc_name,
            restore_point_name=rp_name,
            disk_restore_point_name=drp_name,
            grant_access_data=grant_params,
        )
        access_result = await await_lro(result, f"grant_access/{drp_name}", timeout_seconds=300, poll_interval=5)
        return access_result.access_sas

    async def _revoke_disk_access(self, compute_client, rg_name: str,
                                   rpc_name: str, rp_name: str, drp_name: str):
        """Revoke SAS access to a disk restore point."""
        await compute_client.disk_restore_point_pairs.begin_revoke_access(
            resource_group_name=rg_name,
            restore_point_collection_name=rpc_name,
            restore_point_name=rp_name,
            disk_restore_point_name=drp_name,
        )

    def _extract_drp_name(self, disk_restore_point) -> str:
        """Extract disk restore point name from its ID."""
        drp_id = getattr(disk_restore_point, 'id', '')
        if drp_id:
            return drp_id.split('/')[-1]
        return getattr(disk_restore_point, 'name', 'unknown')

    def _extract_disk_size(self, disk_restore_point) -> int:
        """Get disk size in GB."""
        rp_disk = getattr(disk_restore_point, 'disk_restore_point', None)
        if rp_disk:
            return getattr(rp_disk, 'disk_size_gb', 0) or 0
        return getattr(disk_restore_point, 'disk_size_gb', 0) or 0

    async def _enumerate_disks_from_rp(self, compute_client, rg_name, rpc_name, rp_name):
        """Fallback: list disks from a restore point by querying the RP details."""
        rp = await compute_client.restore_points.get(rg_name, rpc_name, rp_name)
        try:
            storage_profile = rp.source_metadata.storage_profile
            disks = list(storage_profile.data_disks or [])
            if storage_profile.os_disk:
                disks.insert(0, storage_profile.os_disk)
            return disks
        except Exception:
            return []

    def _get_chain_depth(self, resource: Resource) -> int:
        """Get current incremental chain depth from resource metadata."""
        if resource.extra_data:
            return resource.extra_data.get("chain_depth", 0)
        return 0


# Helper for copy — import from shared module
async def copy_fromURL_to_blob(source_url: str, container_name: str,
                               blob_path: str, source_size: int,
                               metadata: Dict = None) -> Dict:
    """Wrapper around shared azure_storage copy that handles shard selection."""
    shard = azure_storage_manager.get_default_shard()
    return await shard.copy_from_url_sync(
        source_url=source_url,
        container_name=container_name,
        blob_path=blob_path,
        source_size=source_size,
        metadata=metadata or {},
    )
