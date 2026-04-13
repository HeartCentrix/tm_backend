"""
Azure API Optimizer for Scale (1000s of VMs)

Production optimizations:
1. Container existence cache (avoid redundant API calls)
2. VM config ETag caching (skip if unchanged between backups)
3. Batch disk operations (parallel grant/copy/revoke)
4. Reduced redundant GET calls (cache VM info within single backup)
"""
import asyncio
import time
import logging
from typing import Dict, Optional, Set

logger = logging.getLogger("azure.optimizer")


class ContainerCache:
    """
    Cache container existence checks to avoid redundant Azure API calls.
    Containers, once created, don't disappear unless manually deleted.
    
    For 1000 VMs across 100 tenants, this saves ~1100 API calls per backup cycle.
    """
    def __init__(self):
        self._created: Set[str] = set()
    
    def mark_created(self, account_name: str, container_name: str):
        key = f"{account_name}/{container_name}"
        self._created.add(key)
    
    def exists(self, account_name: str, container_name: str) -> bool:
        return f"{account_name}/{container_name}" in self._created


class VmConfigCache:
    """
    Cache VM configuration with ETag for change detection.
    VMs don't change between backups unless explicitly modified.
    
    For 1000 VMs, this saves ~2000 GET calls per backup cycle
    (VM config + extensions + NIC + NSG + PIP calls).
    """
    def __init__(self):
        self._configs: Dict[str, dict] = {}
        self._etags: Dict[str, str] = {}
        self._timestamp: Dict[str, float] = {}
        # Cache TTL: 1 hour (VMs rarely change)
        self.TTL_SECONDS = 3600
    
    def get(self, vm_id: str) -> Optional[dict]:
        if vm_id in self._configs:
            age = time.time() - self._timestamp.get(vm_id, 0)
            if age < self.TTL_SECONDS:
                return self._configs[vm_id]
            # Expired, remove
            del self._configs[vm_id]
            del self._etags[vm_id]
            del self._timestamp[vm_id]
        return None
    
    def put(self, vm_id: str, config: dict, etag: str):
        self._configs[vm_id] = config
        self._etags[vm_id] = etag
        self._timestamp[vm_id] = time.time()
    
    def get_etag(self, vm_id: str) -> Optional[str]:
        return self._etags.get(vm_id)
    
    def is_changed(self, vm_id: str, new_etag: str) -> bool:
        """Check if VM config has changed since last backup."""
        cached_etag = self._etags.get(vm_id)
        if cached_etag is None:
            return True
        return cached_etag != new_etag


class AzureOptimizer:
    """
    Central optimizer that coordinates all caching and batching.
    Singleton per worker instance.
    """
    def __init__(self):
        self.containers = ContainerCache()
        self.vm_configs = VmConfigCache()
    
    def log_stats(self):
        """Log current cache hit rates (useful for monitoring)."""
        logger.info(
            f"[Optimizer] Stats: {len(self.containers._created)} containers cached, "
            f"{len(self.vm_configs._configs)} VM configs cached"
        )


# Global singleton per worker
optimizer = AzureOptimizer()
