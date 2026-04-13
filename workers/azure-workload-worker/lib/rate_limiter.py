"""
Azure API Rate Limiter

Azure subscription limits:
- 1,200 write operations per 5-minute window per subscription
- 12,000 read operations per 5-minute window per subscription
- High-cost operations (snapshots, VMs) share a sub-pool (~100 per 3 min)

For 1000 VMs backing up simultaneously, we must:
1. Queue operations to stay within limits
2. Use longer polling intervals for LROs
3. Cache repeated calls (container exists, VM config)
4. Parallelize only where safe (disk copies after grant)
"""
import asyncio
import time
import logging
from collections import defaultdict
from typing import Dict

logger = logging.getLogger("azure.rate_limiter")


class AzureApiRateLimiter:
    """
    Client-side rate limiter for Azure API calls.
    
    Tracks API call counts per subscription and throttles
    when approaching Azure's limits.
    
    Limits (per subscription, per 5-minute window):
    - Write operations: 1,200 (we use 800 for safety margin)
    - Read operations: 12,000 (we use 8,000 for safety margin)
    - High-cost operations (snapshots/VMs): 100 per 3 minutes
    """
    
    def __init__(self):
        # Per-subscription tracking
        self._write_counts: Dict[str, list] = defaultdict(list)
        self._read_counts: Dict[str, list] = defaultdict(list)
        self._high_cost_counts: Dict[str, list] = defaultdict(list)
        
        # Limits (with 33% safety margin)
        self.MAX_WRITES_PER_5MIN = 800
        self.MAX_READS_PER_5MIN = 8000
        self.MAX_HIGH_COST_PER_3MIN = 60
        
        # Global semaphore for high-cost operations (max 5 concurrent)
        self._high_cost_semaphore = asyncio.Semaphore(5)
        
        self._lock = asyncio.Lock()
    
    def _cleanup_old_entries(self, counts: dict, window_seconds: int):
        """Remove entries older than the tracking window."""
        now = time.monotonic()
        cutoff = now - window_seconds
        for sub_id in list(counts.keys()):
            counts[sub_id] = [t for t in counts[sub_id] if t > cutoff]
            if not counts[sub_id]:
                del counts[sub_id]
    
    def _get_current_count(self, counts: dict, subscription_id: str, window_seconds: int) -> int:
        """Get current call count within the window."""
        self._cleanup_old_entries(counts, window_seconds)
        return len(counts.get(subscription_id, []))
    
    async def acquire_write(self, subscription_id: str):
        """Acquire permission for a write API call."""
        while True:
            async with self._lock:
                count = self._get_current_count(self._write_counts, subscription_id, 300)
                if count < self.MAX_WRITES_PER_5MIN:
                    self._write_counts[subscription_id].append(time.monotonic())
                    return
            # Rate limit approaching — wait
            wait_time = 5
            logger.warning(
                "[RateLimiter] Write limit approaching for %s (%d/5min), waiting %ds",
                subscription_id[:8], count, wait_time,
            )
            await asyncio.sleep(wait_time)
    
    async def acquire_read(self, subscription_id: str):
        """Acquire permission for a read API call."""
        while True:
            async with self._lock:
                count = self._get_current_count(self._read_counts, subscription_id, 300)
                if count < self.MAX_READS_PER_5MIN:
                    self._read_counts[subscription_id].append(time.monotonic())
                    return
            wait_time = 2
            logger.warning(
                "[RateLimiter] Read limit approaching for %s (%d/5min), waiting %ds",
                subscription_id[:8], count, wait_time,
            )
            await asyncio.sleep(wait_time)
    
    async def acquire_high_cost(self, subscription_id: str):
        """
        Acquire permission for a high-cost operation (snapshot, VM create/delete).
        Uses both rate limit tracking AND a concurrency semaphore.
        """
        await self._high_cost_semaphore.acquire()
        try:
            while True:
                async with self._lock:
                    count = self._get_current_count(self._high_cost_counts, subscription_id, 180)
                    if count < self.MAX_HIGH_COST_PER_3MIN:
                        self._high_cost_counts[subscription_id].append(time.monotonic())
                        return
                wait_time = 10
                logger.warning(
                    "[RateLimiter] High-cost limit approaching for %s (%d/3min), waiting %ds",
                    subscription_id[:8], count, wait_time,
                )
                await asyncio.sleep(wait_time)
        finally:
            self._high_cost_semaphore.release()
    
    def get_status(self, subscription_id: str) -> Dict[str, int]:
        """Get current API usage stats for a subscription."""
        return {
            "writes_5min": self._get_current_count(self._write_counts, subscription_id, 300),
            "reads_5min": self._get_current_count(self._read_counts, subscription_id, 300),
            "high_cost_3min": self._get_current_count(self._high_cost_counts, subscription_id, 180),
            "write_limit": self.MAX_WRITES_PER_5MIN,
            "read_limit": self.MAX_READS_PER_5MIN,
            "high_cost_limit": self.MAX_HIGH_COST_PER_3MIN,
        }


# Global singleton
rate_limiter = AzureApiRateLimiter()
