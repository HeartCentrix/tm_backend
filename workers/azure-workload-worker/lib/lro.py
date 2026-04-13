"""Long-running operation polling helper for Azure ARM operations."""
import asyncio
import time
import logging
from typing import Any

logger = logging.getLogger("azure.lro")


async def await_lro(poller, name: str, timeout_seconds: int = 3600,
                    poll_interval: int = 15) -> Any:
    """
    Poll an Azure Long-Running Operation (LRO) with timeout and progress logging.
    
    Works with both sync and async Azure SDK pollers.
    """
    start = time.monotonic()
    
    while True:
        elapsed = time.monotonic() - start
        if elapsed > timeout_seconds:
            raise TimeoutError(f"LRO '{name}' exceeded {timeout_seconds}s timeout (ran {elapsed:.0f}s)")
        
        # Check if done
        if poller.done():
            result = poller.result()
            if asyncio.iscoroutine(result):
                result = await result
            return result
        
        # For async pollers, also try awaiting result() directly
        try:
            result = poller.result()
            if asyncio.iscoroutine(result):
                result = await result
            if result is not None:
                return result
        except Exception:
            pass  # Not ready yet
        
        logger.info("[LRO] %s in progress (%.0fs elapsed)", name, elapsed)
        await asyncio.sleep(poll_interval)
