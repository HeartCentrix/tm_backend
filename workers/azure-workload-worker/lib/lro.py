"""Long-running operation polling helper for Azure ARM operations."""
import asyncio
import time
from typing import Any


async def await_lro(poller, name: str, timeout_seconds: int = 14400,
                    poll_interval: int = 30) -> Any:
    """
    Poll an Azure Long-Running Operation (LRO) with timeout and progress logging.

    Args:
        poller: Azure SDK poller object (from begin_* methods)
        name: Human-readable name for logging (e.g., "create_restore_point/rp-abc123")
        timeout_seconds: Maximum time to wait before raising TimeoutError (default: 4 hours)
        poll_interval: Seconds between status checks (default: 30)

    Returns:
        The result of the LRO operation

    Raises:
        TimeoutError: If the operation exceeds timeout_seconds
    """
    start = time.monotonic()
    while not poller.done():
        if time.monotonic() - start > timeout_seconds:
            raise TimeoutError(f"LRO '{name}' exceeded {timeout_seconds}s timeout")
        elapsed = int(time.monotonic() - start)
        print(f"[LRO] {name} in progress ({elapsed}s elapsed)")
        await asyncio.sleep(poll_interval)
    return await poller.result()
