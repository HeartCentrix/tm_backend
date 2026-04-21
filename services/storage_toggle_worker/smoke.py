"""Post-cutover smoke: canary backup + restore + passthrough restore."""
from __future__ import annotations

import logging
import uuid

from shared.storage.router import router

log = logging.getLogger("toggle.smoke")


async def run_smoke(db, new_backend_id: str, old_backend_id: str) -> None:
    store = router.get_store_by_id(new_backend_id).shard_for(
        "smoke-tenant", "smoke-res",
    )
    container = "tmvault-smoke"
    key = f"smoke-{uuid.uuid4().hex[:8]}.bin"
    body = b"smoke" * 2048
    await store.upload(container, key, body, metadata={"kind": "smoke"})
    got = await store.download(container, key)
    assert got == body, "smoke: round-trip mismatch"
    log.info("smoke: canary backup + restore ok on %s", new_backend_id)
    try:
        await store.delete(container, key)
    except Exception as e:
        log.warning("smoke: cleanup failed (non-fatal): %s", e)

    # Passthrough check — read a fresh blob on the OLD backend to prove the
    # router still serves it after the flip.
    old_store = router.get_store_by_id(old_backend_id).shard_for(
        "smoke-tenant", "smoke-res",
    )
    old_key = f"smoke-prev-{uuid.uuid4().hex[:8]}.bin"
    await old_store.upload(container, old_key, body, metadata={"kind": "smoke-prev"})

    class _Item:
        def __init__(self, bid: str):
            self.backend_id = bid
    item = _Item(old_backend_id)
    restored = await router.get_store_for_item(item).shard_for(
        "smoke-tenant", "smoke-res",
    ).download(container, old_key)
    assert restored == body, "smoke: passthrough mismatch"
    log.info("smoke: passthrough restore ok from %s", old_backend_id)
    try:
        await old_store.delete(container, old_key)
    except Exception:
        pass
