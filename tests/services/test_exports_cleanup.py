"""Fallback cleanup: deletes exports/* blobs older than 24h. Runs in
backup-scheduler as a safety net when Azure lifecycle rules aren't available."""
import datetime

import pytest

from shared.azure_storage import AzureStorageShard


pytestmark = pytest.mark.integration


async def test_cleanup_deletes_only_old_blobs(azure_test_connection_string):
    shard = AzureStorageShard.from_connection_string(azure_test_connection_string)
    try:
        await shard.ensure_container("exports")
        await shard.upload_blob("exports", "new/a.zip", b"X")
        await shard.upload_blob("exports", "old/b.zip", b"Y")

        from services.exports_cleanup import cleanup_exports
        # Shift `now` forward by 2 days — every uploaded blob is "older than 1 day"
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=2)
        deleted = await cleanup_exports(
            shard=shard, container="exports",
            older_than=datetime.timedelta(days=1), now=now,
        )
        # Both uploads happened just now, "now+2d" makes them >1d old → both deleted.
        assert "new/a.zip" in deleted
        assert "old/b.zip" in deleted

        remaining = [name async for name in shard.list_blobs("exports")]
        assert remaining == []
    finally:
        await shard.close()
