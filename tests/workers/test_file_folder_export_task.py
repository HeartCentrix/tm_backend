"""FileFolderExportTask emits (arcname, container, blob_path) members for ZIP assembly."""
import pytest

from tests.workers.workers_restore_worker_shim import FileFolderExportTask


class FakeShard:
    def __init__(self):
        self.blobs: dict = {}

    async def upload_blob(self, c, p, content, metadata=None):
        self.blobs[(c, p)] = content
        return {"success": True}

    async def download_blob(self, c, p):
        return self.blobs.get((c, p))

    async def list_blobs(self, c):
        for (cc, p) in self.blobs:
            if cc == c:
                yield p


class _Item:
    def __init__(self, external_id, name, folder_path, blob_path, size=100, extra_data=None):
        self.external_id = external_id
        self.name = name
        self.folder_path = folder_path
        self.blob_path = blob_path
        self.content_size = size
        self.snapshot_id = "snap-1"
        self.tenant_id = "tenant-1"
        self.resource_id = "res-1"
        self.extra_data = extra_data or {}


async def test_emits_blob_path_members_for_items_with_blob_path():
    shard = FakeShard()
    items = [
        _Item("id-1", "a.txt", "Documents", "tenant/res/snap/20260101T000000/id-1", size=50),
        _Item("id-2", "b.pdf", "Documents/Q1", "tenant/res/snap/20260101T000000/id-2", size=1024),
    ]
    task = FileFolderExportTask(
        folder_name="Documents",
        items=items,
        shard=shard,
        source_container="backup-files-tenant",
        dest_container="backup-exports-tenant",
        missing_policy="skip",
        max_file_bytes=10 ** 12,
        path_max_len=260,
        sanitize_chars='<>:"/\\|?*',
        manifest=None,
    )
    result = await task.run()
    assert result.failed_count == 0
    assert len(result.produced_members) == 2
    arcs = [m[0] for m in result.produced_members]
    assert "Documents/a.txt" in arcs
    assert "Documents/Q1/b.pdf" in arcs
    for arc, container, blob_path in result.produced_members:
        assert container == "backup-files-tenant"
        assert blob_path.startswith("tenant/res/snap/")


async def test_skip_policy_records_failure_for_null_blob():
    shard = FakeShard()
    items = [
        _Item("id-a", "ready.xlsx", "Documents", "tenant/res/snap/20260101T000000/id-a", size=10),
        _Item("id-b", "pending.docx", "Documents", None, size=20),
    ]
    task = FileFolderExportTask(
        folder_name="Documents",
        items=items,
        shard=shard,
        source_container="backup-files-tenant",
        dest_container="backup-exports-tenant",
        missing_policy="skip",
        max_file_bytes=10 ** 12,
        path_max_len=260,
        sanitize_chars='<>:"/\\|?*',
        manifest=None,
    )
    result = await task.run()
    assert len(result.produced_members) == 1
    assert result.failed_count == 1
    assert result.failed_items[0]["reason"] == "not_yet_backed_up"


async def test_fail_policy_raises_on_null_blob():
    shard = FakeShard()
    items = [_Item("id-b", "pending.docx", "Documents", None, size=20)]
    task = FileFolderExportTask(
        folder_name="Documents",
        items=items,
        shard=shard,
        source_container="backup-files-tenant",
        dest_container="backup-exports-tenant",
        missing_policy="fail",
        max_file_bytes=10 ** 12,
        path_max_len=260,
        sanitize_chars='<>:"/\\|?*',
        manifest=None,
    )
    with pytest.raises(RuntimeError, match="not_yet_backed_up"):
        await task.run()


async def test_too_large_file_is_skipped():
    shard = FakeShard()
    big = _Item("id-big", "huge.vhdx", "VMs", "tenant/res/snap/20260101T000000/id-big", size=300 * 1024 ** 3)
    task = FileFolderExportTask(
        folder_name="VMs",
        items=[big],
        shard=shard,
        source_container="backup-files-tenant",
        dest_container="backup-exports-tenant",
        missing_policy="skip",
        max_file_bytes=200 * 1024 ** 3,
        path_max_len=260,
        sanitize_chars='<>:"/\\|?*',
        manifest=None,
    )
    result = await task.run()
    assert len(result.produced_members) == 0
    assert result.failed_items[0]["reason"] == "too_large"


async def test_collision_suffix_applied():
    shard = FakeShard()
    items = [
        _Item("id-1abcdef", "report:q1.xlsx", "F", "tenant/res/snap/20260101T000000/id-1abcdef", size=1),
        _Item("id-2abcdef", "report:q1.xlsx", "F", "tenant/res/snap/20260101T000000/id-2abcdef", size=1),
    ]
    task = FileFolderExportTask(
        folder_name="F",
        items=items,
        shard=shard,
        source_container="backup-files-tenant",
        dest_container="backup-exports-tenant",
        missing_policy="skip",
        max_file_bytes=10 ** 12,
        path_max_len=260,
        sanitize_chars='<>:"/\\|?*',
        manifest=None,
    )
    result = await task.run()
    arcs = sorted(m[0] for m in result.produced_members)
    assert arcs[0] != arcs[1]
    assert any("~" in a for a in arcs)
