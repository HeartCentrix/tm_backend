"""Checkpoint schema + commit cadence helpers."""
import json

from shared.backup_checkpoint import BackupCheckpoint


def test_new_checkpoint_is_empty():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    assert cp.files_done_count == 0
    assert cp.bytes_done == 0
    assert cp.is_done("nope") is False


def test_record_and_is_done():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    cp.record_file_done(external_id="abc", size=100)
    assert cp.is_done("abc") is True
    assert cp.files_done_count == 1
    assert cp.bytes_done == 100


def test_should_commit_every_n_files():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    for i in range(499):
        cp.record_file_done(external_id=f"f{i}", size=10)
    assert cp.should_commit(every_files=500, every_bytes=10 ** 20) is False
    cp.record_file_done(external_id="f499", size=10)
    assert cp.should_commit(every_files=500, every_bytes=10 ** 20) is True


def test_should_commit_every_m_bytes():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    cp.record_file_done(external_id="big", size=2 * 1024 * 1024 * 1024)
    assert cp.should_commit(every_files=10 ** 9, every_bytes=1024 * 1024 * 1024) is True


def test_mark_committed_resets_watermark():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    for i in range(500):
        cp.record_file_done(external_id=f"f{i}", size=1)
    assert cp.should_commit(every_files=500, every_bytes=10 ** 20) is True
    cp.mark_committed()
    assert cp.should_commit(every_files=500, every_bytes=10 ** 20) is False


def test_roundtrip_via_json_result_column():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    cp.record_file_done(external_id="x", size=7)
    cp.delta_token_at_resume = "TOKEN"
    payload = cp.to_dict()
    restored = BackupCheckpoint.from_dict(payload)
    assert restored.is_done("x")
    assert restored.files_done_count == 1
    assert restored.bytes_done == 7
    assert restored.delta_token_at_resume == "TOKEN"
    json.dumps(payload)
