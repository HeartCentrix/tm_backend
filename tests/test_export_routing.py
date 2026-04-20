from shared.export_routing import pick_restore_queue


def test_small_restore_goes_to_normal():
    assert pick_restore_queue(total_bytes=10 * 1024 * 1024) == "restore.normal"


def test_large_restore_goes_to_heavy():
    assert pick_restore_queue(total_bytes=60 * 1024**3) == "restore.heavy"


def test_zero_bytes_defaults_to_normal():
    assert pick_restore_queue(total_bytes=0) == "restore.normal"


def test_boundary_50gb_stays_normal():
    assert pick_restore_queue(total_bytes=50 * 1024**3) == "restore.normal"
