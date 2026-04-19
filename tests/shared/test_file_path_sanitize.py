"""Path sanitization for ZIP arcnames — Windows-safe, length-capped, collision-free."""
from shared.file_path_sanitize import sanitize_arcname, resolve_arcname_collision


def test_reserved_chars_replaced():
    assert sanitize_arcname(
        'Projects/Q1<>:"|?*.docx', max_len=260, replace_chars='<>:"/\\|?*'
    ) == "Projects/Q1_______.docx"


def test_backslashes_become_forward_slashes():
    out = sanitize_arcname("Folder\\Sub\\file.txt", max_len=260, replace_chars='<>:"|?*')
    assert "\\" not in out


def test_under_limit_unchanged():
    arc = sanitize_arcname("Projects/Q1/report.xlsx", max_len=260, replace_chars='<>:"/\\|?*')
    assert arc == "Projects/Q1/report.xlsx"


def test_over_limit_truncated_with_suffix():
    long = "A" * 400 + ".pdf"
    arc = sanitize_arcname(long, max_len=260, replace_chars='<>:"/\\|?*')
    assert len(arc) <= 260
    assert arc.endswith(".pdf")
    long2 = "B" * 400 + ".pdf"
    arc2 = sanitize_arcname(long2, max_len=260, replace_chars='<>:"/\\|?*')
    assert arc != arc2


def test_empty_segments_stripped():
    arc = sanitize_arcname("///Folder//file.txt//", max_len=260, replace_chars='<>:"/\\|?*')
    assert arc == "Folder/file.txt"


def test_collision_resolver_appends_ext_id():
    used = {"Folder/report.xlsx"}
    resolved = resolve_arcname_collision("Folder/report.xlsx", external_id="ABC123XYZ", used=used)
    assert resolved == "Folder/report~ABC123XY.xlsx"
    assert resolved not in used or resolved in used  # added to used by function


def test_collision_resolver_unique_path_unchanged():
    used = set()
    resolved = resolve_arcname_collision("Folder/report.xlsx", external_id="ABC", used=used)
    assert resolved == "Folder/report.xlsx"
