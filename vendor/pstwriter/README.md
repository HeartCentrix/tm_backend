# pstwriter (vendored)

Vendored copy of the standalone C++17 PST writer used by the
`restore-worker` to convert Microsoft Graph JSON exports to Outlook PST.

**Source of truth:** `d:/Work Qfion/PST Dev` (separate git repository).

## What lives here

```
vendor/pstwriter/
├── CMakeLists.txt        # slim — builds libpstwriter.a + pst_convert only
├── include/pstwriter/    # public headers (mail.hpp, contact.hpp, event.hpp, …)
├── src/                  # library sources (block, crc, mail, contact, event, …)
└── tools/
    └── pst_convert.cpp   # CLI entry point used by restore-worker
```

Tests, golden binaries, the second `pst_info` CLI, and Catch2 fetch are
intentionally omitted from this vendor copy — those belong in the upstream
repo and only run there.

## Refreshing from upstream

When upstream `PST Dev` ships changes (new milestones, bug fixes), refresh
the vendor copy by re-syncing the three trees:

```powershell
$src = "d:/Work Qfion/PST Dev"
$dst = "d:/Work Qfion/Begin-TM Vault App/tm_backend/vendor/pstwriter"
robocopy "$src/src"                "$dst/src"                /MIR
robocopy "$src/include/pstwriter"  "$dst/include/pstwriter"  /MIR
copy     "$src/tools/pst_convert.cpp" "$dst/tools/pst_convert.cpp"
```

After refresh, rebuild the worker image to pick up the new binary:

```bash
docker compose build restore-worker
```

## Why vendor instead of submodule

The `tm_backend` tree is not currently a git repository. Once it is, this
vendored copy should be replaced by a git submodule pointing at the
upstream `PST Dev` repo. Until then, the refresh procedure above keeps
the two trees in sync.
