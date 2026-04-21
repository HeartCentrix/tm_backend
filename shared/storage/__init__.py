"""TMvault storage abstraction layer.

Every worker and service that wrote to Azure Blob directly now goes through
this package. The router picks the correct backend based on:
  - `system_config.active_backend_id` for NEW writes
  - `snapshot_item.backend_id` for READS (permanent record)

See: docs/superpowers/specs/2026-04-21-onprem-backup-store-design.md
"""
from shared.storage.base import BackendStore, BlobInfo, BlobProps

__all__ = ["BackendStore", "BlobInfo", "BlobProps"]
