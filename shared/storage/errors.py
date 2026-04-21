"""Storage-layer exceptions."""


class StorageError(Exception):
    """Base for all storage errors."""


class BackendUnreachableError(StorageError):
    """Backend endpoint not reachable (network, DNS, service down)."""


class ImmutableBlobError(StorageError):
    """Attempted to delete / shorten retention of an immutable blob.

    Expected during retention cleanup of locked snapshots — callers must
    catch this specifically to distinguish from real failures.
    """


class TransitionInProgressError(StorageError):
    """Write attempted while system is in draining or flipping state."""


class BackendNotFoundError(StorageError):
    """Router asked for a backend_id not present in storage_backends."""
