"""HTTP 429 / 5xx retry classifier shared across all restore engines.

Detects retryable errors structurally (via the `.response.status_code`
attribute httpx raises on) so we don't hard-couple to httpx in types."""
from typing import Optional


def _is_retryable(exc: BaseException) -> bool:
    """True when the exception carries a retryable HTTP response —
    429 (throttled) or any 5xx. Everything else is a terminal error."""
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None)
    if code is None:
        return False
    return code == 429 or 500 <= code < 600


def _retry_after_seconds(exc: BaseException) -> Optional[float]:
    """Respect Graph's Retry-After header when present, else None
    (caller falls back to exponential backoff)."""
    resp = getattr(exc, "response", None)
    headers = getattr(resp, "headers", None) or {}
    ra = headers.get("Retry-After") or headers.get("retry-after")
    if ra is None:
        return None
    try:
        return float(ra)
    except (TypeError, ValueError):
        return None
