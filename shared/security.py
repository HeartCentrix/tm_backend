"""Shared security utilities."""
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

try:
    from fastapi import Depends, HTTPException, status
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
except ModuleNotFoundError:  # pragma: no cover - worker-only runtimes
    Depends = None
    HTTPException = None
    status = None
    HTTPBearer = None
    HTTPAuthorizationCredentials = Any

from shared.config import settings

security = HTTPBearer() if HTTPBearer else None


# ==================== Secret Encryption ====================

def _get_fernet():
    """Get Fernet cipher instance from ENCRYPTION_KEY env var.

    ENCRYPTION_KEY MUST be a separate random secret from JWT_SECRET. Deriving
    one from the other means a single env-var leak (SSRF, leaked .env, log
    capture) decrypts every stored secret. Generate with:
        python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    """
    from cryptography.fernet import Fernet

    key = settings.ENCRYPTION_KEY
    if not key:
        raise RuntimeError(
            "ENCRYPTION_KEY must be set. Provision a separate random 32-byte "
            "Fernet key in every environment (including CI) — do NOT reuse or "
            "derive from JWT_SECRET."
        )
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception:
        raise RuntimeError("Invalid ENCRYPTION_KEY. Must be a base64-encoded 32-byte Fernet key.")


def encrypt_secret(plaintext: str) -> bytes:
    """Encrypt a secret string using Fernet symmetric encryption.
    
    Returns ciphertext as bytes.
    """
    f = _get_fernet()
    return f.encrypt(plaintext.encode('utf-8'))


def decrypt_secret(ciphertext: bytes) -> str:
    """Decrypt a previously encrypted secret string.
    
    Returns the original plaintext string.
    """
    f = _get_fernet()
    return f.decrypt(ciphertext).decode('utf-8')


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    from jose import jwt

    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(hours=settings.JWT_EXPIRATION_HOURS))
    to_encode.update({"exp": expire, "jti": str(uuid.uuid4()), "type": "access"})
    return jwt.encode(to_encode, settings.ACCESS_TOKEN_SECRET, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(data: dict) -> str:
    from jose import jwt

    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=settings.JWT_REFRESH_EXPIRATION_DAYS)
    # `jti` lets us revoke an individual refresh token without rotating the
    # signing secret. The auth-service stamps the JTI into a Redis denylist
    # on rotation/logout; decode_token's caller checks the denylist on
    # refresh paths (see is_refresh_token_revoked / revoke_refresh_token).
    to_encode.update({"exp": expire, "type": "refresh", "jti": str(uuid.uuid4())})
    return jwt.encode(to_encode, settings.REFRESH_TOKEN_SECRET, algorithm=settings.JWT_ALGORITHM)


# ==================== Refresh-token revocation (denylist) ====================
#
# Refresh tokens carry a `jti` claim. On every successful /auth/refresh we
# add the *used* jti to a Redis-backed denylist with TTL = remaining token
# lifetime, then issue a new refresh token with a fresh jti. If the same
# refresh token is presented twice (legitimate user race or replay attack),
# the second use sees the jti in the denylist and is rejected. /auth/logout
# also revokes the current jti.
#
# The auth-service is the only caller; we keep these helpers async and the
# rest of decode_token sync so every authenticated request doesn't pay a
# Redis round-trip. Access tokens are short-lived (hours) and aren't checked
# against the denylist — use the secret rotation lever for mass revocation
# of access tokens.

_REVOCATION_KEY_PREFIX = "jwt_revoked:"
_revocation_redis: Optional[Any] = None


async def _get_revocation_redis():
    """Lazy async Redis client for the revocation denylist. None when disabled."""
    global _revocation_redis
    if _revocation_redis is not None:
        return _revocation_redis
    if not settings.REDIS_ENABLED:
        return None
    try:
        from redis.asyncio import Redis
        _revocation_redis = Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
            socket_timeout=2,
            socket_connect_timeout=2,
        )
        return _revocation_redis
    except Exception:
        return None


async def revoke_refresh_token(jti: str, ttl_seconds: int) -> None:
    """Add a refresh-token jti to the denylist for the rest of its lifetime."""
    if not jti:
        return
    client = await _get_revocation_redis()
    if client is None:
        return
    try:
        await client.setex(f"{_REVOCATION_KEY_PREFIX}{jti}", max(1, ttl_seconds), "1")
    except Exception:
        # Best-effort: if Redis is down we can't add to the denylist. Log via
        # the FastAPI runtime if the caller wants — return None either way so
        # logout/refresh don't 500 on a transient Redis hiccup.
        return


async def is_refresh_token_revoked(jti: str) -> bool:
    """Return True when the jti has been revoked. Fail-open on Redis errors."""
    if not jti:
        return False
    client = await _get_revocation_redis()
    if client is None:
        return False
    try:
        return bool(await client.exists(f"{_REVOCATION_KEY_PREFIX}{jti}"))
    except Exception:
        return False


def _unauthorized(detail: str):
    if HTTPException is None or status is None:
        raise RuntimeError("JWT decoding requires FastAPI runtime support")
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def decode_token(token: str, expected_type: str = "access") -> dict:
    from jose import JWTError, jwt

    if expected_type == "access":
        secret = settings.ACCESS_TOKEN_SECRET
    elif expected_type == "refresh":
        secret = settings.REFRESH_TOKEN_SECRET
    else:
        raise ValueError(f"Unknown token type: {expected_type!r}")

    try:
        payload = jwt.decode(token, secret, algorithms=[settings.JWT_ALGORITHM])
    except JWTError:
        raise _unauthorized("Invalid or expired token")

    if payload.get("type") != expected_type:
        raise _unauthorized("Invalid token type")

    return payload


def _get_user_from_token(token: str) -> dict:
    payload = decode_token(token, expected_type="access")

    user_id: str = payload.get("sub")
    if user_id is None:
        if HTTPException is None:
            raise RuntimeError("Token validation requires FastAPI runtime support")
        raise HTTPException(status_code=401, detail="Invalid token")

    return {
        "id": user_id,
        "email": payload.get("email"),
        "roles": payload.get("roles", []),
        "org_id": payload.get("orgId"),
        "tenant_ids": payload.get("tenantIds", []),
    }


if Depends is not None and security is not None:
    def get_current_user_from_token(
        credentials: HTTPAuthorizationCredentials = Depends(security),
    ) -> dict:
        """Extract user from JWT token (used by API Gateway)."""
        return _get_user_from_token(credentials.credentials)
else:  # pragma: no cover - non-API runtimes should not call this
    def get_current_user_from_token(credentials: Optional[Any] = None) -> dict:
        raise RuntimeError("FastAPI authentication helpers are unavailable in this runtime")
