"""Emit SSE-bound progress events to Redis pub/sub + last-value cache."""
import json
from redis.asyncio import Redis
from shared.config import settings

_redis: Redis | None = None


def _redis_url() -> str:
    # Always use settings.REDIS_URL_FULL — it carries the Railway auth
    # password parsed from REDIS_URL. The earlier host/port/db-only
    # form silently dropped auth, causing every publish() to crash
    # the worker with `AuthenticationError: Authentication required`.
    return settings.REDIS_URL_FULL


async def _r() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis.from_url(_redis_url())
    return _redis


async def publish(job_id: str, event: str, payload: dict) -> None:
    body = json.dumps({"event": event, **payload})
    r = await _r()
    await r.publish(f"chat_export:progress:{job_id}", body)
    await r.setex(f"chat_export:last:{job_id}", 600, body)
