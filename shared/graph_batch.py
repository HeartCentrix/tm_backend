"""Microsoft Graph /v1.0/$batch wrapper.

Bundles up to 20 non-paginated GET sub-requests into one HTTP call.
Retries 429 sub-responses in a follow-up batch honoring each sub's
Retry-After. Rejects endpoints known to paginate (delta, $skiptoken,
$top) at submission time — $batch cannot follow @odata.nextLink inside
a sub-response and using it for paged data silently truncates.

Outlook caveat: inside a single batch, Graph serializes Outlook
sub-requests 4-at-a-time. Batch still saves outer-bucket cost (one
throttle accounting per batch) but does not linearly parallelize mail
reads. See:
https://learn.microsoft.com/en-us/graph/throttling

Spec: docs/superpowers/specs/2026-04-19-graph-api-throttle-hardening-design.md
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import httpx

from shared.graph_ratelimit import (
    GraphRetryExhaustedError, parse_retry_after,
)


GRAPH_BATCH_URL = "https://graph.microsoft.com/v1.0/$batch"

# Sub-paths that return paginated responses. $batch sub-requests cannot
# follow their @odata.nextLink — using them silently truncates.
_PAGINATED_MARKERS = ("/delta", "$skiptoken=", "$top=")


@dataclass
class BatchRequest:
    id: str
    method: str
    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[dict] = None


@dataclass
class BatchResponse:
    id: str
    status: int
    headers: Dict[str, str]
    body: dict


class BatchClient:
    """Async wrapper around /v1.0/$batch."""

    def __init__(self, graph_client):
        self._gc = graph_client

    @staticmethod
    def validate_requests(requests: List[BatchRequest]) -> None:
        for r in requests:
            if any(marker in r.url for marker in _PAGINATED_MARKERS):
                raise ValueError(
                    f"BatchRequest url {r.url!r} looks paginated; "
                    f"$batch sub-responses cannot follow @odata.nextLink. "
                    f"Use GraphClient._iter_pages instead."
                )

    async def batch(
        self, requests: List[BatchRequest],
    ) -> Dict[str, BatchResponse]:
        from shared.config import settings as s
        self.validate_requests(requests)
        result: Dict[str, BatchResponse] = {}
        chunk_size = s.GRAPH_BATCH_MAX_SIZE
        for i in range(0, len(requests), chunk_size):
            chunk = requests[i:i + chunk_size]
            sub_result = await self._send_chunk_with_retry(chunk)
            result.update(sub_result)
        return result

    async def _send_chunk_with_retry(
        self, chunk: List[BatchRequest],
    ) -> Dict[str, BatchResponse]:
        from shared.config import settings as s
        pending = list(chunk)
        collected: Dict[str, BatchResponse] = {}
        attempts = 0
        while pending and attempts < s.GRAPH_MAX_RETRIES + 1:
            attempts += 1
            responses = await self._send_once(pending)
            failed: List[BatchRequest] = []
            max_retry_after = 0.0
            by_id = {r.id: r for r in pending}
            for resp in responses:
                if resp.status in (429, 503):
                    ra = parse_retry_after(resp.headers.get("Retry-After"))
                    if ra is not None:
                        max_retry_after = max(max_retry_after, ra)
                    failed.append(by_id[resp.id])
                else:
                    collected[resp.id] = resp
            pending = failed
            if pending:
                await asyncio.sleep(max(max_retry_after, 1.0))
        # Any still-failing requests: return as-is with their last 429 so
        # the caller can decide whether to surface or skip.
        for req in pending:
            collected[req.id] = BatchResponse(
                id=req.id, status=429, headers={}, body={},
            )
        return collected

    async def _send_once(
        self, chunk: List[BatchRequest],
    ) -> List[BatchResponse]:
        policy = self._gc._policy
        await policy.stream_bucket.acquire()
        from shared.multi_app_manager import multi_app_manager
        await multi_app_manager.acquire_app_token(self._gc.client_id)

        token = await self._gc._get_token()
        payload = {
            "requests": [
                {
                    "id": r.id, "method": r.method, "url": r.url,
                    **({"headers": r.headers} if r.headers else {}),
                    **({"body": r.body} if r.body is not None else {}),
                }
                for r in chunk
            ]
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                GRAPH_BATCH_URL, json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
        if resp.status_code != 200:
            # Outer batch itself rejected — e.g., whole app throttled.
            # Treat as 429 on every sub-request so the retry loop handles it.
            return [
                BatchResponse(
                    id=r.id, status=resp.status_code,
                    headers=dict(resp.headers),
                    body={},
                )
                for r in chunk
            ]
        data = resp.json() or {}
        out: List[BatchResponse] = []
        for sub in (data.get("responses") or []):
            out.append(BatchResponse(
                id=str(sub.get("id")),
                status=int(sub.get("status", 200)),
                headers=dict(sub.get("headers") or {}),
                body=sub.get("body") or {},
            ))
        return out
