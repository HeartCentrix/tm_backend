"""Microsoft Graph /$batch helper.

Graph caps /$batch at 20 requests per POST. This helper hides the
chunking so callers can pass arbitrary-sized request lists and get a
list of per-op responses back in input order.

Caller injects a `post` coroutine (usually GraphClient._post) so tests
can stub it without spinning up a real HTTP stack."""
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional


GRAPH_BATCH_URL = "https://graph.microsoft.com/v1.0/$batch"
_BATCH_CHUNK_SIZE = 20


@dataclass
class BatchRequest:
    """One logical operation inside a /$batch POST. `id` MUST be unique
    within the surrounding batch so we can re-order the responses back
    to input order."""
    id: str
    method: str
    url: str
    body: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None


PostFn = Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]


async def batch_requests(post: PostFn, requests: List[BatchRequest]) -> List[Dict[str, Any]]:
    """POST `requests` to Graph's /$batch endpoint in chunks of 20.
    Returns a flat list of response dicts (one per request) in input
    order. Each response dict has at least `status` and `body` / `headers`."""
    if not requests:
        return []

    out: List[Dict[str, Any]] = [None] * len(requests)  # type: ignore[list-item]
    id_to_index: Dict[str, int] = {r.id: i for i, r in enumerate(requests)}

    for chunk_start in range(0, len(requests), _BATCH_CHUNK_SIZE):
        chunk = requests[chunk_start:chunk_start + _BATCH_CHUNK_SIZE]
        payload = {
            "requests": [
                {
                    "id": r.id,
                    "method": r.method,
                    "url": r.url,
                    **({"body": r.body} if r.body is not None else {}),
                    **({"headers": r.headers} if r.headers else {}),
                }
                for r in chunk
            ],
        }
        resp = await post(GRAPH_BATCH_URL, payload)
        for item in (resp or {}).get("responses", []) or []:
            idx = id_to_index.get(item.get("id"))
            if idx is not None:
                out[idx] = item
    return out
