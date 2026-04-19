"""Tiny asyncio HTTP server impersonating Graph — yields scripted responses.

Used by throttle integration tests to verify the hardened client handles
429 / Retry-After / HTTP-date / sub-response batches without depending on
Microsoft's actual service."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from aiohttp import web


@dataclass
class ScriptedResponse:
    status: int = 200
    body: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)


class FakeGraphServer:
    """Serves scripted responses for /v1.0/* paths. One response per call."""

    def __init__(self):
        self.script: List[ScriptedResponse] = []
        self.calls: List[str] = []
        self.app = web.Application()
        self.app.router.add_route("*", "/{tail:.*}", self._handle)
        self._runner: Optional[web.AppRunner] = None
        self.port: Optional[int] = None

    def queue(self, response: ScriptedResponse) -> None:
        self.script.append(response)

    async def _handle(self, request: web.Request) -> web.Response:
        self.calls.append(str(request.url))
        if not self.script:
            return web.json_response({}, status=200)
        resp = self.script.pop(0)
        return web.json_response(
            resp.body, status=resp.status, headers=resp.headers,
        )

    async def start(self) -> str:
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
