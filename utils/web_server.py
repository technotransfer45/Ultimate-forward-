from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Optional

from aiohttp import web

logger = logging.getLogger(__name__)


async def start_web_server(port: int = 8080, info_provider: Optional[Callable[[], Dict[str, Any]]] = None) -> web.AppRunner:
    """Start the keep-alive server answering `/` and `/health`."""
    started = time.time()

    async def root(request: web.Request) -> web.Response:
        return web.Response(text="Telegram automation system is running.")

    async def health(request: web.Request) -> web.Response:
        payload: Dict[str, Any] = {"status": "ok", "uptime_seconds": int(time.time() - started)}
        if info_provider is not None:
            try:
                payload.update(info_provider())
            except Exception as exc:
                payload["info_error"] = str(exc)
        return web.json_response(payload)

    app = web.Application()
    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_head("/", root)
    app.router.add_head("/health", health)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Web server listening on 0.0.0.0:%d", port)
    return runner


async def stop_web_server(runner: Optional[web.AppRunner]) -> None:
    if runner is not None:
        await runner.cleanup()
