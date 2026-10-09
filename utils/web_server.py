import logging
from aiohttp import web
import config

logger = logging.getLogger(__name__)

async def root_handler(request):
    return web.Response(text="Master Dual-Userbot Forwarder Engine is active 24/7.", status=200)

async def health_handler(request):
    return web.json_response({"status": "running", "engine": "master_forwarder"})

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", root_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.PORT)
    await site.start()
    logger.info(f"Health check web server running on port {config.PORT}")
