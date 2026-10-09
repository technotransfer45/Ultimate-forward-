import asyncio
import logging
from pyrogram.errors import FloodWait

logger = logging.getLogger(__name__)

async def handle_floodwait(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            logger.warning(f"Telegram FloodWait hit: waiting for {e.value + 2} seconds.")
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            raise e
