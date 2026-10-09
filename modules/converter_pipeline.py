import asyncio
import logging
from pyrogram import Client
from pyrogram.types import Message
import config
from modules.link_extractor import LinkExtractor
from utils.retry import handle_floodwait
from database.stats_repo import StatsRepo

logger = logging.getLogger("ConverterPipeline")

class ConverterPipeline:
    def __init__(self, userbot: Client, main_bot: Client):
        self.userbot = userbot
        self.main_bot = main_bot
        self.pending_tasks = {}

    def get_active_future(self) -> tuple:
        for link, fut in list(self.pending_tasks.items()):
            if not fut.done():
                return link, fut
        return None, None

    def resolve_pending(self, response_message: Message):
        link, fut = self.get_active_future()
        if fut:
            fut.set_result(response_message)

    async def execute_conversion(self, original_caption: str, original_link: str):
        if not self.userbot:
            logger.warning("Userbot client is missing for converter automation.")
            return False

        if not config.CONVERTER_BOT_USERNAME or not config.FINAL_CONVERTED_TARGET:
            logger.warning("Converter bot username or Final converted target channel is not configured.")
            return False

        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self.pending_tasks[original_link] = future

        target_bot = config.CONVERTER_BOT_USERNAME
        try:
            logger.info(f"Sending link to converter bot @{target_bot}: {original_link}")
            await handle_floodwait(self.userbot.send_message, target_bot, original_link)

            try:
                converter_reply: Message = await asyncio.wait_for(
                    future,
                    timeout=config.CONVERTER_TIMEOUT_SEC
                )
            except asyncio.TimeoutError:
                logger.error(f"Timed out waiting for response from @{target_bot} for link {original_link}")
                return False

            reply_text = converter_reply.text or converter_reply.caption or ""
            reply_urls = LinkExtractor.extract_urls(reply_text)
            if not reply_urls:
                logger.error(f"Converter bot response contained no URL: {reply_text}")
                return False

            converted_url = reply_urls[0]
            logger.info(f"Obtained converted link: {converted_url}")

            # Substitute original link with converted link inside message caption
            final_caption = original_caption.replace(original_link, converted_url)

            # Post into Final Converted Channel
            await handle_floodwait(
                self.main_bot.send_message,
                config.FINAL_CONVERTED_TARGET,
                final_caption
            )
            await StatsRepo.increment("converted_forwarded", 1)
            logger.info(f"Successfully posted converted content to {config.FINAL_CONVERTED_TARGET}")
            return True

        except Exception as e:
            logger.error(f"Error in converter pipeline execution: {str(e)}")
            return False
        finally:
            self.pending_tasks.pop(original_link, None)
