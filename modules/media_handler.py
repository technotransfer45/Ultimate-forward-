import config
import hashlib
from pyrogram import Client
from pyrogram.types import Message
from utils.retry import handle_floodwait

class MediaHandler:
    @staticmethod
    def check_eligibility(message: Message) -> bool:
        media = message.video or message.document
        if not media:
            return False

        file_size_bytes = getattr(media, "file_size", 0) or 0
        file_size_mb = file_size_bytes / (1024 * 1024)
        duration_sec = getattr(media, "duration", 0) or 0

        # Size >= 50MB OR Duration >= 10 minutes
        if file_size_mb >= config.MIN_MEDIA_SIZE_MB or duration_sec >= config.MIN_MEDIA_DURATION_SEC:
            return True

        return False

    @staticmethod
    def get_media_hash(message: Message) -> str:
        media = message.video or message.document
        unique_id = getattr(media, "file_unique_id", "")
        file_size = str(getattr(media, "file_size", ""))
        payload = f"{unique_id}_{file_size}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    async def copy_media_clean(client: Client, target_chat_id: int, message: Message):
        clean_caption = message.caption or ""
        return await handle_floodwait(
            client.copy_message,
            chat_id=target_chat_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
            caption=clean_caption
        )
