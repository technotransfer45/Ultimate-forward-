import asyncio
import hashlib
import logging
from pyrogram import Client
from database.mongo import duplicates_col, checkpoints_col
from modules.link_extractor import LinkExtractor
from utils.retry import handle_floodwait

logger = logging.getLogger(__name__)

class TargetCleaner:
    def __init__(self, client: Client, channel_id: int):
        self.client = client
        self.channel_id = channel_id

    async def execute_cleanup(self):
        if not self.channel_id:
            return

        checkpoint_key = f"cleaner_{self.channel_id}"
        record = await checkpoints_col.find_one({"source_id": checkpoint_key})
        last_scanned_id = record["last_processed_id"] if record else 0

        logger.info(f"Target Cleaner starting for {self.channel_id} at message ID {last_scanned_id}")
        current_max_id = last_scanned_id

        try:
            async for message in self.client.get_chat_history(self.channel_id):
                if message.id <= last_scanned_id:
                    break

                if message.id > current_max_id:
                    current_max_id = message.id

                text = message.caption or message.text or ""
                urls = LinkExtractor.extract_urls(text)

                is_dup = False
                for u in urls:
                    canon = LinkExtractor.canonicalize(u)
                    u_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()
                    existing = await duplicates_col.find_one({"hash": u_hash})
                    if existing and existing.get("message_id") != message.id:
                        is_dup = True
                        break
                    elif not existing:
                        await duplicates_col.insert_one({
                            "hash": u_hash,
                            "raw_data": canon,
                            "destination": self.channel_id,
                            "message_id": message.id
                        })

                if is_dup:
                    try:
                        await handle_floodwait(self.client.delete_messages, self.channel_id, message.id)
                        logger.info(f"Scrubbed duplicate message {message.id} from target {self.channel_id}")
                    except Exception as err:
                        logger.error(f"Failed deleting duplicate message {message.id}: {str(err)}")

                await asyncio.sleep(0.05)

            await checkpoints_col.update_one(
                {"source_id": checkpoint_key},
                {"$set": {"last_processed_id": current_max_id}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Cleanup failure for {self.channel_id}: {str(e)}")
