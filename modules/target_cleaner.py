import asyncio
import hashlib
import logging
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError, ChannelPrivate, ChatAdminRequired
from database.mongo import duplicates_col, checkpoints_col
from modules.link_extractor import LinkExtractor
from utils.retry import handle_floodwait

logger = logging.getLogger(__name__)

class TargetCleaner:
    def __init__(self, client: Client, channel_id):
        self.client = client
        # String numeric ID ko proper integer me cast karna zaroori hai
        if isinstance(channel_id, str):
            clean_str = channel_id.strip()
            if clean_str.startswith("-100") or clean_str.isdigit() or (clean_str.startswith("-") and clean_str[1:].isdigit()):
                self.channel_id = int(clean_str)
            else:
                self.channel_id = clean_str
        else:
            self.channel_id = channel_id

    async def execute_cleanup(self):
        if not self.channel_id:
            return

        checkpoint_key = f"cleaner_{self.channel_id}"
        record = await checkpoints_col.find_one({"source_id": checkpoint_key})
        last_scanned_id = record["last_processed_id"] if record else 0

        logger.info(f"Target Cleaner starting for {self.channel_id} at message ID {last_scanned_id}")
        current_max_id = last_scanned_id

        # Peer Validation Check (Prevents Peer id invalid crash)
        try:
            await self.client.get_chat(self.channel_id)
        except (ValueError, KeyError) as e:
            logger.warning(f"Target Cleaner skipped for {self.channel_id} (Uncached Peer / Missing Access Hash): {e}")
            return
        except (ChannelPrivate, ChatAdminRequired):
            logger.warning(f"Target Cleaner skipped for {self.channel_id}: Bot lacks admin permissions or access.")
            return
        except RPCError as e:
            logger.warning(f"Target Cleaner RPC error on {self.channel_id}: {e}")
            return
        except Exception as e:
            logger.error(f"Target Cleaner pre-check failed for {self.channel_id}: {e}")
            return

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
        except FloodWait as fw:
            logger.warning(f"FloodWait during cleanup on {self.channel_id}. Sleeping {fw.value}s.")
            await asyncio.sleep(fw.value + 2)
        except (ValueError, KeyError) as e:
            logger.warning(f"Cleanup peer resolution lost for {self.channel_id}: {e}")
        except Exception as e:
            logger.error(f"Cleanup failure for {self.channel_id}: {str(e)}")
        
