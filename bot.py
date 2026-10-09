import asyncio
import hashlib
import logging
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import UserNotParticipant, ChatAdminRequired, ChannelPrivate, FloodWait

import config
from database.mongo import init_db
from database.sources_repo import SourcesRepo
from database.checkpoints_repo import CheckpointsRepo
from database.duplicates_repo import DuplicatesRepo
from database.stats_repo import StatsRepo

from modules.metadata_parser import MetadataParser
from modules.media_handler import MediaHandler
from modules.link_extractor import LinkExtractor
from modules.link_recovery import LinkRecovery
from modules.content_filter import ContentFilter
from modules.terabox_scraper import TeraBoxAppScraper
from modules.converter_pipeline import ConverterPipeline
from modules.target_cleaner import TargetCleaner
from modules.live_status import LiveStatus
from utils.web_server import start_web_server
from utils.retry import handle_floodwait

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("MasterForwarder")

# 1. Main Bot Client (Sends messages, Admin interface, Status)
bot = Client(
    "master_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)

# 2. Dual Userbot Clients
userbot_1 = None
if config.USERBOT_SESSION_1:
    userbot_1 = Client(
        "ub_account_1",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=config.USERBOT_SESSION_1
    )

userbot_2 = None
if config.USERBOT_SESSION_2:
    userbot_2 = Client(
        "ub_account_2",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=config.USERBOT_SESSION_2
    )

# Converter Pipeline Instance
converter_pipeline = ConverterPipeline(userbot_1 if userbot_1 else userbot_2, bot)

worker_status_ledger = {"Worker-1": "Idle", "Worker-2": "Idle"}

# ----------------- ADMIN COMMANDS ----------------- #

@bot.on_message(filters.command("start") & filters.user(config.ADMINS))
async def cmd_start(_, message: Message):
    welcome_text = (
        "🤖 **Master Dual-Account Forwarder & Converter Bot**\n\n"
        "Commands:\n"
        "• `/addterabox <ID / LINK>` - Add TeraBox Telegram ID, Share Link, or Hive In-App URL\n"
        "• `/addtelegram <ID>` - Add Standard Telegram Source Channel\n"
        "• `/removesource <ID>` - Remove Source from database\n"
        "• `/sources` - View list of all registered sources\n"
        "• `/status` - Live statistics, queue, and worker states\n"
        "• `/cleantargets` - Scrub duplicate posts in target channels"
    )
    await message.reply(welcome_text)

@bot.on_message(filters.command("addterabox") & filters.user(config.ADMINS))
async def cmd_add_terabox(_, message: Message):
    if len(message.command) < 2:
        return await message.reply(
            "⚠️ **Usage:**\n"
            "• Telegram Channel: `/addterabox -100xxxxxxxxxx`\n"
            "• TeraBox In-App Channel: `/addterabox https://dm.nephobox.com/wap/hive/channelShare?bot_uk=xxxxx`\n"
            "• TeraBox Share Link: `/addterabox https://teraboxapp.com/s/1xxxxxxxxx`"
        )

    input_val = message.command[1].strip()

    # Case A: Telegram Channel ID (-100...)
    if input_val.startswith("-100") or (input_val.startswith("-") and input_val[1:].isdigit()):
        success = await SourcesRepo.add_source(input_val, "terabox")
        if not success:
            return await message.reply(f"⚠️ Source `{input_val}` already exists in database.")
        return await message.reply(f"✅ Registered Telegram TeraBox Channel: `{input_val}`.\nQueued for scanning.")

    # Case B: TeraBox / NephoBox Hive In-App Channel or Web Share Link
    elif ContentFilter.is_terabox(input_val):
        await message.reply("⏳ Scanning TeraBox Channel / Link for videos...")
        items = await TeraBoxAppScraper.fetch_terabox_data(input_val)

        if not items:
            return await message.reply("❌ TeraBox channel se koi link extract nahi ho paya ya link invalid/expired hai.")

        stored = 0
        for item in items:
            target_url = item["url"]
            canonical_url = LinkExtractor.canonicalize(target_url)
            item_hash = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()

            if await DuplicatesRepo.is_duplicate(item_hash):
                await StatsRepo.increment("duplicates_skipped", 1)
                continue

            meta = MetadataParser.parse_text(item["title"])
            caption = MetadataParser.build_terabox_caption(meta, canonical_url)

            # Step 1: Send to Raw TeraBox Target
            try:
                sent = await handle_floodwait(bot.send_message, config.TERABOX_TARGET, caption)
                await DuplicatesRepo.add_item(item_hash, canonical_url, config.TERABOX_TARGET, sent.id)
                await StatsRepo.increment("terabox_stored", 1)
                stored += 1

                # Step 2: Auto Converter Pipeline trigger (Next Channel)
                if config.CONVERTER_BOT_USERNAME and config.FINAL_CONVERTED_TARGET:
                    asyncio.create_task(converter_pipeline.execute_conversion(caption, canonical_url))

            except Exception as e:
                logger.error(f"Error posting item from channel: {str(e)}")
                await StatsRepo.increment("failed", 1)

        # Register channel for recurring monitoring
        await SourcesRepo.add_source(input_val, "terabox_hive")
        return await message.reply(
            f"✅ **TeraBox Source Processed!**\n\n"
            f"• Total Links Found: `{len(items)}`\n"
            f"• Successfully Stored: `{stored}`\n"
            f"• Converter Automation: Triggered in Background"
        )
    else:
        return await message.reply("❌ Invalid format! Valid Telegram ID (`-100...`) ya TeraBox / NephoBox Channel Link bhejein.")

@bot.on_message(filters.command("addtelegram") & filters.user(config.ADMINS))
async def cmd_add_telegram(_, message: Message):
    if len(message.command) < 2:
        return await message.reply("⚠️ Usage: `/addtelegram <CHANNEL_ID>`")
    src_id = message.command[1].strip()
    success = await SourcesRepo.add_source(src_id, "telegram")
    if not success:
        return await message.reply(f"⚠️ Source `{src_id}` already exists. Duplicate skipped.")
    await message.reply(f"✅ Registered Telegram Source: `{src_id}`.\nQueued for scanning.")

@bot.on_message(filters.command("removesource") & filters.user(config.ADMINS))
async def cmd_remove_source(_, message: Message):
    if len(message.command) < 2:
        return await message.reply("⚠️ Usage: `/removesource <CHANNEL_ID>`")
    src_id = message.command[1].strip()
    removed = await SourcesRepo.remove_source(src_id)
    if removed:
        await message.reply(f"🗑️ Source `{src_id}` removed successfully.")
    else:
        await message.reply(f"❌ Source `{src_id}` database me nahi mila.")

@bot.on_message(filters.command("sources") & filters.user(config.ADMINS))
async def cmd_sources(_, message: Message):
    all_src = await SourcesRepo.get_all_sources()
    if not all_src:
        return await message.reply("No sources configured.")
    text = f"📋 **Configured Sources ({len(all_src)} total):**\n\n"
    for s in all_src:
        text += f"• `{s['source_id']}` | Type: `{s['source_type']}` | Status: `{s.get('status', 'pending')}` | Last ID: `{s.get('last_processed_id', 0)}`\n"
    await message.reply(text[:4000])

@bot.on_message(filters.command("status") & filters.user(config.ADMINS))
async def cmd_status(_, message: Message):
    worker_str = f"Worker 1: {worker_status_ledger['Worker-1']} | Worker 2: {worker_status_ledger['Worker-2']}"
    report = await LiveStatus.generate_status_report(worker_str)
    await message.reply(report)

@bot.on_message(filters.command("cleantargets") & filters.user(config.ADMINS))
async def cmd_clean_targets(_, message: Message):
    await message.reply("🧹 Starting target duplicate scrubber tasks...")
    asyncio.create_task(TargetCleaner(bot, config.TERABOX_TARGET).execute_cleanup())
    asyncio.create_task(TargetCleaner(bot, config.STREAMING_TARGET).execute_cleanup())
    if config.FINAL_CONVERTED_TARGET:
        asyncio.create_task(TargetCleaner(bot, config.FINAL_CONVERTED_TARGET).execute_cleanup())
    await message.reply("✅ Target duplicate cleaners running in background.")

# ----------------- MESSAGE DISPATCHER ----------------- #

async def process_incoming_content(client: Client, message: Message, source_type: str):
    await StatsRepo.increment("scanned", 1)

    # 1. Media handling rule (Size >= 50MB OR Duration >= 10m)
    if MediaHandler.check_eligibility(message):
        media_hash = MediaHandler.get_media_hash(message)
        if await DuplicatesRepo.is_duplicate(media_hash):
            await StatsRepo.increment("duplicates_skipped", 1)
            return

        try:
            sent_media = await MediaHandler.copy_media_clean(bot, config.MEDIA_TARGET, message)
            await DuplicatesRepo.add_item(media_hash, "media_file", config.MEDIA_TARGET, sent_media.id)
            await StatsRepo.increment("media_stored", 1)
        except Exception as e:
            logger.error(f"Failed copying media to target: {str(e)}")
            await StatsRepo.increment("failed", 1)
        return

    # 2. Extract and Process Links
    raw_text = message.caption or message.text or ""
    if not raw_text:
        return

    urls = LinkExtractor.extract_urls(raw_text)
    if not urls:
        return

    meta = MetadataParser.parse_text(raw_text)

    for raw_url in urls:
        resolved_url, resolved_ok = await LinkRecovery.resolve_url(raw_url)

        # Check blacklist
        if ContentFilter.is_blocked(resolved_url):
            await StatsRepo.increment("filtered_out", 1)
            continue

        canonical_url = LinkExtractor.canonicalize(resolved_url)
        item_hash = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()

        # Global Duplicate Check
        if await DuplicatesRepo.is_duplicate(item_hash):
            await StatsRepo.increment("duplicates_skipped", 1)
            continue

        # Route A: TeraBox Link Target -> Trigger Converter -> Final Target
        if ContentFilter.is_terabox(canonical_url) or source_type in ["terabox", "terabox_hive"]:
            caption = MetadataParser.build_terabox_caption(meta, canonical_url)
            try:
                sent = await handle_floodwait(bot.send_message, config.TERABOX_TARGET, caption)
                await DuplicatesRepo.add_item(item_hash, canonical_url, config.TERABOX_TARGET, sent.id)
                await StatsRepo.increment("terabox_stored", 1)

                # Automation: Trigger converter bot pipeline in background
                if config.CONVERTER_BOT_USERNAME and config.FINAL_CONVERTED_TARGET:
                    asyncio.create_task(converter_pipeline.execute_conversion(caption, canonical_url))

            except Exception as e:
                logger.error(f"Error posting to TeraBox target: {str(e)}")
                await StatsRepo.increment("failed", 1)
            continue

        # Route B: Streaming Link Target
        if ContentFilter.is_streaming(canonical_url):
            caption = MetadataParser.build_streaming_caption(meta, canonical_url)
            try:
                sent = await handle_floodwait(bot.send_message, config.STREAMING_TARGET, caption)
                await DuplicatesRepo.add_item(item_hash, canonical_url, config.STREAMING_TARGET, sent.id)
                await StatsRepo.increment("streaming_stored", 1)
            except Exception as e:
                logger.error(f"Error posting to Streaming target: {str(e)}")
                await StatsRepo.increment("failed", 1)
            continue

        # Route C: Unresolved Link -> Rough Target
        if not resolved_ok and config.ROUGH_TARGET:
            rough_caption = f"⚠️ **Unresolved Link**\n\nTitle: `{meta['title']}`\nOriginal URL: {raw_url}"
            try:
                await handle_floodwait(bot.send_message, config.ROUGH_TARGET, rough_caption)
            except Exception:
                pass

# ----------------- LIVE LISTENERS ----------------- #

def register_live_listeners(client: Client, client_tag: str):
    @client.on_message(~filters.service)
    async def on_channel_post(_, message: Message):
        # Check if this is the converter bot responding in private chat
        if message.chat.type.name == "PRIVATE":
            sender = message.from_user
            if sender and sender.username:
                if sender.username.lower() == config.CONVERTER_BOT_USERNAME.lower():
                    converter_pipeline.resolve_pending(message)
                    return

        # Check if message is from registered source channels
        chat_id_str = str(message.chat.id)
        source = await SourcesRepo.get_source(chat_id_str)
        if not source:
            return

        logger.info(f"[{client_tag}] Received live message from {chat_id_str} (Msg ID: {message.id})")
        await process_incoming_content(client, message, source["source_type"])
        await CheckpointsRepo.update_checkpoint(chat_id_str, message.id)

# ----------------- PARALLEL HISTORICAL WORKERS ----------------- #

async def run_history_worker(client: Client, worker_name: str, worker_index: int, total_workers: int):
    while True:
        try:
            all_sources = await SourcesRepo.get_all_sources()
            assigned_sources = [
                s for idx, s in enumerate(all_sources)
                if (idx % total_workers) == worker_index and s.get("status") in ["pending", "processing"]
            ]

            for src in assigned_sources:
                src_id = src["source_id"]
                source_type = src["source_type"]

                # Handle Hive URL periodically
                if source_type == "terabox_hive":
                    worker_status_ledger[worker_name] = f"Hive Link {src_id[:25]}..."
                    await SourcesRepo.update_source_status(src_id, "processing")
                    items = await TeraBoxAppScraper.fetch_terabox_data(src_id)
                    for item in items:
                        canon = LinkExtractor.canonicalize(item["url"])
                        i_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()
                        if not await DuplicatesRepo.is_duplicate(i_hash):
                            meta = MetadataParser.parse_text(item["title"])
                            cap = MetadataParser.build_terabox_caption(meta, canon)
                            sent = await handle_floodwait(bot.send_message, config.TERABOX_TARGET, cap)
                            await DuplicatesRepo.add_item(i_hash, canon, config.TERABOX_TARGET, sent.id)
                            await StatsRepo.increment("terabox_stored", 1)
                            if config.CONVERTER_BOT_USERNAME and config.FINAL_CONVERTED_TARGET:
                                asyncio.create_task(converter_pipeline.execute_conversion(cap, canon))
                    await SourcesRepo.update_source_status(src_id, "completed")
                    worker_status_ledger[worker_name] = "Idle"
                    await asyncio.sleep(2)
                    continue

                chat_id = int(src_id) if src_id.startswith("-100") or src_id.isdigit() else src_id
                last_checkpoint = await CheckpointsRepo.get_last_id(src_id)

                worker_status_ledger[worker_name] = f"{src_id} ({source_type})"
                await SourcesRepo.update_source_status(src_id, "processing")

                try:
                    highest_id = last_checkpoint
                    async for msg in client.get_chat_history(chat_id):
                        if msg.id <= last_checkpoint:
                            break

                        if msg.id > highest_id:
                            highest_id = msg.id

                        await process_incoming_content(client, msg, source_type)
                        await CheckpointsRepo.update_checkpoint(src_id, msg.id)
                        await asyncio.sleep(0.08)

                    await SourcesRepo.update_source_status(src_id, "completed")
                except (UserNotParticipant, ChannelPrivate, ChatAdminRequired):
                    logger.warning(f"[{worker_name}] No access to {src_id}. Alternate userbot will attempt.")
                    await SourcesRepo.update_source_status(src_id, "access_error")
                except FloodWait as fw:
                    logger.warning(f"[{worker_name}] FloodWait {fw.value}s. Sleeping.")
                    await asyncio.sleep(fw.value + 5)
                except Exception as src_err:
                    logger.error(f"[{worker_name}] Error scanning {src_id}: {str(src_err)}")
                    await SourcesRepo.update_source_status(src_id, "error")

                worker_status_ledger[worker_name] = "Idle"
                await asyncio.sleep(2)

        except Exception as loop_err:
            logger.error(f"[{worker_name}] Loop error: {str(loop_err)}")

        await asyncio.sleep(45)

# ----------------- MAIN INITIALIZER ----------------- #

async def main():
    logger.info("Connecting to MongoDB and creating indexes...")
    await init_db()

    logger.info("Starting aiohttp keep-alive web service...")
    await start_web_server()

    logger.info("Starting Telegram Controller Bot...")
    await bot.start()

    active_userbots = []
    if userbot_1:
        logger.info("Starting Userbot Account 1...")
        await userbot_1.start()
        register_live_listeners(userbot_1, "Userbot-1")
        active_userbots.append(("Worker-1", userbot_1))

    if userbot_2:
        logger.info("Starting Userbot Account 2...")
        await userbot_2.start()
        register_live_listeners(userbot_2, "Userbot-2")
        active_userbots.append(("Worker-2", userbot_2))

    if not active_userbots:
        register_live_listeners(bot, "Main-Bot")
        asyncio.create_task(run_history_worker(bot, "Worker-1", 0, 1))
    else:
        num_workers = len(active_userbots)
        for idx, (w_name, ub_client) in enumerate(active_userbots):
            asyncio.create_task(run_history_worker(ub_client, w_name, idx, num_workers))

    # Run Startup Target channel Scrubbers
    asyncio.create_task(TargetCleaner(bot, config.TERABOX_TARGET).execute_cleanup())
    asyncio.create_task(TargetCleaner(bot, config.STREAMING_TARGET).execute_cleanup())
    if config.FINAL_CONVERTED_TARGET:
        asyncio.create_task(TargetCleaner(bot, config.FINAL_CONVERTED_TARGET).execute_cleanup())

    logger.info("Master Dual-Userbot Forwarder Engine is active and operational.")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
    
