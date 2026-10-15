from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, RPCError
from pyrogram.handlers import EditedMessageHandler, MessageHandler

import config
from database import CheckpointsRepo, DuplicatesRepo, Mongo, SourcesRepo, StatsRepo
from modules.content_filter import classify, is_stream_host
from modules.converter_pipeline import ConversionJob, ConverterPipeline
from modules.link_extractor import extract_links, get_host, hive_key, link_contexts, normalize_url, url_hash
from modules.link_recovery import resolve_url
from modules.live_status import LiveStatus
from modules.media_handler import (
    copy_clean, describe_media, get_media_info, is_forward_restricted, media_digest, qualifies,
)
from modules.metadata_parser import (
    extract_title_line, format_rough_message, format_stream_caption, format_terabox_caption, parse_metadata,
)
from modules.target_cleaner import TargetCleaner
from modules.terabox_scraper import ScraperError, fetch_share_title, scrape_hive
from utils.retry import safe_call, send_text
from utils.web_server import start_web_server, stop_web_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logger = logging.getLogger("bot")

ACCESS_ERRORS = {
    "CHANNEL_PRIVATE", "CHANNEL_INVALID", "PEER_ID_INVALID", "CHAT_ID_INVALID", "USERNAME_INVALID",
    "USERNAME_NOT_OCCUPIED", "CHAT_ADMIN_REQUIRED", "CHAT_FORBIDDEN",
}
HELP_TEXT = (
    "Admin commands:\n"
    "/status - live dashboard\n"
    "/sources - list sources\n"
    "/addsource <ids / @usernames / hive urls> - add sources (or reply to a message that lists them)\n"
    "/delsource <id or hive url> - remove a source\n"
    "/scan - rescan all sources now\n"
    "/resetcp <channel id> - reset a channel checkpoint to 0\n"
    "/clean - run the duplicate cleaner now\n"
    "/stats - all-time counters"
)


def rpc_code(exc: Exception) -> str:
    return str(getattr(exc, "ID", None) or getattr(exc, "MESSAGE", None) or type(exc).__name__).upper()


def message_link(message: Any) -> str:
    try:
        link = message.link
        if link:
            return str(link)
    except Exception:
        logger.debug("message.link unavailable")
    chat = getattr(message, "chat", None)
    return "%s/%s" % (getattr(chat, "id", "?"), getattr(message, "id", "?"))


def has_letters(text: str) -> bool:
    return bool(re.search(r"[A-Za-z]{3,}", text or ""))


class App:
    def __init__(self) -> None:
        self.mongo = Mongo(config.MONGO_URI, config.DB_NAME)
        self.master_bot = Client(
            "master_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN, in_memory=True
        )
        self.userbots: List[Client] = [
            Client("userbot_1", api_id=config.API_ID, api_hash=config.API_HASH, session_string=config.SESSION_1, in_memory=True),
            Client("userbot_2", api_id=config.API_ID, api_hash=config.API_HASH, session_string=config.SESSION_2, in_memory=True),
        ]
        self.total_workers = len(self.userbots)
        self.sources: Optional[SourcesRepo] = None
        self.checkpoints: Optional[CheckpointsRepo] = None
        self.duplicates: Optional[DuplicatesRepo] = None
        self.stats: Optional[StatsRepo] = None
        self.status: Optional[LiveStatus] = None
        self.terabox_pipeline: Optional[ConverterPipeline] = None
        self.streamnet_pipeline: Optional[ConverterPipeline] = None
        self.mdisk_pipeline: Optional[ConverterPipeline] = None
        self.diskwala_pipeline: Optional[ConverterPipeline] = None
        self.cleaner: Optional[TargetCleaner] = None
        self.active_sources: Set[int] = set()
        self.post_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.scan_events: List[asyncio.Event] = [asyncio.Event() for _ in self.userbots]
        self.live_sem = asyncio.Semaphore(config.LIVE_CONCURRENCY)
        self.tasks: List["asyncio.Task[None]"] = []
        self.web_runner: Any = None
        self.started = time.time()

    # ------------------------------------------------------------------ posting
    async def post(self, target: int, text: str) -> Any:
        async with self.post_locks[target]:
            message = await send_text(self.master_bot, target, text)
            await asyncio.sleep(config.POST_INTERVAL)
        return message

    async def copy_media(self, message: Any, target: int) -> Any:
        async with self.post_locks[target]:
            result = await copy_clean(message, target)
            await asyncio.sleep(config.POST_INTERVAL)
        return result

    # --------------------------------------------------------------- link logic
    async def handle_rough(self, url: str, reason: str, message: Any) -> None:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = "rough:%s:%s" % (url_hash(url), day)
        if not await self.duplicates.register(key, "rough", {"url": url, "reason": reason}):
            return
        try:
            await self.post(config.TARGET_ROUGH, format_rough_message(url, reason, message_link(message)))
        except Exception:
            await self.duplicates.remove(key)
            raise
        self.status.inc("rough_posted")

    async def handle_terabox(self, link: str, title_source: str = "") -> None:
        digest = url_hash(link)
        if not await self.duplicates.register(digest, "terabox", {"url": link}):
            self.status.inc("duplicates_skipped")
            return
        try:
            if config.FETCH_SHARE_TITLES and not has_letters(title_source):
                fetched = await fetch_share_title(link)
                if fetched:
                    title_source = fetched
            meta = parse_metadata(title_source)
            caption = format_terabox_caption(meta, link)
            await self.post(config.PIPELINES["terabox"]["intermediate"], caption)
        except Exception:
            await self.duplicates.remove(digest)
            raise
        self.status.inc("terabox_posted")
        await self.terabox_pipeline.enqueue(ConversionJob(original_link=link, caption=caption))

    def pipeline_for(self, kind: str) -> ConverterPipeline:
        return getattr(self, kind + "_pipeline")

    def all_pipelines(self) -> Dict[str, ConverterPipeline]:
        pipelines: Dict[str, ConverterPipeline] = {}
        for kind in config.PIPELINES:
            pipeline = getattr(self, kind + "_pipeline", None)
            if pipeline is not None:
                pipelines[kind] = pipeline
        return pipelines

    async def handle_hosted(self, kind: str, link: str, title_source: str = "") -> None:
        """StreamNet / MDisk / Diskwala: post to that pipeline's own intermediate channel, then convert."""
        digest = url_hash(link)
        if not await self.duplicates.register(digest, kind, {"url": link}):
            self.status.inc("duplicates_skipped")
            return
        try:
            meta = parse_metadata(title_source)
            caption = format_stream_caption(meta, link, get_host(link))
            await self.post(config.PIPELINES[kind]["intermediate"], caption)
        except Exception:
            await self.duplicates.remove(digest)
            raise
        self.status.inc("%s_posted" % kind)
        await self.pipeline_for(kind).enqueue(ConversionJob(original_link=link, caption=caption))

    async def handle_hive(self, hive_url: str, message: Any = None) -> None:
        try:
            entries = await scrape_hive(hive_url)
        except ScraperError as exc:
            logger.warning("Hive scrape failed for %s: %s", hive_url, exc)
            await self.handle_rough(hive_url, "Hive extraction failed: %s" % exc, message if message is not None else _NullMessage())
            return
        for entry in entries:
            await self.handle_terabox(entry["url"], entry.get("title", ""))

    async def handle_link(self, link: str, title_source: str, message: Any) -> None:
        kind = classify(link)
        if kind == "blacklist":
            self.status.inc("blacklisted")
            return
        if kind == "shortener":
            resolved = await resolve_url(link)
            if not resolved:
                await self.handle_rough(link, "Unresolvable shortened link", message)
                return
            link = normalize_url(resolved)
            kind = classify(link)
            if kind == "blacklist":
                self.status.inc("blacklisted")
                return
            if kind == "shortener":
                await self.handle_rough(link, "Shortener chain did not resolve", message)
                return
        if kind == "hive":
            await self.handle_hive(link, message)
        elif kind == "terabox":
            await self.handle_terabox(link, title_source)
        elif kind in ("streamnet", "mdisk", "diskwala"):
            await self.handle_hosted(kind, link, title_source)
        elif kind == "stream_other":
            self.status.inc("other_stream_ignored")
        else:
            self.status.inc("ignored_links")

    # -------------------------------------------------------------- media logic
    async def handle_media(self, message: Any) -> None:
        info = get_media_info(message)
        if info is None or not qualifies(info):
            return
        digest = media_digest(info)
        if not await self.duplicates.register(digest, "media", {"name": info.file_name, "size": info.size}):
            self.status.inc("duplicates_skipped")
            return
        try:
            await self.copy_media(message, config.TARGET_MEDIA)
        except RPCError as exc:
            if is_forward_restricted(exc):
                await self.post(
                    config.TARGET_ROUGH,
                    format_rough_message(describe_media(info), "Protected media (copy not allowed)", message_link(message)),
                )
                self.status.inc("rough_posted")
                return
            await self.duplicates.remove(digest)
            raise
        except Exception:
            await self.duplicates.remove(digest)
            raise
        self.status.inc("media_copied")

    # ---------------------------------------------------------- message routing
    async def process_message(self, client: Client, message: Any) -> None:
        if message is None or getattr(message, "empty", False):
            return
        self.status.inc("messages_seen")
        text = getattr(message, "text", None) or getattr(message, "caption", None) or ""
        file_name = ""
        for attr in ("video", "document", "animation"):
            media = getattr(message, attr, None)
            if media is not None and getattr(media, "file_name", None):
                file_name = media.file_name
                break
        fallback_title = extract_title_line(text) or file_name
        contexts = link_contexts(text)
        for link in extract_links(message):
            await self.handle_link(link, contexts.get(link) or fallback_title, message)
        await self.handle_media(message)

    async def process_with_retry(self, client: Client, message: Any) -> bool:
        for attempt in range(1, config.MESSAGE_RETRIES + 1):
            try:
                await self.process_message(client, message)
                return True
            except Exception as exc:
                logger.warning("Message %s failed (attempt %d/%d): %s", getattr(message, "id", "?"), attempt, config.MESSAGE_RETRIES, exc)
                await asyncio.sleep(3 * attempt)
        self.status.inc("message_failures")
        return False

    # --------------------------------------------------------------- live feeds
    def make_live_handler(self, client: Client, index: int):
        async def handler(_: Client, message: Any) -> None:
            async with self.live_sem:
                try:
                    await self.process_message(client, message)
                    self.status.inc("live_u%d" % (index + 1))
                except Exception as exc:
                    logger.error("Live handler (userbot_%d) failed: %s", index + 1, exc)
                    self.status.inc("message_failures")

        return handler

    def register_live_handlers(self) -> None:
        async def _is_source(_, __, message) -> bool:
            chat = getattr(message, "chat", None)
            return chat is not None and chat.id in self.active_sources

        source_filter = filters.create(_is_source)
        for index, client in enumerate(self.userbots):
            client.add_handler(MessageHandler(self.make_live_handler(client, index), source_filter), group=1)

    # ----------------------------------------------------------------- scanning
    async def _first_history(self, client: Client, chat_id: int) -> int:
        async for message in client.get_chat_history(chat_id, limit=1):
            return int(message.id)
        return 0

    async def scan_channel(self, client: Client, index: int, source: Dict[str, Any]) -> None:
        key = source["_id"]
        chat_id = int(source["ref"])
        last = await self.checkpoints.get(chat_id)
        try:
            top = await safe_call(self._first_history, client, chat_id)
        except RPCError as exc:
            code = rpc_code(exc)
            logger.warning("userbot_%d cannot read %s: %s", index + 1, chat_id, code)
            if code in ACCESS_ERRORS:
                await self.sources.mark_error(key, code)
                self.status.inc("access_errors")
                return
            raise
        if last == 0 and config.INITIAL_SCAN_LIMIT > 0 and top > config.INITIAL_SCAN_LIMIT:
            last = top - config.INITIAL_SCAN_LIMIT
            await self.checkpoints.reset(chat_id, last)
        if top <= last:
            return

        total = top - last
        done = 0
        current = last + 1
        logger.info("userbot_%d scanning %s: ids %d..%d", index + 1, chat_id, current, top)
        while current <= top:
            batch_ids = list(range(current, min(current + config.SCAN_BATCH_SIZE, top + 1)))
            try:
                fetched = await safe_call(client.get_messages, chat_id, batch_ids)
            except RPCError as exc:
                code = rpc_code(exc)
                if code in ACCESS_ERRORS:
                    await self.sources.mark_error(key, code)
                    self.status.inc("access_errors")
                    return
                raise
            if not isinstance(fetched, list):
                fetched = [fetched]
            by_id = {m.id: m for m in fetched if m is not None}
            for message_id in batch_ids:
                message = by_id.get(message_id)
                if message is not None and not getattr(message, "empty", False):
                    await self.process_with_retry(client, message)
                    await self.checkpoints.advance(chat_id, message_id)
                done += 1
            await self.checkpoints.advance(chat_id, batch_ids[-1])
            self.status.set_worker(index, state="scanning %s" % chat_id, done=done, total=total)
            current = batch_ids[-1] + 1
            await asyncio.sleep(config.SCAN_DELAY)

    async def _wait_or_event(self, index: int, timeout: float) -> None:
        event = self.scan_events[index]
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            logger.debug("Worker %d timer elapsed", index)
        event.clear()

    async def scan_worker(self, index: int) -> None:
        client = self.userbots[index]
        while True:
            try:
                channels = await self.sources.list_sources("channel")
                mine = [s for position, s in enumerate(channels) if position % self.total_workers == index]
                self.status.set_worker(index, state="scan cycle (%d channels)" % len(mine), done=0, total=0)
                for source in mine:
                    try:
                        await self.scan_channel(client, index, source)
                    except Exception as exc:
                        logger.error("Scan of %s failed: %s", source.get("ref"), exc)
                self.status.set_worker(index, state="idle", done=0, total=0)
            except Exception as exc:
                logger.error("Scan worker %d loop error: %s", index + 1, exc)
            await self._wait_or_event(index, config.RESCAN_INTERVAL)

    async def hive_worker(self, index: int) -> None:
        while True:
            try:
                hives = await self.sources.list_sources("hive")
                for position, source in enumerate(hives):
                    if position % self.total_workers != index:
                        continue
                    try:
                        await self.handle_hive(str(source["ref"]))
                    except Exception as exc:
                        logger.error("Hive scan of %s failed: %s", source.get("ref"), exc)
            except Exception as exc:
                logger.error("Hive worker %d loop error: %s", index + 1, exc)
            await asyncio.sleep(config.HIVE_INTERVAL)

    # ---------------------------------------------------------------- housekeeping
    async def refresh_sources(self) -> None:
        channels = await self.sources.list_sources("channel")
        self.active_sources = {int(s["ref"]) for s in channels}

    async def source_refresher(self) -> None:
        while True:
            try:
                await self.refresh_sources()
            except Exception as exc:
                logger.error("Source refresh failed: %s", exc)
            await asyncio.sleep(60)

    async def seed_sources(self) -> None:
        for chat_id in config.SOURCE_CHANNELS:
            await self.sources.add(str(chat_id), "channel", chat_id)
        for url in config.HIVE_SOURCES:
            await self.sources.add(hive_key(url), "hive", normalize_url(url))

    async def run_cleaner(self) -> Dict[str, int]:
        results = await self.cleaner.clean_all()
        self.status.note_clean()
        return results

    async def cleaner_loop(self) -> None:
        if config.CLEAN_ON_STARTUP:
            await asyncio.sleep(30)
            try:
                await self.run_cleaner()
            except Exception as exc:
                logger.error("Startup cleaner failed: %s", exc)
        while config.CLEAN_INTERVAL_HOURS > 0:
            await asyncio.sleep(config.CLEAN_INTERVAL_HOURS * 3600)
            try:
                await self.run_cleaner()
            except Exception as exc:
                logger.error("Scheduled cleaner failed: %s", exc)
        return

    async def warm_dialogs(self, client: Client, label: str) -> None:
        for attempt in range(3):
            try:
                count = 0
                async for _ in client.get_dialogs():
                    count += 1
                logger.info("%s cached %d dialogs", label, count)
                return
            except FloodWait as exc:
                await asyncio.sleep(int(getattr(exc, "value", 10) or 10) + 1)
            except RPCError as exc:
                logger.warning("%s dialog warm-up error: %s", label, exc)
                await asyncio.sleep(5 * (attempt + 1))

    # --------------------------------------------------------------- admin bot
    async def reply(self, message: Any, text: str) -> None:
        await send_text(self.master_bot, message.chat.id, text)

    async def parse_source_tokens(self, message: Any) -> List[str]:
        tokens: List[str] = list(message.command[1:]) if getattr(message, "command", None) else []
        replied = getattr(message, "reply_to_message", None)
        if replied is not None:
            tokens.extend(re.split(r"[\s,;]+", replied.text or replied.caption or ""))
        return [t for t in tokens if t]

    async def cmd_start(self, client: Client, message: Any) -> None:
        await self.reply(message, "Automation system online.\n\n" + HELP_TEXT)

    async def cmd_status(self, client: Client, message: Any) -> None:
        await self.reply(message, self.status.render())

    async def cmd_stats(self, client: Client, message: Any) -> None:
        totals = await self.stats.totals()
        lines = ["All-time counters:"] + ["%s: %d" % (k, v) for k, v in sorted(totals.items())]
        lines.append("ledger size: %d" % await self.duplicates.count())
        lines.append("checkpoints: %d" % await self.checkpoints.count())
        await self.reply(message, "\n".join(lines))

    async def cmd_sources(self, client: Client, message: Any) -> None:
        channels = await self.sources.list_sources("channel", enabled_only=False)
        hives = await self.sources.list_sources("hive", enabled_only=False)
        lines = ["Channels: %d | Hive pages: %d" % (len(channels), len(hives))]
        for source in channels[:40]:
            error = source.get("last_error")
            lines.append("• %s%s" % (source["ref"], " (error: %s)" % error if error else ""))
        if len(channels) > 40:
            lines.append("… and %d more channels" % (len(channels) - 40))
        for source in hives[:20]:
            lines.append("• %s" % source["ref"])
        await self.reply(message, "\n".join(lines))

    async def cmd_addsource(self, client: Client, message: Any) -> None:
        tokens = await self.parse_source_tokens(message)
        if not tokens:
            await self.reply(message, "Usage: /addsource <-100id | @username | hive url> (several allowed)")
            return
        added, existing, failed = 0, 0, []
        for token in tokens:
            try:
                if re.fullmatch(r"-?\d+", token):
                    chat_id = int(token)
                    created = await self.sources.add(str(chat_id), "channel", chat_id)
                elif token.startswith("http") and classify(normalize_url(token)) == "hive":
                    created = await self.sources.add(hive_key(token), "hive", normalize_url(token))
                elif token.startswith("@") or "t.me/" in token:
                    username = token.rstrip("/").split("/")[-1].lstrip("@")
                    chat = await safe_call(self.userbots[0].get_chat, username)
                    created = await self.sources.add(str(chat.id), "channel", chat.id, getattr(chat, "title", "") or "")
                else:
                    failed.append(token)
                    continue
                if created:
                    added += 1
                else:
                    existing += 1
            except Exception as exc:
                failed.append("%s (%s)" % (token, rpc_code(exc) if isinstance(exc, RPCError) else exc))
        await self.refresh_sources()
        for event in self.scan_events:
            event.set()
        text = "Added %d, already present %d." % (added, existing)
        if failed:
            text += "\nFailed: " + ", ".join(failed[:15])
        await self.reply(message, text)

    async def cmd_delsource(self, client: Client, message: Any) -> None:
        tokens = await self.parse_source_tokens(message)
        removed = 0
        for token in tokens:
            if re.fullmatch(r"-?\d+", token):
                key = str(int(token))
            elif token.startswith("http"):
                key = hive_key(token)
            else:
                continue
            if await self.sources.remove(key):
                removed += 1
        await self.refresh_sources()
        await self.reply(message, "Removed %d source(s)." % removed)

    async def cmd_scan(self, client: Client, message: Any) -> None:
        for event in self.scan_events:
            event.set()
        await self.reply(message, "Rescan triggered for both userbots.")

    async def cmd_resetcp(self, client: Client, message: Any) -> None:
        if len(message.command) < 2 or not re.fullmatch(r"-?\d+", message.command[1]):
            await self.reply(message, "Usage: /resetcp <channel id>")
            return
        await self.checkpoints.reset(int(message.command[1]), 0)
        await self.reply(message, "Checkpoint reset. Run /scan to re-ingest (duplicates are skipped automatically).")

    async def cmd_clean(self, client: Client, message: Any) -> None:
        if self.cleaner.running:
            await self.reply(message, "Cleaner is already running.")
            return
        await self.reply(message, "Cleaner started in the background.")

        async def job() -> None:
            try:
                results = await self.run_cleaner()
                summary = ", ".join("%s: %d" % (k, v) for k, v in results.items()) or "nothing to do"
                await self.reply(message, "Cleaner finished. " + summary)
            except Exception as exc:
                logger.error("Manual cleaner failed: %s", exc)
                await self.reply(message, "Cleaner failed: %s" % exc)

        self.tasks.append(asyncio.create_task(job()))

    def register_admin_handlers(self) -> None:
        admin = filters.user(config.ADMINS) if config.ADMINS else filters.user([0])
        commands = {
            "start": self.cmd_start, "help": self.cmd_start, "status": self.cmd_status, "stats": self.cmd_stats,
            "sources": self.cmd_sources, "addsource": self.cmd_addsource, "delsource": self.cmd_delsource,
            "scan": self.cmd_scan, "resetcp": self.cmd_resetcp, "clean": self.cmd_clean,
        }
        for name, callback in commands.items():
            self.master_bot.add_handler(MessageHandler(callback, filters.command(name) & admin))

    # ------------------------------------------------------------------ lifecycle
    def health_info(self) -> Dict[str, Any]:
        return {
            "sources": len(self.active_sources),
            "queues": {kind: pipeline.queue.qsize() for kind, pipeline in self.all_pipelines().items()},
            "uptime_seconds": int(time.time() - self.started),
        }

    def build_pipeline(self, kind: str) -> ConverterPipeline:
        cfg = config.PIPELINES[kind]
        return ConverterPipeline(
            userbot=self.userbots[0],
            converter_username=cfg["converter"],
            final_target=cfg["final"],
            rough_target=config.TARGET_ROUGH,
            poster=self.post,
            status=self.status,
            name=kind,
        )

    async def start(self) -> None:
        self.web_runner = await start_web_server(config.PORT, self.health_info)
        db = await self.mongo.connect()
        self.sources, self.checkpoints = SourcesRepo(db), CheckpointsRepo(db)
        self.duplicates, self.stats = DuplicatesRepo(db), StatsRepo(db)
        for repo in (self.sources, self.checkpoints, self.duplicates, self.stats):
            await repo.ensure_indexes()
        await self.seed_sources()

        await self.master_bot.start()
        for client in self.userbots:
            await client.start()
        await asyncio.gather(
            self.warm_dialogs(self.userbots[0], "userbot_1"),
            self.warm_dialogs(self.userbots[1], "userbot_2"),
        )

        self.status = LiveStatus(self.master_bot, self.stats, config.STATUS_CHAT_ID)
        await self.status.load()
        self.terabox_pipeline = self.build_pipeline("terabox")
        self.streamnet_pipeline = self.build_pipeline("streamnet")
        self.mdisk_pipeline = self.build_pipeline("mdisk")
        self.diskwala_pipeline = self.build_pipeline("diskwala")
        self.status.queue_size_fn = lambda: sum(p.queue.qsize() for p in self.all_pipelines().values())
        self.cleaner = TargetCleaner(self.userbots[1], config.TARGETS, self.duplicates, self.status)

        for kind, pipeline in self.all_pipelines().items():
            converter_filter = filters.private & filters.user(config.PIPELINES[kind]["converter"])
            self.userbots[0].add_handler(MessageHandler(pipeline.on_converter_message, converter_filter), group=-1)
            self.userbots[0].add_handler(EditedMessageHandler(pipeline.on_converter_message, converter_filter), group=-1)

        await self.refresh_sources()
        self.register_live_handlers()
        self.register_admin_handlers()

        for pipeline in self.all_pipelines().values():
            pipeline.start()
        self.status.start()
        for index in range(self.total_workers):
            self.tasks.append(asyncio.create_task(self.scan_worker(index)))
            self.tasks.append(asyncio.create_task(self.hive_worker(index)))
        self.tasks.append(asyncio.create_task(self.source_refresher()))
        self.tasks.append(asyncio.create_task(self.cleaner_loop()))
        logger.info("System started: %d channel sources, %d workers", len(self.active_sources), self.total_workers)

    async def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            try:
                await task
            except asyncio.CancelledError:
                logger.debug("Task cancelled")
            except Exception as exc:
                logger.error("Task ended with error: %s", exc)
        for pipeline in self.all_pipelines().values():
            await pipeline.stop()
        if self.status is not None:
            await self.status.stop()
        for client in [self.master_bot] + self.userbots:
            try:
                await client.stop()
            except Exception as exc:
                logger.warning("Client stop error: %s", exc)
        await self.mongo.close()
        await stop_web_server(self.web_runner)


class _NullMessage:
    """Stand-in message used when a hive scrape has no originating Telegram message."""

    id = 0
    chat = None
    link = "scheduled hive scan"


async def main() -> None:
    problems = config.validate_config()
    if problems:
        logger.error("Missing configuration: %s", ", ".join(problems))
        sys.exit(1)
    app = App()
    await app.start()
    try:
        await idle()
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
