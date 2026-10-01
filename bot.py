"""Dual-userbot Telegram batch forwarder, auto-ingestion and filter bot (Pyrofork)."""

import asyncio

LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(LOOP)

import hashlib
import html
import inspect
import json
import logging
import os
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Hashable, Iterator, List, Optional, Set, Tuple, Union
from urllib.parse import urlsplit

from aiohttp import web
from pyrogram import Client, enums, filters, idle
from pyrogram.errors import (
    BadRequest,
    ChannelInvalid,
    ChannelPrivate,
    ChatAdminRequired,
    FloodWait,
    MessageDeleteForbidden,
    MessageNotModified,
    PeerIdInvalid,
    RPCError,
    UsernameInvalid,
    UsernameNotOccupied,
)
from pyrogram.handlers import MessageHandler
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

import config

config.validate()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("forwarder")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

ChatRef = Union[int, str]

# --------------------------------------------------------------------------- #
# Web Server for Render Keep-Alive
# --------------------------------------------------------------------------- #
async def dummy_web_server() -> None:
    async def handle_ping(request):
        return web.Response(text="Bot is running!")

    app = web.Application()
    app.router.add_get("/", handle_ping)
    app.router.add_get("/health", handle_ping)
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Render keep-alive server listening on port %d", port)

# --------------------------------------------------------------------------- #
# Clients: 1-2 userbots + delivery bot
# --------------------------------------------------------------------------- #
USERBOTS: List[Client] = [
    Client(
        f"userbot_{index}",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=session,
        in_memory=True,
    )
    for index, session in enumerate(config.SESSION_STRINGS, start=1)
]
user_1: Client = USERBOTS[0]
user_2: Optional[Client] = USERBOTS[1] if len(USERBOTS) > 1 else None

bot = Client(
    "forwarder_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    in_memory=True,
)


def _no_preview_kwargs() -> Dict[str, Any]:
    try:
        params = inspect.signature(Client.send_message).parameters
    except (TypeError, ValueError):
        return {"disable_web_page_preview": True}
    if "disable_web_page_preview" in params:
        return {"disable_web_page_preview": True}
    if "link_preview_options" in params:
        from pyrogram.types import LinkPreviewOptions  # type: ignore

        return {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
    return {}


NO_PREVIEW = _no_preview_kwargs()
HTML = enums.ParseMode.HTML


# --------------------------------------------------------------------------- #
# Small data structures
# --------------------------------------------------------------------------- #
class RecentSet:
    def __init__(self, maxsize: int) -> None:
        self.maxsize = maxsize
        self._items: "OrderedDict[Hashable, None]" = OrderedDict()

    def add(self, key: Hashable) -> bool:
        if key in self._items:
            self._items.move_to_end(key)
            return False
        self._items[key] = None
        while len(self._items) > self.maxsize:
            self._items.popitem(last=False)
        return True

    def discard(self, key: Hashable) -> None:
        self._items.pop(key, None)

    def __contains__(self, key: Hashable) -> bool:
        return key in self._items

    def __len__(self) -> int:
        return len(self._items)


class MediaSeen(RecentSet):
    def __init__(self, path: str, maxsize: int) -> None:
        super().__init__(maxsize)
        self.path = path
        self.dirty = False
        self.last_save = time.monotonic()

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, list):
                raise ValueError("media_seen file is not a list")
            for key in data:
                if isinstance(key, str):
                    super().add(key)
        except (OSError, ValueError) as exc:
            log.error("Could not read %s (%s); starting with an empty media cache", self.path, exc)

    def add(self, key: Hashable) -> bool:
        added = super().add(key)
        if added:
            self.dirty = True
        return added

    def discard(self, key: Hashable) -> None:
        if key in self:
            self.dirty = True
        super().discard(key)

    def save(self, force: bool = False) -> None:
        if not self.dirty:
            return
        if not force and time.monotonic() - self.last_save < 30:
            return
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(list(self._items.keys()), fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
            self.dirty = False
            self.last_save = time.monotonic()
        except OSError as exc:
            log.error("Could not save media cache: %s", exc)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class State:
    def __init__(self, path: str) -> None:
        self.path = path
        self.sources: List[int] = []
        self.source_set: Set[int] = set()
        self.env_known: List[int] = []
        self.target_links: int = config.TARGET_LINKS_CHAT
        self.target_media: int = config.TARGET_MEDIA_CHAT
        self.min_duration: int = config.MIN_VIDEO_DURATION
        self.min_size_mb: float = config.MIN_FILE_SIZE_MB
        self.paused: bool = False
        self.checkpoints: Dict[str, Dict[str, Any]] = {}

    @property
    def min_size_bytes(self) -> int:
        return int(self.min_size_mb * 1024 * 1024)

    def set_sources(self, ids: List[int]) -> None:
        self.sources = list(dict.fromkeys(ids))
        self.source_set = set(self.sources)

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                raise ValueError("state file root is not an object")
        except (OSError, ValueError) as exc:
            backup = self.path + ".corrupt"
            log.error("State file unreadable (%s); moving it to %s", exc, backup)
            try:
                os.replace(self.path, backup)
            except OSError:
                pass
            return

        self.set_sources([s for s in data.get("sources", []) if isinstance(s, int)])
        self.env_known = [s for s in data.get("env_known", []) if isinstance(s, int)]
        self.target_links = _as_int(data.get("target_links"), 0) or self.target_links
        self.target_media = _as_int(data.get("target_media"), 0) or self.target_media
        self.min_duration = _as_int(data.get("min_duration"), self.min_duration)
        self.min_size_mb = _as_float(data.get("min_size_mb"), self.min_size_mb)
        self.paused = bool(data.get("paused", False))
        checkpoints = data.get("checkpoints", {})
        if isinstance(checkpoints, dict):
            self.checkpoints = {
                str(k): {
                    "last_read_id": _as_int(v.get("last_read_id"), 0),
                    "top_id": _as_int(v.get("top_id"), 0),
                    "done": bool(v.get("done", False)),
                }
                for k, v in checkpoints.items()
                if isinstance(v, dict)
            }

    def save(self) -> None:
        payload = {
            "sources": self.sources,
            "env_known": self.env_known,
            "target_links": self.target_links,
            "target_media": self.target_media,
            "min_duration": self.min_duration,
            "min_size_mb": self.min_size_mb,
            "paused": self.paused,
            "checkpoints": self.checkpoints,
        }
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=1, ensure_ascii=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except OSError as exc:
            log.error("Could not save state: %s", exc)


STATE = State(config.STATE_FILE)
MEDIA_SEEN = MediaSeen(config.MEDIA_SEEN_FILE, config.MEDIA_SEEN_MAX)
SEEN = RecentSet(config.SEEN_MESSAGES_MAX)


class Runtime:
    def __init__(self) -> None:
        self.stop_event = asyncio.Event()
        self.ingest_task: Optional["asyncio.Task[None]"] = None
        self.worker_task: Optional["asyncio.Task[None]"] = None
        self.ingest_mode: str = ""
        self.current_source: Optional[int] = None
        self.ingesting: Set[int] = set()
        self.live_max: Dict[int, int] = {}
        self.owner: Dict[Any, int] = {}
        self.no_bot_copy: Set[int] = set()
        self.stats: Dict[str, int] = {
            "media_sent": 0,
            "links_sent": 0,
            "failed": 0,
            "dropped_adult": 0,
            "dropped_ignored": 0,
            "dropped_dup_media": 0,
            "dropped_no_content": 0,
            "dropped_queue_full": 0,
            "dropped_seen": 0,
        }

    @property
    def ingest_active(self) -> bool:
        return self.ingest_task is not None and not self.ingest_task.done()


RT = Runtime()
QUEUE: "asyncio.Queue[Job]" = asyncio.Queue(maxsize=config.QUEUE_MAXSIZE)
PENDING_SESSION: Dict[int, Dict[str, Any]] = {}
PENDING_CLONE: Dict[int, bool] = {}
DIALOGS_REFRESHED: Dict[str, float] = {}

ACCESS_ERRORS = (PeerIdInvalid, ChannelPrivate, ChannelInvalid, KeyError, ValueError)


async def flood_retry(factory: Callable[[], Awaitable[Any]], label: str = "call") -> Any:
    while True:
        try:
            return await factory()
        except FloodWait as exc:
            wait = int(getattr(exc, "value", 5)) + 1
            log.warning("FloodWait in %s: sleeping %ss then retrying", label, wait)
            await asyncio.sleep(wait)


def friendly_error(exc: BaseException) -> str:
    if isinstance(exc, (ChatAdminRequired, MessageDeleteForbidden)):
        return "Missing permission. Make the bot/userbot admin with Post/Delete rights."
    if isinstance(exc, (PeerIdInvalid, ChannelPrivate, ChannelInvalid, KeyError)):
        return "Can't access chat. Userbot must be a member; bot must be admin in targets."
    if isinstance(exc, (UsernameNotOccupied, UsernameInvalid)):
        return "That username doesn't exist or is invalid."
    if isinstance(exc, RPCError):
        return f"Telegram error: <code>{html.escape(str(exc))}</code>"
    return f"Unexpected error: <code>{html.escape(repr(exc))}</code>"


async def reply_html(message: Message, text: str) -> Optional[Message]:
    try:
        return await flood_retry(
            lambda: message.reply_text(text[:4096], parse_mode=HTML, **NO_PREVIEW), "reply"
        )
    except Exception:
        log.exception("reply failed")
        return None


async def edit_html(target: Optional[Message], text: str) -> None:
    if target is None:
        return
    try:
        await flood_retry(
            lambda: target.edit_text(text[:4096], parse_mode=HTML, **NO_PREVIEW), "edit"
        )
    except MessageNotModified:
        pass
    except Exception:
        log.exception("edit failed")


async def refresh_dialogs(client: Client, force: bool = False) -> bool:
    now = time.monotonic()
    last = DIALOGS_REFRESHED.get(client.name)
    if not force and last is not None and now - last < config.DIALOG_REFRESH_INTERVAL:
        return False
    DIALOGS_REFRESHED[client.name] = now
    try:
        async for _ in client.get_dialogs():
            pass
    except Exception as exc:
        log.warning("get_dialogs failed for %s: %s", client.name, exc)
    return True


async def userbot_call(chat_id: Any, fn: Callable[[Client], Awaitable[Any]], label: str) -> Any:
    preferred = RT.owner.get(chat_id)
    order = list(range(len(USERBOTS)))
    if preferred is not None and preferred in order:
        order.remove(preferred)
        order.insert(0, preferred)

    last_exc: Optional[BaseException] = None
    for index in order:
        client = USERBOTS[index]
        for attempt in (1, 2):
            try:
                result = await flood_retry(lambda: fn(client), label)
                RT.owner[chat_id] = index
                return result
            except ACCESS_ERRORS as exc:
                last_exc = exc
                if attempt == 1 and await refresh_dialogs(client):
                    continue
                break
    assert last_exc is not None
    raise last_exc


def _adult_alternative(keyword: str) -> str:
    escaped = re.escape(keyword)
    if keyword[-1].isalnum():
        plural = "s?" if keyword[-1].isalpha() else ""
        return escaped + plural + r"(?![A-Za-z0-9])"
    return escaped


ADULT_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    + "|".join(_adult_alternative(k) for k in sorted(config.ADULT_KEYWORDS, key=len, reverse=True))
    + ")",
    re.IGNORECASE,
)
SPAM_RE = re.compile("|".join(f"(?:{p})" for p in config.SPAM_PATTERNS), re.IGNORECASE)
MENTION_RE = re.compile(r"(?<![\w/.=?&%#@\-])@[A-Za-z0-9_]{4,32}(?![A-Za-z0-9_])")
PROMO_TAIL_RE = re.compile(
    r"\b(?:join(?:\s+us)?|credits?|powered\s+by|uploaded\s+by|shared\s+by|via|source|support|"
    r"backup|follow|share)\s*[:\-–—=]*\s*(?:@[A-Za-z0-9_]{4,32}[\s,|&/]*)+",
    re.IGNORECASE,
)


def message_haystack(message: Message) -> str:
    parts = [str(message.text or ""), str(message.caption or "")]
    for attr in ("video", "document", "audio", "animation"):
        name = getattr(getattr(message, attr, None), "file_name", None)
        if name:
            parts.append(str(name))
    return "\n".join(parts)


def is_adult(message: Message) -> bool:
    return bool(ADULT_RE.search(message_haystack(message)))


def sanitize_text(raw: str) -> str:
    out: List[str] = []
    for line in (raw or "").splitlines():
        if SPAM_RE.search(line):
            continue
        new = PROMO_TAIL_RE.sub("", line)
        new = MENTION_RE.sub("", new)
        if new != line:
            new = re.sub(r"[ \t]{2,}", " ", new).strip()
            if not re.search(r"\w", new):
                continue
        out.append(new.rstrip())
    text = "\n".join(out)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


URL_RE = re.compile(r"https?://[^\s<>\"'\[\]\(\)]+", re.IGNORECASE)
TRAILING_JUNK = ".,;:!?)]}>*_`~'\""


def clean_url(raw: str) -> str:
    url = (raw or "").strip().strip("<>[]()")
    return url.rstrip(TRAILING_JUNK).strip()


def _host_of(url: str) -> str:
    try:
        host = (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def is_ignored_host(host: str) -> bool:
    for domain in config.IGNORED_DOMAINS:
        if host == domain or host.endswith("." + domain):
            return True
    return False


def is_valid_external_url(url: str) -> bool:
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    if parts.scheme.lower() not in ("http", "https"):
        return False
    host = _host_of(url)
    if not host or "." not in host:
        return False
    return not is_ignored_host(host)


def url_priority(url: str) -> int:
    host = _host_of(url)
    for domain in config.PRIORITY_DOMAINS:
        if host == domain or host.endswith("." + domain):
            return 0
    for keyword in config.PRIORITY_KEYWORDS:
        if keyword in host:
            return 0
    return 1


def rank_urls(urls: List[str]) -> List[str]:
    return sorted(urls, key=url_priority)


def _entity_text(text: str, offset: int, length: int) -> str:
    raw = text.encode("utf-16-le")
    return raw[offset * 2 : (offset + length) * 2].decode("utf-16-le", errors="ignore")


def _iter_button_items(node: Any) -> Iterator[Tuple[str, str]]:
    if node is None:
        return
    if isinstance(node, (list, tuple)):
        for child in node:
            yield from _iter_button_items(child)
        return
    rows = getattr(node, "inline_keyboard", None)
    if rows is not None:
        yield from _iter_button_items(rows)
        return
    label = getattr(node, "text", "")
    label = label if isinstance(label, str) else ""
    url = getattr(node, "url", None)
    if isinstance(url, str) and url:
        yield url, label
    for attr in ("login_url", "web_app"):
        nested = getattr(getattr(node, attr, None), "url", None)
        if isinstance(nested, str) and nested:
            yield nested, label


def extract_valid_urls(message: Message) -> List[str]:
    candidates: List[str] = []
    for blob in (message.text, message.caption):
        if blob:
            candidates.extend(URL_RE.findall(str(blob)))

    for blob, entities in (
        (message.text, message.entities),
        (message.caption, message.caption_entities),
    ):
        if not entities:
            continue
        text = str(blob) if blob else ""
        for entity in entities:
            if entity.type == enums.MessageEntityType.TEXT_LINK and entity.url:
                candidates.append(entity.url)
            elif entity.type == enums.MessageEntityType.URL and text:
                candidates.append(_entity_text(text, entity.offset, entity.length))

    candidates.extend(url for url, _ in _iter_button_items(message.reply_markup))

    seen: Set[str] = set()
    result: List[str] = []
    for raw in candidates:
        url = clean_url(raw)
        if not url or not is_valid_external_url(url):
            continue
        key = url.rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        result.append(url)
    return result


def button_labels(message: Message) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for url, text in _iter_button_items(message.reply_markup):
        key = clean_url(url).rstrip("/")
        label = sanitize_text(text).replace("\n", " ").strip()
        if key and key not in labels and label and re.search(r"\w", label) and len(label) <= 40:
            labels[key] = label
    return labels


@dataclass
class Job:
    kind: str
    source_id: int
    message_id: int = 0
    caption: str = ""
    media_key: str = ""
    reserved: bool = False
    text: str = ""
    urls: List[str] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
    future: Optional["asyncio.Future[str]"] = None


def media_qualifies(message: Message) -> bool:
    if message.video:
        return (message.video.duration or 0) >= STATE.min_duration
    if message.document:
        return (message.document.file_size or 0) >= STATE.min_size_bytes
    return False


def media_unique_id(message: Message) -> str:
    media = message.video or message.document
    return str(getattr(media, "file_unique_id", "") or "")


def build_links_text(raw_text: str, urls: List[str]) -> str:
    bullets = "\n".join(f"• {html.escape(u)}" for u in urls[:20])
    block = f"🔗 <b>Links:</b>\n{bullets}"
    description = sanitize_text(raw_text)
    room = max(0, min(3000, 4000 - len(block) - 4))
    description = html.escape(description[: room // 2]) if description else ""
    text = f"{description}\n\n{block}" if description else block
    return text[:4096]


def build_job(message: Message, allow_dup: bool = False) -> Optional[Job]:
    if message.empty or message.service or not message.chat:
        return None
    chat_id = message.chat.id

    if chat_id in config.IGNORED_SOURCE_CHATS:
        RT.stats["dropped_ignored"] += 1
        return None
    if is_adult(message):
        RT.stats["dropped_adult"] += 1
        return None

    if media_qualifies(message):
        key = media_unique_id(message)
        if key and not allow_dup and key in MEDIA_SEEN:
            RT.stats["dropped_dup_media"] += 1
            return None
        caption = sanitize_text(str(message.caption or ""))[:1024]
        return Job(
            kind="media",
            source_id=chat_id,
            message_id=message.id,
            caption=caption,
            media_key=key,
        )

    urls = rank_urls(extract_valid_urls(message))
    if not urls:
        RT.stats["dropped_no_content"] += 1
        return None

    labels_by_url = button_labels(message)
    top = urls[: config.MAX_BUTTONS]
    return Job(
        kind="links",
        source_id=chat_id,
        message_id=message.id,
        text=build_links_text(str(message.text or message.caption or ""), urls),
        urls=top,
        labels=[labels_by_url.get(u.rstrip("/"), "") for u in top],
    )


def reserve_job(job: Job) -> None:
    if job.kind == "media" and job.media_key:
        job.reserved = MEDIA_SEEN.add(job.media_key)


def release_job(job: Job) -> None:
    if job.reserved and job.media_key:
        MEDIA_SEEN.discard(job.media_key)
        job.reserved = False


def submit_live(job: Job) -> None:
    reserve_job(job)
    try:
        QUEUE.put_nowait(job)
    except asyncio.QueueFull:
        release_job(job)
        RT.stats["dropped_queue_full"] += 1
        log.warning("Queue full (%d): dropped live %s job", QUEUE.maxsize, job.kind)


async def submit_ingest(job: Job) -> str:
    reserve_job(job)
    job.future = asyncio.get_running_loop().create_future()
    try:
        await QUEUE.put(job)
    except asyncio.CancelledError:
        release_job(job)
        raise
    return await job.future


def drain_queue() -> int:
    drained = 0
    while True:
        try:
            job = QUEUE.get_nowait()
        except asyncio.QueueEmpty:
            break
        QUEUE.task_done()
        drained += 1
        release_job(job)
        if job.future is not None and not job.future.done():
            job.future.set_result("cancelled")
    return drained


async def deliver_media(job: Job) -> None:
    target = STATE.target_media
    if not target:
        raise RuntimeError("TARGET_MEDIA_CHAT is not configured")

    kwargs: Dict[str, Any] = dict(
        chat_id=target,
        from_chat_id=job.source_id,
        message_id=job.message_id,
        caption=job.caption,
        parse_mode=enums.ParseMode.DISABLED,
    )

    if job.source_id not in RT.no_bot_copy:
        try:
            await flood_retry(lambda: bot.copy_message(**kwargs), "bot.copy_message")
            return
        except Exception as exc:
            if isinstance(exc, ACCESS_ERRORS):
                RT.no_bot_copy.add(job.source_id)
            log.info("bot.copy_message failed (%s); falling back to userbot", exc)

    await userbot_call(
        job.source_id, lambda client: client.copy_message(**kwargs), "userbot.copy_message"
    )


async def deliver_links(job: Job) -> None:
    target = STATE.target_links
    if not target:
        raise RuntimeError("TARGET_LINKS_CHAT is not configured")

    rows = []
    for index, url in enumerate(job.urls[: config.MAX_BUTTONS], start=1):
        label = job.labels[index - 1] if index - 1 < len(job.labels) else ""
        rows.append([InlineKeyboardButton(f"🔗 {label or f'Open Link {index}'}", url=url)])
    markup = InlineKeyboardMarkup(rows)

    async def _send(with_markup: bool) -> Any:
        return await bot.send_message(
            chat_id=target,
            text=job.text,
            parse_mode=HTML,
            reply_markup=markup if with_markup else None,
            **NO_PREVIEW,
        )

    try:
        await flood_retry(lambda: _send(True), "bot.send_message")
    except BadRequest as exc:
        log.warning("send_message with buttons failed (%s); retrying plain", exc)
        await flood_retry(lambda: _send(False), "bot.send_message")


async def delivery_worker() -> None:
    log.info("Delivery worker started")
    while True:
        job = await QUEUE.get()
        outcome = "failed"
        try:
            if job.kind == "media":
                await deliver_media(job)
                RT.stats["media_sent"] += 1
            else:
                await deliver_links(job)
                RT.stats["links_sent"] += 1
            outcome = "ok"
        except asyncio.CancelledError:
            release_job(job)
            if job.future is not None and not job.future.done():
                job.future.set_result("cancelled")
            QUEUE.task_done()
            raise
        except Exception:
            RT.stats["failed"] += 1
            log.exception("Delivery failed (%s %s/%s)", job.kind, job.source_id, job.message_id)

        if outcome != "ok":
            release_job(job)
        if job.future is not None and not job.future.done():
            job.future.set_result(outcome)
        QUEUE.task_done()
        MEDIA_SEEN.save()
        await asyncio.sleep(config.DELIVERY_DELAY)


def stopped() -> bool:
    return RT.stop_event.is_set() or STATE.paused


async def _source_filter(_, __, message: Message) -> bool:
    chat = message.chat
    return chat is not None and chat.id in STATE.source_set


source_filter = filters.create(_source_filter)


def note_live_message(chat_id: int, message_id: int) -> None:
    if chat_id in RT.ingesting:
        RT.live_max[chat_id] = max(RT.live_max.get(chat_id, 0), message_id)
        return
    entry = STATE.checkpoints.get(str(chat_id))
    if entry and entry.get("done"):
        entry["last_read_id"] = max(entry.get("last_read_id", 0), message_id)


async def on_source_message(client: Client, message: Message) -> None:
    try:
        chat = message.chat
        if chat is None:
            return
        chat_id = chat.id
        if chat_id in config.IGNORED_SOURCE_CHATS:
            RT.stats["dropped_ignored"] += 1
            return
        if STATE.paused:
            return
        if not SEEN.add((chat_id, message.id)):
            RT.stats["dropped_seen"] += 1
            return
        if client in USERBOTS:
            RT.owner.setdefault(chat_id, USERBOTS.index(client))

        job = build_job(message)
        if job is not None:
            submit_live(job)
        note_live_message(chat_id, message.id)
    except Exception:
        log.exception("Error while processing live message")


for _client in USERBOTS:
    _client.add_handler(MessageHandler(on_source_message, source_filter & ~filters.service))


@dataclass
class IngestResult:
    status: str = "done"
    sent: int = 0
    failed: int = 0
    dropped: int = 0
    note: str = ""


@dataclass
class RunSummary:
    lines: List[str] = field(default_factory=list)
    sources: int = 0
    sent: int = 0
    failed: int = 0
    dropped: int = 0
    halted: str = ""


async def _fetch_top(client: Client, chat_id: int) -> int:
    top = 0
    async for msg in client.get_chat_history(chat_id, limit=1):
        top = msg.id
    return top


async def ingest_source(sid: int, allow_dup: bool) -> IngestResult:
    result = IngestResult()
    key = str(sid)
    top = await userbot_call(sid, lambda c: _fetch_top(c, sid), "get_chat_history(top)")

    entry = STATE.checkpoints.get(key)
    if entry is None:
        entry = {"last_read_id": 0, "top_id": top, "done": False}
        STATE.checkpoints[key] = entry
    entry["top_id"] = top
    entry["done"] = False
    if config.MAX_HISTORY_PER_SOURCE > 0 and entry["last_read_id"] == 0:
        entry["last_read_id"] = max(0, top - config.MAX_HISTORY_PER_SOURCE)

    RT.ingesting.add(sid)
    RT.current_source = sid
    consecutive_failures = handled = 0
    try:
        cursor = entry["last_read_id"] + 1
        while cursor <= top:
            if stopped():
                result.status = "interrupted"
                return result

            ids = list(range(cursor, min(cursor + config.CLONE_BATCH_SIZE, top + 1)))
            fetched = await userbot_call(sid, lambda c: c.get_messages(sid, ids), "get_messages")
            if not isinstance(fetched, list):
                fetched = [fetched]
            by_id = {m.id: m for m in fetched if m is not None and not m.empty}

            for mid in ids:
                if stopped():
                    result.status = "interrupted"
                    return result

                job: Optional[Job] = None
                message = by_id.get(mid)
                if message is not None:
                    is_new = SEEN.add((sid, mid))
                    if is_new or allow_dup:
                        job = build_job(message, allow_dup=allow_dup)
                    else:
                        RT.stats["dropped_seen"] += 1

                if job is None:
                    result.dropped += 1
                else:
                    outcome = await submit_ingest(job)
                    if outcome == "cancelled":
                        result.status = "interrupted"
                        return result
                    if outcome == "ok":
                        result.sent += 1
                        consecutive_failures = 0
                    else:
                        result.failed += 1
                        consecutive_failures += 1
                        if consecutive_failures >= config.MAX_CONSECUTIVE_FAILURES:
                            result.status = "aborted"
                            result.note = f"{consecutive_failures} failures in a row"
                            return result

                entry["last_read_id"] = mid
                handled += 1
                if handled % 10 == 0:
                    STATE.save()

            cursor = ids[-1] + 1
            STATE.save()
            await asyncio.sleep(config.HISTORY_FETCH_DELAY)

        entry["done"] = True
        entry["last_read_id"] = max(top, RT.live_max.pop(sid, 0))
        result.status = "done"
        return result
    finally:
        RT.ingesting.discard(sid)
        RT.live_max.pop(sid, None)
        RT.current_source = None
        STATE.save()


def select_sources(mode: str) -> List[int]:
    ids = [s for s in STATE.sources if s not in config.IGNORED_SOURCE_CHATS]
    if mode in ("continue", "duplicate"):
        return ids
    if mode == "skip":
        return [s for s in ids if str(s) not in STATE.checkpoints]
    return [s for s in ids if not STATE.checkpoints.get(str(s), {}).get("done")]


async def sleep_or_stop(seconds: float) -> bool:
    try:
        await asyncio.wait_for(RT.stop_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return STATE.paused


async def notify_run(summary: RunSummary, notify_chat: Optional[int]) -> None:
    header = "<b>Ingestion halted</b>" if summary.halted else "<b>Ingestion finished</b>"
    body = [
        header,
        f"Sources processed: <b>{summary.sources}</b> | delivered: <b>{summary.sent}</b> | "
        f"dropped: <b>{summary.dropped}</b> | failed: <b>{summary.failed}</b>",
    ]
    if summary.halted:
        body.append(f"⚠️ {summary.halted}")
    body.extend(summary.lines[:40])
    text = "\n".join(body)[:4096]

    for chat in [notify_chat] if notify_chat else list(config.ADMINS):
        try:
            await flood_retry(
                lambda: bot.send_message(chat, text, parse_mode=HTML, **NO_PREVIEW), "notify"
            )
        except Exception as exc:
            log.warning("Could not send summary to %s: %s", chat, exc)


async def auto_clone_new_sources(mode: str = "auto", notify_chat: Optional[int] = None) -> None:
    RT.ingest_mode = mode
    summary = RunSummary()
    processed: Set[int] = set()
    cancelled = False
    try:
        if mode == "duplicate":
            for sid in STATE.sources:
                STATE.checkpoints.pop(str(sid), None)
            STATE.save()

        first = True
        while not stopped():
            todo = [s for s in select_sources(mode) if s not in processed]
            if not todo:
                break
            sid = todo[0]
            if not first and await sleep_or_stop(config.INGEST_CHANNEL_DELAY):
                break
            first = False
            processed.add(sid)
            summary.sources += 1

            try:
                result = await ingest_source(sid, allow_dup=(mode == "duplicate"))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Ingestion of %s failed: %s", sid, exc)
                summary.lines.append(f"❌ <code>{sid}</code>: {friendly_error(exc)}")
                summary.failed += 1
                continue

            summary.sent += result.sent
            summary.failed += result.failed
            summary.dropped += result.dropped
            if result.status == "done":
                summary.lines.append(f"✅ <code>{sid}</code>: {result.sent} delivered")
            else:
                summary.lines.append(f"⏸ <code>{sid}</code>: {result.status}")
                if result.status == "aborted":
                    summary.halted = f"<code>{sid}</code>: {result.note}"
                    break
        if stopped() and not summary.halted:
            summary.halted = "stopped by /stop"
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        RT.current_source = None
        STATE.save()
        if not cancelled and summary.sources:
            await notify_run(summary, notify_chat)


def schedule_auto_ingestion(mode: str = "auto", notify_chat: Optional[int] = None) -> bool:
    if STATE.paused or RT.stop_event.is_set() or RT.ingest_active:
        return False
    if not (STATE.target_links and STATE.target_media):
        return False
    if not select_sources(mode):
        return False
    RT.ingest_task = asyncio.create_task(auto_clone_new_sources(mode, notify_chat))
    return True


async def stop_all(timeout: float = 45.0) -> str:
    was_ingesting = RT.ingest_active
    STATE.paused = True
    STATE.save()
    RT.stop_event.set()
    drained = drain_queue()

    if RT.ingest_task is not None and not RT.ingest_task.done():
        _, pending = await asyncio.wait({RT.ingest_task}, timeout=timeout)
        if pending:
            RT.ingest_task.cancel()
            await asyncio.gather(RT.ingest_task, return_exceptions=True)
    MEDIA_SEEN.save(force=True)
    STATE.save()
    parts = ["⏹ Live forwarding paused"]
    if was_ingesting:
        parts.append("ingestion halted")
    parts.append(f"{drained} queued item(s) discarded")
    return ", ".join(parts) + ".\nUse /resume to continue."


async def _collect_history(client: Client, chat: ChatRef, limit: int) -> List[Message]:
    out: List[Message] = []
    async for msg in client.get_chat_history(chat, limit=limit):
        out.append(msg)
    return out


async def fetch_history(chat: ChatRef, limit: int) -> List[Message]:
    try:
        return await flood_retry(lambda: _collect_history(bot, chat, limit), "bot.get_chat_history")
    except Exception as exc:
        log.info("bot.get_chat_history failed (%s); using userbot", exc)
    return await userbot_call(chat, lambda c: _collect_history(c, chat, limit), "get_chat_history")


def message_fingerprint(message: Message) -> Optional[str]:
    if message.empty or message.service:
        return None
    if message.media:
        media = getattr(message, message.media.value, None)
        unique_id = getattr(media, "file_unique_id", None)
        if unique_id:
            return f"m:{unique_id}"
    text = message.text or message.caption
    if text:
        normalized = " ".join(str(text).lower().split())
        if len(normalized) > 8:
            return "t:" + hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return None


async def delete_batch(chat: ChatRef, ids: List[int]) -> int:
    try:
        result = await flood_retry(lambda: bot.delete_messages(chat, ids), "bot.delete_messages")
    except Exception as bot_exc:
        try:
            result = await userbot_call(
                chat, lambda c: c.delete_messages(chat, ids), "userbot.delete_messages"
            )
        except Exception:
            raise bot_exc
    if isinstance(result, bool):
        return len(ids) if result else 0
    if isinstance(result, int):
        return result
    return len(ids)


async def dedupe_chat(chat: ChatRef) -> Tuple[int, int]:
    history = await fetch_history(chat, config.CLEANDUP_SCAN_LIMIT)
    seen: Set[str] = set()
    duplicate_ids: List[int] = []
    for msg in reversed(history):
        fingerprint = message_fingerprint(msg)
        if fingerprint is None:
            continue
        if fingerprint in seen:
            duplicate_ids.append(msg.id)
        else:
            seen.add(fingerprint)

    deleted = 0
    size = config.DELETE_BATCH_SIZE
    for start in range(0, len(duplicate_ids), size):
        deleted += await delete_batch(chat, duplicate_ids[start : start + size])
        if start + size < len(duplicate_ids):
            await asyncio.sleep(config.DELETE_BATCH_DELAY)
    return len(history), deleted


def parse_chat_arg(message: Message) -> Optional[ChatRef]:
    if len(message.command) < 2:
        return None
    parsed = config.parse_chat_list(message.command[1])
    return parsed[0] if parsed else None


async def check_target(chat_id: int, label: str) -> Tuple[bool, str]:
    try:
        chat = await flood_retry(lambda: bot.get_chat(chat_id), "bot.get_chat")
        member = await flood_retry(
            lambda: bot.get_chat_member(chat_id, "me"), "bot.get_chat_member"
        )
    except Exception as exc:
        return False, f"❌ {label} <code>{chat_id}</code>: {friendly_error(exc)}"

    title = html.escape(chat.title or str(chat_id))
    if member.status not in (
        enums.ChatMemberStatus.ADMINISTRATOR,
        enums.ChatMemberStatus.OWNER,
    ):
        return False, f"❌ {label} {title}: the bot is not an admin there"

    priv = getattr(member, "privileges", None)
    if (
        member.status == enums.ChatMemberStatus.ADMINISTRATOR
        and chat.type == enums.ChatType.CHANNEL
        and priv is not None
        and not getattr(priv, "can_post_messages", False)
    ):
        return False, f"❌ {label} {title}: the bot lacks Post messages right"

    warn = ""
    if priv is not None and not getattr(priv, "can_delete_messages", True):
        warn = " ⚠️ no Delete messages right"
    return True, f"✅ {label} {title}{warn}"


async def apply_session(data: Dict[str, Any]) -> Tuple[bool, str]:
    report: List[str] = []
    ok = True

    new_links: Optional[int] = data.get("links")
    new_media: Optional[int] = data.get("media")
    if new_links is not None:
        good, line = await check_target(new_links, "Links target")
        report.append(line)
        ok = ok and good
    if new_media is not None:
        good, line = await check_target(new_media, "Media target")
        report.append(line)
        ok = ok and good
    if not ok:
        return False, "<b>Nothing saved</b> — fix permissions:\n" + "\n".join(report)

    if new_links is not None:
        STATE.target_links = new_links
    if new_media is not None:
        STATE.target_media = new_media

    ids: List[int] = data.get("source_ids") or []
    mode = data.get("mode", "keep")
    if mode != "keep":
        ids = [i for i in ids if i not in config.IGNORED_SOURCE_CHATS]
        before = set(STATE.sources)
        merged = list(dict.fromkeys(STATE.sources + ids)) if mode == "add" else list(dict.fromkeys(ids))
        STATE.set_sources(merged)
        for key in [k for k in STATE.checkpoints if _as_int(k) not in STATE.source_set]:
            STATE.checkpoints.pop(key, None)
        added = len(STATE.source_set - before)
        removed = len(before - STATE.source_set)
        report.append(f"✅ Sources: <b>{len(STATE.sources)}</b> (+{added} / -{removed})")
    else:
        report.append(f"ℹ️ Sources unchanged (<b>{len(STATE.sources)}</b>)")

    STATE.save()
    pending = len(select_sources("auto"))
    if pending:
        started = schedule_auto_ingestion()
        report.append(f"📥 {pending} source(s) will be ingested automatically")
    return True, "<b>Session saved</b>\n" + "\n".join(report)


admin_only = filters.user(config.ADMINS)


def live_status_text() -> str:
    state = "⏸ paused" if STATE.paused else "▶️ running"
    total = len(STATE.sources)
    done = sum(1 for s in STATE.sources if STATE.checkpoints.get(str(s), {}).get("done"))
    started = sum(1 for s in STATE.sources if str(s) in STATE.checkpoints)
    if RT.ingest_active:
        current = ""
        if RT.current_source is not None:
            entry = STATE.checkpoints.get(str(RT.current_source), {})
            current = f" — <code>{RT.current_source}</code> ({entry.get('last_read_id', 0)}/{entry.get('top_id', 0)})"
        ingestion = f"running ({RT.ingest_mode}){current}"
    else:
        ingestion = "idle"
    s = RT.stats
    return (
        "📡 <b>Live status</b>\n\n"
        f"• Forwarding: <b>{state}</b>\n"
        f"• Ingestion: <b>{ingestion}</b>\n"
        f"• Userbots: <b>{len(USERBOTS)}</b> | Bot: <b>1</b>\n"
        f"• Sources: <b>{total}</b> (done {done}, in progress {started - done}, new {total - started})\n"
        f"• Queue: <b>{QUEUE.qsize()}</b>/{config.QUEUE_MAXSIZE}\n"
        f"• Links target: <code>{STATE.target_links or '—'}</code>\n"
        f"• Media target: <code>{STATE.target_media or '—'}</code>\n"
        f"• Min video: <b>{STATE.min_duration}s</b> | Min file: <b>{STATE.min_size_mb:g} MB</b>\n\n"
        "<b>Statistics</b>\n"
        f"• Delivered: media <b>{s['media_sent']}</b>, links <b>{s['links_sent']}</b>, failed <b>{s['failed']}</b>\n"
        f"• Dropped: adult {s['dropped_adult']}, duplicate media {s['dropped_dup_media']}, no link/media {s['dropped_no_content']}"
    )


@bot.on_message(filters.command(["start", "help"]) & admin_only)
async def help_handler(client: Client, message: Message) -> None:
    await reply_html(
        message,
        "<b>Admin commands</b>\n"
        "/session — set sources + targets\n"
        "/clone — start history clone\n"
        "/status — check live progress\n"
        "/stop — pause forwarding and ingestion\n"
        "/resume — resume background operations\n"
        "/set_duration &lt;sec&gt; — min video duration\n"
        "/set_size &lt;mb&gt; — min file size",
    )


@bot.on_message(filters.command("session") & admin_only)
async def session_handler(client: Client, message: Message) -> None:
    PENDING_SESSION[message.from_user.id] = {"step": "sources", "data": {"source_ids": []}}
    await reply_html(
        message,
        "🛠 <b>Session setup (1/3)</b>\nSend source channel IDs (<code>-100…</code>).\n"
        "• <code>done</code> — finish\n"
        "• <code>keep</code> — keep current\n"
        "• start with <code>add</code> to append\n\n"
        f"Current: <b>{len(STATE.sources)}</b>",
    )


@bot.on_message(filters.text & admin_only & ~filters.regex(r"^/"), group=1)
async def session_dialog(client: Client, message: Message) -> None:
    uid = message.from_user.id
    pending = PENDING_SESSION.get(uid)
    if not pending:
        return
    text = (message.text or "").strip()
    lowered = text.lower()

    if lowered == "cancel":
        PENDING_SESSION.pop(uid, None)
        await reply_html(message, "Session setup cancelled.")
        return

    step = pending["step"]
    data = pending["data"]

    if step == "sources":
        if lowered in ("keep", "done"):
            if lowered == "keep" or not data["source_ids"]:
                data["mode"] = "keep"
            else:
                data.setdefault("mode", "replace")
            pending["step"] = "links"
            await reply_html(message, "🛠 <b>Session setup (2/3)</b>\nSend TARGET_LINKS_CHAT or <code>keep</code>.")
            return

        if lowered.startswith("add") and "mode" not in data:
            data["mode"] = "add"
            text = text[3:]
        ids, _ = config.parse_chat_ids(text)
        for cid in ids:
            if cid not in config.IGNORED_SOURCE_CHATS and cid not in data["source_ids"]:
                data["source_ids"].append(cid)
        await reply_html(message, f"➕ Collected <b>{len(data['source_ids'])}</b> ID(s). Send more or <code>done</code>.")
        return

    if step in ("links", "media"):
        if lowered == "keep":
            data[step] = None
        elif text.lstrip("-").isdigit():
            data[step] = int(text)
        else:
            await reply_html(message, "Send a numeric chat ID or <code>keep</code>.")
            return

        if step == "links":
            pending["step"] = "media"
            await reply_html(message, "🛠 <b>Session setup (3/3)</b>\nSend TARGET_MEDIA_CHAT or <code>keep</code>.")
            return

        PENDING_SESSION.pop(uid, None)
        status = await reply_html(message, "🔎 Checking target permissions…")
        try:
            ok, report = await apply_session(data)
        except Exception as exc:
            ok, report = False, f"Crash: {friendly_error(exc)}"
        await edit_html(status, report)


async def start_manual_clone(message: Message, mode: str) -> None:
    RT.stop_event.clear()
    STATE.paused = False
    STATE.save()
    if RT.ingest_active:
        await reply_html(message, "Ingestion already running.")
        return
    if not schedule_auto_ingestion(mode, message.chat.id):
        await reply_html(message, "Nothing to clone.")
        return
    await reply_html(message, f"▶️ Started clone ({mode}). Use /status.")


@bot.on_message(filters.command("clone") & admin_only)
async def clone_handler(client: Client, message: Message) -> None:
    uid = message.from_user.id
    if RT.ingest_active:
        await reply_html(message, "Ingestion already running. Use /status or /stop.")
        return
    previous = [s for s in STATE.sources if str(s) in STATE.checkpoints]
    if previous:
        PENDING_CLONE[uid] = True
        await reply_html(
            message,
            f"<b>{len(previous)}</b> checkpoints exist.\n"
            "/force_continue — resume\n"
            "/force_duplicate — restart all\n"
            "/skip — only untouched",
        )
        return
    await start_manual_clone(message, "auto")


@bot.on_message(filters.command("force_continue") & admin_only)
async def force_continue_handler(client: Client, message: Message) -> None:
    await start_manual_clone(message, "continue")


@bot.on_message(filters.command("force_duplicate") & admin_only)
async def force_duplicate_handler(client: Client, message: Message) -> None:
    await start_manual_clone(message, "duplicate")


@bot.on_message(filters.command("skip") & admin_only)
async def skip_handler(client: Client, message: Message) -> None:
    await start_manual_clone(message, "skip")


@bot.on_message(filters.command("status") & admin_only)
async def status_handler(client: Client, message: Message) -> None:
    await reply_html(message, live_status_text())


@bot.on_message(filters.command("stop") & admin_only)
async def stop_handler(client: Client, message: Message) -> None:
    status = await reply_html(message, "⏳ Stopping…")
    await edit_html(status, await stop_all())


@bot.on_message(filters.command("resume") & admin_only)
async def resume_handler(client: Client, message: Message) -> None:
    RT.stop_event.clear()
    STATE.paused = False
    STATE.save()
    schedule_auto_ingestion()
    await reply_html(message, "▶️ Resumed.")


@bot.on_message(filters.command("set_duration") & admin_only)
async def set_duration_handler(client: Client, message: Message) -> None:
    try:
        val = int(float(message.command[1]))
    except (IndexError, ValueError):
        await reply_html(message, "Usage: <code>/set_duration 60</code>")
        return
    STATE.min_duration = val
    STATE.save()
    await reply_html(message, f"✅ Video duration threshold: <b>{val}s</b>")


@bot.on_message(filters.command("set_size") & admin_only)
async def set_size_handler(client: Client, message: Message) -> None:
    try:
        val = float(message.command[1])
    except (IndexError, ValueError):
        await reply_html(message, "Usage: <code>/set_size 10</code>")
        return
    STATE.min_size_mb = val
    STATE.save()
    await reply_html(message, f"✅ File size threshold: <b>{val:g} MB</b>")


async def bootstrap() -> None:
    await asyncio.gather(*(refresh_dialogs(c, force=True) for c in USERBOTS))
    new_env = [s for s in config.SOURCE_CHATS if s not in STATE.env_known]
    if new_env:
        STATE.set_sources(STATE.sources + [s for s in new_env if s not in config.IGNORED_SOURCE_CHATS])
        STATE.env_known = list(dict.fromkeys(STATE.env_known + new_env))
        STATE.save()
        log.info("Imported %d new sources from env", len(new_env))

    for target in (STATE.target_links, STATE.target_media):
        if not target:
            continue
        try:
            await bot.get_chat(target)
        except Exception as exc:
            log.warning("Bot cannot resolve %s yet: %s", target, exc)


async def graceful_shutdown() -> None:
    log.info("Shutting down gracefully…")
    RT.stop_event.set()
    drain_queue()
    if RT.ingest_task and not RT.ingest_task.done():
        RT.ingest_task.cancel()
    if RT.worker_task and not RT.worker_task.done():
        RT.worker_task.cancel()
    STATE.save()
    MEDIA_SEEN.save(force=True)
    await asyncio.gather(*(c.stop() for c in USERBOTS), bot.stop(), return_exceptions=True)
    log.info("Stopped.")


async def main() -> None:
    # Render keep-alive server binds first
    await dummy_web_server()

    STATE.load()
    MEDIA_SEEN.load()

    await asyncio.gather(*(c.start() for c in USERBOTS), bot.start())
    RT.worker_task = asyncio.create_task(delivery_worker())
    await bootstrap()

    me_bot = await bot.get_me()
    log.info(
        "Userbots: %d | Bot: @%s | Sources: %d",
        len(USERBOTS),
        me_bot.username,
        len(STATE.sources),
    )

    schedule_auto_ingestion()

    try:
        await idle()
    finally:
        await graceful_shutdown()


if __name__ == "__main__":
    try:
        LOOP.run_until_complete(main())
    except KeyboardInterrupt:
        log.info("Interrupted")
      
