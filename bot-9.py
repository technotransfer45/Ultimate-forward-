"""Dual-userbot Telegram batch forwarder, auto-ingestion and filter bot (Pyrofork).

Userbots (SESSION_STRING_1 / SESSION_STRING_2): listen to source channels and read their
                          history. Every channel joined on EITHER account feeds one pipeline.
Bot (BOT_TOKEN):          delivers to the two targets (no forward tag), deletes duplicates
                          and answers admin commands.

All deliveries go through ONE bounded FIFO queue with a mandatory pause between items.
"""

# The event loop MUST exist before pyrogram is imported / clients are created,
# otherwise the clients bind to a different loop than the one we run.
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
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Dict, Hashable, Iterator, List, Optional, Set, Tuple, Union
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
    """disable_web_page_preview=True, or its newer equivalent if this build dropped it."""
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
    """Bounded insertion-ordered set (oldest entries are evicted first)."""

    def __init__(self, maxsize: int) -> None:
        self.maxsize = maxsize
        self._items: "OrderedDict[Hashable, None]" = OrderedDict()

    def add(self, key: Hashable) -> bool:
        """Add key; return True if it was new, False if it was already present."""
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
    """file_unique_ids already sent to Target 2, persisted to disk."""

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


# --------------------------------------------------------------------------- #
# Persistent state
# --------------------------------------------------------------------------- #
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
    """Runtime settings + per-source checkpoints, persisted atomically to a JSON file."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.sources: List[int] = []
        self.source_set: Set[int] = set()
        self.env_known: List[int] = []  # env-provided sources that were already imported
        self.target_links: int = config.TARGET_LINKS_CHAT
        self.target_media: int = config.TARGET_MEDIA_CHAT
        self.min_duration: int = config.MIN_VIDEO_DURATION
        self.min_size_mb: float = config.MIN_FILE_SIZE_MB
        self.paused: bool = False
        # str(source id) -> {"last_read_id": int, "top_id": int, "done": bool}
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
SEEN = RecentSet(config.SEEN_MESSAGES_MAX)  # (chat_id, message_id) seen by any userbot


class Runtime:
    """Non-persistent runtime flags and counters."""

    def __init__(self) -> None:
        self.stop_event = asyncio.Event()
        self.ingest_task: Optional["asyncio.Task[None]"] = None
        self.worker_task: Optional["asyncio.Task[None]"] = None
        self.ingest_mode: str = ""
        self.current_source: Optional[int] = None
        self.ingesting: Set[int] = set()
        self.live_max: Dict[int, int] = {}
        self.owner: Dict[Any, int] = {}  # chat id -> index of the userbot that can read it
        self.no_bot_copy: Set[int] = set()  # sources the bot itself can't copy from
        self.fetch_delay: float = config.HISTORY_FETCH_DELAY  # adaptive get_messages pause
        self.fetch_floods: int = 0  # FloodWaits seen on get_messages
        self.fail_streak: int = 0  # consecutive failed deliveries (any source)
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


# --------------------------------------------------------------------------- #
# FloodWait-safe helper, error text, replies
# --------------------------------------------------------------------------- #
async def flood_retry(factory: Callable[[], Awaitable[Any]], label: str = "call") -> Any:
    """Await factory(); on FloodWait sleep e.value + 1 seconds and retry, forever."""
    while True:
        try:
            return await factory()
        except FloodWait as exc:
            wait = int(getattr(exc, "value", 5)) + 1
            if label == "get_messages":
                RT.fetch_floods += 1
            log.warning("FloodWait in %s: sleeping %ss then retrying", label, wait)
            await asyncio.sleep(wait)


def friendly_error(exc: BaseException) -> str:
    if isinstance(exc, (ChatAdminRequired, MessageDeleteForbidden)):
        return (
            "Missing permission. Make the bot (or the userbot account) an admin with the "
            "<b>Delete messages</b> / <b>Post messages</b> rights."
        )
    if isinstance(exc, (PeerIdInvalid, ChannelPrivate, ChannelInvalid, KeyError)):
        return (
            "Can't access that chat. A userbot must be a member to read it; the bot must be "
            "admin in the targets. For a fresh bot session, post something in the channel and retry."
        )
    if isinstance(exc, (UsernameNotOccupied, UsernameInvalid)):
        return "That username doesn't exist or is invalid."
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "Telegram did not answer in time (fetch timed out)."
    if isinstance(exc, RPCError):
        return f"Telegram error: <code>{html.escape(str(exc))}</code>"
    return f"Unexpected error: <code>{html.escape(repr(exc))}</code>"


async def reply_html(message: Message, text: str) -> Optional[Message]:
    try:
        return await flood_retry(
            lambda: message.reply_text(text[:4096], parse_mode=HTML, **NO_PREVIEW), "reply"
        )
    except Exception:  # noqa: BLE001
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
    except Exception:  # noqa: BLE001
        log.exception("edit failed")


# --------------------------------------------------------------------------- #
# Userbot access (no per-channel get_chat calls)
# --------------------------------------------------------------------------- #
async def refresh_dialogs(client: Client, force: bool = False) -> bool:
    """Populate a userbot's peer cache with ONE paginated get_dialogs() pass.

    Rate-limited per client. Returns True if a refresh was actually performed.
    """
    now = time.monotonic()
    last = DIALOGS_REFRESHED.get(client.name)
    if not force and last is not None and now - last < config.DIALOG_REFRESH_INTERVAL:
        return False
    DIALOGS_REFRESHED[client.name] = now
    try:
        async for _ in client.get_dialogs():
            pass
    except Exception as exc:  # noqa: BLE001
        log.warning("get_dialogs failed for %s: %s", client.name, exc)
    return True


async def userbot_call(chat_id: Any, fn: Callable[[Client], Awaitable[Any]], label: str) -> Any:
    """Run fn(client) on the userbot that can read chat_id (tries every account)."""
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
                    continue  # peer cache was stale: retry once on the same account
                break
    assert last_exc is not None
    raise last_exc


# --------------------------------------------------------------------------- #
# Pre-filters and sanitization
# --------------------------------------------------------------------------- #
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


def _movie_alternative(keyword: str) -> str:
    escaped = re.escape(keyword).replace(r"\-", r"[\s._-]?").replace(r"\ ", r"[\s._-]?")
    if keyword[-1].isalpha():
        escaped += "s?"
    return escaped


MOVIE_CONTEXT_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    + "|".join(
        [_movie_alternative(k) for k in sorted(config.MOVIE_KEYWORDS, key=len, reverse=True)]
        + [r"s\d{1,2}[\s._-]?e\d{1,3}"]  # S01E02 style episode tags
    )
    + r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)
MENTION_RE = re.compile(r"(?<![\w/.=?&%#@\-])@[A-Za-z0-9_]{4,32}(?![A-Za-z0-9_])")
PROMO_TAIL_RE = re.compile(
    r"\b(?:join(?:\s+us)?|credits?|powered\s+by|uploaded\s+by|shared\s+by|via|source|support|"
    r"backup|follow|share)\s*[:\-–—=]*\s*(?:@[A-Za-z0-9_]{4,32}[\s,|&/]*)+",
    re.IGNORECASE,
)


def message_haystack(message: Message) -> str:
    """Text + caption + file names, for the adult-content shield."""
    parts = [str(message.text or ""), str(message.caption or "")]
    for attr in ("video", "document", "audio", "animation"):
        name = getattr(getattr(message, attr, None), "file_name", None)
        if name:
            parts.append(str(name))
    return "\n".join(parts)


def is_adult(message: Message) -> bool:
    return bool(ADULT_RE.search(message_haystack(message)))


def sanitize_text(raw: str) -> str:
    """Strip @mentions, 'Join:/Credit: @x' promos and spam lines; normalize whitespace."""
    out: List[str] = []
    for line in (raw or "").splitlines():
        if SPAM_RE.search(line):
            continue
        new = PROMO_TAIL_RE.sub("", line)
        new = MENTION_RE.sub("", new)
        if new != line:
            new = re.sub(r"[ \t]{2,}", " ", new).strip()
            if not re.search(r"\w", new):
                continue  # nothing but leftover symbols
        out.append(new.rstrip())
    text = "\n".join(out)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# --------------------------------------------------------------------------- #
# Deep link scanner
# --------------------------------------------------------------------------- #
URL_RE = re.compile(r"https?://[^\s<>\"'\[\]\(\)]+", re.IGNORECASE)
TRAILING_JUNK = ".,;:!?)]}>*_`~'\""


def clean_url(raw: str) -> str:
    """Strip whitespace, markdown brackets and trailing punctuation."""
    url = (raw or "").strip().strip("<>[]()")
    return url.rstrip(TRAILING_JUNK).strip()


def _host_of(url: str) -> str:
    try:
        host = (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def is_ignored_host(host: str) -> bool:
    for domain in config.BLOCKED_DOMAINS:
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
    """0 for known streaming hosts / TeraBox family, 1 for everything else."""
    host = _host_of(url)
    for domain in config.PRIORITY_DOMAINS:
        if host == domain or host.endswith("." + domain):
            return 0
    for keyword in config.PRIORITY_KEYWORDS:
        if keyword in host:
            return 0
    return 1


def is_recognized_host(url: str) -> bool:
    """True for TeraBox/streaming hosts, cloud/file hosts and well-known shorteners."""
    if url_priority(url) == 0:
        return True
    host = _host_of(url)
    if any(fragment in host for fragment in config.MOVIE_DOMAINS):
        return True
    if any(keyword in host for keyword in config.HOST_KEYWORDS):
        return True
    return any(
        host == domain or host.endswith("." + domain) for domain in config.KNOWN_SHORTENER_DOMAINS
    )


def rank_urls(urls: List[str]) -> List[str]:
    return sorted(urls, key=lambda u: 0 if is_recognized_host(u) else 1)  # stable per tier


def has_movie_context(message: Message, labels: Optional[Dict[str, str]] = None) -> bool:
    """Movie/TV keywords in text/caption only (button labels do not qualify)."""
    parts = [
        sanitize_text(str(message.text or "")),
        sanitize_text(str(message.caption or "")),
    ]
    return bool(MOVIE_CONTEXT_RE.search("\n".join(parts)))


def _entity_text(text: str, offset: int, length: int) -> str:
    """Telegram entity offsets are UTF-16 code units, so slice accordingly."""
    raw = text.encode("utf-16-le")
    return raw[offset * 2 : (offset + length) * 2].decode("utf-16-le", errors="ignore")


def _iter_button_items(node: Any) -> Iterator[Tuple[str, str]]:
    """Recursively walk a keyboard (markup -> rows -> buttons) yielding (url, button text)."""
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
    """Deep-scan URLs and drop the ENTIRE message if any URL is blocklisted."""
    candidates: List[str] = []

    # Layer 1: raw text and caption regex.
    for blob in (message.text, message.caption):
        if blob:
            candidates.extend(URL_RE.findall(str(blob)))

    # Layer 2: entities and caption entities (hyperlinks + plain URL entities).
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

    # Layer 3: inline keyboard, scanned recursively.
    candidates.extend(url for url, _ in _iter_button_items(message.reply_markup))

    cleaned: List[str] = []
    for raw in candidates:
        url = clean_url(raw)
        if not url:
            continue
        host = _host_of(url)
        # STRICT BLOCK: if even one URL points to a blocked/social-media host,
        # reject the whole message. Another URL cannot bypass this block.
        if host and is_ignored_host(host):
            return []
        cleaned.append(url)

    seen: Set[str] = set()
    result: List[str] = []
    for url in cleaned:
        if not is_valid_external_url(url):
            continue
        key = url.rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        result.append(url)
    return result


def button_labels(message: Message) -> Dict[str, str]:
    """Sanitized inline-button texts keyed by URL (used as nicer button labels)."""
    labels: Dict[str, str] = {}
    for url, text in _iter_button_items(message.reply_markup):
        key = clean_url(url).rstrip("/")
        label = sanitize_text(text).replace("\n", " ").strip()
        if key and key not in labels and label and re.search(r"\w", label) and len(label) <= 40:
            labels[key] = label
    return labels


# --------------------------------------------------------------------------- #
# Jobs and routing decision
# --------------------------------------------------------------------------- #
@dataclass
class Job:
    kind: str  # "media" | "links"
    source_id: int
    message_id: int = 0
    caption: Optional[str] = None  # None = keep the original caption untouched
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
    """Apply every pre-filter and routing rule. None means: drop the message."""
    if message.empty or message.service or not message.chat:
        return None
    chat_id = message.chat.id

    if chat_id in config.IGNORED_SOURCE_CHATS:
        RT.stats["dropped_ignored"] += 1
        return None

    # Target 2, fast path: a qualifying movie file is accepted right here. No link regex,
    # no text sanitizing (unless SANITIZE_MEDIA_CAPTIONS is on); only the cheap adult shield.
    if media_qualifies(message):
        if is_adult(message):
            RT.stats["dropped_adult"] += 1
            return None
        key = media_unique_id(message)
        if key and not allow_dup and key in MEDIA_SEEN:
            RT.stats["dropped_dup_media"] += 1
            return None
        caption: Optional[str] = None
        if config.SANITIZE_MEDIA_CAPTIONS:
            caption = sanitize_text(str(message.caption or ""))[:1024]
        return Job(
            kind="media",
            source_id=chat_id,
            message_id=message.id,
            caption=caption,
            media_key=key,
        )

    # Target 1: movie / TV download links only.
    if not (message.text or message.caption or message.reply_markup):
        RT.stats["dropped_no_content"] += 1  # photos, stickers, short clips, small files...
        return None
    if is_adult(message):
        RT.stats["dropped_adult"] += 1
        return None

    urls = extract_valid_urls(message)
    if not urls:
        RT.stats["dropped_no_content"] += 1
        return None

    # STRICT MOVIE-LINK QUALIFICATION:
    #   1) text/caption contains a movie/TV keyword, OR
    #   2) at least one URL host matches MOVIE_DOMAINS.
    # Blocklisted URLs were already handled by extract_valid_urls().
    movie_context = has_movie_context(message)
    movie_url = any(
        any(fragment in _host_of(url) for fragment in config.MOVIE_DOMAINS)
        for url in urls
    )
    if not (movie_context or movie_url):
        RT.stats["dropped_no_content"] += 1
        return None

    # Put recognized movie hosts first, while retaining all non-blocked URLs
    # when the message itself has valid movie context.
    urls = rank_urls(urls)
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
    """Register the media fingerprint as soon as a job is committed to the queue."""
    if job.kind == "media" and job.media_key:
        job.reserved = MEDIA_SEEN.add(job.media_key)


def release_job(job: Job) -> None:
    """Undo the fingerprint reservation of a job that was not delivered."""
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


async def queue_ingest(job: Job) -> "asyncio.Future[str]":
    """Queue a history item WITHOUT waiting for its delivery; returns its result future."""
    reserve_job(job)
    job.future = asyncio.get_running_loop().create_future()
    try:
        QUEUE.put_nowait(job)
    except asyncio.QueueFull:  # live posts filled the headroom: apply back-pressure
        try:
            await QUEUE.put(job)
        except asyncio.CancelledError:
            release_job(job)
            raise
    return job.future


def drain_queue() -> int:
    """Discard every pending job, releasing anyone awaiting it."""
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


# --------------------------------------------------------------------------- #
# Delivery (runs only inside the single worker)
# --------------------------------------------------------------------------- #
async def deliver_media(job: Job) -> None:
    target = STATE.target_media
    if not target:
        raise RuntimeError("TARGET_MEDIA_CHAT is not configured")

    kwargs: Dict[str, Any] = dict(
        chat_id=target,
        from_chat_id=job.source_id,
        message_id=job.message_id,
    )
    if job.caption is not None:  # sanitized caption replaces the original one
        kwargs["caption"] = job.caption
        kwargs["parse_mode"] = enums.ParseMode.DISABLED

    if job.source_id not in RT.no_bot_copy:
        try:
            await flood_retry(lambda: bot.copy_message(**kwargs), "bot.copy_message")
            return
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, ACCESS_ERRORS):
                # The bot can't see this source channel: don't retry it for every item.
                RT.no_bot_copy.add(job.source_id)
            log.info("bot.copy_message failed (%s); falling back to a userbot copy", exc)

    # Same server-side copy (still no "Forwarded from" header), done by a userbot.
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
        log.warning("send_message with buttons failed (%s); retrying without buttons", exc)
        await flood_retry(lambda: _send(False), "bot.send_message")


async def delivery_worker() -> None:
    """Persistent FIFO worker: one delivery at a time, 1.5 s apart."""
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
            RT.fail_streak = 0
        except asyncio.CancelledError:
            release_job(job)
            if job.future is not None and not job.future.done():
                job.future.set_result("cancelled")
            QUEUE.task_done()
            raise
        except Exception:  # noqa: BLE001
            RT.stats["failed"] += 1
            RT.fail_streak += 1
            log.exception("Delivery failed (%s %s/%s)", job.kind, job.source_id, job.message_id)

        if outcome != "ok":
            release_job(job)
        if job.future is not None and not job.future.done():
            job.future.set_result(outcome)
        QUEUE.task_done()
        MEDIA_SEEN.save()
        await asyncio.sleep(config.DELIVERY_DELAY)


# --------------------------------------------------------------------------- #
# Live listener (attached to BOTH userbots)
# --------------------------------------------------------------------------- #
def stopped() -> bool:
    return RT.stop_event.is_set() or STATE.paused


async def _source_filter(_, __, message: Message) -> bool:
    chat = message.chat
    return chat is not None and chat.id in STATE.source_set


source_filter = filters.create(_source_filter)


def note_live_message(chat_id: int, message_id: int) -> None:
    """Keep last_read_id accurate for sources that are already fully ingested."""
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
        # Both userbots may be members of the same channel: process each post only once.
        if not SEEN.add((chat_id, message.id)):
            RT.stats["dropped_seen"] += 1
            return
        if client in USERBOTS:
            RT.owner.setdefault(chat_id, USERBOTS.index(client))

        job = build_job(message)
        if job is not None:
            submit_live(job)
        note_live_message(chat_id, message.id)
    except Exception:  # noqa: BLE001
        log.exception("Error while processing live message")


for _client in USERBOTS:
    _client.add_handler(MessageHandler(on_source_message, source_filter & ~filters.service))


# --------------------------------------------------------------------------- #
# Automatic / manual history ingestion
# --------------------------------------------------------------------------- #
@dataclass
class RunSummary:
    lines: List[str] = field(default_factory=list)
    sources: int = 0
    sent: int = 0
    failed: int = 0
    dropped: int = 0
    halted: str = ""


@dataclass
class OpenSource:
    """A source whose history is being queued; it is 'done' once every job has settled."""

    sid: int
    entry: Dict[str, Any]
    top: int
    processed_id: int
    status: str = "running"  # running | queued | done | interrupted | aborted | error
    inflight: "Deque[Tuple[int, asyncio.Future[str]]]" = field(default_factory=deque)
    cancelled: bool = False
    sent: int = 0
    failed: int = 0
    dropped: int = 0
    consecutive_failures: int = 0
    note: str = ""


def close_source(src: OpenSource) -> None:
    """Release live-tracking for a source that will not be finished in this run."""
    if src.status in ("running", "queued"):
        src.status = "interrupted"
    RT.ingesting.discard(src.sid)
    RT.live_max.pop(src.sid, None)


def settle_source(src: OpenSource) -> bool:
    """Account for delivered jobs and advance last_read_id. True when the source is finished.

    last_read_id never passes a job that is still queued, so /stop or a crash can lose
    nothing: unfinished items are fetched again on resume.
    """
    if src.status == "done":  # already finalized (e.g. inside ingest_source)
        return True
    while src.inflight and not src.cancelled and src.inflight[0][1].done():
        outcome = src.inflight[0][1].result()
        if outcome == "cancelled":
            src.cancelled = True
            break
        src.inflight.popleft()
        if outcome == "ok":
            src.sent += 1
            src.consecutive_failures = 0
        else:
            src.failed += 1
            src.consecutive_failures += 1

    safe = src.inflight[0][0] - 1 if src.inflight else src.processed_id
    if safe > src.entry["last_read_id"]:
        src.entry["last_read_id"] = safe

    if src.cancelled and src.status in ("running", "queued"):
        close_source(src)  # /stop discarded part of its queue: finish as 'interrupted'
        return True
    if src.status == "queued" and not src.inflight and not src.cancelled:
        src.entry["done"] = True
        src.entry["last_read_id"] = max(
            src.top, RT.live_max.pop(src.sid, 0), src.entry["last_read_id"]
        )
        src.status = "done"
        RT.ingesting.discard(src.sid)
        return True
    if src.status in ("interrupted", "aborted", "error") and (not src.inflight or src.cancelled):
        close_source(src)
        return True
    return False


async def wait_for_queue_room(src: OpenSource) -> None:
    """Back-pressure: keep headroom in the bounded queue for live posts."""
    while QUEUE.qsize() >= config.INGEST_QUEUE_LIMIT and not stopped():
        settle_source(src)  # keeps checkpoints moving while we wait
        await asyncio.sleep(0.5)


async def _fetch_top(client: Client, chat_id: int) -> int:
    top = 0
    async for msg in client.get_chat_history(chat_id, limit=1):
        top = msg.id
    return top


async def fetch_with_retries(
    sid: int, make: Callable[[Client], Awaitable[Any]], label: str
) -> Any:
    """Run a userbot fetch with a per-call timeout and a few retries.

    FloodWaits are still slept through (inside flood_retry, outside the timeout), so only a
    call that never answers or keeps failing is given up on.
    """
    failures = 0
    while True:
        try:
            return await userbot_call(
                sid, lambda c: asyncio.wait_for(make(c), config.FETCH_TIMEOUT), label
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            failures += 1
            if isinstance(exc, ACCESS_ERRORS) or failures >= config.MAX_FETCH_FAILURES:
                raise  # no access / repeatedly failing: the caller gives up on this channel
            log.warning(
                "%s failed for %s (%d/%d): %r", label, sid, failures, config.MAX_FETCH_FAILURES, exc
            )
            await asyncio.sleep(2 * failures)


async def ingest_source(sid: int, allow_dup: bool, open_sources: List[OpenSource]) -> OpenSource:
    """Producer: fetch one source oldest -> newest and push qualified jobs to the queue.

    Jobs are queued without awaiting their delivery (the queue only blocks the producer once
    it holds INGEST_QUEUE_LIMIT items), and last_read_id is settled/saved every
    CHECKPOINT_EVERY messages. If fetching keeps failing or hangs, or deliveries keep failing,
    the channel is logged, marked done, saved, and this function RETURNS so the caller moves
    on to the next channel. (/force_continue retries a skipped channel from its checkpoint.)
    """
    key = str(sid)
    entry = STATE.checkpoints.get(key)
    if entry is None:
        entry = {"last_read_id": 0, "top_id": 0, "done": False}
        STATE.checkpoints[key] = entry
    src = OpenSource(
        sid=sid, entry=entry, top=entry.get("top_id", 0), processed_id=entry["last_read_id"]
    )
    open_sources.append(src)
    RT.ingesting.add(sid)
    RT.current_source = sid

    def check_abort() -> bool:
        if src.consecutive_failures >= config.MAX_CONSECUTIVE_FAILURES:
            src.status = "aborted"
            src.note = (
                f"{src.consecutive_failures} deliveries failed in a row — channel skipped "
                "(check the bot's rights in the targets)"
            )
            entry["done"] = True  # give up on this channel; the run continues with the next
            return True
        return False

    since_checkpoint = 0
    try:
        top = await fetch_with_retries(sid, lambda c: _fetch_top(c, sid), "get_chat_history(top)")
        src.top = top
        entry["top_id"] = top
        entry["done"] = False
        if config.MAX_HISTORY_PER_SOURCE > 0 and entry["last_read_id"] == 0:
            entry["last_read_id"] = max(0, top - config.MAX_HISTORY_PER_SOURCE)
            src.processed_id = entry["last_read_id"]

        cursor = entry["last_read_id"] + 1
        while cursor <= top and src.status == "running":
            if stopped():
                src.status = "interrupted"
                break

            ids = list(range(cursor, min(cursor + config.CLONE_BATCH_SIZE, top + 1)))
            floods, started = RT.fetch_floods, time.monotonic()
            fetched = await fetch_with_retries(
                sid, lambda c: c.get_messages(sid, ids), "get_messages"
            )
            # Adaptive pause: back off after a FloodWait / slow call, relax towards 0.3 s.
            if RT.fetch_floods != floods or time.monotonic() - started > 4.0:
                RT.fetch_delay = min(config.FETCH_DELAY_MAX, RT.fetch_delay * 2)
            else:
                RT.fetch_delay = max(config.HISTORY_FETCH_DELAY, RT.fetch_delay * 0.8)

            if not isinstance(fetched, list):
                fetched = [fetched]
            by_id = {m.id: m for m in fetched if m is not None and not m.empty}

            for mid in ids:
                job: Optional[Job] = None
                message = by_id.get(mid)
                if message is not None:
                    if SEEN.add((sid, mid)) or allow_dup:
                        job = build_job(message, allow_dup=allow_dup)
                    else:
                        RT.stats["dropped_seen"] += 1  # already handled live

                if job is None:
                    src.dropped += 1
                else:
                    await wait_for_queue_room(src)
                    if stopped():
                        src.status = "interrupted"
                        break
                    src.inflight.append((mid, await queue_ingest(job)))

                src.processed_id = mid
                since_checkpoint += 1
                if since_checkpoint >= config.CHECKPOINT_EVERY:
                    since_checkpoint = 0
                    settle_source(src)
                    STATE.save()
                    if check_abort():
                        break

            if src.status != "running":
                break
            cursor = ids[-1] + 1
            settle_source(src)
            STATE.save()
            if check_abort():
                break
            await asyncio.sleep(RT.fetch_delay)

        if src.status == "running":
            src.status = "queued"  # everything is queued; 'done' is set once it is delivered
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.warning("Ingestion of %s failed, skipping to the next channel: %r", sid, exc)
        src.status = "error"
        src.note = friendly_error(exc)
        entry["done"] = True
        STATE.save()
        # Do not let one inaccessible/stuck source hold up the remaining queue.
        return src
    finally:
        RT.current_source = None
        settle_source(src)
        STATE.save()
    return src


def select_sources(mode: str) -> List[int]:
    ids = [s for s in STATE.sources if s not in config.IGNORED_SOURCE_CHATS]
    if mode in ("continue", "duplicate"):
        return ids
    if mode == "skip":
        return [s for s in ids if str(s) not in STATE.checkpoints]
    # "auto": NEW_SOURCE (no last_read_id) or an interrupted ingestion
    return [s for s in ids if not STATE.checkpoints.get(str(s), {}).get("done")]


async def sleep_or_stop(seconds: float) -> bool:
    """Sleep, waking early if /stop arrives. Returns True if we were stopped."""
    try:
        await asyncio.wait_for(RT.stop_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return STATE.paused


def record_source(src: OpenSource, summary: RunSummary) -> None:
    summary.sent += src.sent
    summary.failed += src.failed
    summary.dropped += src.dropped
    if src.status == "done":
        summary.lines.append(f"✅ <code>{src.sid}</code>: {src.sent} delivered, {src.dropped} dropped")
    elif src.status in ("error", "aborted"):
        summary.lines.append(f"❌ <code>{src.sid}</code>: skipped — {src.note}")
    else:
        summary.lines.append(f"⏸ <code>{src.sid}</code>: {src.status} — {src.sent} delivered")


def settle_open(open_sources: List[OpenSource], summary: RunSummary) -> None:
    for src in list(open_sources):
        if settle_source(src):
            open_sources.remove(src)
            record_source(src, summary)


async def drain_open_sources(open_sources: List[OpenSource], summary: RunSummary) -> None:
    """Wait for the queued tail of every source (or give up cleanly on /stop)."""
    while open_sources:
        settle_open(open_sources, summary)
        if not open_sources:
            return
        if stopped():
            for src in list(open_sources):
                settle_source(src)
                close_source(src)
                record_source(src, summary)
            open_sources.clear()
            return
        await asyncio.sleep(0.5)


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
    if len(summary.lines) > 40:
        body.append(f"…and {len(summary.lines) - 40} more")
    text = "\n".join(body)[:4096]

    for chat in [notify_chat] if notify_chat else list(config.ADMINS):
        try:
            await flood_retry(
                lambda: bot.send_message(chat, text, parse_mode=HTML, **NO_PREVIEW), "notify"
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not send ingestion summary to %s: %s", chat, exc)


async def auto_clone_new_sources(mode: str = "auto", notify_chat: Optional[int] = None) -> None:
    """Background ingestion. 'auto' handles NEW_SOURCE channels without any admin command.

    mode: auto | continue | duplicate | skip. Channels are produced one after another with
    a 5 s pause in between while the delivery worker keeps draining the queue, so the pause
    and the fetching overlap with delivery. Sources added while the task runs are picked up.
    """
    RT.ingest_mode = mode
    RT.fail_streak = 0
    summary = RunSummary()
    processed: Set[int] = set()
    open_sources: List[OpenSource] = []
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
            if RT.fail_streak >= config.MAX_CONSECUTIVE_FAILURES:
                # Targets are unreachable: stop here instead of marking every remaining
                # channel as done. Fix the bot's rights, then /resume.
                summary.halted = (
                    f"{RT.fail_streak} deliveries failed in a row — check the bot's rights "
                    "in the targets, then /resume"
                )
                break
            first = False
            processed.add(sid)
            summary.sources += 1

            try:
                src = await ingest_source(sid, mode == "duplicate", open_sources)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("Ingestion of %s failed: %s", sid, exc)
                summary.lines.append(f"❌ <code>{sid}</code>: {friendly_error(exc)}")
                summary.failed += 1
                continue

            settle_open(open_sources, summary)  # a failed/skipped channel never stops the run

        await drain_open_sources(open_sources, summary)
        if stopped() and not summary.halted:
            summary.halted = "stopped by /stop"
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        for src in open_sources:  # only non-empty when cancelled mid-run
            settle_source(src)
            close_source(src)
        RT.current_source = None
        STATE.save()
        if not cancelled and summary.sources:
            await notify_run(summary, notify_chat)


def schedule_auto_ingestion(mode: str = "auto", notify_chat: Optional[int] = None) -> bool:
    """Start the background ingestion task unless one is running / we're paused."""
    if STATE.paused or RT.stop_event.is_set() or RT.ingest_active:
        return False
    if not (STATE.target_links and STATE.target_media):
        log.info("Ingestion not started: both targets must be set (/session)")
        return False
    if not select_sources(mode):
        return False
    RT.ingest_task = asyncio.create_task(auto_clone_new_sources(mode, notify_chat))
    return True


async def stop_all(timeout: float = 45.0) -> str:
    """Halt ingestion + live forwarding. Checkpoints are saved, queued items discarded."""
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
        parts.append("ingestion halted (checkpoints saved)")
    parts.append(f"{drained} queued item(s) discarded")
    return ", ".join(parts) + ".\nUse /resume to continue."


# --------------------------------------------------------------------------- #
# History access shared by /cleandup and /status
# --------------------------------------------------------------------------- #
async def _collect_history(client: Client, chat: ChatRef, limit: int) -> List[Message]:
    out: List[Message] = []
    async for msg in client.get_chat_history(chat, limit=limit):
        out.append(msg)
    return out


async def fetch_history(chat: ChatRef, limit: int) -> List[Message]:
    """Fetch recent history (newest first).

    Bot accounts normally can't read history (BOT_METHOD_INVALID), so the bot is tried
    first and the userbots are the automatic fallback.
    """
    try:
        return await flood_retry(lambda: _collect_history(bot, chat, limit), "bot.get_chat_history")
    except Exception as exc:  # noqa: BLE001
        log.info("bot.get_chat_history failed (%s); using a userbot", exc)
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
    """Delete via the bot (FloodWait-safe); fall back to a userbot on other errors."""
    try:
        result = await flood_retry(lambda: bot.delete_messages(chat, ids), "bot.delete_messages")
    except Exception as bot_exc:  # noqa: BLE001
        log.info("bot.delete_messages failed (%s); trying a userbot", bot_exc)
        try:
            result = await userbot_call(
                chat, lambda c: c.delete_messages(chat, ids), "userbot.delete_messages"
            )
        except Exception:  # noqa: BLE001
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
    for msg in reversed(history):  # oldest first: keep the original
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
    """Return the chat given as the first command argument (id, @name or link)."""
    if len(message.command) < 2:
        return None
    parsed = config.parse_chat_list(message.command[1])
    return parsed[0] if parsed else None


# --------------------------------------------------------------------------- #
# Session validation (targets only: never one RPC per source)
# --------------------------------------------------------------------------- #
async def check_target(chat_id: int, label: str) -> Tuple[bool, str]:
    try:
        chat = await flood_retry(lambda: bot.get_chat(chat_id), "bot.get_chat")
        member = await flood_retry(
            lambda: bot.get_chat_member(chat_id, "me"), "bot.get_chat_member"
        )
    except Exception as exc:  # noqa: BLE001
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
        return False, f"❌ {label} {title}: the bot lacks the <b>Post messages</b> right"

    warn = ""
    if priv is not None and not getattr(priv, "can_delete_messages", True):
        warn = " ⚠️ no <b>Delete messages</b> right (/cleandup will fail)"
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
        return False, "<b>Nothing was saved</b> — fix these and run /session again:\n" + "\n".join(report)

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
        report.append(
            f"✅ Sources: <b>{len(STATE.sources)}</b> total (+{added} / -{removed})"
        )
        if data.get("ignored_count"):
            report.append(f"ℹ️ {data['ignored_count']} entries skipped (ignored channels)")
    else:
        report.append(f"ℹ️ Sources unchanged (<b>{len(STATE.sources)}</b>)")

    STATE.save()

    pending = len(select_sources("auto"))
    if pending:
        started = schedule_auto_ingestion()
        report.append(
            f"📥 {pending} new/unfinished source(s) will be ingested automatically"
            + ("" if started or RT.ingest_active else " once forwarding is resumed")
        )
    return True, "<b>Session saved</b>\n" + "\n".join(report)


# --------------------------------------------------------------------------- #
# Admin commands
# --------------------------------------------------------------------------- #
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
            current = (
                f" — <code>{RT.current_source}</code> "
                f"({entry.get('last_read_id', 0)}/{entry.get('top_id', 0)})"
            )
        ingestion = f"running ({RT.ingest_mode}){current}"
    else:
        ingestion = "idle"
    s = RT.stats
    return (
        "📡 <b>Live status</b>\n\n"
        f"• Forwarding: <b>{state}</b>\n"
        f"• Ingestion: <b>{ingestion}</b>\n"
        f"• Userbots: <b>{len(USERBOTS)}</b> | Bot: <b>1</b>\n"
        f"• Sources: <b>{total}</b> (ingested {done}, in progress {started - done}, "
        f"new {total - started}) | ignored channels: {len(config.IGNORED_SOURCE_CHATS)}\n"
        f"• Queue: <b>{QUEUE.qsize()}</b>/{config.QUEUE_MAXSIZE}\n"
        f"• Links target: <code>{STATE.target_links or '—'}</code>\n"
        f"• Media target: <code>{STATE.target_media or '—'}</code>\n"
        f"• Min video: <b>{STATE.min_duration}s</b> | Min file: <b>{STATE.min_size_mb:g} MB</b>\n\n"
        "<b>Statistics</b>\n"
        f"• Delivered: media <b>{s['media_sent']}</b>, links <b>{s['links_sent']}</b>, failed <b>{s['failed']}</b>\n"
        f"• Dropped: adult {s['dropped_adult']}, ignored {s['dropped_ignored']}, "
        f"duplicate media {s['dropped_dup_media']}, no link/media {s['dropped_no_content']}, "
        f"already seen {s['dropped_seen']}, queue full {s['dropped_queue_full']}\n"
        f"• Media fingerprints tracked: {len(MEDIA_SEEN)}"
    )


async def chat_report(chat: ChatRef) -> str:
    history = await fetch_history(chat, config.STATUS_SCAN_LIMIT)
    videos = documents = photos = audios = 0
    links: Dict[str, str] = {}
    for msg in history:
        if msg.empty or msg.service:
            continue
        videos += 1 if msg.video else 0
        documents += 1 if msg.document else 0
        photos += 1 if msg.photo else 0
        audios += 1 if (msg.audio or msg.voice) else 0
        for url in extract_valid_urls(msg):
            links.setdefault(url.rstrip("/"), url)

    sample = []
    for url in list(links.values())[:10]:
        shown = url if len(url) <= 80 else url[:77] + "…"
        sample.append(f"• {html.escape(shown)}")
    return (
        f"📊 <b>Chat report</b> (<code>{html.escape(str(chat))}</code>)\n\n"
        f"• Messages scanned: <b>{len(history)}</b>\n"
        f"• 📁 Files/Documents: <b>{documents}</b>\n"
        f"• 🎬 Videos: <b>{videos}</b>\n"
        f"• 🖼 Photos: <b>{photos}</b>\n"
        f"• 🎧 Audio/Voice: <b>{audios}</b>\n"
        f"• 🔗 Unique external links: <b>{len(links)}</b>\n\n"
        f"<b>Link previews:</b>\n{chr(10).join(sample) if sample else '—'}"
    )


@bot.on_message(filters.command(["start", "help"]) & admin_only)
async def help_handler(client: Client, message: Message) -> None:
    await reply_html(
        message,
        "<b>Admin commands</b>\n"
        "/session — set sources (IDs) + target channels\n"
        "/clone — manual re-clone / force continue\n"
        "/status [chat] — live status, or scan a chat\n"
        "/stop — pause cloning and live forwarding\n"
        "/resume — resume forwarding and background ingestion\n"
        "/remove — reset saved sources and checkpoints\n"
        "/cleandup [chat] — delete duplicates (default: both targets)\n"
        "/set_duration &lt;seconds&gt; — min video duration\n"
        "/set_size &lt;mb&gt; — min file size",
    )


# ---- /session (interactive) ------------------------------------------------ #
@bot.on_message(filters.command("session") & admin_only)
async def session_handler(client: Client, message: Message) -> None:
    PENDING_SESSION[message.from_user.id] = {"step": "sources", "data": {"source_ids": []}}
    await reply_html(
        message,
        "🛠 <b>Session setup (1/3)</b>\n"
        "Send source channel IDs (<code>-100…</code>), separated by spaces/commas/new lines. "
        "You can send several messages. Usernames are not accepted (no per-channel lookups).\n\n"
        "• <code>done</code> — finish this step\n"
        "• <code>keep</code> — leave the sources unchanged\n"
        "• start your first message with <code>add</code> to append instead of replace\n"
        "• <code>cancel</code> — abort\n\n"
        f"Current sources: <b>{len(STATE.sources)}</b>",
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
            await reply_html(
                message,
                "🛠 <b>Session setup (2/3)</b>\nSend <b>TARGET_LINKS_CHAT</b> (numeric ID like "
                f"<code>-100…</code>) or <code>keep</code>.\nCurrent: <code>{STATE.target_links or '—'}</code>",
            )
            return

        if lowered.startswith("add") and "mode" not in data:
            data["mode"] = "add"
            text = text[3:]
        ids, rejected = config.parse_chat_ids(text)
        skipped = [i for i in ids if i in config.IGNORED_SOURCE_CHATS]
        data["ignored_count"] = data.get("ignored_count", 0) + len(skipped)
        for chat_id in ids:
            if chat_id not in config.IGNORED_SOURCE_CHATS and chat_id not in data["source_ids"]:
                data["source_ids"].append(chat_id)
        note = f" ⚠️ {len(rejected)} non-numeric entr{'y' if len(rejected) == 1 else 'ies'} ignored." if rejected else ""
        await reply_html(
            message,
            f"➕ Collected <b>{len(data['source_ids'])}</b> ID(s).{note} Send more, or <code>done</code>.",
        )
        return

    if step in ("links", "media"):
        if lowered == "keep":
            data[step] = None
        elif text.lstrip("-").isdigit():
            data[step] = int(text)
        else:
            await reply_html(message, "Please send a numeric chat ID (e.g. <code>-1001234567890</code>) or <code>keep</code>.")
            return

        if step == "links":
            pending["step"] = "media"
            await reply_html(
                message,
                "🛠 <b>Session setup (3/3)</b>\nSend <b>TARGET_MEDIA_CHAT</b> (numeric ID) or "
                f"<code>keep</code>.\nCurrent: <code>{STATE.target_media or '—'}</code>",
            )
            return

        PENDING_SESSION.pop(uid, None)
        status = await reply_html(message, "🔎 Checking the bot's rights in the targets…")
        try:
            ok, report = await apply_session(data)
        except Exception as exc:  # noqa: BLE001
            log.exception("apply_session failed")
            ok, report = False, f"Validation crashed: {friendly_error(exc)}"
        await edit_html(status, report)


# ---- /clone ---------------------------------------------------------------- #
async def start_manual_clone(message: Message, mode: str) -> None:
    RT.stop_event.clear()
    STATE.paused = False
    STATE.save()
    if RT.ingest_active:
        await reply_html(message, "Ingestion is already running. Use /status or /stop.")
        return
    if not schedule_auto_ingestion(mode, message.chat.id):
        await reply_html(message, "Nothing to clone (check /status: sources and both targets).")
        return
    labels = {
        "auto": "Ingesting new/unfinished sources…",
        "continue": "Resuming from saved checkpoints (and catching up finished sources)…",
        "duplicate": "Re-cloning every source from the start (duplicates allowed)…",
        "skip": "Cloning only sources that were never started…",
    }
    await reply_html(message, f"▶️ {labels.get(mode, 'Starting…')}\nUse /status to follow progress.")


@bot.on_message(filters.command("clone") & admin_only)
async def clone_handler(client: Client, message: Message) -> None:
    uid = message.from_user.id
    if RT.ingest_active:
        await reply_html(message, "Ingestion is already running. Use /status or /stop.")
        return
    if not STATE.sources:
        await reply_html(message, "No sources configured. Run /session first.")
        return
    if not STATE.target_links or not STATE.target_media:
        await reply_html(message, "Both targets must be set. Run /session first.")
        return

    previous = [s for s in STATE.sources if str(s) in STATE.checkpoints]
    if previous:
        PENDING_CLONE[uid] = True
        await reply_html(
            message,
            f"<b>{len(previous)}</b> of {len(STATE.sources)} sources already have a checkpoint.\n\n"
            "/force_continue — resume unfinished sources and catch up finished ones\n"
            "/force_duplicate — restart every source from the beginning (duplicates allowed)\n"
            "/skip — only clone sources that were never started",
        )
        return
    await start_manual_clone(message, "auto")


async def _clone_choice(message: Message, mode: str) -> None:
    if not PENDING_CLONE.pop(message.from_user.id, False):
        await reply_html(message, "Nothing pending. Use /clone first.")
        return
    await start_manual_clone(message, mode)


@bot.on_message(filters.command("force_continue") & admin_only)
async def force_continue_handler(client: Client, message: Message) -> None:
    await _clone_choice(message, "continue")


@bot.on_message(filters.command("force_duplicate") & admin_only)
async def force_duplicate_handler(client: Client, message: Message) -> None:
    await _clone_choice(message, "duplicate")


@bot.on_message(filters.command("skip") & admin_only)
async def skip_handler(client: Client, message: Message) -> None:
    await _clone_choice(message, "skip")


# ---- /status --------------------------------------------------------------- #
@bot.on_message(filters.command("status") & admin_only)
async def status_handler(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await reply_html(message, live_status_text())
        return

    chat = parse_chat_arg(message)
    if chat is None:
        await reply_html(message, "I couldn't read that chat. Use an @username, -100… ID or t.me link.")
        return

    status = await reply_html(message, "⏳ Scanning chat content…")
    try:
        await edit_html(status, await chat_report(chat))
    except Exception as exc:  # noqa: BLE001
        log.exception("/status failed")
        await edit_html(status, f"❌ <b>Scan failed</b>\n{friendly_error(exc)}")


# ---- /stop, /resume, /remove ---------------------------------------------- #
@bot.on_message(filters.command("stop") & admin_only)
async def stop_handler(client: Client, message: Message) -> None:
    status = await reply_html(message, "⏳ Stopping…")
    await edit_html(status, await stop_all())


@bot.on_message(filters.command("resume") & admin_only)
async def resume_handler(client: Client, message: Message) -> None:
    RT.stop_event.clear()
    STATE.paused = False
    STATE.save()
    started = schedule_auto_ingestion()
    await reply_html(
        message,
        "▶️ Live forwarding resumed."
        + (" Background ingestion continues from the saved checkpoints." if started or RT.ingest_active else ""),
    )


@bot.on_message(filters.command("remove") & admin_only)
async def remove_handler(client: Client, message: Message) -> None:
    status = await reply_html(message, "⏳ Stopping and resetting…")
    await stop_all()
    STATE.set_sources([])
    STATE.checkpoints = {}
    STATE.save()
    PENDING_CLONE.clear()
    PENDING_SESSION.clear()
    await edit_html(
        status,
        "🗑 Saved sources and checkpoints were reset. Targets, thresholds and the media "
        "de-duplication cache were kept. Forwarding is paused: run /session, then /resume.",
    )


# ---- /cleandup ------------------------------------------------------------- #
@bot.on_message(filters.command("cleandup") & admin_only)
async def cleandup_handler(client: Client, message: Message) -> None:
    if len(message.command) > 1:
        chat = parse_chat_arg(message)
        if chat is None:
            await reply_html(message, "I couldn't read that chat. Use an @username, -100… ID or t.me link.")
            return
        chats: List[ChatRef] = [chat]
    else:
        chats = [c for c in (STATE.target_links, STATE.target_media) if c]
        if not chats:
            await reply_html(message, "No target chats configured. Run /session or pass a chat.")
            return

    status = await reply_html(message, "⏳ Scanning for duplicates…")
    lines: List[str] = []
    for chat in chats:
        try:
            scanned, deleted = await dedupe_chat(chat)
            lines.append(
                f"<code>{html.escape(str(chat))}</code>: scanned <b>{scanned}</b>, "
                f"deleted <b>{deleted}</b> duplicates"
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("/cleandup failed for %s", chat)
            lines.append(f"<code>{html.escape(str(chat))}</code>: ❌ {friendly_error(exc)}")
        await edit_html(status, "⏳ Cleaning…\n" + "\n".join(lines))
    await edit_html(status, "🧹 <b>Duplicate cleanup finished</b>\n\n" + "\n".join(lines))


# ---- runtime thresholds ---------------------------------------------------- #
@bot.on_message(filters.command("set_duration") & admin_only)
async def set_duration_handler(client: Client, message: Message) -> None:
    try:
        value = int(float(message.command[1]))
        if value < 0:
            raise ValueError
    except (IndexError, ValueError):
        await reply_html(message, "Usage: <code>/set_duration &lt;seconds&gt;</code> (e.g. 600)")
        return
    STATE.min_duration = value
    STATE.save()
    await reply_html(message, f"✅ Minimum video duration is now <b>{value}s</b>.")


@bot.on_message(filters.command("set_size") & admin_only)
async def set_size_handler(client: Client, message: Message) -> None:
    try:
        value = float(message.command[1])
        if value < 0:
            raise ValueError
    except (IndexError, ValueError):
        await reply_html(message, "Usage: <code>/set_size &lt;mb&gt;</code> (e.g. 100)")
        return
    STATE.min_size_mb = value
    STATE.save()
    await reply_html(message, f"✅ Minimum file size is now <b>{value:g} MB</b>.")


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #
WEB_RUNNER: Optional["web.AppRunner"] = None


async def _web_ok(request: "web.Request") -> "web.Response":
    return web.Response(text="Bot is running!", status=200)


async def dummy_web_server() -> None:
    """Keep-alive HTTP server so Render's Web Service sees an open PORT."""
    global WEB_RUNNER
    app = web.Application()
    app.router.add_get("/", _web_ok)
    app.router.add_get("/health", _web_ok)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    WEB_RUNNER = runner
    log.info("Keep-alive web server listening on 0.0.0.0:%d", port)


async def bootstrap() -> None:
    """Warm peer caches (one get_dialogs pass per userbot) and import env sources."""
    await asyncio.gather(*(refresh_dialogs(c, force=True) for c in USERBOTS))

    # Env sources that were never imported before (new IDs added to SOURCE_CHATS later
    # are picked up on the next start; sources removed via /session are not re-added).
    new_env = [s for s in config.SOURCE_CHATS if s not in STATE.env_known]
    if new_env:
        STATE.set_sources(
            STATE.sources + [s for s in new_env if s not in config.IGNORED_SOURCE_CHATS]
        )
        STATE.env_known = list(dict.fromkeys(STATE.env_known + new_env))
        STATE.save()
        log.info("Imported %d new source(s) from SOURCE_CHATS", len(new_env))

    for target in (STATE.target_links, STATE.target_media):
        if not target:
            continue
        try:
            await bot.get_chat(target)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Bot cannot resolve target %s yet (%s). Make it admin there; it may resolve "
                "after the first update from that chat.",
                target,
                exc,
            )


async def graceful_shutdown() -> None:
    log.info("Shutting down gracefully…")
    RT.stop_event.set()
    drain_queue()

    if RT.ingest_task is not None and not RT.ingest_task.done():
        _, pending = await asyncio.wait({RT.ingest_task}, timeout=30)
        if pending:
            RT.ingest_task.cancel()
            await asyncio.gather(RT.ingest_task, return_exceptions=True)

    if RT.worker_task is not None and not RT.worker_task.done():
        RT.worker_task.cancel()
        await asyncio.gather(RT.worker_task, return_exceptions=True)

    STATE.save()
    MEDIA_SEEN.save(force=True)
    await asyncio.gather(*(c.stop() for c in USERBOTS), bot.stop(), return_exceptions=True)
    if WEB_RUNNER is not None:
        await asyncio.gather(WEB_RUNNER.cleanup(), return_exceptions=True)
    log.info("Stopped.")


async def main() -> None:
    await dummy_web_server()
    STATE.load()
    MEDIA_SEEN.load()

    # user_1 (+ user_2 when configured) and the bot start concurrently.
    await asyncio.gather(*(c.start() for c in USERBOTS), bot.start())
    RT.worker_task = asyncio.create_task(delivery_worker())
    await bootstrap()

    me_bot = await bot.get_me()
    log.info(
        "Userbots: %d | Bot: @%s | Sources: %d | Ignored channels: %d",
        len(USERBOTS),
        me_bot.username,
        len(STATE.sources),
        len(config.IGNORED_SOURCE_CHATS),
    )

    # NEW_SOURCE channels (no last_read_id yet) are ingested without any /clone command.
    schedule_auto_ingestion()

    try:
        # idle() installs SIGINT / SIGTERM (and SIGABRT) handlers and returns when one
        # arrives; the finally block then performs the graceful shutdown.
        await idle()
    finally:
        await graceful_shutdown()


if __name__ == "__main__":
    try:
        LOOP.run_until_complete(main())
    except KeyboardInterrupt:
        log.info("Interrupted")
