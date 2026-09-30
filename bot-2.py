"""Dual-client Telegram batch forwarder, channel cloner, auto-router and cleaner.

Userbot (SESSION_STRING): listens to / batch-clones source chats where the bot is NOT admin.
Bot (BOT_TOKEN):          delivers to the target channels (no forward tag), deletes
                          duplicates and answers admin commands.

All deliveries go through ONE global FIFO queue with a mandatory pause between items.
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
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union
from urllib.parse import urlsplit

from pyrogram import Client, enums, filters, idle
from pyrogram.errors import (
    BadRequest,
    ChannelPrivate,
    ChatAdminRequired,
    FloodWait,
    MessageDeleteForbidden,
    MessageNotModified,
    PeerIdInvalid,
    RPCError,
    UserNotParticipant,
    UsernameInvalid,
    UsernameNotOccupied,
)
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
# Clients
# --------------------------------------------------------------------------- #
user = Client(
    "forwarder_userbot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    session_string=config.SESSION_STRING,
    in_memory=True,
)

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
    """Runtime settings + clone progress, persisted atomically to a JSON file."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.sources: List[Dict[str, Any]] = []  # {"id": int, "ref": str|int, "title": str}
        self.target_links: int = config.TARGET_LINKS_CHAT
        self.target_media: int = config.TARGET_MEDIA_CHAT
        self.min_duration: int = config.MIN_VIDEO_DURATION
        self.min_size_mb: float = config.MIN_FILE_SIZE_MB
        self.paused: bool = False
        self.progress: Dict[str, Dict[str, Any]] = {}  # str(source id) -> progress

    @property
    def min_size_bytes(self) -> int:
        return int(self.min_size_mb * 1024 * 1024)

    def source_ids(self) -> Set[int]:
        return {s["id"] for s in self.sources}

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

        self.sources = [
            {"id": s["id"], "ref": s.get("ref", s["id"]), "title": str(s.get("title", ""))}
            for s in data.get("sources", [])
            if isinstance(s, dict) and isinstance(s.get("id"), int)
        ]
        self.target_links = _as_int(data.get("target_links"), 0) or self.target_links
        self.target_media = _as_int(data.get("target_media"), 0) or self.target_media
        self.min_duration = _as_int(data.get("min_duration"), self.min_duration)
        self.min_size_mb = _as_float(data.get("min_size_mb"), self.min_size_mb)
        self.paused = bool(data.get("paused", False))
        progress = data.get("progress", {})
        if isinstance(progress, dict):
            self.progress = {
                str(k): {
                    "last_id": _as_int(v.get("last_id"), 0),
                    "top_id": _as_int(v.get("top_id"), 0),
                    "done": bool(v.get("done", False)),
                }
                for k, v in progress.items()
                if isinstance(v, dict)
            }

    def save(self) -> None:
        payload = {
            "sources": self.sources,
            "target_links": self.target_links,
            "target_media": self.target_media,
            "min_duration": self.min_duration,
            "min_size_mb": self.min_size_mb,
            "paused": self.paused,
            "progress": self.progress,
        }
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except OSError as exc:
            log.error("Could not save state: %s", exc)


STATE = State(config.STATE_FILE)


class Runtime:
    """Non-persistent runtime flags."""

    def __init__(self) -> None:
        self.stop_event = asyncio.Event()
        self.clone_task: Optional["asyncio.Task[None]"] = None
        self.worker_task: Optional["asyncio.Task[None]"] = None
        self.current_source: Optional[Dict[str, Any]] = None
        self.stats: Dict[str, int] = {"media_sent": 0, "links_sent": 0, "failed": 0}

    @property
    def cloning(self) -> bool:
        return self.clone_task is not None and not self.clone_task.done()


RT = Runtime()
QUEUE: "asyncio.Queue[Job]" = asyncio.Queue()
PENDING_SESSION: Dict[int, Dict[str, Any]] = {}
PENDING_CLONE: Dict[int, bool] = {}


# --------------------------------------------------------------------------- #
# FloodWait-safe helper + error text
# --------------------------------------------------------------------------- #
async def flood_retry(factory: Callable[[], Awaitable[Any]], label: str = "call") -> Any:
    """Await factory(); on FloodWait sleep e.value + 1 seconds and retry, forever."""
    while True:
        try:
            return await factory()
        except FloodWait as exc:
            wait = int(getattr(exc, "value", 5)) + 1
            log.warning("FloodWait in %s: sleeping %ss then retrying", label, wait)
            await asyncio.sleep(wait)


def friendly_error(exc: BaseException) -> str:
    if isinstance(exc, (ChatAdminRequired, MessageDeleteForbidden)):
        return (
            "Missing permission. Make the bot (or the userbot account) an admin with the "
            "<b>Delete messages</b> / <b>Post messages</b> rights."
        )
    if isinstance(exc, (PeerIdInvalid, ChannelPrivate, KeyError)):
        return (
            "Can't access that chat. Add the bot/userbot to it (the userbot must be a member "
            "to read history; the bot must be admin in targets). For a fresh bot session, "
            "post something in the channel and try again."
        )
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
# Deep link scanner
# --------------------------------------------------------------------------- #
URL_RE = re.compile(r"https?://[^\s<>\"'\[\]\(\)]+", re.IGNORECASE)
TRAILING_JUNK = ".,;:!?)]}>*_`~'\""
SPAM_RE = re.compile("|".join(f"(?:{p})" for p in config.SPAM_PATTERNS), re.IGNORECASE)


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
    """0 for known streaming hosts / TeraBox family, 1 for everything else."""
    host = _host_of(url)
    for domain in config.PRIORITY_DOMAINS:
        if host == domain or host.endswith("." + domain):
            return 0
    for keyword in config.PRIORITY_KEYWORDS:
        if keyword in host:
            return 0
    return 1


def rank_urls(urls: List[str]) -> List[str]:
    return sorted(urls, key=url_priority)  # stable: keeps original order inside a tier


def _entity_text(text: str, offset: int, length: int) -> str:
    """Telegram entity offsets are UTF-16 code units, so slice accordingly."""
    raw = text.encode("utf-16-le")
    return raw[offset * 2 : (offset + length) * 2].decode("utf-16-le", errors="ignore")


def _iter_button_urls(node: Any) -> Iterator[str]:
    """Recursively walk a keyboard (markup -> rows -> buttons) yielding every URL."""
    if node is None:
        return
    if isinstance(node, (list, tuple)):
        for child in node:
            yield from _iter_button_urls(child)
        return
    rows = getattr(node, "inline_keyboard", None)
    if rows is not None:
        yield from _iter_button_urls(rows)
        return
    url = getattr(node, "url", None)
    if isinstance(url, str) and url:
        yield url
    for attr in ("login_url", "web_app"):
        nested = getattr(getattr(node, attr, None), "url", None)
        if isinstance(nested, str) and nested:
            yield nested


def extract_valid_urls(message: Message) -> List[str]:
    """Deep-scan text/caption, entities and inline keyboard for external URLs."""
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
    candidates.extend(_iter_button_urls(message.reply_markup))

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


def clean_description(raw: str) -> str:
    """Drop spam lines such as 'Just Click Blue Link' / 'Movie Uploading...'."""
    kept = [line.rstrip() for line in (raw or "").splitlines() if not SPAM_RE.search(line)]
    text = "\n".join(kept)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


# --------------------------------------------------------------------------- #
# Jobs and routing decision
# --------------------------------------------------------------------------- #
@dataclass
class Job:
    kind: str  # "media" | "links"
    source_id: int
    message_id: int = 0
    text: str = ""
    urls: List[str] = field(default_factory=list)
    future: Optional["asyncio.Future[str]"] = None


def media_qualifies(message: Message) -> bool:
    if message.video:
        return (message.video.duration or 0) >= STATE.min_duration
    if message.document:
        return (message.document.file_size or 0) >= STATE.min_size_bytes
    return False


def build_links_text(raw_text: str, urls: List[str]) -> str:
    bullets = "\n".join(f"• {html.escape(u)}" for u in urls[:20])
    block = f"🔗 <b>Links:</b>\n{bullets}"
    description = clean_description(raw_text)
    room = max(0, min(3000, 4000 - len(block) - 4))
    description = html.escape(description[: room // 2]) if description else ""
    text = f"{description}\n\n{block}" if description else block
    return text[:4096]


def build_job(message: Message) -> Optional[Job]:
    """Decide where a message goes. None means: drop it."""
    if message.empty or message.service or not message.chat:
        return None

    if media_qualifies(message):
        return Job(kind="media", source_id=message.chat.id, message_id=message.id)

    urls = rank_urls(extract_valid_urls(message))
    if not urls:
        return None  # pure text / spam / only ignored domains

    raw = message.text or message.caption or ""
    return Job(
        kind="links",
        source_id=message.chat.id,
        message_id=message.id,
        text=build_links_text(str(raw), urls),
        urls=urls[: config.MAX_BUTTONS],
    )


def enqueue(job: Job, wait: bool = False) -> None:
    if wait:
        job.future = asyncio.get_running_loop().create_future()
    QUEUE.put_nowait(job)


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
    try:
        await flood_retry(
            lambda: bot.copy_message(
                chat_id=target, from_chat_id=job.source_id, message_id=job.message_id
            ),
            "bot.copy_message",
        )
    except Exception as exc:  # noqa: BLE001
        # The bot can't see channels it hasn't joined; the userbot performs the same
        # server-side copy (still no "Forwarded from" header).
        log.info("bot.copy_message failed (%s); falling back to userbot", exc)
        await flood_retry(
            lambda: user.copy_message(
                chat_id=target, from_chat_id=job.source_id, message_id=job.message_id
            ),
            "user.copy_message",
        )


async def deliver_links(job: Job) -> None:
    target = STATE.target_links
    if not target:
        raise RuntimeError("TARGET_LINKS_CHAT is not configured")

    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"🔗 Open Link {i}", url=u)]
            for i, u in enumerate(job.urls[: config.MAX_BUTTONS], start=1)
        ]
    )

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
        except asyncio.CancelledError:
            if job.future is not None and not job.future.done():
                job.future.set_result("cancelled")
            QUEUE.task_done()
            raise
        except Exception:  # noqa: BLE001
            RT.stats["failed"] += 1
            log.exception("Delivery failed (%s %s/%s)", job.kind, job.source_id, job.message_id)
        if job.future is not None and not job.future.done():
            job.future.set_result(outcome)
        QUEUE.task_done()
        await asyncio.sleep(config.DELIVERY_DELAY)


# --------------------------------------------------------------------------- #
# Live listener (userbot)
# --------------------------------------------------------------------------- #
async def _source_filter(_, __, message: Message) -> bool:
    chat = message.chat
    return bool(chat) and chat.id in STATE.source_ids()


source_filter = filters.create(_source_filter)


@user.on_message(source_filter & ~filters.service)
async def on_source_message(client: Client, message: Message) -> None:
    try:
        if STATE.paused:
            return
        job = build_job(message)
        if job is None:
            return
        enqueue(job)
        # Keep clone progress accurate for sources that are fully cloned.
        prog = STATE.progress.get(str(message.chat.id))
        if prog and prog.get("done"):
            prog["last_id"] = max(prog.get("last_id", 0), message.id)
    except Exception:  # noqa: BLE001
        log.exception("Error while processing live message")


# --------------------------------------------------------------------------- #
# Batch clone
# --------------------------------------------------------------------------- #
async def _top_message_id(chat_id: int) -> int:
    async def _fetch() -> int:
        top = 0
        async for msg in user.get_chat_history(chat_id, limit=1):
            top = msg.id
        return top

    return await flood_retry(_fetch, "get_chat_history(top)")


async def clone_worker(mode: str, notify_chat: int) -> None:
    """mode: 'fresh' | 'continue' | 'duplicate' | 'skip'."""
    RT.stop_event.clear()
    STATE.paused = False
    STATE.save()

    lines: List[str] = []
    aborted = ""
    try:
        for src in list(STATE.sources):
            if RT.stop_event.is_set():
                break
            sid = src["id"]
            key = str(sid)
            title = html.escape(src.get("title") or str(sid))
            prev = STATE.progress.get(key)

            if mode == "skip" and prev and prev.get("last_id", 0) > 0:
                lines.append(f"⏭ {title}: skipped (already cloned)")
                continue
            if mode == "duplicate":
                prev = None

            try:
                top = await _top_message_id(sid)
            except Exception as exc:  # noqa: BLE001
                lines.append(f"❌ {title}: {friendly_error(exc)}")
                continue

            prog = prev or {"last_id": 0, "top_id": top, "done": False}
            prog["top_id"] = top
            prog["done"] = False
            STATE.progress[key] = prog
            RT.current_source = src

            cursor = prog["last_id"] + 1
            sent = dropped = failed = consecutive_failures = handled = 0

            while cursor <= top and not RT.stop_event.is_set() and not aborted:
                ids = list(range(cursor, min(cursor + config.CLONE_BATCH_SIZE, top + 1)))
                try:
                    fetched = await flood_retry(
                        lambda: user.get_messages(sid, ids), "get_messages"
                    )
                except Exception as exc:  # noqa: BLE001
                    aborted = f"{title}: {friendly_error(exc)}"
                    break
                if not isinstance(fetched, list):
                    fetched = [fetched]
                by_id = {m.id: m for m in fetched if m is not None and not m.empty}

                for mid in ids:
                    if RT.stop_event.is_set():
                        break
                    msg = by_id.get(mid)
                    job = build_job(msg) if msg is not None else None
                    if job is None:
                        dropped += 1
                    else:
                        enqueue(job, wait=True)
                        assert job.future is not None
                        result = await job.future
                        if result == "cancelled":
                            break  # not delivered: do not advance progress
                        if result == "ok":
                            sent += 1
                            consecutive_failures = 0
                        else:
                            failed += 1
                            consecutive_failures += 1
                            if consecutive_failures >= config.MAX_CONSECUTIVE_FAILURES:
                                aborted = (
                                    f"{title}: {consecutive_failures} deliveries failed in a row "
                                    "(check the bot's admin rights in the targets)"
                                )
                    prog["last_id"] = mid
                    handled += 1
                    if handled % 10 == 0:
                        STATE.save()
                    if aborted:
                        break
                else:
                    cursor = ids[-1] + 1
                    STATE.save()
                    continue
                break  # inner loop was broken (stop / cancelled / aborted)

            if cursor > top and not RT.stop_event.is_set() and not aborted:
                prog["done"] = True
                lines.append(
                    f"✅ {title}: done — {sent} delivered, {dropped} dropped, {failed} failed"
                )
            else:
                lines.append(
                    f"⏸ {title}: stopped at id {prog['last_id']}/{top} — "
                    f"{sent} delivered, {failed} failed"
                )
            STATE.save()
            if aborted:
                break
    except asyncio.CancelledError:
        lines.append("⚠️ Clone cancelled")
        raise
    finally:
        RT.current_source = None
        STATE.save()
        summary = "<b>Clone finished</b>\n" if not (aborted or RT.stop_event.is_set()) else "<b>Clone halted</b>\n"
        if aborted:
            summary += f"⚠️ {aborted}\n"
        summary += "\n".join(lines) if lines else "Nothing to do."
        try:
            await bot.send_message(notify_chat, summary[:4096], parse_mode=HTML, **NO_PREVIEW)
        except Exception:  # noqa: BLE001
            log.exception("Could not send clone summary")


async def stop_all(timeout: float = 45.0) -> str:
    """Halt cloning + live forwarding. Progress is saved, queued items are discarded."""
    was_cloning = RT.cloning
    STATE.paused = True
    STATE.save()
    RT.stop_event.set()
    drained = drain_queue()

    if RT.clone_task is not None and not RT.clone_task.done():
        done, pending = await asyncio.wait({RT.clone_task}, timeout=timeout)
        if pending:
            RT.clone_task.cancel()
            await asyncio.gather(RT.clone_task, return_exceptions=True)
    STATE.save()
    parts = ["⏹ Live forwarding paused"]
    if was_cloning:
        parts.append("batch clone halted (progress saved)")
    parts.append(f"{drained} queued item(s) discarded")
    return ", ".join(parts) + ".\nUse /clone or /resume to continue."


# --------------------------------------------------------------------------- #
# History access shared by /cleandup and /status
# --------------------------------------------------------------------------- #
async def fetch_history(chat: ChatRef, limit: int) -> List[Message]:
    """Fetch recent history (newest first).

    Bot accounts normally can't read history (BOT_METHOD_INVALID), so the bot is
    tried first and the userbot is the automatic fallback.
    """
    last_exc: Optional[BaseException] = None
    for client in (bot, user):
        try:
            async def _collect(c: Client = client) -> List[Message]:
                out: List[Message] = []
                async for msg in c.get_chat_history(chat, limit=limit):
                    out.append(msg)
                return out

            return await flood_retry(_collect, "get_chat_history")
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.info("get_chat_history via %s failed: %s", client.name, exc)
    assert last_exc is not None
    raise last_exc


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
    """Delete via the bot (FloodWait-safe); fall back to the userbot on other errors."""
    try:
        result = await flood_retry(lambda: bot.delete_messages(chat, ids), "bot.delete_messages")
    except Exception as bot_exc:  # noqa: BLE001
        log.info("bot.delete_messages failed (%s); trying userbot", bot_exc)
        try:
            result = await flood_retry(
                lambda: user.delete_messages(chat, ids), "user.delete_messages"
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
# Session validation
# --------------------------------------------------------------------------- #
async def refresh_user_dialogs() -> None:
    try:
        async for _ in user.get_dialogs():
            pass
    except Exception as exc:  # noqa: BLE001
        log.warning("Dialog refresh failed: %s", exc)


async def resolve_source(ref: ChatRef) -> Any:
    for attempt in (1, 2):
        try:
            return await flood_retry(lambda: user.get_chat(ref), "user.get_chat")
        except (PeerIdInvalid, KeyError, ValueError):
            if attempt == 2:
                raise
            await refresh_user_dialogs()


async def check_source(ref: ChatRef) -> Tuple[Optional[Dict[str, Any]], str]:
    try:
        chat = await resolve_source(ref)
    except Exception as exc:  # noqa: BLE001
        return None, f"❌ <code>{html.escape(str(ref))}</code>: {friendly_error(exc)}"
    title = chat.title or chat.first_name or str(chat.id)
    note = ""
    try:
        await flood_retry(lambda: user.get_chat_member(chat.id, "me"), "get_chat_member")
    except UserNotParticipant:
        note = " ⚠️ userbot is not a member — join it to receive live posts"
    except Exception:  # noqa: BLE001
        pass
    entry = {"id": chat.id, "ref": ref, "title": title}
    return entry, f"✅ {html.escape(title)} (<code>{chat.id}</code>){note}"


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

    new_sources: Optional[List[Dict[str, Any]]] = None
    if data.get("sources") is not None:
        new_sources = []
        for ref in data["sources"]:
            entry, line = await check_source(ref)
            report.append(line)
            if entry is None:
                ok = False
            else:
                new_sources.append(entry)

    new_links = data.get("links")
    new_media = data.get("media")
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

    if new_sources is not None:
        keep = {s["id"] for s in new_sources}
        STATE.progress = {k: v for k, v in STATE.progress.items() if _as_int(k) in keep}
        STATE.sources = new_sources
    if new_links is not None:
        STATE.target_links = new_links
    if new_media is not None:
        STATE.target_media = new_media
    STATE.save()
    return True, "<b>Session saved</b>\n" + "\n".join(report)


# --------------------------------------------------------------------------- #
# Admin commands
# --------------------------------------------------------------------------- #
admin_only = filters.user(config.ADMINS)


def describe_sources() -> str:
    if not STATE.sources:
        return "—"
    return "\n".join(
        f"• {html.escape(s.get('title') or '')} (<code>{s['id']}</code>)" for s in STATE.sources
    )


def live_status_text() -> str:
    state = "⏸ paused" if STATE.paused else "▶️ running"
    if RT.cloning and RT.current_source:
        prog = STATE.progress.get(str(RT.current_source["id"]), {})
        clone = (
            f"running — {html.escape(RT.current_source.get('title') or '')} "
            f"({prog.get('last_id', 0)}/{prog.get('top_id', 0)})"
        )
    else:
        clone = "idle"
    progress_lines = []
    for src in STATE.sources:
        prog = STATE.progress.get(str(src["id"]))
        if prog:
            flag = "done" if prog.get("done") else "partial"
            progress_lines.append(
                f"• {html.escape(src.get('title') or str(src['id']))}: "
                f"{prog.get('last_id', 0)}/{prog.get('top_id', 0)} ({flag})"
            )
    return (
        "📡 <b>Live status</b>\n\n"
        f"• Forwarding: <b>{state}</b>\n"
        f"• Batch clone: <b>{clone}</b>\n"
        f"• Queue length: <b>{QUEUE.qsize()}</b>\n"
        f"• Links target: <code>{STATE.target_links or '—'}</code>\n"
        f"• Media target: <code>{STATE.target_media or '—'}</code>\n"
        f"• Min video: <b>{STATE.min_duration}s</b> | Min file: <b>{STATE.min_size_mb:g} MB</b>\n"
        f"• Delivered: media <b>{RT.stats['media_sent']}</b>, links <b>{RT.stats['links_sent']}</b>, "
        f"failed <b>{RT.stats['failed']}</b>\n\n"
        f"<b>Active sources ({len(STATE.sources)}):</b>\n{describe_sources()}"
        + (("\n\n<b>Clone progress:</b>\n" + "\n".join(progress_lines)) if progress_lines else "")
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
        "/session — set sources + target channels\n"
        "/clone — batch-clone history from the sources\n"
        "/status [chat] — live status, or scan a chat\n"
        "/stop — halt cloning and live forwarding\n"
        "/resume — resume live forwarding\n"
        "/remove — clear saved sources and clone progress\n"
        "/cleandup [chat] — delete duplicates (default: both targets)\n"
        "/set_duration &lt;seconds&gt; — min video duration\n"
        "/set_size &lt;mb&gt; — min file size",
    )


# ---- /session (interactive) ------------------------------------------------ #
@bot.on_message(filters.command("session") & admin_only)
async def session_handler(client: Client, message: Message) -> None:
    if RT.cloning:
        await reply_html(message, "A batch clone is running. Use /stop first.")
        return
    PENDING_SESSION[message.from_user.id] = {"step": "sources", "data": {}}
    await reply_html(
        message,
        "🛠 <b>Session setup (1/3)</b>\n"
        "Send the source channels/groups (space-separated <code>@username</code> or "
        "<code>-100…</code> IDs). This <b>replaces</b> the current list.\n"
        "Send <code>keep</code> to leave it unchanged or <code>cancel</code> to abort.\n\n"
        f"<b>Current sources:</b>\n{describe_sources()}",
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
        if lowered == "keep":
            data["sources"] = None
        else:
            refs = config.parse_chat_list(text)
            if not refs:
                await reply_html(message, "I couldn't read any chat there. Try again, or send <code>keep</code> / <code>cancel</code>.")
                return
            data["sources"] = refs
        pending["step"] = "links"
        await reply_html(
            message,
            "🛠 <b>Session setup (2/3)</b>\nSend <b>TARGET_LINKS_CHAT</b> (numeric ID like "
            f"<code>-100…</code>) or <code>keep</code>.\nCurrent: <code>{STATE.target_links or '—'}</code>",
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
        status = await reply_html(message, "🔎 Validating access…")
        try:
            ok, report = await apply_session(data)
        except Exception as exc:  # noqa: BLE001
            log.exception("apply_session failed")
            ok, report = False, f"Validation crashed: {friendly_error(exc)}"
        await edit_html(status, report)
        return


# ---- /clone ---------------------------------------------------------------- #
async def start_clone(message: Message, mode: str) -> None:
    if RT.cloning:
        await reply_html(message, "A batch clone is already running. Use /stop to halt it.")
        return
    RT.clone_task = asyncio.create_task(clone_worker(mode, message.chat.id))
    labels = {
        "fresh": "Starting batch clone…",
        "continue": "Resuming batch clone from saved progress…",
        "duplicate": "Re-cloning everything from the start (duplicates allowed)…",
        "skip": "Cloning only sources that were never cloned…",
    }
    await reply_html(message, f"▶️ {labels.get(mode, 'Starting…')}\nUse /status to follow progress.")


@bot.on_message(filters.command("clone") & admin_only)
async def clone_handler(client: Client, message: Message) -> None:
    uid = message.from_user.id
    if RT.cloning:
        await reply_html(message, "A batch clone is already running. Use /status or /stop.")
        return
    if not STATE.sources:
        await reply_html(message, "No sources configured. Run /session first.")
        return
    if not STATE.target_links or not STATE.target_media:
        await reply_html(message, "Both targets must be set. Run /session first.")
        return

    previous = [
        s for s in STATE.sources if STATE.progress.get(str(s["id"]), {}).get("last_id", 0) > 0
    ]
    if previous:
        PENDING_CLONE[uid] = True
        names = "\n".join(f"• {html.escape(s.get('title') or str(s['id']))}" for s in previous)
        await reply_html(
            message,
            "These sources were already cloned (at least partly):\n"
            f"{names}\n\nChoose:\n"
            "/force_continue — resume from the saved position\n"
            "/force_duplicate — restart from the beginning (creates duplicates)\n"
            "/skip — only clone sources that were never cloned",
        )
        return
    await start_clone(message, "fresh")


async def _clone_choice(message: Message, mode: str) -> None:
    uid = message.from_user.id
    if not PENDING_CLONE.pop(uid, False):
        await reply_html(message, "Nothing pending. Use /clone first.")
        return
    await start_clone(message, mode)


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
    if RT.cloning:
        await reply_html(message, "Already running.")
        return
    RT.stop_event.clear()
    STATE.paused = False
    STATE.save()
    await reply_html(message, "▶️ Live forwarding resumed.")


@bot.on_message(filters.command("remove") & admin_only)
async def remove_handler(client: Client, message: Message) -> None:
    status = await reply_html(message, "⏳ Stopping and clearing…")
    await stop_all()
    STATE.sources = []
    STATE.progress = {}
    STATE.save()
    PENDING_CLONE.clear()
    PENDING_SESSION.clear()
    await edit_html(
        status,
        "🗑 Saved sources and clone progress cleared. Targets and thresholds were kept.\n"
        "Run /session to configure new sources.",
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
async def bootstrap() -> None:
    """Populate peer caches and seed sources from env on the very first run."""
    await refresh_user_dialogs()

    if not STATE.sources and config.SOURCE_CHATS:
        for ref in config.SOURCE_CHATS:
            entry, line = await check_source(ref)
            if entry is not None:
                STATE.sources.append(entry)
            else:
                log.warning("Env source %s skipped: %s", ref, line)
        STATE.save()

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

    if RT.clone_task is not None and not RT.clone_task.done():
        _, pending = await asyncio.wait({RT.clone_task}, timeout=30)
        if pending:
            RT.clone_task.cancel()
            await asyncio.gather(RT.clone_task, return_exceptions=True)

    if RT.worker_task is not None and not RT.worker_task.done():
        RT.worker_task.cancel()
        await asyncio.gather(RT.worker_task, return_exceptions=True)

    STATE.save()
    await asyncio.gather(user.stop(), bot.stop(), return_exceptions=True)
    log.info("Stopped.")


async def main() -> None:
    STATE.load()
    await asyncio.gather(user.start(), bot.start())
    RT.worker_task = asyncio.create_task(delivery_worker())
    await bootstrap()

    me_user = await user.get_me()
    me_bot = await bot.get_me()
    log.info(
        "Userbot: %s | Bot: @%s | Sources: %d | Queue worker running",
        me_user.first_name,
        me_bot.username,
        len(STATE.sources),
    )

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
