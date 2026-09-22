"""
Ultimate Batch Forwarder & Channel Cloner Bot
=============================================

Userbot (Pyrofork / Pyrogram v2) that copies messages from one or more
source chats into a single target chat with Message.copy() (no "Forwarded
from" header), one source after another in a queue, with per-(target,
source) pair duplicate protection, FloodWait handling, a live progress
dashboard and a keep-alive aiohttp server for 24/7 hosting.

Commands (you, or anyone listed in OWNER_IDS):
    /session          interactive setup: one or more sources, then a target
    /clone            start / resume the queue
    /status           configuration, queue state and live progress
    /stop             stop gracefully after the current message (resumable)
    /remove           delete the saved session (source/target queue)

    While a duplicate pair is detected, respond with:
    /force_continue   resume that source from last_scanned_id + 1
    /force_duplicate  ignore history, copy that source from message 1 again
    /skip             skip that source and move to the next one in the queue

Environment variables:
    API_ID, API_HASH     required (my.telegram.org)
    SESSION_STRING       required: a Pyrogram user-account session string
    OWNER_IDS            optional comma/space separated extra user IDs
                          allowed to command the bot (your own account,
                          filters.me, is always allowed)
    PORT                 web server port (default 8080)
    STATE_FILE           session/queue state path (default clone_state.json)
    HISTORY_FILE         pair duplicate-protection store (default history.json)
    MSG_DELAY            seconds between copies (default 1.2)
    DASHBOARD_INTERVAL   seconds between progress edits (default 15)
"""

import asyncio
import html
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ChatType, ParseMode
from pyrogram.errors import FloodWait, MessageNotModified, PeerIdInvalid, RPCError
from pyrogram.handlers import MessageHandler

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
log = logging.getLogger("cloner")


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


API_ID = int(_require("API_ID"))
API_HASH = _require("API_HASH")
SESSION_STRING = _require("SESSION_STRING")

OWNER_IDS: List[int] = [
    int(x) for x in re.split(r"[\s,]+", os.environ.get("OWNER_IDS", "").strip()) if x
]

PORT = int(os.environ.get("PORT", "8080"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "clone_state.json"))
HISTORY_FILE = Path(os.environ.get("HISTORY_FILE", "history.json"))
BATCH_SIZE = 100
MSG_DELAY = float(os.environ.get("MSG_DELAY", "1.2"))
BATCH_COOLDOWN = (2.0, 3.0)
DASHBOARD_INTERVAL = int(os.environ.get("DASHBOARD_INTERVAL", "15"))
MAX_QUEUE_LINES = 15

COMMANDS = [
    "start", "help", "session", "clone", "stop", "remove", "status",
    "force_continue", "force_duplicate", "skip",
]

# Errors that will never succeed on retry: abort the current source, keep progress.
FATAL_IDS = {
    "CHAT_WRITE_FORBIDDEN",
    "CHAT_ADMIN_REQUIRED",
    "CHAT_RESTRICTED",
    "CHAT_SEND_PLAIN_FORBIDDEN",
    "CHANNEL_PRIVATE",
    "CHANNEL_INVALID",
    "USER_BANNED_IN_CHANNEL",
    "PEER_ID_INVALID",
    "CHAT_FORWARDS_RESTRICTED",
}

esc = html.escape


class Stopped(Exception):
    """Raised when /stop was requested while waiting or working."""


class Fatal(Exception):
    """Unrecoverable error for the current source; the queue position is preserved."""


# --------------------------------------------------------------------------- #
# Persistent session / queue state
# --------------------------------------------------------------------------- #


@dataclass
class Session:
    target_id: int = 0
    target_title: str = ""
    sources: List[Dict[str, Any]] = field(default_factory=list)  # [{"id":..,"title":..}]
    queue_index: int = 0

    # progress for the source currently being processed (sources[queue_index])
    cur_source_id: int = 0
    cur_source_title: str = ""
    in_progress: bool = False          # True once range_start/next_id are live for cur_source_id
    range_start: int = 0
    range_end: int = 0
    next_id: int = 0
    copied: int = 0
    skipped: int = 0
    failed: int = 0
    done_groups: List[str] = field(default_factory=list)
    hist_baseline_copied: int = 0      # total_copied already on record before this run started

    status: str = "idle"   # idle | running | awaiting_decision | stopped | completed | error
    last_error: str = ""
    elapsed_before: float = 0.0

    @property
    def configured(self) -> bool:
        return bool(self.target_id and self.sources)


def load_state() -> Session:
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            known = {k: v for k, v in data.items() if k in Session.__dataclass_fields__}
            sess = Session(**known)
            if sess.status in ("running", "awaiting_decision"):  # died mid-way: resumable
                sess.status = "stopped"
            return sess
        except Exception:
            log.exception("Could not read %s, starting with an empty session", STATE_FILE)
    return Session()


def save_state() -> None:
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(S), indent=2), encoding="utf-8")
        os.replace(tmp, STATE_FILE)
    except Exception:
        log.exception("Could not save state")


def reset_session() -> None:
    for key, value in asdict(Session()).items():
        setattr(S, key, value)
    try:
        STATE_FILE.unlink()
    except FileNotFoundError:
        pass


def load_history() -> Dict[str, Dict[str, int]]:
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            log.exception("Could not read %s, starting with empty history", HISTORY_FILE)
    return {}


def save_history() -> None:
    try:
        tmp = HISTORY_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(HIST, indent=2), encoding="utf-8")
        os.replace(tmp, HISTORY_FILE)
    except Exception:
        log.exception("Could not save history")


def hist_key(target_id: int, source_id: int) -> str:
    return f"{target_id}:{source_id}"


S = load_state()
HIST: Dict[str, Dict[str, int]] = load_history()


class Runtime:
    """Non-persistent, per-process state."""

    def __init__(self) -> None:
        self.task: Optional[asyncio.Task] = None
        self.stop_event: Optional[asyncio.Event] = None
        self.decision_event: Optional[asyncio.Event] = None
        self.decision_value: Optional[str] = None
        self.awaiting_decision: bool = False
        self.run_started: float = 0.0
        self.source_started: float = 0.0
        self.source_first_id: int = 0
        self.report_chat: int = 0
        self.pending: Dict[int, Dict[str, Any]] = {}   # chat_id -> /session wizard step
        self.sent_ids: set = set()                     # (chat_id, msg_id) we sent
        self.dialogs_warmed: bool = False

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()


RT = Runtime()

# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #


def err_id(e: Exception) -> str:
    return str(getattr(e, "ID", "") or e.__class__.__name__)


def err_text(e: Exception) -> str:
    return esc(str(e))[:300]


def fmt_dur(seconds: float) -> str:
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def progress_bar(pct: float, width: int = 12) -> str:
    filled = int(width * max(0.0, min(100.0, pct)) / 100)
    return "█" * filled + "░" * (width - filled)


def title_of(chat) -> str:
    return (
        chat.title
        or " ".join(x for x in (chat.first_name, chat.last_name) if x)
        or str(chat.id)
    )


_USERNAME = r"[A-Za-z][A-Za-z0-9_]{3,31}"


def parse_chat_ref(text: str) -> Optional[Union[int, str]]:
    """@username, username, -100... id, or a t.me link -> int id / username."""
    t = text.strip()
    if re.fullmatch(r"-?\d{5,}", t):
        return int(t)
    m = re.fullmatch(r"(?:https?://)?(?:www\.)?(?:t|telegram)\.me/c/(\d+)(?:/\d+)*/?", t, re.I)
    if m:
        return int("-100" + m.group(1))
    m = re.fullmatch(
        rf"(?:https?://)?(?:www\.)?(?:t|telegram)\.me/({_USERNAME})(?:/\d+)?/?", t, re.I
    )
    if m:
        return m.group(1)
    m = re.fullmatch(rf"@?({_USERNAME})", t)
    if m:
        return m.group(1)
    return None


def split_refs(text: str) -> List[str]:
    """Split a "sources" message on commas/whitespace into individual tokens."""
    return [tok for tok in re.split(r"[,\s]+", text.strip()) if tok]


async def nap(seconds: float) -> bool:
    """Sleep, but wake early if /stop is requested. Returns False if stopped."""
    try:
        await asyncio.wait_for(RT.stop_event.wait(), timeout=seconds)
        return False
    except asyncio.TimeoutError:
        return True


async def call(factory: Callable[[], Awaitable[Any]]) -> Any:
    """Run a Telegram request with FloodWait handling: sleep value+2s, retry the same request."""
    net_tries = 0
    while True:
        try:
            return await factory()
        except FloodWait as e:
            wait = int(e.value) + 2
            log.warning("FloodWait: sleeping %ss before retrying", wait)
            if not await nap(wait):
                raise Stopped()
        except RPCError as e:
            eid = err_id(e)
            wait = int(getattr(e, "value", 0) or 0)
            if eid.startswith(("SLOWMODE_WAIT", "FLOOD_")) and wait:
                log.warning("%s: sleeping %ss before retrying", eid, wait + 2)
                if not await nap(wait + 2):
                    raise Stopped()
                continue
            raise
        except (asyncio.TimeoutError, ConnectionError, OSError) as e:
            net_tries += 1
            if net_tries > 5:
                raise
            log.warning("Network error (%s), retry %s/5", e, net_tries)
            if not await nap(5 * net_tries):
                raise Stopped()


async def send(client: Client, chat_id: int, text: str):
    try:
        sent = await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        sent = await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    RT.sent_ids.add((sent.chat.id, sent.id))
    if len(RT.sent_ids) > 1000:
        RT.sent_ids = set(list(RT.sent_ids)[-500:])
    return sent


async def safe_edit(msg, text: str) -> None:
    try:
        await msg.edit_text(text, parse_mode=ParseMode.HTML)
    except MessageNotModified:
        pass
    except FloodWait as e:
        if e.value <= 60:
            await asyncio.sleep(e.value + 1)
    except RPCError as e:
        log.debug("Dashboard edit failed: %s", e)


async def warm_dialogs(client: Client) -> None:
    RT.dialogs_warmed = True
    try:
        async for _ in client.get_dialogs():
            pass
    except Exception as e:
        log.warning("Could not load dialogs: %s", e)


async def resolve_chat(client: Client, ref: Union[int, str]):
    for attempt in (1, 2):
        try:
            return await call(lambda: client.get_chat(ref))
        except (PeerIdInvalid, KeyError, ValueError):
            if attempt == 2 or RT.dialogs_warmed:
                raise
            await warm_dialogs(client)


async def check_target_permissions(client: Client, chat) -> "tuple[bool, str]":
    try:
        member = await call(lambda: client.get_chat_member(chat.id, "me"))
    except RPCError as e:
        return False, f"Could not read my membership in the target: {err_text(e)}"
    if member.status == ChatMemberStatus.OWNER:
        return True, ""
    if member.status == ChatMemberStatus.ADMINISTRATOR:
        priv = member.privileges
        if chat.type == ChatType.CHANNEL and not (priv and priv.can_post_messages):
            return False, "I'm an admin in the target channel but lack the <b>Post Messages</b> permission."
        return True, ""
    if chat.type == ChatType.CHANNEL:
        return False, "I must be an <b>admin with Post Messages</b> permission in the target channel."
    if member.status == ChatMemberStatus.MEMBER:
        return True, ""
    return False, "I can't post in the target chat (restricted or not a member)."


async def get_last_message_id(client: Client, chat_id: int) -> int:
    msgs = await call(lambda: _first_history_page(client, chat_id))
    return msgs[0].id if msgs else 0


async def _first_history_page(client: Client, chat_id: int):
    return [m async for m in client.get_chat_history(chat_id, limit=1)]


# --------------------------------------------------------------------------- #
# Status / dashboard rendering
# --------------------------------------------------------------------------- #

STATE_LABELS = {
    "idle": ("💤", "Ready"),
    "running": ("🔄", "Running"),
    "awaiting_decision": ("⚠️", "Waiting for your decision"),
    "stopped": ("⏸", "Stopped (resumable)"),
    "completed": ("✅", "Queue completed"),
    "error": ("🛑", "Stopped by error (resumable)"),
}


def elapsed_now() -> float:
    elapsed = S.elapsed_before
    if S.status == "running" and RT.run_started:
        elapsed += time.monotonic() - RT.source_started
    return elapsed


def render_queue_lines() -> List[str]:
    total = len(S.sources)
    lines: List[str] = []
    shown = S.sources
    truncated = False
    if total > MAX_QUEUE_LINES:
        # always show a window around the current index
        start = max(0, S.queue_index - 3)
        end = min(total, start + MAX_QUEUE_LINES)
        start = max(0, end - MAX_QUEUE_LINES)
        shown = list(enumerate(S.sources))[start:end]
        truncated = True
    else:
        shown = list(enumerate(S.sources))
    for i, src in shown:
        if i < S.queue_index:
            marker = "✅"
        elif i == S.queue_index:
            marker = "▶️"
        else:
            marker = "⏳"
        lines.append(f"{marker} {esc(src['title'])} (<code>{src['id']}</code>)")
    if truncated:
        lines.append(f"… {total} sources total")
    return lines


def render_status() -> str:
    if not S.configured:
        return "📭 <b>No session configured.</b>\nSend /session to set up sources and a target."

    icon, label = STATE_LABELS.get(S.status, ("ℹ️", S.status))
    lines = [
        f"{icon} <b>Clone status: {label}</b>",
        "",
        f"🎯 <b>Target:</b> {esc(S.target_title)} (<code>{S.target_id}</code>)",
        f"📚 <b>Queue:</b> {min(S.queue_index + 1, len(S.sources))}/{len(S.sources)}",
        *render_queue_lines(),
        "",
    ]

    if S.queue_index >= len(S.sources):
        lines.append("All sources in the queue have been processed.")
        return "\n".join(lines)

    if not S.in_progress:
        lines.append(f"Next up: <b>{esc(S.cur_source_title or S.sources[S.queue_index]['title'])}</b>")
        if S.status != "awaiting_decision":
            lines.append("Send /clone to continue.")
        return "\n".join(lines)

    total = max(0, S.range_end - S.range_start + 1)
    processed = min(total, max(0, S.next_id - S.range_start))
    pct = (processed / total * 100) if total else 100.0
    lines += [
        f"<b>Current source:</b> {esc(S.cur_source_title)} (<code>{S.cur_source_id}</code>)",
        f"<code>[{progress_bar(pct)}] {pct:.1f}%</code>",
        f"🔎 Scanned: <b>{processed:,}</b> / <b>{total:,}</b>  (IDs {S.range_start:,}–{S.range_end:,})",
        f"✅ Copied: <b>{S.copied:,}</b> (total on record: <b>{S.hist_baseline_copied + S.copied:,}</b>)",
        f"⏭ Skipped/deleted: <b>{S.skipped:,}</b>",
        f"❌ Failed: <b>{S.failed:,}</b>",
        f"➡️ Next message ID: <code>{S.next_id}</code>",
        "",
        f"⏱ Elapsed (this source): <b>{fmt_dur(elapsed_now())}</b>",
    ]
    if RT.running and S.status == "running":
        run_done = S.next_id - RT.source_first_id
        run_time = time.monotonic() - RT.source_started
        if run_done > 0 and run_time > 0:
            rate = run_done / run_time
            eta = (total - processed) / rate if rate else 0
            lines.append(f"⏳ ETA: <b>{fmt_dur(eta)}</b>  ({rate * 60:.0f} msg/min)")
        else:
            lines.append("⏳ ETA: calculating…")
    if S.last_error:
        lines += ["", f"⚠️ Last error: <code>{esc(S.last_error[:300])}</code>"]
    return "\n".join(lines)


async def dashboard_loop(progress_msg, done: asyncio.Event) -> None:
    while True:
        try:
            await asyncio.wait_for(done.wait(), timeout=DASHBOARD_INTERVAL)
            return
        except asyncio.TimeoutError:
            await safe_edit(progress_msg, render_status())


# --------------------------------------------------------------------------- #
# Duplicate-protection decision
# --------------------------------------------------------------------------- #


async def wait_for_decision() -> Optional[str]:
    """Block until a /force_continue, /force_duplicate or /skip command arrives, or /stop."""
    RT.decision_event.clear()
    RT.decision_value = None
    RT.awaiting_decision = True
    try:
        stop_wait = asyncio.create_task(RT.stop_event.wait())
        dec_wait = asyncio.create_task(RT.decision_event.wait())
        try:
            done, pending = await asyncio.wait(
                {stop_wait, dec_wait}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for t in (stop_wait, dec_wait):
                if not t.done():
                    t.cancel()
        if stop_wait in done:
            return None
        return RT.decision_value
    finally:
        RT.awaiting_decision = False


async def prompt_duplicate(client: Client, src: Dict[str, Any], hist: Dict[str, int]) -> None:
    text = (
        f"⚠️ <b>Duplicate detected</b>\n"
        f"<b>{esc(src['title'])}</b> (<code>{src['id']}</code>) was already cloned into "
        f"<b>{esc(S.target_title)}</b>.\n\n"
        f"Previously copied: <b>{hist.get('total_copied', 0):,}</b> messages\n"
        f"Last scanned message ID: <code>{hist.get('last_scanned_id', 0)}</code>\n\n"
        "Choose one:\n"
        "/force_continue – resume from last_scanned_id + 1 to the latest message\n"
        "/force_duplicate – ignore history, copy from message 1 again\n"
        "/skip – skip this source and move to the next one in the queue"
    )
    await send(client, RT.report_chat, text)


# --------------------------------------------------------------------------- #
# Cloning engine
# --------------------------------------------------------------------------- #


def stopping() -> bool:
    return RT.stop_event.is_set()


def reset_current_source() -> None:
    S.cur_source_id = 0
    S.cur_source_title = ""
    S.in_progress = False
    S.range_start = S.range_end = S.next_id = 0
    S.copied = S.skipped = S.failed = 0
    S.done_groups = []
    S.hist_baseline_copied = 0


def commit_history(final: bool) -> None:
    """Write current-source progress into the pair history file."""
    if not S.cur_source_id:
        return
    key = hist_key(S.target_id, S.cur_source_id)
    entry = HIST.setdefault(
        key, {"last_scanned_id": 0, "total_copied": 0, "total_skipped": 0, "total_failed": 0}
    )
    scanned_to = max(entry.get("last_scanned_id", 0), S.next_id - 1, S.range_start - 1 if final else 0)
    entry["last_scanned_id"] = max(scanned_to, 0)
    entry["total_copied"] = S.hist_baseline_copied + S.copied
    entry["total_skipped"] = entry.get("total_skipped", 0) + 0  # updated via deltas below at call sites if needed
    save_history()


async def fetch_batch(client: Client, ids: List[int]):
    try:
        msgs = await call(lambda: client.get_messages(S.cur_source_id, ids))
    except RPCError as e:
        raise Fatal(f"Cannot read the source chat: {err_id(e)}") from e
    msgs = [m for m in msgs if m is not None]
    msgs.sort(key=lambda m: m.id)
    return msgs


async def process_message(client: Client, msg) -> str:
    """Copy one message into the target. Returns copied | skipped | failed | grouped."""
    if msg is None or msg.empty or msg.service:
        S.skipped += 1
        return "skipped"

    gid = str(msg.media_group_id) if msg.media_group_id else None
    if gid and gid in S.done_groups:
        return "grouped"

    strip_markup = False
    while True:
        try:
            if gid:
                sent = await call(
                    lambda: client.copy_media_group(S.target_id, S.cur_source_id, msg.id)
                )
                S.copied += len(sent)
                S.done_groups.append(gid)
                del S.done_groups[:-200]
            else:
                kwargs = {"reply_markup": None} if strip_markup else {}
                sent = await call(lambda: msg.copy(S.target_id, **kwargs))
                if not sent:
                    S.skipped += 1
                    return "skipped"
                S.copied += 1
            return "copied"
        except Stopped:
            raise
        except RPCError as e:
            eid = err_id(e)
            if eid in FATAL_IDS:
                raise Fatal(f"{eid} while copying message {msg.id}") from e
            if not gid and not strip_markup and ("REPLY_MARKUP" in eid or "BUTTON" in eid):
                strip_markup = True
                continue
            log.warning("Failed to copy message %s: %s", msg.id, e)
            S.failed += 1
            return "failed"
        except Exception as e:
            log.warning("Failed to copy message %s: %r", msg.id, e)
            S.failed += 1
            return "failed"


async def prepare_source(client: Client, progress_msg) -> Optional[str]:
    """
    Ensure S.cur_source_* / range_* / next_id are ready to process the source
    at S.queue_index. Returns None to proceed, or "skip" if the user chose to
    skip this source. Raises Stopped if the user stopped while deciding.
    """
    src = S.sources[S.queue_index]

    if S.in_progress and S.cur_source_id == src["id"]:
        # Resuming a run that was already under way for this exact source.
        end = await call(lambda: get_last_message_id(client, S.cur_source_id))
        S.range_end = max(S.range_end, end)
        save_state()
        return None

    S.cur_source_id = src["id"]
    S.cur_source_title = src["title"]
    save_state()

    chat = await resolve_chat(client, src["id"])
    end = await call(lambda: get_last_message_id(client, chat.id))
    key = hist_key(S.target_id, src["id"])
    hist = HIST.get(key)

    start = 1
    if hist and hist.get("last_scanned_id", 0) > 0:
        S.status = "awaiting_decision"
        save_state()
        await prompt_duplicate(client, src, hist)
        decision = await wait_for_decision()
        if decision is None:
            raise Stopped()
        if decision == "skip":
            S.queue_index += 1
            reset_current_source()
            save_state()
            return "skip"
        if decision == "continue":
            start = hist["last_scanned_id"] + 1
            S.hist_baseline_copied = hist.get("total_copied", 0)
        else:  # duplicate
            start = 1
            S.hist_baseline_copied = hist.get("total_copied", 0)
    else:
        S.hist_baseline_copied = 0

    S.range_start = S.next_id = start
    S.range_end = max(end, start - 1)
    S.copied = S.skipped = S.failed = 0
    S.done_groups = []
    S.in_progress = True
    S.status = "running"
    save_state()
    return None


async def clone_worker(client: Client, progress_msg) -> None:
    done = asyncio.Event()
    dashboard = asyncio.create_task(dashboard_loop(progress_msg, done))
    error: Optional[str] = None
    try:
        await safe_edit(progress_msg, render_status())
        while S.queue_index < len(S.sources) and not stopping():
            outcome = await prepare_source(client, progress_msg)
            if outcome == "skip":
                await send(
                    client, RT.report_chat,
                    f"⏭ Skipped <b>{esc(S.sources[S.queue_index - 1]['title'])}</b>. Moving on…",
                )
                continue
            if stopping():
                break

            RT.source_started = time.monotonic()
            RT.source_first_id = S.next_id

            while S.next_id <= S.range_end and not stopping():
                ids = list(range(S.next_id, min(S.next_id + BATCH_SIZE, S.range_end + 1)))
                msgs = await fetch_batch(client, ids)

                for msg in msgs:
                    if stopping():
                        break
                    if msg.id < S.next_id:
                        continue
                    result = await process_message(client, msg)
                    S.next_id = msg.id + 1
                    save_state()
                    commit_history(final=False)
                    if result in ("copied", "failed"):
                        await nap(MSG_DELAY)
                else:
                    S.next_id = max(S.next_id, ids[-1] + 1)
                    save_state()
                    commit_history(final=False)
                    if S.next_id <= S.range_end:
                        await nap(random.uniform(*BATCH_COOLDOWN))

            S.elapsed_before += time.monotonic() - RT.source_started

            if stopping():
                break

            # source finished (fully scanned)
            commit_history(final=True)
            finished_title = S.cur_source_title
            copied_this_run = S.copied
            S.queue_index += 1
            reset_current_source()
            save_state()
            if S.queue_index < len(S.sources):
                await send(
                    client, RT.report_chat,
                    f"✅ Finished <b>{esc(finished_title)}</b> ({copied_this_run:,} copied this run). "
                    f"Moving to the next source…",
                )
    except Stopped:
        pass
    except Fatal as e:
        error = str(e)
        S.elapsed_before += time.monotonic() - RT.source_started
        commit_history(final=False)
    except Exception as e:
        log.exception("Unexpected error in clone worker")
        error = f"Unexpected error: {e}"
        S.elapsed_before += time.monotonic() - RT.source_started
        commit_history(final=False)

    if error:
        S.status, S.last_error = "error", error
    elif S.queue_index >= len(S.sources):
        S.status = "completed"
    else:
        S.status = "stopped"
    save_state()

    done.set()
    await dashboard
    await safe_edit(progress_msg, render_status())

    if S.status == "completed":
        note = "✅ <b>All sources in the queue have been cloned!</b>"
    elif S.status == "error":
        note = (
            f"🛑 <b>Cloning aborted:</b> <code>{esc(error or '')}</code>\n"
            "Progress is saved. Fix the problem and send /clone to resume."
        )
    else:
        note = "⏸ <b>Cloning stopped.</b> Progress saved. Send /clone to resume."
    try:
        await send(client, RT.report_chat, note)
    except Exception:
        log.exception("Could not send final notification")
    RT.stop_event.clear()


# --------------------------------------------------------------------------- #
# Command handlers
# --------------------------------------------------------------------------- #

HELP_TEXT = (
    "🤖 <b>Ultimate Batch Forwarder &amp; Channel Cloner</b>\n\n"
    "/session – set one or more sources, then the target\n"
    "/clone – start or resume the queue\n"
    "/status – configuration and live progress\n"
    "/stop – stop gracefully (resume later with /clone)\n"
    "/remove – delete the saved session\n\n"
    "When a source was already cloned to the current target you'll be asked to choose "
    "/force_continue, /force_duplicate or /skip.\n\n"
    "Messages are copied without \"Forwarded from\" headers."
)


async def cmd_help(client: Client, message) -> None:
    await send(client, message.chat.id, HELP_TEXT)


async def cmd_session(client: Client, message) -> None:
    chat_id = message.chat.id
    if RT.running:
        await send(client, chat_id, "⚠️ A clone job is running. Use /stop first.")
        return
    if S.configured:
        await send(
            client, chat_id,
            f"⚠️ A session already exists (target <b>{esc(S.target_title)}</b>, "
            f"{len(S.sources)} source(s)).\nUse /remove first to register a new one.",
        )
        return
    RT.pending[chat_id] = {"step": "sources"}
    await send(
        client, chat_id,
        "📤 Send one or more <b>Source Chats</b>, separated by spaces or commas.\n"
        "Each can be <code>@channel</code>, a <code>-100…</code> ID or a t.me link.\n"
        "Example: <code>@chan1 -1001234567890 @chan3</code>\n"
        "(Send <code>cancel</code> to abort.)",
    )


async def on_input(client: Client, message) -> None:
    chat_id = message.chat.id
    state = RT.pending.get(chat_id)
    if not state:
        return
    if (chat_id, message.id) in RT.sent_ids:
        return
    text = (message.text or "").strip()
    if not text or not text[0].isascii():
        return

    if text.lower() == "cancel":
        RT.pending.pop(chat_id, None)
        await send(client, chat_id, "🚫 Session setup cancelled.")
        return

    if state["step"] == "sources":
        tokens = split_refs(text)
        if not tokens:
            await send(client, chat_id, "❌ Send at least one source chat.")
            return
        valid: List[Dict[str, Any]] = []
        invalid: List[str] = []
        seen_ids = set()
        for tok in tokens:
            ref = parse_chat_ref(tok)
            if ref is None:
                invalid.append(tok)
                continue
            try:
                chat = await resolve_chat(client, ref)
                await call(lambda: client.get_messages(chat.id, 1))  # readability probe
            except (RPCError, KeyError, ValueError) as e:
                invalid.append(f"{tok} ({err_text(e)})")
                continue
            if chat.id in seen_ids:
                continue
            seen_ids.add(chat.id)
            valid.append({"id": chat.id, "title": title_of(chat)})

        if invalid:
            await send(
                client, chat_id,
                "⚠️ Could not use these (skipped): " + esc(", ".join(invalid)),
            )
        if not valid:
            await send(client, chat_id, "❌ None of those sources worked. Send the source list again.")
            return

        state["step"] = "target"
        state["sources"] = valid
        listing = "\n".join(f"• {esc(s['title'])} (<code>{s['id']}</code>)" for s in valid)
        await send(
            client, chat_id,
            f"✅ <b>{len(valid)} source(s) added:</b>\n{listing}\n\n"
            "📥 Now send the <b>Target Chat</b> (I must be an admin there with Post Messages).",
        )
        return

    # step == "target"
    ref = parse_chat_ref(text)
    if ref is None:
        await send(
            client, chat_id,
            "❌ I couldn't understand that. Send <code>@username</code>, a <code>-100…</code> ID or a t.me link.",
        )
        return
    try:
        chat = await resolve_chat(client, ref)
        ok, why = await check_target_permissions(client, chat)
        if not ok:
            await send(client, chat_id, f"❌ {why}\nFix the permissions and send the target again.")
            return
    except (RPCError, KeyError, ValueError) as e:
        await send(client, chat_id, f"❌ I can't use that chat: <code>{err_text(e)}</code>\nTry another one.")
        return

    sources = state["sources"]
    if any(chat.id == s["id"] for s in sources):
        await send(client, chat_id, "❌ The target can't also be one of the sources. Send another target.")
        return

    S.sources = sources
    S.target_id, S.target_title = chat.id, title_of(chat)
    S.queue_index = 0
    reset_current_source()
    S.status = "idle"
    S.last_error = ""
    save_state()
    RT.pending.pop(chat_id, None)
    listing = "\n".join(f"• {esc(s['title'])} (<code>{s['id']}</code>)" for s in S.sources)
    await send(
        client, chat_id,
        f"✅ <b>Session saved</b>\n🎯 Target: {esc(S.target_title)} (<code>{S.target_id}</code>)\n"
        f"📚 Queue ({len(S.sources)}):\n{listing}\n\nSend /clone to start.",
    )


async def cmd_clone(client: Client, message) -> None:
    chat_id = message.chat.id
    if RT.running:
        await send(client, chat_id, "⚠️ A clone job is already running. Use /status or /stop.")
        return
    if not S.configured:
        await send(client, chat_id, "📭 No session yet. Send /session first.")
        return
    if chat_id in RT.pending:
        await send(client, chat_id, "⚠️ Finish the /session setup first (or send <code>cancel</code>).")
        return
    if S.queue_index >= len(S.sources):
        await send(
            client, chat_id,
            "✅ The queue is already fully processed. Use /remove then /session to clone a new set.",
        )
        return

    note = await send(client, chat_id, "🔎 Checking the target and preparing the queue…")
    try:
        target = await resolve_chat(client, S.target_id)
        ok, why = await check_target_permissions(client, target)
        if not ok:
            await safe_edit(note, f"❌ {why}")
            return
    except (RPCError, KeyError, ValueError) as e:
        await safe_edit(note, f"❌ Cannot start: <code>{err_text(e)}</code>")
        return

    S.status = "running"
    S.last_error = ""
    save_state()

    RT.stop_event.clear()
    RT.decision_event = asyncio.Event()
    RT.run_started = time.monotonic()
    RT.report_chat = chat_id
    RT.task = asyncio.create_task(clone_worker(client, note))


async def cmd_stop(client: Client, message) -> None:
    if not RT.running:
        await send(client, message.chat.id, "ℹ️ Nothing is running.")
        return
    RT.stop_event.set()
    await send(
        client, message.chat.id,
        "🛑 Stopping after the current message… progress is saved and /clone will resume.",
    )


async def cmd_remove(client: Client, message) -> None:
    chat_id = message.chat.id
    if RT.running:
        await send(client, chat_id, "⚠️ A clone job is running. Use /stop first.")
        return
    RT.pending.pop(chat_id, None)
    reset_session()
    await send(
        client, chat_id,
        "🗑 Session cleared. (Duplicate-protection history for already-cloned pairs is kept.)\n"
        "Send /session to register a new one.",
    )


async def cmd_status(client: Client, message) -> None:
    await send(client, message.chat.id, render_status())


async def _decide(client: Client, message, value: str, label: str) -> None:
    if not RT.awaiting_decision:
        await send(client, message.chat.id, "ℹ️ No duplicate decision is pending right now.")
        return
    RT.decision_value = value
    RT.decision_event.set()
    await send(client, message.chat.id, f"👍 {label}")


async def cmd_force_continue(client: Client, message) -> None:
    await _decide(client, message, "continue", "Resuming from where it left off…")


async def cmd_force_duplicate(client: Client, message) -> None:
    await _decide(client, message, "duplicate", "Copying from message 1 again…")


async def cmd_skip(client: Client, message) -> None:
    await _decide(client, message, "skip", "Skipping this source…")


def register_handlers(client: Client) -> None:
    owner = filters.me
    if OWNER_IDS:
        owner = owner | filters.user(OWNER_IDS)

    routes = [
        (["start", "help"], cmd_help),
        (["session"], cmd_session),
        (["clone"], cmd_clone),
        (["stop"], cmd_stop),
        (["remove"], cmd_remove),
        (["status"], cmd_status),
        (["force_continue"], cmd_force_continue),
        (["force_duplicate"], cmd_force_duplicate),
        (["skip"], cmd_skip),
    ]
    for names, handler in routes:
        client.add_handler(MessageHandler(handler, filters.command(names) & owner))
    client.add_handler(
        MessageHandler(on_input, owner & filters.text & ~filters.command(COMMANDS)), group=1
    )


# --------------------------------------------------------------------------- #
# Keep-alive web server + entrypoint
# --------------------------------------------------------------------------- #


async def health(_request: web.Request) -> web.Response:
    return web.Response(text="OK", status=200)


async def start_web_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Keep-alive web server listening on 0.0.0.0:%s", PORT)
    return runner


async def main() -> None:
    RT.stop_event = asyncio.Event()
    RT.decision_event = asyncio.Event()
    runner = await start_web_server()  # bind the port first (Render/Koyeb health checks)

    client = Client(
        "cloner", api_id=API_ID, api_hash=API_HASH,
        session_string=SESSION_STRING, in_memory=True,
    )
    register_handlers(client)
    await client.start()
    me = await client.get_me()
    log.info("Started as %s (userbot mode)", me.username or me.first_name)

    try:
        await idle()
    finally:
        if RT.running:
            log.info("Shutting down: stopping the clone job and saving progress")
            RT.stop_event.set()
            try:
                await asyncio.wait_for(asyncio.shield(RT.task), timeout=30)
            except Exception:
                pass
        await client.stop()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
