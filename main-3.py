"""
Ultimate Batch Forwarder & Channel Cloner Bot
=============================================

Copies every message from a source chat to a target chat with Message.copy()
(so no "Forwarded from" header), in batches of 100, with FloodWait handling,
a live progress dashboard and a keep-alive web server for Render/Koyeb/Heroku.

Commands (owner only):
    /session               interactive setup: source chat, then target chat
    /clone [start] [end]   start / resume cloning (default: resume, or from 1)
    /status                show configuration and live progress
    /stop                  stop gracefully; progress is saved, /clone resumes
    /remove                delete the saved session so a new one can be set

Environment variables:
    API_ID, API_HASH   required (my.telegram.org)
    SESSION_STRING     Pyrogram user-account session string (recommended: a
                       plain channel *member* can only be a user account)
    BOT_TOKEN          alternative to SESSION_STRING (bot must be admin in the
                       source; bots cannot read history, so pass an end id)
    OWNER_IDS          comma/space separated user IDs allowed to command the bot
                       (required in BOT_TOKEN mode, optional with SESSION_STRING
                       where your own account is always allowed)
    PORT               web server port (default 8080)
    STATE_FILE         path of the persisted session (default clone_state.json)
    MSG_DELAY          seconds between copies (default 1.2)
    DASHBOARD_INTERVAL seconds between progress edits (default 15)
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
from collections import deque
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
SESSION_STRING = os.environ.get("SESSION_STRING", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
IS_USERBOT = bool(SESSION_STRING)
if not IS_USERBOT and not BOT_TOKEN:
    sys.exit("Set SESSION_STRING (user account, recommended) or BOT_TOKEN.")

OWNER_IDS: List[int] = [
    int(x) for x in re.split(r"[\s,]+", os.environ.get("OWNER_IDS", "").strip()) if x
]
if not IS_USERBOT and not OWNER_IDS:
    sys.exit("OWNER_IDS is required when running with BOT_TOKEN.")

PORT = int(os.environ.get("PORT", "8080"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "clone_state.json"))
BATCH_SIZE = 100
MSG_DELAY = float(os.environ.get("MSG_DELAY", "1.2"))
BATCH_COOLDOWN = (2.0, 3.0)
DASHBOARD_INTERVAL = int(os.environ.get("DASHBOARD_INTERVAL", "15"))

COMMANDS = ["start", "help", "session", "clone", "stop", "remove", "status"]

# Errors that will never succeed on retry: abort the job, keep progress.
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
    """Raised when a stop was requested while waiting (e.g. during FloodWait)."""


class Fatal(Exception):
    """Unrecoverable error; the job is aborted but its progress is preserved."""


# --------------------------------------------------------------------------- #
# Persistent session state
# --------------------------------------------------------------------------- #


@dataclass
class Session:
    source_id: int = 0
    target_id: int = 0
    source_title: str = ""
    target_title: str = ""
    range_start: int = 0      # first message id of the current job
    range_end: int = 0        # last message id of the current job
    next_id: int = 0          # next message id to process (resume point)
    copied: int = 0
    skipped: int = 0          # deleted / service / unsupported
    failed: int = 0
    done_groups: List[str] = field(default_factory=list)  # albums already copied
    status: str = "idle"      # idle | running | stopped | completed | error
    last_error: str = ""
    elapsed_before: float = 0.0

    @property
    def configured(self) -> bool:
        return bool(self.source_id and self.target_id)


def load_state() -> Session:
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            known = {k: v for k, v in data.items() if k in Session.__dataclass_fields__}
            sess = Session(**known)
            if sess.status == "running":  # process died mid-run: resumable
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


S = load_state()


class Runtime:
    """Non-persistent, per-process state."""

    def __init__(self) -> None:
        self.task: Optional[asyncio.Task] = None
        self.stop_event: Optional[asyncio.Event] = None
        self.run_started: float = 0.0
        self.run_first_id: int = 0
        self.report_chat: int = 0
        self.pending: Dict[int, Dict[str, Any]] = {}   # chat_id -> setup step
        self.sent_ids: deque = deque(maxlen=500)      # (chat_id, msg_id) we sent
        self.dialogs_warmed: bool = False

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()


RT = Runtime()

# --------------------------------------------------------------------------- #
# Helpers
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


async def nap(seconds: float) -> bool:
    """Sleep, but wake early if /stop is requested. Returns False if stopped."""
    try:
        await asyncio.wait_for(RT.stop_event.wait(), timeout=seconds)
        return False
    except asyncio.TimeoutError:
        return True


async def call(factory: Callable[[], Awaitable[Any]]) -> Any:
    """
    Run a Telegram request with FloodWait handling: on FloodWait sleep
    value + 2 seconds and retry the *same* request. Transient network errors
    are retried with backoff.
    """
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
    """
    Send an HTML message. INVARIANT: every message we send begins with an
    emoji; the interactive /session handler relies on this to never mistake
    our own messages (userbot mode) for user input.
    """
    try:
        sent = await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        sent = await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    RT.sent_ids.append((sent.chat.id, sent.id))
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
    """Populate the peer cache of an in-memory user session (needed for -100 ids)."""
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
            if attempt == 2 or not IS_USERBOT or RT.dialogs_warmed:
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


async def get_last_message_id(client: Client, chat_id: int) -> Optional[int]:
    """Latest message id in the chat, 0 if empty, None if history is unavailable (bots)."""
    try:
        msgs = await call(lambda: _first_history_page(client, chat_id))
        return msgs[0].id if msgs else 0
    except RPCError:
        return None


async def _first_history_page(client: Client, chat_id: int):
    return [m async for m in client.get_chat_history(chat_id, limit=1)]


# --------------------------------------------------------------------------- #
# Status / dashboard rendering
# --------------------------------------------------------------------------- #

STATE_LABELS = {
    "idle": ("💤", "Ready"),
    "running": ("🔄", "Running"),
    "stopped": ("⏸", "Stopped (resumable)"),
    "completed": ("✅", "Completed"),
    "error": ("🛑", "Stopped by error (resumable)"),
}


def elapsed_now() -> float:
    elapsed = S.elapsed_before
    if S.status == "running" and RT.run_started:
        elapsed += time.monotonic() - RT.run_started
    return elapsed


def render_status() -> str:
    if not S.configured:
        return "📭 <b>No session configured.</b>\nSend /session to set a source and a target."

    icon, label = STATE_LABELS.get(S.status, ("ℹ️", S.status))
    lines = [
        f"{icon} <b>Clone status: {label}</b>",
        "",
        f"📤 <b>Source:</b> {esc(S.source_title)} (<code>{S.source_id}</code>)",
        f"📥 <b>Target:</b> {esc(S.target_title)} (<code>{S.target_id}</code>)",
        "",
    ]
    if not S.range_start:
        lines.append("Nothing copied yet. Send /clone to begin.")
        return "\n".join(lines)

    total = max(0, S.range_end - S.range_start + 1)
    processed = min(total, max(0, S.next_id - S.range_start))
    pct = (processed / total * 100) if total else 100.0
    lines += [
        f"<code>[{progress_bar(pct)}] {pct:.1f}%</code>",
        f"🔎 Scanned: <b>{processed:,}</b> / <b>{total:,}</b>  (IDs {S.range_start:,}–{S.range_end:,})",
        f"✅ Copied: <b>{S.copied:,}</b>",
        f"⏭ Skipped/deleted: <b>{S.skipped:,}</b>",
        f"❌ Failed: <b>{S.failed:,}</b>",
        f"➡️ Next message ID: <code>{S.next_id}</code>",
        "",
        f"⏱ Elapsed: <b>{fmt_dur(elapsed_now())}</b>",
    ]
    if RT.running and S.status == "running":
        run_done = S.next_id - RT.run_first_id
        run_time = time.monotonic() - RT.run_started
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
# Cloning engine
# --------------------------------------------------------------------------- #


def stopping() -> bool:
    return RT.stop_event.is_set()


async def fetch_batch(client: Client, ids: List[int]):
    try:
        msgs = await call(lambda: client.get_messages(S.source_id, ids))
    except RPCError as e:
        raise Fatal(f"Cannot read the source chat: {err_id(e)}") from e
    msgs = [m for m in msgs if m is not None]
    msgs.sort(key=lambda m: m.id)
    return msgs


async def process_message(client: Client, msg) -> str:
    """Copy one message. Returns copied | skipped | failed | grouped."""
    if msg is None or msg.empty or msg.service:
        S.skipped += 1
        return "skipped"

    gid = str(msg.media_group_id) if msg.media_group_id else None
    if gid and gid in S.done_groups:
        return "grouped"  # rest of an album that was already copied as a whole

    strip_markup = False
    while True:
        try:
            if gid:
                # Albums: copy the whole group at once so it stays an album.
                sent = await call(
                    lambda: client.copy_media_group(S.target_id, S.source_id, msg.id)
                )
                S.copied += len(sent)
                S.done_groups.append(gid)
                del S.done_groups[:-200]
            else:
                kwargs = {"reply_markup": None} if strip_markup else {}
                sent = await call(lambda: msg.copy(S.target_id, **kwargs))
                if not sent:  # copy() returns None for unsupported types
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
                strip_markup = True  # retry once without the inline keyboard
                continue
            log.warning("Failed to copy message %s: %s", msg.id, e)
            S.failed += 1
            return "failed"
        except Exception as e:
            log.warning("Failed to copy message %s: %r", msg.id, e)
            S.failed += 1
            return "failed"


async def clone_worker(client: Client, progress_msg) -> None:
    done = asyncio.Event()
    dashboard = asyncio.create_task(dashboard_loop(progress_msg, done))
    error: Optional[str] = None
    try:
        await safe_edit(progress_msg, render_status())
        while S.next_id <= S.range_end and not stopping():
            ids = list(range(S.next_id, min(S.next_id + BATCH_SIZE, S.range_end + 1)))
            msgs = await fetch_batch(client, ids)

            for msg in msgs:
                if stopping():
                    break
                if msg.id < S.next_id:
                    continue
                result = await process_message(client, msg)
                S.next_id = msg.id + 1          # advance only after success
                save_state()
                if result in ("copied", "failed"):
                    await nap(MSG_DELAY)        # ~1.2s between copies
            else:
                # whole batch handled (also covers ids the API did not return)
                S.next_id = max(S.next_id, ids[-1] + 1)
                save_state()
                if S.next_id <= S.range_end:
                    await nap(random.uniform(*BATCH_COOLDOWN))  # 2-3s per batch
    except Stopped:
        pass
    except Fatal as e:
        error = str(e)
    except Exception as e:
        log.exception("Unexpected error in clone worker")
        error = f"Unexpected error: {e}"

    S.elapsed_before += time.monotonic() - RT.run_started
    if error:
        S.status, S.last_error = "error", error
    elif S.next_id > S.range_end:
        S.status = "completed"
    else:
        S.status = "stopped"
    save_state()

    done.set()
    await dashboard
    await safe_edit(progress_msg, render_status())

    took = fmt_dur(S.elapsed_before)
    if S.status == "completed":
        note = (
            f"✅ <b>Cloning complete!</b>\nCopied <b>{S.copied:,}</b> · skipped "
            f"<b>{S.skipped:,}</b> · failed <b>{S.failed:,}</b> in {took}."
        )
    elif S.status == "error":
        note = (
            f"🛑 <b>Cloning aborted:</b> <code>{esc(error or '')}</code>\n"
            f"Progress is saved at ID <code>{S.next_id}</code>. Fix the problem and send /clone to resume."
        )
    else:
        note = (
            f"⏸ <b>Cloning stopped.</b> Progress saved at ID <code>{S.next_id}</code>.\n"
            "Send /clone to resume."
        )
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
    "/session – set the source and target chats\n"
    "/clone [start_id] [end_id] – start or resume cloning\n"
    "/status – configuration and live progress\n"
    "/stop – stop gracefully (resume later with /clone)\n"
    "/remove – delete the saved session\n\n"
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
            client,
            chat_id,
            f"⚠️ A session already exists:\n<b>{esc(S.source_title)}</b> → <b>{esc(S.target_title)}</b>\n"
            "Use /remove first to register a new one.",
        )
        return
    RT.pending[chat_id] = {"step": "source"}
    await send(
        client,
        chat_id,
        "📤 Send the <b>Source Chat</b>: <code>@channel</code>, a <code>-100…</code> ID or a t.me link.\n"
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
    # Our own messages start with an emoji; real input is always plain ASCII.
    if not text or not text[0].isascii():
        return

    if text.lower() == "cancel":
        RT.pending.pop(chat_id, None)
        await send(client, chat_id, "🚫 Session setup cancelled.")
        return

    ref = parse_chat_ref(text)
    if ref is None:
        await send(
            client,
            chat_id,
            "❌ I couldn't understand that. Send <code>@username</code>, a <code>-100…</code> ID or a "
            "t.me link (invite links can't be resolved: join the chat first and send its ID).",
        )
        return

    try:
        chat = await resolve_chat(client, ref)
        if state["step"] == "source":
            await call(lambda: client.get_messages(chat.id, 1))  # readability probe
        else:
            ok, why = await check_target_permissions(client, chat)
            if not ok:
                await send(client, chat_id, f"❌ {why}\nFix the permissions and send the target again.")
                return
    except (RPCError, KeyError, ValueError) as e:
        await send(client, chat_id, f"❌ I can't use that chat: <code>{err_text(e)}</code>\nTry another one.")
        return

    title = title_of(chat)
    if state["step"] == "source":
        state.update(step="target", source=(chat.id, title))
        await send(
            client,
            chat_id,
            f"✅ Source: <b>{esc(title)}</b> (<code>{chat.id}</code>)\n\n"
            "📥 Now send the <b>Target Chat</b> (I must be an admin there with Post Messages).",
        )
        return

    source_id, source_title = state["source"]
    if chat.id == source_id:
        await send(client, chat_id, "❌ Source and target must be different chats. Send the target again.")
        return
    S.source_id, S.source_title = source_id, source_title
    S.target_id, S.target_title = chat.id, title
    S.status = "idle"
    save_state()
    RT.pending.pop(chat_id, None)
    await send(
        client,
        chat_id,
        f"✅ <b>Session saved</b>\n📤 {esc(source_title)} (<code>{source_id}</code>)\n"
        f"📥 {esc(title)} (<code>{chat.id}</code>)\n\nSend /clone to start.",
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

    start_arg = end_arg = None
    try:
        args = message.command[1:]
        if len(args) >= 1:
            start_arg = int(args[0])
            if start_arg < 1:
                raise ValueError
        if len(args) >= 2:
            end_arg = int(args[1])
            if end_arg < start_arg:
                raise ValueError
    except ValueError:
        await send(client, chat_id, "🧭 Usage: <code>/clone [start_message_id] [end_message_id]</code>")
        return

    note = await send(client, chat_id, "🔎 Checking both chats and scanning the source…")
    try:
        await resolve_chat(client, S.source_id)
        target = await resolve_chat(client, S.target_id)
        ok, why = await check_target_permissions(client, target)
        if not ok:
            await safe_edit(note, f"❌ {why}")
            return
        last_id = await get_last_message_id(client, S.source_id)
    except (RPCError, KeyError, ValueError) as e:
        await safe_edit(note, f"❌ Cannot start: <code>{err_text(e)}</code>")
        return

    # Decide between resuming the saved job and starting a fresh one.
    if start_arg is None:
        resume = bool(S.range_start and S.next_id)
        start = S.next_id if resume else 1
    else:
        start = start_arg
        resume = bool(S.range_start and start == S.next_id)

    if end_arg:
        end = end_arg
    elif last_id:
        end = last_id
    elif last_id is None and resume and S.range_end >= start:
        end = S.range_end
    elif last_id is None:
        await safe_edit(
            note,
            "❌ This account can't read the source history (bot accounts can't). Pass an end ID, e.g. "
            "<code>/clone 1 5000</code>, or run the bot with a user SESSION_STRING.",
        )
        return
    else:
        await safe_edit(note, "📭 The source chat has no messages.")
        return

    if start > end:
        await safe_edit(
            note, f"✅ Nothing to copy: next ID <code>{start}</code> is beyond the last message (<code>{end}</code>)."
        )
        return

    if not resume:
        S.range_start = S.next_id = start
        S.copied = S.skipped = S.failed = 0
        S.done_groups = []
        S.elapsed_before = 0.0
    S.range_end = end
    S.status = "running"
    S.last_error = ""
    save_state()

    RT.stop_event.clear()
    RT.run_started = time.monotonic()
    RT.run_first_id = S.next_id
    RT.report_chat = chat_id
    RT.task = asyncio.create_task(clone_worker(client, note))


async def cmd_stop(client: Client, message) -> None:
    if not RT.running:
        await send(client, message.chat.id, "ℹ️ Nothing is running.")
        return
    RT.stop_event.set()
    await send(
        client,
        message.chat.id,
        "🛑 Stopping after the current message… progress is saved and /clone will resume.",
    )


async def cmd_remove(client: Client, message) -> None:
    chat_id = message.chat.id
    if RT.running:
        await send(client, chat_id, "⚠️ A clone job is running. Use /stop first.")
        return
    RT.pending.pop(chat_id, None)
    reset_session()
    await send(client, chat_id, "🗑 Session cleared. Send /session to register a new one.")


async def cmd_status(client: Client, message) -> None:
    await send(client, message.chat.id, render_status())


def register_handlers(client: Client) -> None:
    owner = filters.user(OWNER_IDS) if OWNER_IDS else None
    if IS_USERBOT:  # a user account always obeys its own outgoing commands
        owner = (filters.me | owner) if owner is not None else filters.me

    routes = [
        (["start", "help"], cmd_help),
        (["session"], cmd_session),
        (["clone"], cmd_clone),
        (["stop"], cmd_stop),
        (["remove"], cmd_remove),
        (["status"], cmd_status),
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
    runner = await start_web_server()  # bind the port first (Render/Koyeb health checks)

    if IS_USERBOT:
        client = Client(
            "cloner", api_id=API_ID, api_hash=API_HASH,
            session_string=SESSION_STRING, in_memory=True,
        )
    else:
        client = Client("cloner_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

    register_handlers(client)
    await client.start()
    me = await client.get_me()
    log.info("Started as %s (%s mode)", me.username or me.first_name, "userbot" if IS_USERBOT else "bot")

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
