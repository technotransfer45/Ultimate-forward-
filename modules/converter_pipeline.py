from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

import config
from modules.link_extractor import extract_links, get_host, url_hash
from modules.metadata_parser import format_rough_message
from utils.retry import safe_call

logger = logging.getLogger(__name__)

BUSY_RE = re.compile(
    r"\bbusy\b|server\s+busy|system\s+busy|limit\s+reached|try\s+again\s+later|too\s+many\s+requests|"
    r"rate\s*limit|overload|please\s+wait\s+and\s+try|queue\s+is\s+full|cool\s*down|temporarily\s+unavailable",
    re.I,
)
FAILURE_RE = re.compile(r"invalid|error|failed|not supported|unable|expired|not found|unsupported", re.I)
TELEGRAM_HOSTS = ("t.me", "telegram.me", "telegram.dog")


class ConverterError(Exception):
    """A converter bot answered with an explicit failure."""


class BotBusyError(ConverterError):
    """A converter bot said it is busy / rate-limited."""


@dataclass
class ConversionJob:
    """One source post: every link in `caption` is converted, then the whole post is re-posted."""

    links: List[str]
    caption: str
    photo: Optional[Any] = None


@dataclass
class PendingRequest:
    """The in-flight request of ONE converter bot: which link it is converting and what to ignore."""

    link: str
    original_hash: str
    future: "asyncio.Future[str]"
    baseline_id: int


@dataclass
class BotState:
    """Pool entry for one converter bot (own lock, own pending request, own cooldown)."""

    username: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending: Optional[PendingRequest] = None
    chat_id: int = 0
    last_seen_id: int = 0
    cooldown_until: float = 0.0
    successes: int = 0
    failures: int = 0


def drop_links_from_caption(caption: str, failed_links: List[str]) -> str:
    """Remove links that could not be converted (and the label on their line) from a caption."""
    kept: List[str] = []
    for line in caption.split("\n"):
        updated = line
        touched = False
        for link in failed_links:
            if link in updated:
                updated = updated.replace(link, "")
                touched = True
        if touched and "http" not in updated:
            continue
        kept.append(updated.rstrip() if touched else updated)
    return "\n".join(kept)


class ConverterPipeline:
    """Converts links through a load-balanced pool of converter bots (round-robin + failover).

    * Every conversion request takes the next bot in the rotation (bot1, bot2, bot3, bot4, bot1, ...).
    * A bot that is busy, times out or errors is put on a short cooldown and the SAME link is retried
      immediately on the next bot. A link only goes to the rough target after every attempt failed.
    * Each bot has its own lock and its own pending future, so replies are matched to the exact
      (bot, link) pair that is waiting. One worker per bot lets the whole pool run in parallel.
    """

    def __init__(
        self,
        userbot: Any,
        converter_usernames: List[str],
        final_target: int,
        rough_target: int,
        poster: Callable[..., Awaitable[Any]],
        status: Optional[Any] = None,
        name: str = "converter",
    ) -> None:
        if not converter_usernames:
            raise ValueError("ConverterPipeline '%s' needs at least one converter bot" % name)
        self.name = name
        self.userbot = userbot
        self.final_target = final_target
        self.rough_target = rough_target
        self.poster = poster
        self.status = status
        self.states: List[BotState] = [BotState(username=username.lstrip("@")) for username in converter_usernames]
        self._by_username: Dict[str, BotState] = {state.username.lower(): state for state in self.states}
        self._by_chat: Dict[int, BotState] = {}
        self._rr = 0
        self.queue: "asyncio.Queue[ConversionJob]" = asyncio.Queue()
        self._tasks: List["asyncio.Task[None]"] = []

    @property
    def converter_usernames(self) -> List[str]:
        return [state.username for state in self.states]

    def _inc(self, name: str) -> None:
        if self.status is not None:
            self.status.inc(name)

    async def enqueue(self, job: ConversionJob) -> None:
        await self.queue.put(job)

    def start(self) -> None:
        if any(not task.done() for task in self._tasks):
            return
        self._tasks = [asyncio.create_task(self._worker(index)) for index in range(len(self.states))]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                logger.info("Converter worker of pipeline '%s' stopped", self.name)
        self._tasks = []

    # ------------------------------------------------------------------ pool selection
    def pick_bot(self) -> BotState:
        """Round-robin: next bot in rotation, skipping bots on cooldown while any other bot is available."""
        count = len(self.states)
        now = time.monotonic()
        chosen = -1
        for offset in range(count):
            candidate = (self._rr + offset) % count
            if self.states[candidate].cooldown_until <= now:
                chosen = candidate
                break
        if chosen < 0:
            chosen = min(range(count), key=lambda index: self.states[index].cooldown_until)
        self._rr = (chosen + 1) % count
        return self.states[chosen]

    def pool_summary(self) -> str:
        now = time.monotonic()
        parts: List[str] = []
        for state in self.states:
            remaining = int(state.cooldown_until - now)
            flag = "cooldown %ds" % remaining if remaining > 0 else "ready"
            parts.append("@%s %s (ok %d / fail %d)" % (state.username, flag, state.successes, state.failures))
        return "%s: %s" % (self.name, " | ".join(parts))

    # ------------------------------------------------------------------ reply handling
    def _state_for(self, message: Any) -> Optional[BotState]:
        chat = getattr(message, "chat", None)
        sender = getattr(message, "from_user", None)
        username = (getattr(chat, "username", None) or getattr(sender, "username", None) or "").lower()
        if username and username in self._by_username:
            return self._by_username[username]
        chat_id = int(getattr(chat, "id", 0) or 0)
        return self._by_chat.get(chat_id)

    async def on_converter_message(self, client: Any, message: Any) -> None:
        """Handler for new and edited DMs from ANY bot in this pool; resolves that bot's pending future."""
        state = self._state_for(message)
        if state is None:
            return
        message_id = int(getattr(message, "id", 0) or 0)
        if message_id > state.last_seen_id:
            state.last_seen_id = message_id
        pending = state.pending
        if pending is None or pending.future.done() or message_id <= pending.baseline_id:
            return
        text = getattr(message, "text", None) or getattr(message, "caption", None) or ""
        converted = [
            link for link in extract_links(message)
            if url_hash(link) != pending.original_hash and get_host(link) not in TELEGRAM_HOSTS
        ]
        if converted:
            pending.future.set_result(converted[-1])
            return
        if not text:
            return
        if BUSY_RE.search(text):
            pending.future.set_exception(BotBusyError("@%s busy: %s" % (state.username, text[:120])))
        elif FAILURE_RE.search(text):
            pending.future.set_exception(ConverterError("@%s: %s" % (state.username, text[:120])))

    # ------------------------------------------------------------------ conversion
    async def _convert_via(self, state: BotState, link: str) -> str:
        async with state.lock:
            loop = asyncio.get_running_loop()
            pending = PendingRequest(
                link=link,
                original_hash=url_hash(link),
                future=loop.create_future(),
                baseline_id=state.last_seen_id,
            )
            state.pending = pending
            try:
                sent = await safe_call(self.userbot.send_message, state.username, link)
                state.chat_id = int(sent.chat.id)
                self._by_chat[state.chat_id] = state
                pending.baseline_id = max(pending.baseline_id, int(sent.id))
                return await asyncio.wait_for(pending.future, timeout=config.CONVERTER_TIMEOUT)
            finally:
                state.pending = None

    async def _convert(self, link: str) -> str:
        """Convert one link: round-robin bot, immediate failover to the next bot on busy/timeout/error."""
        count = len(self.states)
        attempts = max(config.CONVERTER_RETRIES, count)
        errors: List[str] = []
        for attempt in range(1, attempts + 1):
            state = self.pick_bot()
            wait = state.cooldown_until - time.monotonic()
            if wait > 0:
                await asyncio.sleep(min(wait, float(config.CONVERTER_BUSY_COOLDOWN)))
            try:
                converted = await self._convert_via(state, link)
                state.successes += 1
                return converted
            except asyncio.CancelledError:
                raise
            except BotBusyError as exc:
                state.cooldown_until = time.monotonic() + config.CONVERTER_BUSY_COOLDOWN
                reason = str(exc)
            except asyncio.TimeoutError:
                state.cooldown_until = time.monotonic() + config.CONVERTER_BUSY_COOLDOWN / 2
                reason = "@%s timed out after %ss" % (state.username, config.CONVERTER_TIMEOUT)
            except ConverterError as exc:
                reason = str(exc)
            except Exception as exc:
                state.cooldown_until = time.monotonic() + config.CONVERTER_BUSY_COOLDOWN / 2
                reason = "@%s error: %s" % (state.username, str(exc) or type(exc).__name__)
            state.failures += 1
            errors.append(reason)
            logger.warning("[%s] attempt %d/%d failed for %s: %s", self.name, attempt, attempts, link, reason)
            if attempt < attempts:
                cycles_done = attempt // count if attempt % count == 0 else 0
                await asyncio.sleep(5 * cycles_done if cycles_done else 1)
        raise ConverterError("all converter attempts failed: %s" % " | ".join(errors[-count:]))

    async def _process_job(self, job: ConversionJob) -> None:
        converted: Dict[str, str] = {}
        failures: List[str] = []
        failed_links: List[str] = []
        for index, link in enumerate(dict.fromkeys(job.links)):
            if index > 0:
                await asyncio.sleep(config.CONVERTER_DELAY / len(self.states))
            try:
                converted[link] = await self._convert(link)
                self._inc("%s_converted" % self.name)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("[%s] conversion failed for %s: %s", self.name, link, exc)
                self._inc("conversion_failures")
                failed_links.append(link)
                failures.append("%s (%s)" % (link, str(exc) or type(exc).__name__))

        if converted:
            caption = job.caption
            for link, new_link in converted.items():
                caption = caption.replace(link, new_link)
            caption = drop_links_from_caption(caption, failed_links)
            await self.poster(self.final_target, caption, job.photo)
        if failures:
            suffix = " (post sent without them)" if converted else ""
            await self.poster(
                self.rough_target,
                format_rough_message(
                    "\n".join(failures),
                    "%s: every converter bot failed for %d link(s)%s" % (self.name.capitalize(), len(failures), suffix),
                    "%s converter pool" % self.name,
                ),
                None,
            )

    async def _worker(self, index: int) -> None:
        while True:
            job = await self.queue.get()
            try:
                await self._process_job(job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("[%s] worker %d job failed: %s", self.name, index + 1, exc)
                self._inc("conversion_failures")
            finally:
                self.queue.task_done()
            await asyncio.sleep(config.CONVERTER_DELAY)
