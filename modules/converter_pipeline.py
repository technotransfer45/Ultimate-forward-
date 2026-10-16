from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

import config
from modules.link_extractor import extract_links, url_hash
from modules.metadata_parser import format_rough_message
from utils.retry import safe_call

logger = logging.getLogger(__name__)

FAILURE_RE = re.compile(r"invalid|error|failed|not supported|unable|limit reached|expired|not found|unsupported", re.I)


class ConverterError(Exception):
    """Raised when the converter bot answers with an explicit failure."""


@dataclass
class ConversionJob:
    """One source post: every link in `caption` is converted, then the whole post is re-posted."""

    links: List[str]
    caption: str
    photo: Optional[Any] = None


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
    """Sends links to one converter bot via a userbot DM and re-posts the converted post.

    Replies are matched with an asyncio.Future. Only one conversion is in flight at a time
    (lock) because the converter bot's replies carry no job id.
    """

    def __init__(
        self,
        userbot: Any,
        converter_username: str,
        final_target: int,
        rough_target: int,
        poster: Callable[..., Awaitable[Any]],
        status: Optional[Any] = None,
        name: str = "converter",
    ) -> None:
        self.name = name
        self.userbot = userbot
        self.converter_username = converter_username
        self.final_target = final_target
        self.rough_target = rough_target
        self.poster = poster
        self.status = status
        self.queue: "asyncio.Queue[ConversionJob]" = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._pending: Optional["asyncio.Future[str]"] = None
        self._original_hash = ""
        self._sent_id = 0
        self._last_seen_id = 0
        self._task: Optional["asyncio.Task[None]"] = None

    def _inc(self, name: str) -> None:
        if self.status is not None:
            self.status.inc(name)

    async def enqueue(self, job: ConversionJob) -> None:
        await self.queue.put(job)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("Converter worker '%s' stopped", self.name)

    async def on_converter_message(self, client: Any, message: Any) -> None:
        """Handler for new and edited messages coming from the converter bot."""
        message_id = int(getattr(message, "id", 0) or 0)
        baseline = self._sent_id
        if message_id > self._last_seen_id:
            self._last_seen_id = message_id
        future = self._pending
        if future is None or future.done() or message_id <= baseline:
            return
        text = getattr(message, "text", None) or getattr(message, "caption", None) or ""
        links = [link for link in extract_links(message) if url_hash(link) != self._original_hash]
        if links:
            future.set_result(links[-1])
            return
        if text and FAILURE_RE.search(text):
            future.set_exception(ConverterError(text[:200]))

    async def _convert_once(self, link: str) -> str:
        async with self._lock:
            loop = asyncio.get_running_loop()
            self._pending = loop.create_future()
            self._original_hash = url_hash(link)
            self._sent_id = self._last_seen_id
            try:
                sent = await safe_call(self.userbot.send_message, self.converter_username, link)
                self._sent_id = max(self._sent_id, int(sent.id))
                return await asyncio.wait_for(self._pending, timeout=config.CONVERTER_TIMEOUT)
            finally:
                self._pending = None

    async def _convert(self, link: str) -> str:
        last_error: Exception = ConverterError("conversion never attempted")
        for attempt in range(1, config.CONVERTER_RETRIES + 1):
            try:
                return await self._convert_once(link)
            except (asyncio.TimeoutError, ConverterError) as exc:
                last_error = exc
                logger.warning("[%s] attempt %d/%d failed for %s: %s", self.name, attempt, config.CONVERTER_RETRIES, link, exc)
                await asyncio.sleep(5 * attempt)
        raise last_error

    async def _process_job(self, job: ConversionJob) -> None:
        converted: Dict[str, str] = {}
        failures: List[str] = []
        failed_links: List[str] = []
        for index, link in enumerate(dict.fromkeys(job.links)):
            if index > 0:
                await asyncio.sleep(config.CONVERTER_DELAY)
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
                    "%s converter failed for %d link(s)%s" % (self.name.capitalize(), len(failures), suffix),
                    "%s converter pipeline" % self.name,
                ),
                None,
            )

    async def _worker(self) -> None:
        while True:
            job = await self.queue.get()
            try:
                await self._process_job(job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("[%s] job failed: %s", self.name, exc)
                self._inc("conversion_failures")
            finally:
                self.queue.task_done()
            await asyncio.sleep(config.CONVERTER_DELAY)
