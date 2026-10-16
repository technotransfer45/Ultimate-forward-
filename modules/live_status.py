from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

import config
from utils.retry import send_text, safe_call

logger = logging.getLogger(__name__)

DISPLAY = [
    ("messages_seen", "📥 Messages seen"),
    ("terabox_posted", "📦 TeraBox raw posted"),
    ("streamnet_posted", "▶️ StreamNet posted"),
    ("mdisk_posted", "💿 MDisk posted"),
    ("diskwala_posted", "💽 Diskwala posted"),
    ("media_copied", "🎞 Media copied"),
    ("rough_posted", "⚠️ Rough posted"),
    ("terabox_converted", "✅ TeraBox converted"),
    ("streamnet_converted", "✅ StreamNet converted"),
    ("mdisk_converted", "✅ MDisk converted"),
    ("diskwala_converted", "✅ Diskwala converted"),
    ("conversion_failures", "❌ Conversion failures"),
    ("duplicates_skipped", "♻️ Duplicates skipped"),
    ("blacklisted", "🚫 Blacklisted links"),
    ("cleaner_deleted", "🧹 Cleaner deleted"),
    ("message_failures", "💥 Message failures"),
]


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    parts = []
    if days:
        parts.append("%dd" % days)
    if hours or days:
        parts.append("%dh" % hours)
    parts.append("%dm" % minutes)
    return " ".join(parts)


class LiveStatus:
    """In-memory counters flushed to MongoDB, plus an auto-updating dashboard message."""

    def __init__(self, master_bot: Any, stats_repo: Any, chat_id: int) -> None:
        self.master_bot = master_bot
        self.stats_repo = stats_repo
        self.chat_id = chat_id
        self.session: Dict[str, int] = defaultdict(int)
        self.unflushed: Dict[str, int] = defaultdict(int)
        self.base_totals: Dict[str, int] = {}
        self.workers: Dict[int, Dict[str, Any]] = {}
        self.started = time.time()
        self.queue_size_fn: Optional[Callable[[], int]] = None
        self.last_clean: str = "never"
        self._task: Optional["asyncio.Task[None]"] = None

    def inc(self, name: str, amount: int = 1) -> None:
        self.session[name] += amount
        self.unflushed[name] += amount

    def set_worker(self, index: int, **info: Any) -> None:
        self.workers.setdefault(index, {}).update(info)

    def note_clean(self) -> None:
        self.last_clean = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    async def load(self) -> None:
        self.base_totals = await self.stats_repo.totals()

    async def flush(self) -> None:
        if not self.unflushed:
            return
        batch = dict(self.unflushed)
        self.unflushed.clear()
        try:
            await self.stats_repo.incr_many(batch)
        except Exception as exc:
            logger.error("Could not flush stats: %s", exc)
            for name, value in batch.items():
                self.unflushed[name] += value

    def render(self) -> str:
        lines = ["📊 LIVE STATUS", "⏱ Uptime: %s" % _fmt_duration(time.time() - self.started), ""]
        for index in sorted(self.workers):
            info = self.workers[index]
            lines.append(
                "🤖 userbot_%d: %s | %s/%s msgs | live %d"
                % (index + 1, info.get("state", "idle"), info.get("done", 0), info.get("total", 0), self.session.get("live_u%d" % (index + 1), 0))
            )
        queue_size = self.queue_size_fn() if self.queue_size_fn else 0
        lines.append("🔄 Converter queue: %d" % queue_size)
        lines.append("🧹 Last clean: %s" % self.last_clean)
        lines.append("")
        lines.append("Session / All-time")
        for key, label in DISPLAY:
            session_value = self.session.get(key, 0)
            all_time = self.base_totals.get(key, 0) + session_value
            lines.append("%s: %d / %d" % (label, session_value, all_time))
        lines.append("")
        lines.append("Updated: %s" % datetime.now(timezone.utc).strftime("%H:%M:%S UTC"))
        return "\n".join(lines)

    async def _update_dashboard(self) -> None:
        if not self.chat_id:
            return
        text = self.render()
        message_id = await self.stats_repo.get_meta("dashboard_message_id")
        if message_id:
            try:
                await safe_call(self.master_bot.edit_message_text, self.chat_id, int(message_id), text)
                return
            except Exception as exc:
                code = str(getattr(exc, "ID", "") or exc).upper()
                if "MESSAGE_NOT_MODIFIED" in code:
                    return
                logger.info("Dashboard edit failed (%s), sending a new message", code)
        sent = await send_text(self.master_bot, self.chat_id, text)
        await self.stats_repo.set_meta("dashboard_message_id", sent.id)

    async def run(self) -> None:
        while True:
            await asyncio.sleep(config.STATUS_INTERVAL)
            await self.flush()
            try:
                await self._update_dashboard()
            except Exception as exc:
                logger.warning("Dashboard update failed: %s", exc)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("Status loop stopped")
        await self.flush()
