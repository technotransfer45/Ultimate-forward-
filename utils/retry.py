from __future__ import annotations

import asyncio
import functools
import logging
import random
from typing import Any, Awaitable, Callable, Tuple, Type

from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

logger = logging.getLogger(__name__)

MAX_FLOOD_WAITS = 12
TELEGRAM_TEXT_LIMIT = 4096


def flood_seconds(exc: Exception) -> int:
    value = getattr(exc, "value", None)
    if value is None:
        value = getattr(exc, "x", None)
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return 5


async def safe_call(func: Callable[..., Awaitable[Any]], *args: Any, max_retries: int = 5, **kwargs: Any) -> Any:
    """Await func(*args, **kwargs) with FloodWait backoff and network retries."""
    flood_hits = 0
    attempt = 0
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWait as exc:
            flood_hits += 1
            if flood_hits > MAX_FLOOD_WAITS:
                logger.error("Too many FloodWaits for %s, giving up", getattr(func, "__name__", func))
                raise
            wait = flood_seconds(exc) + random.randint(1, 3)
            logger.warning("FloodWait %ss in %s (hit %d)", wait, getattr(func, "__name__", func), flood_hits)
            await asyncio.sleep(wait)
        except (asyncio.TimeoutError, ConnectionError, OSError) as exc:
            attempt += 1
            if attempt > max_retries:
                logger.error("Network error persisted in %s: %s", getattr(func, "__name__", func), exc)
                raise
            delay = min(2 ** attempt, 60)
            logger.warning("Network error in %s: %s (retry in %ss)", getattr(func, "__name__", func), exc, delay)
            await asyncio.sleep(delay)


def flood_guard(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Decorator version of safe_call for coroutine functions."""

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        return await safe_call(func, *args, **kwargs)

    return wrapper


async def retry_async(
    func: Callable[..., Awaitable[Any]],
    *args: Any,
    retries: int = 3,
    delay: float = 2.0,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    **kwargs: Any,
) -> Any:
    """Generic exponential-backoff retry (used for HTTP work)."""
    last_error: BaseException = RuntimeError("retry_async never ran")
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions as exc:
            last_error = exc
            if attempt == retries:
                break
            await asyncio.sleep(delay * (2 ** (attempt - 1)))
    raise last_error


def truncate(text: str, limit: int = TELEGRAM_TEXT_LIMIT) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


async def send_text(client: Any, chat_id: int, text: str, **kwargs: Any) -> Any:
    """Send plain text (no markdown parsing, no link preview) with flood protection."""
    text = truncate(text)
    try:
        return await safe_call(
            client.send_message,
            chat_id,
            text,
            parse_mode=ParseMode.DISABLED,
            disable_web_page_preview=True,
            **kwargs,
        )
    except TypeError:
        return await safe_call(client.send_message, chat_id, text, parse_mode=ParseMode.DISABLED, **kwargs)
