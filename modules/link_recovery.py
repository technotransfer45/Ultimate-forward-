from __future__ import annotations

import html
import logging
import re
from typing import Optional
from urllib.parse import urljoin

import aiohttp

import config
from modules.content_filter import is_shortener_host
from modules.link_extractor import get_host

logger = logging.getLogger(__name__)

META_REFRESH_RE = re.compile(
    r"""<meta[^>]+http-equiv=["']?refresh["']?[^>]+content=["'][^"']*url=([^"'>\s]+)""", re.I
)
JS_REDIRECT_RE = re.compile(r"""(?:window\.)?location(?:\.href)?\s*=\s*["'](https?://[^"']+)["']""", re.I)


async def resolve_url(url: str, max_hops: int = 6) -> Optional[str]:
    """Follow a shortened link until it leaves the shortener domain.

    Returns the final URL, or None when it cannot be resolved (ad walls,
    dead links, loops, timeouts) so the caller can send it to the rough target.
    """
    current = url
    timeout = aiohttp.ClientTimeout(total=20, connect=10)
    headers = {"User-Agent": config.HTTP_USER_AGENT, "Accept": "text/html,*/*;q=0.8"}
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            for _ in range(max_hops):
                if not is_shortener_host(get_host(current)):
                    return current
                async with session.get(current, allow_redirects=False) as response:
                    if response.status in (301, 302, 303, 307, 308):
                        location = response.headers.get("Location", "")
                        if not location:
                            return None
                        current = urljoin(current, location)
                        continue
                    if response.status == 200:
                        body = (await response.content.read(65536)).decode("utf-8", errors="ignore")
                        target = None
                        refresh = META_REFRESH_RE.search(body)
                        if refresh:
                            target = refresh.group(1)
                        else:
                            js = JS_REDIRECT_RE.search(body)
                            if js:
                                target = js.group(1)
                        if not target:
                            return None
                        current = urljoin(current, html.unescape(target))
                        continue
                    return None
            if not is_shortener_host(get_host(current)):
                return current
            return None
    except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
        logger.info("Could not resolve %s: %s", url, exc)
        return None
