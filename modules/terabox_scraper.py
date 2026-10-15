from __future__ import annotations

import asyncio
import html
import logging
import re
from typing import Dict, List

import aiohttp

import config
from modules.link_extractor import normalize_url
from utils.retry import retry_async

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    """Raised when a hive/share page cannot be fetched or yields nothing usable."""


SHARE_URL_RE = re.compile(r"https?://[A-Za-z0-9.\-]+/s/(1?[A-Za-z0-9_-]{6,})")
SURL_RE = re.compile(r"[?&]surl=([A-Za-z0-9_-]{6,})")
REL_SHARE_RE = re.compile(r"""["'(=]/s/(1[A-Za-z0-9_-]{6,})""")
TITLE_KEY_RE = re.compile(
    r"""["'](?:title|name|server_filename|file_name|fileName|filename|desc|share_title)["']\s*:\s*["']([^"']{3,200})["']""",
    re.I,
)
OG_TITLE_RE = re.compile(r"""<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']""", re.I)
HTML_TITLE_RE = re.compile(r"<title[^>]*>([^<]{3,200})</title>", re.I)
GENERIC_TITLES = {"terabox", "nephobox", "share", "sharing", "hive", "channel", "undefined", "null", "download"}


def _unescape(body: str) -> str:
    text = html.unescape(body)
    text = text.replace("\\/", "/").replace("\\u002F", "/").replace("\\u0026", "&").replace("\\u003d", "=")
    return text


def _good_title(candidate: str) -> bool:
    cleaned = candidate.strip()
    if len(cleaned) < 3 or cleaned.lower() in GENERIC_TITLES:
        return False
    if cleaned.lower().startswith(("http", "/")):
        return False
    return bool(re.search(r"[A-Za-z]{3,}", cleaned))


def _title_near(text: str, position: int) -> str:
    window = text[max(0, position - 400): position + 600]
    for match in TITLE_KEY_RE.finditer(window):
        candidate = match.group(1)
        if _good_title(candidate):
            return candidate.strip()
    return ""


def parse_hive_html(body: str) -> List[Dict[str, str]]:
    """Extract {url, title} pairs for every /s/ share link on a hive page."""
    text = _unescape(body)
    hits: Dict[str, int] = {}
    for regex in (SHARE_URL_RE, REL_SHARE_RE):
        for match in regex.finditer(text):
            share_id = match.group(1)
            hits.setdefault(share_id, match.start())
    for match in SURL_RE.finditer(text):
        share_id = match.group(1)
        if not share_id.startswith("1"):
            share_id = "1" + share_id
        hits.setdefault(share_id, match.start())

    entries: List[Dict[str, str]] = []
    for share_id, position in sorted(hits.items(), key=lambda item: item[1]):
        entries.append({"url": normalize_url("https://terabox.com/s/" + share_id), "title": _title_near(text, position)})
    return entries


async def _fetch(url: str) -> str:
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    headers = {"User-Agent": config.HTTP_USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url, allow_redirects=True) as response:
            if response.status != 200:
                raise ScraperError("HTTP %d for %s" % (response.status, url))
            return await response.text(errors="ignore")


async def scrape_hive(url: str) -> List[Dict[str, str]]:
    """Scrape a TeraBox/NephoBox hive channel page; raises ScraperError on failure."""
    try:
        body = await retry_async(
            _fetch, url, retries=3, delay=3.0,
            exceptions=(aiohttp.ClientError, asyncio.TimeoutError, ScraperError),
        )
    except (aiohttp.ClientError, asyncio.TimeoutError, ScraperError) as exc:
        raise ScraperError("Could not fetch hive page: %s" % exc) from exc
    entries = parse_hive_html(body)
    if not entries:
        raise ScraperError("No /s/ share links found on hive page")
    logger.info("Hive %s yielded %d links", url, len(entries))
    return entries


async def fetch_share_title(url: str) -> str:
    """Best-effort title lookup for a single share link; returns '' when unavailable."""
    try:
        body = await _fetch(url)
    except (aiohttp.ClientError, asyncio.TimeoutError, ScraperError):
        return ""
    for regex in (OG_TITLE_RE, HTML_TITLE_RE):
        match = regex.search(body)
        if match:
            title = html.unescape(match.group(1)).strip()
            title = re.sub(r"\s*[-|–]\s*(?:Tera|Nepho|Du)?Box.*$", "", title, flags=re.I).strip()
            if _good_title(title):
                return title
    return ""
