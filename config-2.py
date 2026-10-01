"""Configuration for the dual-userbot Telegram batch forwarder / ingestion / filter bot.

All values come from environment variables and act as *defaults*: sources, targets
and thresholds changed at runtime (/session, /set_duration, /set_size) are persisted
in STATE_FILE and take precedence on the next start.

This module never imports bot.py, so there are no circular imports.
"""

import os
import re
from typing import List, Optional, Set, Tuple, Union

ChatId = Union[int, str]


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #
def _int_env(name: str, default: int = 0) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _is_int_token(token: str) -> bool:
    stripped = token.lstrip("-")
    return stripped != "" and stripped.isdigit()


def _parse_token(token: str) -> Optional[ChatId]:
    """Turn one token into an int id, an '@username', or None if unreadable."""
    token = token.strip()
    if not token:
        return None

    private_link = re.match(
        r"^(?:https?://)?(?:t|telegram)\.(?:me|dog)/c/(\d+)", token, re.IGNORECASE
    )
    if private_link:
        return int("-100" + private_link.group(1))

    public_link = re.match(
        r"^(?:https?://)?(?:t|telegram)\.(?:me|dog)/([A-Za-z][A-Za-z0-9_]{3,31})",
        token,
        re.IGNORECASE,
    )
    if public_link:
        return "@" + public_link.group(1)

    if _is_int_token(token):
        return int(token)

    if re.match(r"^@?[A-Za-z][A-Za-z0-9_]{3,31}$", token):
        return "@" + token.lstrip("@")
    return None


def parse_chat_list(raw: str) -> List[ChatId]:
    """Parse ids / @usernames / t.me links (used for single-chat command arguments)."""
    result: List[ChatId] = []
    seen = set()
    for token in re.split(r"[\s,]+", raw or ""):
        item = _parse_token(token)
        if item is None:
            continue
        key = item.lower() if isinstance(item, str) else item
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def parse_chat_ids(raw: str) -> Tuple[List[int], List[str]]:
    """Parse SOURCE chat ids only (raw integers, or t.me/c/ links).

    Returns (ids, rejected_tokens). Usernames are rejected on purpose: resolving
    hundreds of usernames would need one get_chat() RPC each, which triggers floods.
    """
    ids: List[int] = []
    rejected: List[str] = []
    for token in re.split(r"[\s,]+", raw or ""):
        token = token.strip()
        if not token:
            continue
        item = _parse_token(token)
        if isinstance(item, int):
            if item not in ids:
                ids.append(item)
        else:
            rejected.append(token)
    return ids, rejected


def parse_int_list(raw: str) -> List[int]:
    values: List[int] = []
    for token in re.split(r"[\s,]+", raw or ""):
        token = token.strip()
        if _is_int_token(token):
            value = int(token)
            if value not in values:
                values.append(value)
    return values


def parse_domains(raw: str) -> List[str]:
    domains: List[str] = []
    for token in re.split(r"[\s,]+", raw or ""):
        token = token.strip().lower().lstrip(".")
        if token.startswith("www."):
            token = token[4:]
        if token and token not in domains:
            domains.append(token)
    return domains


def parse_phrases(raw: str) -> List[str]:
    """Split a '|' separated list of plain phrases."""
    return [p.strip() for p in (raw or "").split("|") if p.strip()]


# --------------------------------------------------------------------------- #
# Telegram credentials (two userbots supported)
# --------------------------------------------------------------------------- #
API_ID: int = _int_env("API_ID", 0)
API_HASH: str = os.environ.get("API_HASH", "").strip()
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "").strip()

# SESSION_STRING_1 is the primary userbot; SESSION_STRING is the single-account fallback.
SESSION_STRING_1: str = (
    os.environ.get("SESSION_STRING_1", "").strip() or os.environ.get("SESSION_STRING", "").strip()
)
SESSION_STRING_2: str = os.environ.get("SESSION_STRING_2", "").strip()
if SESSION_STRING_2 and SESSION_STRING_2 == SESSION_STRING_1:
    SESSION_STRING_2 = ""  # the same session twice would kick itself out (AUTH_KEY_DUPLICATED)
SESSION_STRINGS: List[str] = [s for s in (SESSION_STRING_1, SESSION_STRING_2) if s]

# --------------------------------------------------------------------------- #
# Default chats (can be replaced at runtime with /session)
# --------------------------------------------------------------------------- #
SOURCE_CHATS: List[int] = parse_chat_ids(os.environ.get("SOURCE_CHATS", ""))[0]
TARGET_LINKS_CHAT: int = _int_env("TARGET_LINKS_CHAT", 0)
TARGET_MEDIA_CHAT: int = _int_env("TARGET_MEDIA_CHAT", 0)

# Channels that are permanently ignored (live events AND history).
IGNORED_SOURCE_CHATS: Set[int] = set(parse_int_list(os.environ.get("IGNORED_SOURCE_CHATS", "")))

# --------------------------------------------------------------------------- #
# Content filters (defaults, adjustable at runtime)
# --------------------------------------------------------------------------- #
MIN_VIDEO_DURATION: int = _int_env("MIN_VIDEO_DURATION", 600)  # seconds (10 min)
MIN_FILE_SIZE_MB: float = _float_env("MIN_FILE_SIZE_MB", 100)  # megabytes

# Domains that are always dropped.
_BASE_IGNORED_DOMAINS = [
    "t.me",
    "telegram.me",
    "telegram.dog",
    "youtube.com",
    "youtu.be",
    "drive.google.com",
    "google.com",       # also covers play.google.com and google search results
    "play.google.com",
    "rigi.club",
    "telegra.ph",
    "bit.ly",
]
IGNORED_DOMAINS: List[str] = list(
    dict.fromkeys(
        _BASE_IGNORED_DOMAINS + parse_domains(os.environ.get("EXTRA_IGNORED_DOMAINS", ""))
    )
)

# Streaming hosts / shorteners ranked first when choosing the top buttons. Every other
# external (non-ignored) domain is still allowed because general shorteners are allowed.
_BASE_PRIORITY_DOMAINS = ["terabox.com", "1024tera.com", "terasharelink.com"]
PRIORITY_DOMAINS: List[str] = list(
    dict.fromkeys(
        _BASE_PRIORITY_DOMAINS + parse_domains(os.environ.get("EXTRA_PRIORITY_DOMAINS", ""))
    )
)
_BASE_PRIORITY_KEYWORDS = [
    "terabox",
    "streamnet",
    "streaam",
    "vidhide",
    "pddisk",
    "diskwala",
    "shareus",
]
PRIORITY_KEYWORDS: List[str] = list(
    dict.fromkeys(
        _BASE_PRIORITY_KEYWORDS
        + [k.lower() for k in parse_phrases(os.environ.get("EXTRA_PRIORITY_KEYWORDS", ""))]
    )
)

# Movie / TV context keywords. A link post qualifies when its text, caption or button
# labels contain one of these (whole words; plurals and "web-dl"/"web dl"/"web.dl" variants
# are matched), or when it carries a link to a recognized movie/cloud/file host (below).
_BASE_MOVIE_KEYWORDS = [
    "4k", "2160p", "1080p", "720p", "480p", "360p",
    "hevc", "x264", "x265", "h264", "h265", "10bit",
    "web-dl", "webrip", "bluray", "brrip", "bdrip", "hdrip", "dvdrip", "hdtv", "hdcam", "camrip",
    "movie", "episode", "season", "dual audio",
]
MOVIE_KEYWORDS: List[str] = list(
    dict.fromkeys(
        _BASE_MOVIE_KEYWORDS
        + [k.lower() for k in parse_phrases(os.environ.get("EXTRA_MOVIE_KEYWORDS", ""))]
    )
)

# Recognized movie / cloud / file hosts. A host matches when it contains one of these
# keywords; the streaming hosts in PRIORITY_DOMAINS / PRIORITY_KEYWORDS always count too.
_BASE_HOST_KEYWORDS = ["terabox", "1024tera", "hubcloud", "gdtot", "drive", "dood", "filecrypt"]
HOST_KEYWORDS: List[str] = list(
    dict.fromkeys(
        _BASE_HOST_KEYWORDS
        + [k.lower() for k in parse_phrases(os.environ.get("EXTRA_HOST_KEYWORDS", ""))]
    )
)

# Well-known shortener / file-locker domains (exact domain or any subdomain).
_BASE_SHORTENER_DOMAINS = [
    "gplinks.co", "gplinks.in", "droplink.co", "ouo.io", "ouo.press", "shorte.st",
    "shrinkme.io", "shrinkearn.com", "adrinolinks.in", "tnlink.in", "arolinks.com",
    "linkshortify.com", "urlshortx.com", "krakenfiles.com", "gofile.io", "pixeldrain.com",
    "streamtape.com", "filepress.cloud", "mega.nz",
]
KNOWN_SHORTENER_DOMAINS: List[str] = list(
    dict.fromkeys(_BASE_SHORTENER_DOMAINS + parse_domains(os.environ.get("EXTRA_SHORTENER_DOMAINS", "")))
)

# Adult content shield: matched as whole words in text, caption and file names.
_BASE_ADULT_KEYWORDS = ["18+", "adult", "porn", "sex", "nude", "hot", "leaks", "desidesi"]
ADULT_KEYWORDS: List[str] = list(
    dict.fromkeys(
        _BASE_ADULT_KEYWORDS
        + [k.lower() for k in parse_phrases(os.environ.get("EXTRA_ADULT_KEYWORDS", ""))]
    )
)

# Promotional lines that are removed entirely (regex fragments, case-insensitive).
_BASE_SPAM_PATTERNS = [
    r"just\s+click\s+(?:on\s+)?(?:the\s+)?blue\s+link",
    r"click\s+(?:on\s+)?(?:the\s+)?blue\s+link",
    r"movies?\s+uploading",
    r"uploading\s*\.{2,}",
]
SPAM_PATTERNS: List[str] = _BASE_SPAM_PATTERNS + [
    re.escape(p) for p in parse_phrases(os.environ.get("EXTRA_SPAM_PHRASES", ""))
]

# --------------------------------------------------------------------------- #
# Admins, persistence, limits
# --------------------------------------------------------------------------- #
ADMINS: List[int] = parse_int_list(os.environ.get("ADMINS", ""))
STATE_FILE: str = os.environ.get("STATE_FILE", "state.json").strip() or "state.json"
MEDIA_SEEN_FILE: str = os.environ.get("MEDIA_SEEN_FILE", "media_seen.json").strip() or "media_seen.json"
MEDIA_SEEN_MAX: int = _int_env("MEDIA_SEEN_MAX", 200000)  # remembered file_unique_ids
SEEN_MESSAGES_MAX: int = 50000  # in-memory (chat_id, message_id) cache for live de-duplication

QUEUE_MAXSIZE: int = 1000
DELIVERY_DELAY: float = 1.5           # mandatory pause between outgoing messages (seconds)
INGEST_CHANNEL_DELAY: float = 5.0     # pause between channels during batch ingestion
HISTORY_FETCH_DELAY: float = 0.5      # pause between get_messages batches
CLONE_BATCH_SIZE: int = 100           # message ids fetched per get_messages call
MAX_HISTORY_PER_SOURCE: int = _int_env("MAX_HISTORY_PER_SOURCE", 0)  # 0 = whole history
DIALOG_REFRESH_INTERVAL: float = 600.0  # min seconds between get_dialogs() per userbot

CLEANDUP_SCAN_LIMIT: int = 2000  # messages scanned by /cleandup
STATUS_SCAN_LIMIT: int = 1000    # messages scanned by /status <chat>
DELETE_BATCH_SIZE: int = 100
DELETE_BATCH_DELAY: float = 1.0
MAX_BUTTONS: int = 5
MAX_CONSECUTIVE_FAILURES: int = 5


def validate() -> None:
    """Raise RuntimeError listing every missing / invalid setting."""
    problems: List[str] = []
    if not API_ID:
        problems.append("API_ID (int) is missing")
    if not API_HASH:
        problems.append("API_HASH is missing")
    if not BOT_TOKEN:
        problems.append("BOT_TOKEN is missing")
    if not SESSION_STRINGS:
        problems.append("SESSION_STRING_1 (or SESSION_STRING) is missing")
    if not ADMINS:
        problems.append("ADMINS must contain at least one admin user id")
    if problems:
        raise RuntimeError("Invalid configuration: " + "; ".join(problems))
