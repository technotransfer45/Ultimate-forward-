"""Configuration for the dual-client Telegram batch forwarder / cloner / router bot.

All values come from environment variables and act as *defaults*: sources,
targets and thresholds changed at runtime (/session, /set_duration, /set_size)
are persisted in STATE_FILE and take precedence on the next start.

This module never imports bot.py, so there are no circular imports.
"""

import os
import re
from typing import List, Union

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


def parse_chat_list(raw: str) -> List[ChatId]:
    """Parse a space/comma separated string of chat identifiers.

    Accepts '@channel', 'channel', '-1001234567890', 't.me/channel' and
    'https://t.me/c/1234567890/5'. Invalid tokens are ignored, duplicates removed.
    """
    result: List[ChatId] = []
    seen = set()

    for token in re.split(r"[\s,]+", raw or ""):
        token = token.strip()
        if not token:
            continue

        item: ChatId
        private_link = re.match(
            r"^(?:https?://)?(?:t|telegram)\.(?:me|dog)/c/(\d+)", token, re.IGNORECASE
        )
        public_link = re.match(
            r"^(?:https?://)?(?:t|telegram)\.(?:me|dog)/([A-Za-z][A-Za-z0-9_]{3,31})",
            token,
            re.IGNORECASE,
        )

        if private_link:
            item = int("-100" + private_link.group(1))
        elif public_link:
            item = "@" + public_link.group(1)
        elif _is_int_token(token):
            item = int(token)
        elif re.match(r"^@?[A-Za-z][A-Za-z0-9_]{3,31}$", token):
            item = "@" + token.lstrip("@")
        else:
            continue

        key = item.lower() if isinstance(item, str) else item
        if key in seen:
            continue
        seen.add(key)
        result.append(item)

    return result


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
# Telegram credentials
# --------------------------------------------------------------------------- #
API_ID: int = _int_env("API_ID", 0)
API_HASH: str = os.environ.get("API_HASH", "").strip()
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "").strip()
SESSION_STRING: str = os.environ.get("SESSION_STRING", "").strip()

# --------------------------------------------------------------------------- #
# Default chats (can be replaced at runtime with /session)
# --------------------------------------------------------------------------- #
SOURCE_CHATS: List[ChatId] = parse_chat_list(os.environ.get("SOURCE_CHATS", ""))
TARGET_LINKS_CHAT: int = _int_env("TARGET_LINKS_CHAT", 0)
TARGET_MEDIA_CHAT: int = _int_env("TARGET_MEDIA_CHAT", 0)

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
]
IGNORED_DOMAINS: List[str] = list(
    dict.fromkeys(
        _BASE_IGNORED_DOMAINS + parse_domains(os.environ.get("EXTRA_IGNORED_DOMAINS", ""))
    )
)

# Streaming hosts / shorteners that are ranked first when picking the top
# buttons. Every other external (non-ignored) domain is still allowed because
# the policy also permits "general shorteners".
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

# Spam lines that are stripped from descriptions (regex fragments, case-insensitive).
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

DELIVERY_DELAY: float = 1.5      # mandatory pause between deliveries (seconds)
CLONE_BATCH_SIZE: int = 100      # message ids fetched per get_messages call
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
    if not SESSION_STRING:
        problems.append("SESSION_STRING is missing")
    if not ADMINS:
        problems.append("ADMINS must contain at least one admin user id")
    if problems:
        raise RuntimeError("Invalid configuration: " + "; ".join(problems))
