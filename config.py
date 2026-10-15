"""Central configuration. Everything is read from environment variables.

Required: API_ID, API_HASH, BOT_TOKEN, SESSION_1, SESSION_2, MONGO_URI,
TARGET_TERABOX, TARGET_STREAM, TARGET_MEDIA, TARGET_ROUGH,
FINAL_CONVERTED_TARGET, CONVERTER_BOT_USERNAME,
FINAL_STREAM_TARGET, STREAM_CONVERTER_BOT_USERNAME, ADMINS.
STREAM_CONVERTER_USERBOT (1 or 2, default 2) picks which userbot DMs the stream converter.
Optional: SOURCE_CHANNELS, HIVE_SOURCES, EXTRA_STREAM_DOMAINS and the tuning values below.
"""
from __future__ import annotations

import os
import re
from typing import Dict, List


def _get(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _get_int(name: str, default: int = 0) -> int:
    raw = _get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    raw = _get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _get_bool(name: str, default: bool = False) -> bool:
    raw = _get(name).lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _split(raw: str) -> List[str]:
    return [part for part in re.split(r"[,\s;]+", raw) if part]


def _get_int_list(name: str) -> List[int]:
    values: List[int] = []
    for part in _split(_get(name)):
        try:
            values.append(int(part))
        except ValueError:
            continue
    return values


# --- Telegram credentials -------------------------------------------------
API_ID: int = _get_int("API_ID")
API_HASH: str = _get("API_HASH")
BOT_TOKEN: str = _get("BOT_TOKEN")
SESSION_1: str = _get("SESSION_1")
SESSION_2: str = _get("SESSION_2")
ADMINS: List[int] = _get_int_list("ADMINS")
STATUS_CHAT_ID: int = _get_int("STATUS_CHAT_ID", ADMINS[0] if ADMINS else 0)

# --- Database -------------------------------------------------------------
MONGO_URI: str = _get("MONGO_URI")
DB_NAME: str = _get("DB_NAME", "tg_automation")

# --- Targets --------------------------------------------------------------
TARGET_TERABOX: int = _get_int("TARGET_TERABOX")
TARGET_STREAM: int = _get_int("TARGET_STREAM")
TARGET_MEDIA: int = _get_int("TARGET_MEDIA")
TARGET_ROUGH: int = _get_int("TARGET_ROUGH")
FINAL_CONVERTED_TARGET: int = _get_int("FINAL_CONVERTED_TARGET")
CONVERTER_BOT_USERNAME: str = _get("CONVERTER_BOT_USERNAME").lstrip("@")
FINAL_STREAM_TARGET: int = _get_int("FINAL_STREAM_TARGET")
STREAM_CONVERTER_BOT_USERNAME: str = _get("STREAM_CONVERTER_BOT_USERNAME").lstrip("@")
STREAM_CONVERTER_USERBOT: int = 1 if _get_int("STREAM_CONVERTER_USERBOT", 2) == 1 else 2

TARGETS: Dict[str, int] = {
    "terabox_raw": TARGET_TERABOX,
    "streaming": TARGET_STREAM,
    "media": TARGET_MEDIA,
    "rough": TARGET_ROUGH,
    "final_converted": FINAL_CONVERTED_TARGET,
    "final_stream": FINAL_STREAM_TARGET,
}

# --- Sources seeded at startup (more can be added with /addsource) --------
SOURCE_CHANNELS: List[int] = _get_int_list("SOURCE_CHANNELS")
HIVE_SOURCES: List[str] = [u for u in _split(_get("HIVE_SOURCES")) if u.startswith("http")]
EXTRA_STREAM_DOMAINS: List[str] = [d.lower() for d in _split(_get("EXTRA_STREAM_DOMAINS"))]

# --- Media rules ----------------------------------------------------------
MIN_FILE_SIZE: int = _get_int("MIN_FILE_SIZE_MB", 50) * 1024 * 1024
MIN_DURATION: int = _get_int("MIN_DURATION_MINUTES", 10) * 60
ALLOW_ANY_DOCUMENT: bool = _get_bool("ALLOW_ANY_DOCUMENT", False)

# --- Throttling / scanning ------------------------------------------------
POST_INTERVAL: float = _get_float("POST_INTERVAL", 3.0)
SCAN_BATCH_SIZE: int = min(max(_get_int("SCAN_BATCH_SIZE", 100), 1), 200)
SCAN_DELAY: float = _get_float("SCAN_DELAY", 1.5)
RESCAN_INTERVAL: int = _get_int("RESCAN_INTERVAL", 900)
HIVE_INTERVAL: int = _get_int("HIVE_INTERVAL", 3600)
INITIAL_SCAN_LIMIT: int = _get_int("INITIAL_SCAN_LIMIT", 0)
MESSAGE_RETRIES: int = _get_int("MESSAGE_RETRIES", 3)
LIVE_CONCURRENCY: int = _get_int("LIVE_CONCURRENCY", 8)
FETCH_SHARE_TITLES: bool = _get_bool("FETCH_SHARE_TITLES", True)

# --- Converter pipeline ---------------------------------------------------
CONVERTER_TIMEOUT: int = _get_int("CONVERTER_TIMEOUT", 120)
CONVERTER_DELAY: float = _get_float("CONVERTER_DELAY", 4.0)
CONVERTER_RETRIES: int = _get_int("CONVERTER_RETRIES", 3)

# --- Cleaner / dashboard / web -------------------------------------------
CLEAN_ON_STARTUP: bool = _get_bool("CLEAN_ON_STARTUP", True)
CLEAN_INTERVAL_HOURS: int = _get_int("CLEAN_INTERVAL_HOURS", 24)
CLEAN_DELETE_DELAY: float = _get_float("CLEAN_DELETE_DELAY", 1.5)
STATUS_INTERVAL: int = _get_int("STATUS_INTERVAL", 45)
PORT: int = _get_int("PORT", 8080)

HTTP_USER_AGENT: str = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"
)


def validate_config() -> List[str]:
    """Return a list of missing/invalid settings (empty list means OK)."""
    problems: List[str] = []
    required = {
        "API_ID": API_ID,
        "API_HASH": API_HASH,
        "BOT_TOKEN": BOT_TOKEN,
        "SESSION_1": SESSION_1,
        "SESSION_2": SESSION_2,
        "MONGO_URI": MONGO_URI,
        "TARGET_TERABOX": TARGET_TERABOX,
        "TARGET_STREAM": TARGET_STREAM,
        "TARGET_MEDIA": TARGET_MEDIA,
        "TARGET_ROUGH": TARGET_ROUGH,
        "FINAL_CONVERTED_TARGET": FINAL_CONVERTED_TARGET,
        "CONVERTER_BOT_USERNAME": CONVERTER_BOT_USERNAME,
        "FINAL_STREAM_TARGET": FINAL_STREAM_TARGET,
        "STREAM_CONVERTER_BOT_USERNAME": STREAM_CONVERTER_BOT_USERNAME,
    }
    for name, value in required.items():
        if not value:
            problems.append(name)
    if not ADMINS:
        problems.append("ADMINS")
    return problems
