"""Central configuration. Everything is read from environment variables.

Four independent pipelines (never mixed), each with its own intermediate channel,
converter bot pool and final channel:
  TeraBox  : TARGET_TERABOX  -> TERABOX_CONVERTER_BOTS  -> FINAL_TERABOX_TARGET
  StreamNet: TARGET_STREAMNET -> STREAMNET_CONVERTER_BOTS -> FINAL_STREAMNET_TARGET
  MDisk    : TARGET_MDISK    -> MDISK_CONVERTER_BOTS    -> FINAL_MDISK_TARGET
  Diskwala : TARGET_DISKWALA -> DISKWALA_CONVERTER_BOTS -> FINAL_DISKWALA_TARGET
Each *_CONVERTER_BOTS value is a comma-separated list (a JSON-style list such as
["bot1", "bot2", "bot3", "bot4"] also works); requests rotate round-robin with failover.
The old single *_CONVERTER_BOT variables still work as a pool of one.
Shared: TARGET_MEDIA, TARGET_ROUGH.
Also required: API_ID, API_HASH, BOT_TOKEN, SESSION_1, SESSION_2, MONGO_URI, ADMINS.
Optional: SOURCE_CHANNELS, HIVE_SOURCES, EXTRA_STREAM_DOMAINS and the tuning values below.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List


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


def _get_username_list(plural: str, singular: str) -> List[str]:
    """Parse bot usernames from `plural` (comma list or JSON-style list), falling back to `singular`."""
    raw = _get(plural) or _get(singular)
    names: List[str] = []
    for part in re.split(r"[,\s;]+", raw):
        name = part.strip().strip("[]\"'").lstrip("@").strip()
        if name and name.lower() not in [existing.lower() for existing in names]:
            names.append(name)
    return names


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

# --- Pipeline channels and converter bots -------------------------------
TARGET_TERABOX: int = _get_int("TARGET_TERABOX")
FINAL_TERABOX_TARGET: int = _get_int("FINAL_TERABOX_TARGET")
TERABOX_CONVERTER_BOTS: List[str] = _get_username_list("TERABOX_CONVERTER_BOTS", "TERABOX_CONVERTER_BOT")
TERABOX_CONVERTER_BOT: str = TERABOX_CONVERTER_BOTS[0] if TERABOX_CONVERTER_BOTS else ""

TARGET_STREAMNET: int = _get_int("TARGET_STREAMNET")
FINAL_STREAMNET_TARGET: int = _get_int("FINAL_STREAMNET_TARGET")
STREAMNET_CONVERTER_BOTS: List[str] = _get_username_list("STREAMNET_CONVERTER_BOTS", "STREAMNET_CONVERTER_BOT")

TARGET_MDISK: int = _get_int("TARGET_MDISK")
FINAL_MDISK_TARGET: int = _get_int("FINAL_MDISK_TARGET")
MDISK_CONVERTER_BOTS: List[str] = _get_username_list("MDISK_CONVERTER_BOTS", "MDISK_CONVERTER_BOT")

TARGET_DISKWALA: int = _get_int("TARGET_DISKWALA")
FINAL_DISKWALA_TARGET: int = _get_int("FINAL_DISKWALA_TARGET")
DISKWALA_CONVERTER_BOTS: List[str] = _get_username_list("DISKWALA_CONVERTER_BOTS", "DISKWALA_CONVERTER_BOT")

# --- Shared channels ------------------------------------------------------
TARGET_MEDIA: int = _get_int("TARGET_MEDIA")
TARGET_ROUGH: int = _get_int("TARGET_ROUGH")

PIPELINES: Dict[str, Dict[str, Any]] = {
    "terabox": {"intermediate": TARGET_TERABOX, "converters": TERABOX_CONVERTER_BOTS, "final": FINAL_TERABOX_TARGET},
    "streamnet": {"intermediate": TARGET_STREAMNET, "converters": STREAMNET_CONVERTER_BOTS, "final": FINAL_STREAMNET_TARGET},
    "mdisk": {"intermediate": TARGET_MDISK, "converters": MDISK_CONVERTER_BOTS, "final": FINAL_MDISK_TARGET},
    "diskwala": {"intermediate": TARGET_DISKWALA, "converters": DISKWALA_CONVERTER_BOTS, "final": FINAL_DISKWALA_TARGET},
}

TARGETS: Dict[str, int] = {
    "terabox_intermediate": TARGET_TERABOX,
    "terabox_final": FINAL_TERABOX_TARGET,
    "streamnet_intermediate": TARGET_STREAMNET,
    "streamnet_final": FINAL_STREAMNET_TARGET,
    "mdisk_intermediate": TARGET_MDISK,
    "mdisk_final": FINAL_MDISK_TARGET,
    "diskwala_intermediate": TARGET_DISKWALA,
    "diskwala_final": FINAL_DISKWALA_TARGET,
    "media": TARGET_MEDIA,
    "rough": TARGET_ROUGH,
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
OCR_ENABLED: bool = _get_bool("OCR_ENABLED", True)
OCR_LANG: str = _get("OCR_LANG", "eng")

# --- Converter pipeline ---------------------------------------------------
CONVERTER_TIMEOUT: int = _get_int("CONVERTER_TIMEOUT", 120)
CONVERTER_DELAY: float = _get_float("CONVERTER_DELAY", 4.0)
CONVERTER_RETRIES: int = _get_int("CONVERTER_RETRIES", 3)
CONVERTER_BUSY_COOLDOWN: int = _get_int("CONVERTER_BUSY_COOLDOWN", 60)

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
    required: Dict[str, Any] = {
        "API_ID": API_ID,
        "API_HASH": API_HASH,
        "BOT_TOKEN": BOT_TOKEN,
        "SESSION_1": SESSION_1,
        "SESSION_2": SESSION_2,
        "MONGO_URI": MONGO_URI,
        "TARGET_TERABOX": TARGET_TERABOX,
        "FINAL_TERABOX_TARGET": FINAL_TERABOX_TARGET,
        "TERABOX_CONVERTER_BOTS": TERABOX_CONVERTER_BOTS,
        "TARGET_STREAMNET": TARGET_STREAMNET,
        "FINAL_STREAMNET_TARGET": FINAL_STREAMNET_TARGET,
        "STREAMNET_CONVERTER_BOTS": STREAMNET_CONVERTER_BOTS,
        "TARGET_MDISK": TARGET_MDISK,
        "FINAL_MDISK_TARGET": FINAL_MDISK_TARGET,
        "MDISK_CONVERTER_BOTS": MDISK_CONVERTER_BOTS,
        "TARGET_DISKWALA": TARGET_DISKWALA,
        "FINAL_DISKWALA_TARGET": FINAL_DISKWALA_TARGET,
        "DISKWALA_CONVERTER_BOTS": DISKWALA_CONVERTER_BOTS,
        "TARGET_MEDIA": TARGET_MEDIA,
        "TARGET_ROUGH": TARGET_ROUGH,
    }
    for name, value in required.items():
        if not value:
            problems.append(name)
    if not ADMINS:
        problems.append("ADMINS")

    usernames = [name.lower() for cfg in PIPELINES.values() for name in cfg["converters"]]
    if len(usernames) != len(set(usernames)):
        problems.append("converter bot usernames must be unique (a bot cannot serve two pools)")
    channel_ids = [value for key, value in TARGETS.items() if value]
    if len(channel_ids) != len(set(channel_ids)):
        problems.append("every target channel ID must be unique (pipelines must not share channels)")
    return problems
