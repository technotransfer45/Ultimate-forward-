from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from pyrogram.errors import RPCError

import config
from modules.link_extractor import media_hash, text_hash
from utils.retry import safe_call

logger = logging.getLogger(__name__)

VIDEO_EXTENSIONS = (".mkv", ".mp4", ".avi", ".mov", ".webm", ".m4v", ".ts", ".flv", ".wmv", ".mpg", ".mpeg", ".3gp")


@dataclass
class MediaInfo:
    kind: str
    file_unique_id: str
    file_name: str
    size: int
    duration: int
    mime_type: str


def get_media_info(message: Any) -> Optional[MediaInfo]:
    """Describe the video/document attached to a message, if any."""
    for kind in ("video", "document", "animation"):
        media = getattr(message, kind, None)
        if media is None:
            continue
        return MediaInfo(
            kind=kind,
            file_unique_id=getattr(media, "file_unique_id", "") or "",
            file_name=getattr(media, "file_name", "") or "",
            size=int(getattr(media, "file_size", 0) or 0),
            duration=int(getattr(media, "duration", 0) or 0),
            mime_type=getattr(media, "mime_type", "") or "",
        )
    return None


def _looks_like_video(info: MediaInfo) -> bool:
    if info.kind in ("video", "animation"):
        return True
    if info.mime_type.lower().startswith("video/"):
        return True
    return info.file_name.lower().endswith(VIDEO_EXTENSIONS)


def qualifies(info: MediaInfo) -> bool:
    """Rule: FileSize >= 50MB OR Duration >= 10 minutes."""
    if info.kind == "document" and not _looks_like_video(info) and not config.ALLOW_ANY_DOCUMENT:
        return False
    return info.size >= config.MIN_FILE_SIZE or info.duration >= config.MIN_DURATION


def media_digest(info: MediaInfo) -> str:
    if info.file_unique_id:
        return media_hash(info.file_unique_id)
    return text_hash("media-fallback:%d:%d:%s" % (info.size, info.duration, info.file_name.lower()))


def is_forward_restricted(exc: Exception) -> bool:
    code = str(getattr(exc, "ID", "") or getattr(exc, "MESSAGE", "") or exc).upper()
    return "FORWARDS_RESTRICTED" in code or "CHAT_RESTRICTED" in code or "PROTECTED" in code


async def copy_clean(message: Any, target: int) -> Any:
    """Copy a message to the target with no 'Forwarded from' header."""
    return await safe_call(message.copy, target, disable_notification=True)


def describe_media(info: MediaInfo) -> str:
    size_mb = info.size / (1024 * 1024)
    minutes = info.duration / 60
    return "%s | %.1f MB | %.1f min | %s" % (info.kind, size_mb, minutes, info.file_name or "unnamed")


__all__ = [
    "MediaInfo", "get_media_info", "qualifies", "media_digest", "is_forward_restricted",
    "copy_clean", "describe_media", "RPCError",
]
