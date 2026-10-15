from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from pyrogram.errors import RPCError

import config
from modules.link_extractor import extract_links, media_hash, url_hash
from utils.retry import safe_call

logger = logging.getLogger(__name__)


def message_hashes(message: Any) -> List[str]:
    """All dedup hashes carried by a target-channel message (links + media)."""
    hashes: List[str] = [url_hash(link) for link in extract_links(message)]
    for kind in ("video", "document", "animation"):
        media = getattr(message, kind, None)
        unique_id = getattr(media, "file_unique_id", "") if media is not None else ""
        if unique_id:
            hashes.append(media_hash(unique_id))
    return list(dict.fromkeys(hashes))


class TargetCleaner:
    """Scans target channels and deletes duplicate posts, always keeping the oldest."""

    def __init__(self, client: Any, targets: Dict[str, int], duplicates: Any, status: Optional[Any] = None) -> None:
        self.client = client
        self.targets = targets
        self.duplicates = duplicates
        self.status = status
        self.running = False

    async def _page(self, chat_id: int, offset_id: int) -> List[Any]:
        return [m async for m in self.client.get_chat_history(chat_id, limit=100, offset_id=offset_id)]

    async def clean_target(self, name: str, chat_id: int) -> int:
        records: List[Tuple[int, List[str]]] = []
        offset = 0
        while True:
            page = await safe_call(self._page, chat_id, offset)
            if not page:
                break
            for message in page:
                hashes = message_hashes(message)
                if hashes:
                    records.append((message.id, hashes))
            offset = page[-1].id
            await asyncio.sleep(0.6)

        records.sort(key=lambda record: record[0])
        seen: Set[str] = set()
        delete_ids: List[int] = []
        for message_id, hashes in records:
            if all(h in seen for h in hashes):
                delete_ids.append(message_id)
                continue
            seen.update(hashes)

        deleted = 0
        for start in range(0, len(delete_ids), 100):
            chunk = delete_ids[start:start + 100]
            await safe_call(self.client.delete_messages, chat_id, chunk)
            deleted += len(chunk)
            await asyncio.sleep(config.CLEAN_DELETE_DELAY)

        await self.duplicates.register_many(seen, "cleaner")
        logger.info("Cleaner[%s]: scanned %d posts, removed %d duplicates", name, len(records), deleted)
        return deleted

    async def clean_all(self) -> Dict[str, int]:
        if self.running:
            return {}
        self.running = True
        results: Dict[str, int] = {}
        try:
            for name, chat_id in self.targets.items():
                if not chat_id:
                    continue
                try:
                    results[name] = await self.clean_target(name, chat_id)
                    if self.status is not None:
                        self.status.inc("cleaner_deleted", results[name])
                except RPCError as exc:
                    logger.error("Cleaner[%s] failed: %s", name, exc)
                    results[name] = -1
        finally:
            self.running = False
        return results
