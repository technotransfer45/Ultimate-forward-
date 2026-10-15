from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class SourcesRepo:
    """Collection `sources`: channels and hive pages to ingest from."""

    def __init__(self, db) -> None:
        self.col = db["sources"]

    async def ensure_indexes(self) -> None:
        await self.col.create_index("type")
        await self.col.create_index("enabled")

    async def add(self, key: str, source_type: str, ref: Any, title: str = "") -> bool:
        """Insert a source. Returns True if it was newly inserted."""
        document = {
            "type": source_type,
            "ref": ref,
            "title": title,
            "enabled": True,
            "added_at": datetime.now(timezone.utc),
            "last_error": None,
        }
        result = await self.col.update_one({"_id": key}, {"$setOnInsert": document}, upsert=True)
        return result.upserted_id is not None

    async def remove(self, key: str) -> bool:
        result = await self.col.delete_one({"_id": key})
        return result.deleted_count > 0

    async def list_sources(self, source_type: Optional[str] = None, enabled_only: bool = True) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if source_type:
            query["type"] = source_type
        if enabled_only:
            query["enabled"] = True
        cursor = self.col.find(query).sort("_id", 1)
        return [doc async for doc in cursor]

    async def count(self, source_type: Optional[str] = None) -> int:
        query: Dict[str, Any] = {}
        if source_type:
            query["type"] = source_type
        return await self.col.count_documents(query)

    async def set_enabled(self, key: str, enabled: bool) -> None:
        await self.col.update_one({"_id": key}, {"$set": {"enabled": enabled}})

    async def mark_error(self, key: str, error: str) -> None:
        await self.col.update_one(
            {"_id": key},
            {"$set": {"last_error": error, "last_error_at": datetime.now(timezone.utc)}},
        )
