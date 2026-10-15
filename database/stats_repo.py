from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict


class StatsRepo:
    """Collection `statistics`: all-time counters and small metadata values."""

    def __init__(self, db) -> None:
        self.col = db["statistics"]

    async def ensure_indexes(self) -> None:
        await self.col.create_index("updated_at")

    async def incr_many(self, counters: Dict[str, int]) -> None:
        clean = {name: int(value) for name, value in counters.items() if value}
        if not clean:
            return
        await self.col.update_one(
            {"_id": "totals"},
            {"$inc": clean, "$set": {"updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )

    async def totals(self) -> Dict[str, int]:
        document = await self.col.find_one({"_id": "totals"}) or {}
        return {k: int(v) for k, v in document.items() if k not in ("_id", "updated_at")}

    async def set_meta(self, key: str, value: Any) -> None:
        await self.col.update_one({"_id": "meta:" + key}, {"$set": {"value": value}}, upsert=True)

    async def get_meta(self, key: str, default: Any = None) -> Any:
        document = await self.col.find_one({"_id": "meta:" + key})
        if not document:
            return default
        return document.get("value", default)
