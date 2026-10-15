from __future__ import annotations

from datetime import datetime, timezone


class CheckpointsRepo:
    """Collection `checkpoints`: last processed message id per channel."""

    def __init__(self, db) -> None:
        self.col = db["checkpoints"]

    async def ensure_indexes(self) -> None:
        await self.col.create_index("updated_at")

    async def get(self, chat_id: int) -> int:
        document = await self.col.find_one({"_id": str(chat_id)})
        if not document:
            return 0
        return int(document.get("last_processed_id", 0))

    async def advance(self, chat_id: int, message_id: int) -> None:
        """Move the checkpoint forward only (never backwards)."""
        await self.col.update_one(
            {"_id": str(chat_id)},
            {
                "$max": {"last_processed_id": int(message_id)},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )

    async def reset(self, chat_id: int, value: int = 0) -> None:
        await self.col.update_one(
            {"_id": str(chat_id)},
            {"$set": {"last_processed_id": int(value), "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )

    async def count(self) -> int:
        return await self.col.count_documents({})
