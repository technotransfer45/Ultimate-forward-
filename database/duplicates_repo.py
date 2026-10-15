from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from pymongo.errors import BulkWriteError, DuplicateKeyError


class DuplicatesRepo:
    """Collection `duplicates`: global SHA-256 ledger. The hash is the _id, so
    inserts are atomic and race-free across both userbots."""

    def __init__(self, db) -> None:
        self.col = db["duplicates"]

    async def ensure_indexes(self) -> None:
        await self.col.create_index("kind")
        await self.col.create_index("created_at")

    async def register(self, digest: str, kind: str, meta: Optional[Dict[str, Any]] = None) -> bool:
        """Return True if the digest was new (and is now recorded)."""
        try:
            await self.col.insert_one(
                {
                    "_id": digest,
                    "kind": kind,
                    "meta": meta or {},
                    "created_at": datetime.now(timezone.utc),
                }
            )
            return True
        except DuplicateKeyError:
            return False

    async def register_many(self, digests: Iterable[str], kind: str) -> int:
        now = datetime.now(timezone.utc)
        documents = [{"_id": d, "kind": kind, "meta": {}, "created_at": now} for d in set(digests)]
        inserted = 0
        for start in range(0, len(documents), 1000):
            chunk = documents[start:start + 1000]
            try:
                result = await self.col.insert_many(chunk, ordered=False)
                inserted += len(result.inserted_ids)
            except BulkWriteError as exc:
                inserted += int(exc.details.get("nInserted", 0))
        return inserted

    async def exists(self, digest: str) -> bool:
        return await self.col.count_documents({"_id": digest}, limit=1) > 0

    async def remove(self, digest: str) -> None:
        await self.col.delete_one({"_id": digest})

    async def count(self) -> int:
        return await self.col.estimated_document_count()
