from __future__ import annotations

import logging
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

logger = logging.getLogger(__name__)


class Mongo:
    """Thin wrapper around the Motor client."""

    def __init__(self, uri: str, db_name: str) -> None:
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None

    async def connect(self) -> AsyncIOMotorDatabase:
        self.client = AsyncIOMotorClient(self.uri, serverSelectionTimeoutMS=20000, retryWrites=True)
        await self.client.admin.command("ping")
        self.db = self.client[self.db_name]
        logger.info("Connected to MongoDB database '%s'", self.db_name)
        return self.db

    def database(self) -> AsyncIOMotorDatabase:
        if self.db is None:
            raise RuntimeError("MongoDB is not connected yet")
        return self.db

    async def close(self) -> None:
        if self.client is not None:
            self.client.close()
            logger.info("MongoDB connection closed")
