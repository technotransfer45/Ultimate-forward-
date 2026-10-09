from typing import Optional, Dict, Any
from database.mongo import duplicates_col

class DuplicatesRepo:
    @staticmethod
    async def is_duplicate(item_hash: str) -> bool:
        found = await duplicates_col.find_one({"hash": item_hash})
        return found is not None

    @staticmethod
    async def add_item(item_hash: str, raw_data: str, destination: int, message_id: int):
        doc = {
            "hash": item_hash,
            "raw_data": raw_data,
            "destination": destination,
            "message_id": message_id
        }
        try:
            await duplicates_col.insert_one(doc)
        except Exception:
            pass

    @staticmethod
    async def get_by_hash(item_hash: str) -> Optional[Dict[str, Any]]:
        return await duplicates_col.find_one({"hash": item_hash})

    @staticmethod
    async def total_unique_items() -> int:
        return await duplicates_col.count_documents({})
