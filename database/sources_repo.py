from typing import Optional, List, Dict, Any
from database.mongo import sources_col

class SourcesRepo:
    @staticmethod
    async def add_source(source_id: str, source_type: str) -> bool:
        existing = await sources_col.find_one({"source_id": source_id})
        if existing:
            return False
        document = {
            "source_id": source_id,
            "source_type": source_type,
            "status": "pending",
            "last_processed_id": 0,
            "total_scanned": 0
        }
        await sources_col.insert_one(document)
        return True

    @staticmethod
    async def remove_source(source_id: str) -> bool:
        result = await sources_col.delete_one({"source_id": source_id})
        return result.deleted_count > 0

    @staticmethod
    async def get_source(source_id: str) -> Optional[Dict[str, Any]]:
        return await sources_col.find_one({"source_id": source_id})

    @staticmethod
    async def get_all_sources() -> List[Dict[str, Any]]:
        cursor = sources_col.find({})
        return await cursor.to_list(length=None)

    @staticmethod
    async def update_source_status(source_id: str, status: str):
        await sources_col.update_one(
            {"source_id": source_id},
            {"$set": {"status": status}}
        )

    @staticmethod
    async def count_by_type(source_type: str) -> int:
        return await sources_col.count_documents({"source_type": source_type})
