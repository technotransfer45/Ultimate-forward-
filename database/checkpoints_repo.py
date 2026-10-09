from database.mongo import checkpoints_col

class CheckpointsRepo:
    @staticmethod
    async def get_last_id(source_id: str) -> int:
        record = await checkpoints_col.find_one({"source_id": source_id})
        if record and "last_processed_id" in record:
            return int(record["last_processed_id"])
        return 0

    @staticmethod
    async def update_checkpoint(source_id: str, last_id: int):
        await checkpoints_col.update_one(
            {"source_id": source_id},
            {"$set": {"source_id": source_id, "last_processed_id": last_id}},
            upsert=True
        )
