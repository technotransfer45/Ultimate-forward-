from database.mongo import stats_col

class StatsRepo:
    @staticmethod
    async def increment(field: str, amount: int = 1):
        await stats_col.update_one(
            {"stat_key": "global_metrics"},
            {"$inc": {field: amount}},
            upsert=True
        )

    @staticmethod
    async def get_metrics() -> dict:
        record = await stats_col.find_one({"stat_key": "global_metrics"})
        if not record:
            return {
                "scanned": 0,
                "terabox_stored": 0,
                "streaming_stored": 0,
                "media_stored": 0,
                "converted_forwarded": 0,
                "duplicates_skipped": 0,
                "filtered_out": 0,
                "failed": 0
            }
        return {
            "scanned": record.get("scanned", 0),
            "terabox_stored": record.get("terabox_stored", 0),
            "streaming_stored": record.get("streaming_stored", 0),
            "media_stored": record.get("media_stored", 0),
            "converted_forwarded": record.get("converted_forwarded", 0),
            "duplicates_skipped": record.get("duplicates_skipped", 0),
            "filtered_out": record.get("filtered_out", 0),
            "failed": record.get("failed", 0)
        }
