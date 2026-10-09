from motor.motor_asyncio import AsyncIOMotorClient
import config

client = AsyncIOMotorClient(config.MONGO_URI)
db = client[config.DB_NAME]

sources_col = db["sources"]
checkpoints_col = db["checkpoints"]
duplicates_col = db["duplicates"]
stats_col = db["statistics"]

async def init_db():
    await sources_col.create_index("source_id", unique=True)
    await checkpoints_col.create_index("source_id", unique=True)
    await duplicates_col.create_index("hash", unique=True)
    await stats_col.create_index("stat_key", unique=True)
