from stufio.core.migrations.base import MongoMigrationScript
from datetime import datetime


class CreateScheduleCollection(MongoMigrationScript):
    name = "create_schedule_collection"
    description = "Create MongoDB collection for schedules"
    migration_type = "schema"
    order = 10

    async def run(self, db):
        # Check if collection exists already
        existing_collections = await db.list_collection_names()

        # Create schedules collection if it doesn't exist
        if "schedules" not in existing_collections:
            await db.create_collection("schedules")

        # Create schedule_executions collection if it doesn't exist
        if "schedule_executions" not in existing_collections:
            await db.create_collection("schedule_executions")
    
