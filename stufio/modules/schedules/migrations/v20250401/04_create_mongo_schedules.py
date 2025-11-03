from stufio.core.migrations.base import MongoMigrationScript
from datetime import datetime


class CreateMongoSchedulesCollection(MongoMigrationScript):
    name = "create_mongo_schedules_collection"
    description = "Create MongoDB collections for mongo schedules and their indexes"
    migration_type = "schema"
    order = 40

    async def run(self, db):
        # Check if collection exists already
        existing_collections = await db.list_collection_names()

        # Create mongo_schedules collection if it doesn't exist
        if "mongo_schedules" not in existing_collections:
            await db.create_collection("mongo_schedules")

        # Create mongo_schedule_executions collection if it doesn't exist
        if "mongo_schedule_executions" not in existing_collections:
            await db.create_collection("mongo_schedule_executions")

        # Create indexes for the mongo_schedules collection
        schedules_collection = db["mongo_schedules"]
        
        # Get existing indexes to avoid duplicates
        existing_indexes = await schedules_collection.list_indexes().to_list(length=None)
        existing_index_names = {idx.get("name", "") for idx in existing_indexes}

        # Create unique index on name
        if "name_1" not in existing_index_names:
            await schedules_collection.create_index("name", unique=True, name="name_1")

        # Create compound index on event_type and event_action
        if "event_type_1_event_action_1" not in existing_index_names:
            await schedules_collection.create_index([("event_type", 1), ("event_action", 1)], name="event_type_1_event_action_1")

        # Create compound index on status and next_execution for efficient scheduling queries
        if "status_1_next_execution_1" not in existing_index_names:
            await schedules_collection.create_index([("status", 1), ("next_execution", 1)], name="status_1_next_execution_1")

        # Create index on tags for filtering
        if "tags_1" not in existing_index_names:
            await schedules_collection.create_index("tags", name="tags_1")

        # Create index on created_by for user-based queries
        if "created_by_1" not in existing_index_names:
            await schedules_collection.create_index("created_by", name="created_by_1")

        # Create indexes for the mongo_schedule_executions collection
        executions_collection = db["mongo_schedule_executions"]
        
        # Get existing indexes to avoid duplicates
        existing_indexes = await executions_collection.list_indexes().to_list(length=None)
        existing_index_names = {idx.get("name", "") for idx in existing_indexes}

        # Create compound index on schedule_id and execution_time
        if "schedule_id_1_execution_time_1" not in existing_index_names:
            await executions_collection.create_index([("schedule_id", 1), ("execution_time", 1)], name="schedule_id_1_execution_time_1")

        # Create compound index on schedule_id and status
        if "schedule_id_1_status_1" not in existing_index_names:
            await executions_collection.create_index([("schedule_id", 1), ("status", 1)], name="schedule_id_1_status_1")

        # Create index on execution_time for cleanup/TTL
        if "execution_time_1" not in existing_index_names:
            await executions_collection.create_index("execution_time", name="execution_time_1")

        # Create index on status for status-based queries
        if "status_1" not in existing_index_names:
            await executions_collection.create_index("status", name="status_1")

        # Create index on clickhouse_schedule_id for cross-system queries
        if "clickhouse_schedule_id_1" not in existing_index_names:
            await executions_collection.create_index("clickhouse_schedule_id", name="clickhouse_schedule_id_1")
