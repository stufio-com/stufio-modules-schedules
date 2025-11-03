from stufio.core.migrations.base import MongoMigrationScript
from datetime import datetime


class CreateScheduledEventDefinitionsCollection(MongoMigrationScript):
    name = "create_scheduled_event_definitions_collection"
    description = "Create MongoDB collection for scheduled event definitions and their indexes"
    migration_type = "schema"
    order = 30

    async def run(self, db):
        # Check if collection exists already
        existing_collections = await db.list_collection_names()

        # Create scheduled_event_definitions collection if it doesn't exist
        if "scheduled_event_definitions" not in existing_collections:
            await db.create_collection("scheduled_event_definitions")

        # Create indexes for the scheduled_event_definitions collection
        collection = db["scheduled_event_definitions"]
        
        # Get existing indexes to avoid duplicates
        existing_indexes = await collection.list_indexes().to_list(length=None)
        existing_index_names = {idx.get("name", "") for idx in existing_indexes}

        # Create unique index on event_class_name
        if "event_class_name_1" not in existing_index_names:
            await collection.create_index("event_class_name", unique=True, name="event_class_name_1")

        # Create unique index on event_name
        if "event_name_1" not in existing_index_names:
            await collection.create_index("event_name", unique=True, name="event_name_1")

        # Create compound index on entity_type and action
        if "entity_type_1_action_1" not in existing_index_names:
            await collection.create_index([("entity_type", 1), ("action", 1)], name="entity_type_1_action_1")

        # Create index on module_name
        if "module_name_1" not in existing_index_names:
            await collection.create_index("module_name", name="module_name_1")

        # Create compound index on status and next_execution_at for efficient scheduling queries
        if "status_1_next_execution_at_1" not in existing_index_names:
            await collection.create_index([("status", 1), ("next_execution_at", 1)], name="status_1_next_execution_at_1")

        # Create index on created_at for temporal queries
        if "created_at_1" not in existing_index_names:
            await collection.create_index("created_at", name="created_at_1")

        # Create index on updated_at for temporal queries
        if "updated_at_1" not in existing_index_names:
            await collection.create_index("updated_at", name="updated_at_1")
