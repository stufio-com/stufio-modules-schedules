from motor.core import AgnosticDatabase
from stufio.core.migrations.base import MongoMigrationScript


class CreateScheduleIndexes(MongoMigrationScript):
    name = "create_schedule_indexes"
    description = "Create indexes for schedule collections"
    migration_type = "schema"
    order = 20

    async def run(self, db: AgnosticDatabase) -> None:
        # Create indexes for schedules collection
        await db.command(
            {
                "createIndexes": "schedules",
                "indexes": [
                    {
                        "key": {"name": 1},
                        "name": "name_index",
                        "unique": True,
                        "background": True,
                    },
                    {
                        "key": {"enabled": 1, "next_execution": 1},
                        "name": "enabled_next_execution_index",
                        "background": True,
                    },
                    {
                        "key": {"event_type": 1, "event_action": 1},
                        "name": "event_type_action_index",
                        "background": True,
                    },
                    {"key": {"tags": 1}, "name": "tags_index", "background": True},
                    {
                        "key": {"created_at": 1},
                        "name": "created_at_index",
                        "background": True,
                    },
                ],
            }
        )

        # Create indexes for schedule_executions collection
        await db.command(
            {
                "createIndexes": "schedule_executions",
                "indexes": [
                    {
                        "key": {"schedule_id": 1, "execution_time": -1},
                        "name": "schedule_id_time_index",
                        "background": True,
                    },
                    {
                        "key": {"status": 1, "execution_time": -1},
                        "name": "status_time_index",
                        "background": True,
                    },
                    {
                        "key": {"execution_time": 1},
                        "name": "execution_time_ttl_index",
                        "background": True,
                        "expireAfterSeconds": 2592000,  # 30 days
                    },
                ],
            }
        )
