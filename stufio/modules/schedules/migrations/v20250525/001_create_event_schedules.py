"""
ClickHouse migration for EventSchedule table.
"""
import logging
from datetime import datetime

from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn

logger = logging.getLogger(__name__)


class CreateEventScheduleTable(ClickhouseMigrationScript):
    """Create the event_schedules table for storing scheduled Kafka messages."""

    name = "create_event_schedules"
    description = "Create event_schedules table for delayed message scheduling"
    migration_type = "schema"
    order = 10

    async def run(self, db) -> None:
        """Create the event_schedules table."""
        try:
            db_name = get_database_from_dsn()
            
            # Create the main table
            await db.command(f"""
                CREATE TABLE IF NOT EXISTS `{db_name}`.`event_schedules` (
                    schedule_id String,
                    topic String,
                    entity_type String,
                    action String,
                    body String,
                    correlation_id String,
                    headers String,  -- JSON string
                    scheduled_at DateTime64(6),
                    max_delay_seconds UInt32 DEFAULT 86400,
                    priority Int32 DEFAULT 0,
                    status Enum8(
                        'pending' = 1,
                        'processing' = 2, 
                        'completed' = 3,
                        'error' = 4,
                        'skipped' = 5,
                        'transferred' = 6
                    ) DEFAULT 'pending',
                    created_at DateTime64(6) DEFAULT now64(6),
                    updated_at DateTime64(6) DEFAULT now64(6),
                    processing_started_at Nullable(DateTime64(6)),
                    completed_at Nullable(DateTime64(6)),
                    error Nullable(String),
                    retry_count UInt8 DEFAULT 0,
                    node_id Nullable(String),
                    lock_until Nullable(DateTime64(6))
                ) 
                ENGINE = MergeTree()
                ORDER BY (status, scheduled_at, priority)
                PARTITION BY toYYYYMMDD(scheduled_at)
                SETTINGS index_granularity = 8192
            """)
            logger.info("Created event_schedules table")

            # Create indexes for efficient querying
            await db.command(f"""
                CREATE INDEX IF NOT EXISTS idx_event_schedules_correlation_id 
                ON `{db_name}`.`event_schedules` (correlation_id) 
                TYPE bloom_filter GRANULARITY 1
            """)
            logger.info("Created correlation_id index")

            await db.command(f"""
                CREATE INDEX IF NOT EXISTS idx_event_schedules_entity 
                ON `{db_name}`.`event_schedules` (entity_type, action) 
                TYPE bloom_filter GRANULARITY 1
            """)
            logger.info("Created entity_type/action index")

        except Exception as e:
            logger.error(f"Error creating event_schedules table: {str(e)}")
            raise
