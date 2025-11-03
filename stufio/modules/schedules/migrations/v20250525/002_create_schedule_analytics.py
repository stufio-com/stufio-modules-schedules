"""
ClickHouse migration for analytics tables.
"""
import logging
from datetime import datetime

from stufio.core.migrations.base import ClickhouseMigrationScript

logger = logging.getLogger(__name__)


class CreateScheduleAnalyticsTables(ClickhouseMigrationScript):
    """Create analytics tables for schedule execution tracking."""

    name = "create_schedule_analytics"
    description = "Create analytics tables for schedule execution tracking"
    migration_type = "schema"
    order = 20

    async def run(self, db) -> None:
        """Create the analytics tables."""
        try:
            # Create schedule execution results table
            await db.command("""
                CREATE TABLE IF NOT EXISTS schedule_execution_results (
                    execution_id String,
                    schedule_id String,
                    correlation_id String,
                    topic String,
                    entity_type String,
                    action String,
                    scheduled_at DateTime64(6),
                    executed_at DateTime64(6),
                    delay_seconds Float64,
                    status Enum8(
                        'success' = 1,
                        'error' = 2,
                        'timeout' = 3,
                        'skipped' = 4
                    ),
                    error_message Nullable(String),
                    retry_count UInt8,
                    processing_time_ms UInt32,
                    node_id String,
                    created_at DateTime64(6) DEFAULT now64(6)
                )
                ENGINE = MergeTree()
                ORDER BY (toYYYYMMDD(executed_at), status, entity_type, action)
                PARTITION BY toYYYYMMDD(executed_at)
                TTL executed_at + INTERVAL 90 DAY DELETE
                SETTINGS index_granularity = 8192
            """)
            logger.info("Created schedule_execution_results table")

            # Create schedule performance metrics table (aggregated data)
            await db.command("""
                CREATE TABLE IF NOT EXISTS schedule_performance_metrics (
                    date Date,
                    hour UInt8,
                    entity_type String,
                    action String,
                    topic String,
                    total_executions UInt64,
                    successful_executions UInt64,
                    failed_executions UInt64,
                    skipped_executions UInt64,
                    avg_delay_seconds Float64,
                    avg_processing_time_ms Float64,
                    p50_delay_seconds Float64,
                    p95_delay_seconds Float64,
                    p99_delay_seconds Float64,
                    min_delay_seconds Float64,
                    max_delay_seconds Float64,
                    created_at DateTime64(6) DEFAULT now64(6)
                )
                ENGINE = SummingMergeTree()
                ORDER BY (date, hour, entity_type, action, topic)
                PARTITION BY toYYYYMM(date)
                TTL date + INTERVAL 1 YEAR DELETE
                SETTINGS index_granularity = 8192
            """)
            logger.info("Created schedule_performance_metrics table")

            # Create materialized view for automatic aggregation
            await db.command("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS schedule_performance_metrics_mv
                TO schedule_performance_metrics
                AS SELECT
                    toDate(executed_at) as date,
                    toHour(executed_at) as hour,
                    entity_type,
                    action,
                    topic,
                    count() as total_executions,
                    countIf(status = 'success') as successful_executions,
                    countIf(status = 'error') as failed_executions,
                    countIf(status = 'skipped') as skipped_executions,
                    avg(delay_seconds) as avg_delay_seconds,
                    avg(processing_time_ms) as avg_processing_time_ms,
                    quantile(0.5)(delay_seconds) as p50_delay_seconds,
                    quantile(0.95)(delay_seconds) as p95_delay_seconds,
                    quantile(0.99)(delay_seconds) as p99_delay_seconds,
                    min(delay_seconds) as min_delay_seconds,
                    max(delay_seconds) as max_delay_seconds,
                    now64(6) as created_at
                FROM schedule_execution_results
                GROUP BY date, hour, entity_type, action, topic
            """)
            logger.info("Created schedule_performance_metrics_mv materialized view")

            # Create indexes for efficient querying
            await db.command("""
                CREATE INDEX IF NOT EXISTS idx_execution_results_correlation_id 
                ON schedule_execution_results (correlation_id) 
                TYPE bloom_filter GRANULARITY 1
            """)

            await db.command("""
                CREATE INDEX IF NOT EXISTS idx_execution_results_schedule_id 
                ON schedule_execution_results (schedule_id) 
                TYPE bloom_filter GRANULARITY 1
            """)

        except Exception as e:
            logger.error(f"Error creating schedule analytics tables: {str(e)}")
            raise

