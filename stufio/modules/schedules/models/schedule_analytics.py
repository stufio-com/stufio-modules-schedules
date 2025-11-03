import uuid
from datetime import datetime
from typing import Optional
from pydantic import Field
from enum import Enum

from stufio.db.clickhouse_base import ClickhouseBase, datetime_now_sec


class ScheduleExecutionResult(str, Enum):
    """Result of schedule execution."""
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    RETRY = "retry"


class ScheduleType(str, Enum):
    """Type of schedule."""
    MONGO_PERIODIC = "mongo_periodic"
    CLICKHOUSE_DELAYED = "clickhouse_delayed"
    REDIS_IMMEDIATE = "redis_immediate"


class AnalyticsLevel(str, Enum):
    """Level of analytics record for categorization."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class ScheduleAnalytics(ClickhouseBase):
    """ClickHouse model for storing schedule execution analytics and metrics."""

    # Primary fields
    analytics_id: str = Field(
        json_schema_extra={"primary_field": True}, 
        default_factory=lambda: str(uuid.uuid4())
    )

    # Schedule identification
    schedule_type: ScheduleType
    schedule_id: str  # ID from the source system
    schedule_name: Optional[str] = None  # Human-readable name
    
    # Analytics categorization
    level: AnalyticsLevel = AnalyticsLevel.INFO  # Level for categorization
    
    # Event details
    event_type: str
    event_action: str
    event_entity_id: Optional[str] = None
    correlation_id: str

    # Timing metrics
    scheduled_at: datetime  # When it was originally scheduled
    started_processing_at: datetime  # When processing began
    completed_at: datetime = Field(default_factory=datetime_now_sec)  # When it finished
    
    # Queue time metrics
    time_in_mongo_queue_ms: Optional[int] = None  # Time between creation and first processing
    time_in_clickhouse_queue_ms: Optional[int] = None  # Time in ClickHouse before Redis transfer
    time_in_redis_queue_ms: Optional[int] = None  # Time in Redis before Kafka publish
    
    # Processing metrics
    total_processing_time_ms: int  # Total time from schedule to completion
    kafka_publish_time_ms: Optional[int] = None  # Time to publish to Kafka
    
    # Execution details
    execution_result: ScheduleExecutionResult
    retry_count: int = 0
    error_message: Optional[str] = None
    
    # System information
    processed_by_node: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_partition: Optional[int] = None
    kafka_offset: Optional[int] = None
    
    # Source tracking
    source_mongo_execution_id: Optional[str] = None
    source_clickhouse_id: Optional[str] = None
    source_redis_key: Optional[str] = None

    # Table configuration
    model_config = {
        "table_name": "schedule_analytics",
    }
