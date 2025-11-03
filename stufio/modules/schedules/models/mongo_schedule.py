from datetime import datetime
from typing import Dict, Optional, Any, List
from odmantic import Field, Index
from stufio.db.mongo_base import MongoBase, datetime_now_sec
from enum import Enum


class ScheduleStatus(str, Enum):
    """Status of a MongoDB schedule."""
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"
    COMPLETED = "completed"


class ExecutionStatus(str, Enum):
    """Status of a schedule execution."""
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    RUNNING = "running"


class MongoSchedule(MongoBase):
    """MongoDB model for storing periodic event schedules with crontab format."""

    # Basic schedule information
    name: str = Field(index=True)
    description: Optional[str] = None
    status: ScheduleStatus = ScheduleStatus.ACTIVE

    # Event details
    event_type: str  # The event entity_type
    event_action: str  # The event action
    event_entity_id: Optional[str] = None  # Optional entity ID
    event_payload: Optional[Dict[str, Any]] = None  # Event payload
    actor_type: str = "system"  # Default actor type
    actor_id: str = "scheduler"  # Default actor ID

    # Schedule timing
    cron_expression: str  # Cron expression for recurring schedules (required)
    timezone: str = "UTC"  # Timezone for schedule

    # Advanced options
    max_retries: int = 3  # Maximum retry attempts
    retry_delay_seconds: int = 60  # Delay between retries in seconds
    tags: List[str] = Field(default_factory=list)  # Tags for filtering
    
    # Event-specific settings
    event_correlation_id: Optional[str] = None  # Optional correlation ID template
    event_headers: Optional[Dict[str, Any]] = Field(default_factory=dict)  # Additional Kafka headers
    
    # Execution tracking
    last_execution: Optional[datetime] = None
    next_execution: Optional[datetime] = None
    last_status: Optional[ExecutionStatus] = None
    execution_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None

    # Metadata
    created_at: datetime = Field(default_factory=datetime_now_sec)
    updated_at: datetime = Field(default_factory=datetime_now_sec)
    created_by: Optional[str] = None

    model_config = {
        "collection": "mongo_schedules",
        "indexes": lambda: [
            Index("name", unique=True),
            Index("event_type", "event_action"),
            Index("status", "next_execution"),
            Index("tags"),
            Index("created_by"),
        ],
    }


class MongoScheduleExecution(MongoBase):
    """MongoDB model for storing schedule execution history."""

    schedule_id: str = Field(index=True)
    schedule_name: str  # Denormalized for easier queries
    execution_time: datetime = Field(default_factory=datetime_now_sec)
    status: ExecutionStatus
    duration_ms: Optional[int] = None
    
    # Event publishing details
    event_id: Optional[str] = None  # ID of triggered event
    clickhouse_schedule_id: Optional[str] = None  # ID in ClickHouse if scheduled for later
    redis_scheduled: bool = False  # Whether it was scheduled to Redis
    
    # Error handling
    error: Optional[str] = None
    retry_count: int = 0
    
    # Metadata
    node_id: Optional[str] = None  # Which node executed this
    correlation_id: Optional[str] = None

    model_config = {
        "collection": "mongo_schedule_executions",
        "indexes": lambda: [
            Index("schedule_id", "execution_time"),
            Index("schedule_id", "status"),
            Index("execution_time"),  # For cleanup/TTL
            Index("status"),
            Index("clickhouse_schedule_id"),
        ],
    }
