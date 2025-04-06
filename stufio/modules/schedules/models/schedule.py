from datetime import datetime
from typing import Dict, Optional, Any, List
from uuid import uuid4
from odmantic import Field, Index
from pydantic import UUID4
from stufio.db.mongo_base import MongoBase, datetime_now_sec


class Schedule(MongoBase):
    """MongoDB model for storing schedules."""

    # Basic schedule information
    name: str = Field(index=True)
    description: Optional[str] = None
    enabled: bool = True

    # Event details
    event_type: str  # The event entity_type
    event_action: str  # The event action
    event_entity_id: Optional[str] = None  # Optional entity ID
    event_payload: Optional[Dict[str, Any]] = None  # Event payload
    actor_type: str = "system"  # Default actor type
    actor_id: str = "scheduler"  # Default actor ID

    # Schedule timing
    cron_expression: Optional[str] = None  # Cron expression for recurring schedules
    one_time: bool = False  # Is this a one-time schedule?
    execution_time: Optional[datetime] = None  # For one-time schedules
    timezone: str = "UTC"  # Timezone for schedule

    # Advanced options
    max_retries: int = 3  # Maximum retry attempts
    retry_delay: int = 60  # Delay between retries in seconds
    tags: List[str] = Field(default_factory=list)  # Tags for filtering

    # Execution tracking
    last_execution: Optional[datetime] = None
    next_execution: Optional[datetime] = None
    last_status: Optional[str] = None
    execution_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None

    # Metadata
    created_at: datetime = Field(default_factory=datetime_now_sec)
    updated_at: datetime = Field(default_factory=datetime_now_sec)
    created_by: Optional[str] = None

    model_config = {
        "collection": "schedules",
        "indexes": [
            Index("name", unique=True),
            Index("event_type", "event_action"),
            Index("enabled", "next_execution"),
            Index("tags"),
        ],
    }


class ScheduleExecution(MongoBase):
    """MongoDB model for storing schedule execution history."""

    schedule_id: str
    execution_time: datetime = Field(default_factory=datetime_now_sec)
    status: str  # success, failure, skipped
    duration_ms: Optional[int] = None
    event_id: Optional[str] = None  # ID of triggered event
    error: Optional[str] = None

    model_config = {
        "collection": "schedule_executions",
        "indexes": [
            Index("schedule_id", "execution_time"),
            Index("schedule_id", "status"),
            Index("execution_time"),  # For cleanup/TTL
        ],
    }
