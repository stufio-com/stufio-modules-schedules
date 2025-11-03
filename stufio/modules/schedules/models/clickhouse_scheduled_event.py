import uuid
from datetime import datetime
from typing import Dict, Optional, Any
from pydantic import Field
from enum import Enum

from stufio.db.clickhouse_base import ClickhouseBase, datetime_now_sec


class ScheduledEventStatus(str, Enum):
    """Status of a ClickHouse scheduled event."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    ERROR = "error"
    SKIPPED = "skipped"
    TRANSFERRED_TO_REDIS = "transferred_to_redis"


class ScheduledEventSource(str, Enum):
    """Source of the scheduled event."""
    KAFKA_DELAYED = "kafka_delayed"  # From Kafka delayed topic
    MONGO_SCHEDULE = "mongo_schedule"  # From MongoDB periodic schedule
    API_REQUEST = "api_request"  # Direct API scheduling
    SYSTEM = "system"  # System-generated


class ClickhouseScheduledEvent(ClickhouseBase):
    """ClickHouse model for storing scheduled events that will be published to Kafka."""

    # Primary fields
    schedule_id: str = Field(
        json_schema_extra={"primary_field": True}, 
        default_factory=lambda: str(uuid.uuid4())
    )

    # Message content
    topic: str
    entity_type: str
    action: str
    entity_id: Optional[str] = None
    actor_type: str = "system"
    actor_id: str = "scheduler"
    payload: str  # JSON serialized payload
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    headers: Dict[str, Any] = Field(default_factory=dict)

    # Scheduling information
    scheduled_at: datetime  # When it should be executed
    max_delay_seconds: int = 86400  # Maximum delay allowed (24h default)
    priority: int = 0  # Higher priority messages processed first

    # Source tracking
    source: ScheduledEventSource = ScheduledEventSource.API_REQUEST
    source_id: Optional[str] = None  # ID from source system (e.g., mongo schedule ID)
    source_execution_id: Optional[str] = None  # Execution ID from source

    # Status tracking
    status: ScheduledEventStatus = ScheduledEventStatus.PENDING
    created_at: datetime = Field(default_factory=datetime_now_sec)
    updated_at: datetime = Field(default_factory=datetime_now_sec)
    processing_started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3

    # Processing control
    node_id: Optional[str] = None  # Which node is/was processing this
    lock_until: Optional[datetime] = None  # Distributed lock timestamp
    
    # Redis transfer tracking
    transferred_to_redis_at: Optional[datetime] = None
    redis_key: Optional[str] = None  # Redis key if transferred

    # Table configuration
    model_config = {
        "table_name": "clickhouse_scheduled_events",
    }
