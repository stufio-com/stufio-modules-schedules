from datetime import datetime
from typing import Dict, Optional, Any
from pydantic import BaseModel, Field
from enum import Enum


class RedisScheduleStatus(str, Enum):
    """Status of a Redis scheduled event."""
    PENDING = "pending"
    RESERVED = "reserved"
    COMPLETED = "completed"
    ERROR = "error"


class RedisScheduledEvent(BaseModel):
    """Model for Redis scheduled events (stored as JSON in Redis sorted sets)."""

    # Unique identifier
    event_id: str = Field(..., description="Unique event identifier")
    
    # Message content
    topic: str
    entity_type: str
    action: str
    entity_id: Optional[str] = None
    actor_type: str = "system"
    actor_id: str = "scheduler"
    payload: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: str
    headers: Dict[str, Any] = Field(default_factory=dict)

    # Scheduling information
    scheduled_at: datetime  # When it should be executed
    priority: int = 0  # Higher priority = lower score in Redis

    # Source tracking
    source: str = "redis"  # Source system
    source_id: Optional[str] = None  # Original schedule ID
    clickhouse_schedule_id: Optional[str] = None  # ClickHouse ID if transferred

    # Status and processing
    status: RedisScheduleStatus = RedisScheduleStatus.PENDING
    created_at: datetime
    reserved_at: Optional[datetime] = None
    reserved_by: Optional[str] = None  # Node ID that reserved this
    retry_count: int = 0
    max_retries: int = 3
    
    class Config:
        """Pydantic config."""
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
