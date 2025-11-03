from datetime import datetime
from typing import Dict, Optional, Any
from pydantic import BaseModel, Field
from ..models.clickhouse_scheduled_event import ScheduledEventStatus, ScheduledEventSource


class ClickhouseScheduledEventBase(BaseModel):
    """Base schema for ClickHouse scheduled events."""
    topic: str
    entity_type: str
    action: str
    entity_id: Optional[str] = None
    actor_type: str = "system"
    actor_id: str = "scheduler"
    payload: str  # JSON serialized payload
    headers: Dict[str, Any] = Field(default_factory=dict)
    scheduled_at: datetime
    max_delay_seconds: int = 86400
    priority: int = 0
    source: ScheduledEventSource = ScheduledEventSource.API_REQUEST
    source_id: Optional[str] = None
    source_execution_id: Optional[str] = None
    max_retries: int = 3


class ClickhouseScheduledEventCreate(ClickhouseScheduledEventBase):
    """Schema for creating a ClickHouse scheduled event."""
    correlation_id: Optional[str] = None


class ClickhouseScheduledEventUpdate(BaseModel):
    """Schema for updating a ClickHouse scheduled event."""
    topic: Optional[str] = None
    entity_type: Optional[str] = None
    action: Optional[str] = None
    entity_id: Optional[str] = None
    actor_type: Optional[str] = None
    actor_id: Optional[str] = None
    payload: Optional[str] = None
    headers: Optional[Dict[str, Any]] = None
    scheduled_at: Optional[datetime] = None
    max_delay_seconds: Optional[int] = None
    priority: Optional[int] = None
    status: Optional[ScheduledEventStatus] = None
    max_retries: Optional[int] = None
    error: Optional[str] = None


class ClickhouseScheduledEventResponse(ClickhouseScheduledEventBase):
    """Schema for ClickHouse scheduled event responses."""
    schedule_id: str
    correlation_id: str
    status: ScheduledEventStatus
    created_at: datetime
    updated_at: datetime
    processing_started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    retry_count: int
    node_id: Optional[str] = None
    lock_until: Optional[datetime] = None
    transferred_to_redis_at: Optional[datetime] = None
    redis_key: Optional[str] = None


class ClickhouseScheduledEventStats(BaseModel):
    """Statistics for ClickHouse scheduled events."""
    total_count: int
    pending_count: int
    processing_count: int
    completed_count: int
    error_count: int
    transferred_to_redis_count: int
    avg_processing_time_ms: Optional[float] = None
    avg_queue_time_ms: Optional[float] = None
