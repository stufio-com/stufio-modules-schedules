from datetime import datetime
from typing import Dict, Optional, Any
from pydantic import BaseModel, Field
from ..models.redis_scheduled_event import RedisScheduleStatus


class RedisScheduledEventBase(BaseModel):
    """Base schema for Redis scheduled events."""
    topic: str
    entity_type: str
    action: str
    entity_id: Optional[str] = None
    actor_type: str = "system"
    actor_id: str = "scheduler"
    payload: Dict[str, Any] = Field(default_factory=dict)
    headers: Dict[str, Any] = Field(default_factory=dict)
    scheduled_at: datetime
    priority: int = 0
    source: str = "redis"
    source_id: Optional[str] = None
    clickhouse_schedule_id: Optional[str] = None
    max_retries: int = 3


class RedisScheduledEventCreate(RedisScheduledEventBase):
    """Schema for creating a Redis scheduled event."""
    correlation_id: Optional[str] = None
    
class RedisScheduledEventUpdate(BaseModel):
    """Schema for updating a Redis scheduled event."""
    topic: Optional[str] = None
    entity_type: Optional[str] = None
    action: Optional[str] = None
    entity_id: Optional[str] = None
    actor_type: Optional[str] = None
    actor_id: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None
    scheduled_at: Optional[datetime] = None
    priority: Optional[int] = None
    status: Optional[RedisScheduleStatus] = None
    max_retries: Optional[int] = None


class RedisScheduledEventResponse(RedisScheduledEventBase):
    """Schema for Redis scheduled event responses."""
    event_id: str
    correlation_id: str
    status: RedisScheduleStatus
    created_at: datetime
    updated_at: Optional[datetime] = None
    reserved_at: Optional[datetime] = None
    reserved_by: Optional[str] = None
    processed_at: Optional[datetime] = None
    retry_count: int


class RedisScheduledEventStats(BaseModel):
    """Statistics for Redis scheduled events."""
    total_count: int
    pending_count: int
    reserved_count: int
    completed_count: int
    error_count: int
    avg_queue_time_ms: Optional[float] = None
    upcoming_in_next_hour: int
    upcoming_in_next_day: int
