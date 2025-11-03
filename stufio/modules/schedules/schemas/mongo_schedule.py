from datetime import datetime
from typing import Dict, Optional, Any, List
from pydantic import BaseModel, Field
from ..models.mongo_schedule import ScheduleStatus, ExecutionStatus


class MongoScheduleBase(BaseModel):
    """Base schema for MongoDB schedules."""
    name: str
    description: Optional[str] = None
    event_type: str
    event_action: str
    event_entity_id: Optional[str] = None
    event_payload: Optional[Dict[str, Any]] = None
    actor_type: str = "system"
    actor_id: str = "scheduler"
    cron_expression: str
    timezone: str = "UTC"
    max_retries: int = 3
    retry_delay_seconds: int = 60
    tags: List[str] = Field(default_factory=list)
    event_correlation_id: Optional[str] = None
    event_headers: Optional[Dict[str, Any]] = Field(default_factory=dict)


class MongoScheduleCreate(MongoScheduleBase):
    """Schema for creating a MongoDB schedule."""
    pass


class MongoScheduleUpdate(BaseModel):
    """Schema for updating a MongoDB schedule."""
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[ScheduleStatus] = None
    event_type: Optional[str] = None
    event_action: Optional[str] = None
    event_entity_id: Optional[str] = None
    event_payload: Optional[Dict[str, Any]] = None
    actor_type: Optional[str] = None
    actor_id: Optional[str] = None
    cron_expression: Optional[str] = None
    timezone: Optional[str] = None
    max_retries: Optional[int] = None
    retry_delay_seconds: Optional[int] = None
    tags: Optional[List[str]] = None
    event_correlation_id: Optional[str] = None
    event_headers: Optional[Dict[str, Any]] = None


class MongoScheduleResponse(MongoScheduleBase):
    """Schema for MongoDB schedule responses."""
    id: str
    status: ScheduleStatus
    last_execution: Optional[datetime] = None
    next_execution: Optional[datetime] = None
    last_status: Optional[ExecutionStatus] = None
    execution_count: int
    error_count: int
    last_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None


class MongoScheduleExecutionBase(BaseModel):
    """Base schema for MongoDB schedule executions."""
    schedule_id: str
    schedule_name: str
    status: ExecutionStatus
    duration_ms: Optional[int] = None
    event_id: Optional[str] = None
    clickhouse_schedule_id: Optional[str] = None
    redis_scheduled: bool = False
    error: Optional[str] = None
    retry_count: int = 0
    node_id: Optional[str] = None
    correlation_id: Optional[str] = None


class MongoScheduleExecutionCreate(MongoScheduleExecutionBase):
    """Schema for creating a MongoDB schedule execution."""
    execution_time: Optional[datetime] = None  # Will be set to now if not provided


class MongoScheduleExecutionUpdate(BaseModel):
    """Schema for updating a MongoDB schedule execution."""
    status: Optional[ExecutionStatus] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    retry_count: Optional[int] = None
    node_id: Optional[str] = None
    correlation_id: Optional[str] = None


class MongoScheduleExecutionResponse(MongoScheduleExecutionBase):
    """Schema for MongoDB schedule execution responses."""
    id: str
    execution_time: datetime


class MongoScheduleStats(BaseModel):
    """Schema for MongoDB schedule statistics."""
    schedule_id: str
    total_executions: int
    successful_executions: int
    failed_executions: int
    success_rate: float
    avg_duration_ms: float
    last_execution: Optional[Dict[str, Any]] = None
