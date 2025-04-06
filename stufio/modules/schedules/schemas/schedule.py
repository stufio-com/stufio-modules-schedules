from datetime import datetime
from typing import Dict, Optional, Any, List
from pydantic import BaseModel, Field, validator
from croniter import croniter
import pytz


class ScheduleBase(BaseModel):
    """Base schema for schedules."""

    name: str
    description: Optional[str] = None
    enabled: bool = True

    # Event details
    event_type: str
    event_action: str
    event_entity_id: Optional[str] = None
    event_payload: Optional[Dict[str, Any]] = None
    actor_type: str = "system"
    actor_id: str = "scheduler"

    # Schedule timing
    cron_expression: Optional[str] = None
    one_time: bool = False
    execution_time: Optional[datetime] = None
    timezone: str = "UTC"

    # Advanced options
    max_retries: int = 3
    retry_delay: int = 60
    tags: List[str] = Field(default_factory=list)

    @validator("cron_expression")
    def validate_cron(cls, v, values):
        if v is not None:
            if not croniter.is_valid(v):
                raise ValueError("Invalid cron expression")
            if values.get("one_time", False):
                raise ValueError(
                    "Cannot specify both cron_expression and one_time=True"
                )
        return v

    @validator("execution_time")
    def validate_execution_time(cls, v, values):
        if values.get("one_time", False) and v is None:
            raise ValueError("execution_time is required for one-time schedules")
        return v

    @validator("timezone")
    def validate_timezone(cls, v):
        if v not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {v}")
        return v


class ScheduleCreate(ScheduleBase):
    """Schema for creating a new schedule."""

    pass


class ScheduleUpdate(BaseModel):
    """Schema for updating an existing schedule."""

    name: Optional[str] = None
    description: Optional[str] = None
    enabled: Optional[bool] = None

    event_type: Optional[str] = None
    event_action: Optional[str] = None
    event_entity_id: Optional[str] = None
    event_payload: Optional[Dict[str, Any]] = None
    actor_type: Optional[str] = None
    actor_id: Optional[str] = None

    cron_expression: Optional[str] = None
    one_time: Optional[bool] = None
    execution_time: Optional[datetime] = None
    timezone: Optional[str] = None

    max_retries: Optional[int] = None
    retry_delay: Optional[int] = None
    tags: Optional[List[str]] = None

    @validator("cron_expression")
    def validate_cron(cls, v):
        if v is not None and not croniter.is_valid(v):
            raise ValueError("Invalid cron expression")
        return v

    @validator("timezone")
    def validate_timezone(cls, v):
        if v is not None and v not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {v}")
        return v


class ScheduleInDB(ScheduleBase):
    """Schema for schedules as stored in the database."""

    id: str
    last_execution: Optional[datetime] = None
    next_execution: Optional[datetime] = None
    last_status: Optional[str] = None
    execution_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None


class ScheduleResponse(ScheduleInDB):
    """Schema for responding with schedule information."""

    pass


class ScheduleExecutionCreate(BaseModel):
    """Schema for creating a schedule execution record."""

    schedule_id: str
    status: str
    duration_ms: Optional[int] = None
    event_id: Optional[str] = None
    error: Optional[str] = None


class ScheduleExecutionResponse(BaseModel):
    """Schema for responding with schedule execution information."""

    id: str
    schedule_id: str
    execution_time: datetime
    status: str
    duration_ms: Optional[int] = None
    event_id: Optional[str] = None
    error: Optional[str] = None
