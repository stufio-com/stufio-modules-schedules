"""
Pydantic schemas for schedule analytics
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import UUID
from pydantic import BaseModel, Field

from ..models.schedule_analytics import (
    ScheduleExecutionResult, 
    ScheduleType, 
    AnalyticsLevel
)


class ScheduleAnalyticsBase(BaseModel):
    """Base schedule analytics schema"""
    schedule_type: ScheduleType
    schedule_id: str
    schedule_name: Optional[str] = None
    level: AnalyticsLevel = AnalyticsLevel.INFO
    event_type: str
    event_action: str
    event_entity_id: Optional[str] = None
    correlation_id: str
    scheduled_at: datetime
    started_processing_at: datetime
    execution_result: ScheduleExecutionResult = ScheduleExecutionResult.SUCCESS
    retry_count: int = 0
    error_message: Optional[str] = None


class ScheduleAnalyticsCreate(ScheduleAnalyticsBase):
    """Schema for creating schedule analytics"""
    # Optional timing fields that can be calculated later
    completed_at: Optional[datetime] = None
    time_in_mongo_queue_ms: Optional[int] = None
    time_in_clickhouse_queue_ms: Optional[int] = None
    time_in_redis_queue_ms: Optional[int] = None
    total_processing_time_ms: Optional[int] = None
    kafka_publish_time_ms: Optional[int] = None
    
    # Optional system fields
    processed_by_node: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_partition: Optional[int] = None
    kafka_offset: Optional[int] = None
    
    # Optional source tracking
    source_mongo_execution_id: Optional[str] = None
    source_clickhouse_id: Optional[str] = None
    source_redis_key: Optional[str] = None


class ScheduleAnalyticsUpdate(BaseModel):
    """Schema for updating schedule analytics"""
    schedule_name: Optional[str] = None
    level: Optional[AnalyticsLevel] = None
    completed_at: Optional[datetime] = None
    execution_result: Optional[ScheduleExecutionResult] = None
    retry_count: Optional[int] = None
    error_message: Optional[str] = None
    time_in_mongo_queue_ms: Optional[int] = None
    time_in_clickhouse_queue_ms: Optional[int] = None
    time_in_redis_queue_ms: Optional[int] = None
    total_processing_time_ms: Optional[int] = None
    kafka_publish_time_ms: Optional[int] = None
    processed_by_node: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_partition: Optional[int] = None
    kafka_offset: Optional[int] = None


class ScheduleAnalyticsResponse(ScheduleAnalyticsBase):
    """Schema for schedule analytics responses"""
    analytics_id: str
    completed_at: datetime
    time_in_mongo_queue_ms: Optional[int] = None
    time_in_clickhouse_queue_ms: Optional[int] = None
    time_in_redis_queue_ms: Optional[int] = None
    total_processing_time_ms: int
    kafka_publish_time_ms: Optional[int] = None
    processed_by_node: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_partition: Optional[int] = None
    kafka_offset: Optional[int] = None
    source_mongo_execution_id: Optional[str] = None
    source_clickhouse_id: Optional[str] = None
    source_redis_key: Optional[str] = None

    class Config:
        from_attributes = True


class ScheduleAnalyticsStats(BaseModel):
    """Statistics for schedule analytics"""
    total_records: int
    info_count: int
    warning_count: int
    error_count: int
    avg_execution_time: float
    avg_queue_time: float
    success_rate: float
    most_common_errors: List[Dict[str, Any]] = []


class ScheduleAnalyticsQuery(BaseModel):
    """Query parameters for schedule analytics"""
    schedule_type: Optional[ScheduleType] = None
    schedule_id: Optional[str] = None
    level: Optional[AnalyticsLevel] = None
    event_type: Optional[str] = None
    event_action: Optional[str] = None
    execution_result: Optional[ScheduleExecutionResult] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class ErrorPatternsResponse(BaseModel):
    """Response schema for error pattern analysis"""
    total_errors: int
    error_types: Dict[str, int]
    schedule_errors: Dict[str, int]
    time_range: Dict[str, str]
