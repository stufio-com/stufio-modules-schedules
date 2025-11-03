"""
API routes for schedule analytics and monitoring
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPBearer

from ...models.schedule_analytics import ScheduleAnalytics, AnalyticsLevel, ScheduleType
from ...schemas.schedule_analytics import (
    ScheduleAnalyticsCreate,
    ScheduleAnalyticsResponse,
    ScheduleAnalyticsStats,
    ErrorPatternsResponse
)
from ...services.analytics_service import analytics_service

router = APIRouter(prefix="/admin/analytics", tags=["Schedule Analytics"])
security = HTTPBearer()


async def get_analytics_service():
    """Dependency to get analytics service"""
    return analytics_service


@router.post("/", response_model=ScheduleAnalyticsResponse, status_code=status.HTTP_201_CREATED)
async def create_analytics_record(
    analytics_data: ScheduleAnalyticsCreate,
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Create a new analytics record"""
    try:
        analytics_id = await service.record_analytics(**analytics_data.model_dump())
        
        # Return the created record
        created_record = await service.analytics_crud.get(analytics_id)
        return ScheduleAnalyticsResponse.model_validate(created_record)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create analytics record: {str(e)}"
        )


@router.get("/schedule/{schedule_id}", response_model=List[ScheduleAnalyticsResponse])
async def get_schedule_analytics(
    schedule_id: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    level: Optional[AnalyticsLevel] = Query(None, description="Filter by analytics level"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Get analytics records for a specific schedule"""
    try:
        analytics_records = await service.get_schedule_analytics(
            schedule_id=schedule_id,
            limit=limit,
            level=level
        )
        
        return [ScheduleAnalyticsResponse.model_validate(record) for record in analytics_records]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schedule analytics: {str(e)}"
        )


@router.get("/time-range", response_model=List[ScheduleAnalyticsResponse])
async def get_analytics_by_time_range(
    start_time: datetime = Query(..., description="Start time for analytics range"),
    end_time: datetime = Query(..., description="End time for analytics range"),
    schedule_id: Optional[str] = Query(None, description="Filter by specific schedule ID"),
    level: Optional[AnalyticsLevel] = Query(None, description="Filter by analytics level"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Get analytics records within a time range"""
    try:
        if end_time <= start_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="End time must be after start time"
            )
        
        # Limit time range to prevent excessive queries
        max_range = timedelta(days=7)
        if end_time - start_time > max_range:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Time range cannot exceed 7 days"
            )
        
        analytics_records = await service.get_analytics_by_time_range(
            start_time=start_time,
            end_time=end_time,
            schedule_id=schedule_id,
            level=level
        )
        
        return [ScheduleAnalyticsResponse.model_validate(record) for record in analytics_records]
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get analytics by time range: {str(e)}"
        )


@router.get("/level/{level}", response_model=List[ScheduleAnalyticsResponse])
async def get_analytics_by_level(
    level: AnalyticsLevel,
    start_time: Optional[datetime] = Query(None, description="Start time filter"),
    end_time: Optional[datetime] = Query(None, description="End time filter"),
    limit: int = Query(1000, ge=1, le=5000, description="Number of records to return"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Get analytics records by level (INFO, WARNING, ERROR)"""
    try:
        analytics_records = await service.get_analytics_by_level(
            level=level,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        
        return [ScheduleAnalyticsResponse.model_validate(record) for record in analytics_records]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get analytics by level: {str(e)}"
        )


@router.get("/event-type/{event_type}", response_model=List[ScheduleAnalyticsResponse])
async def get_analytics_by_event_type(
    event_type: str,
    start_time: Optional[datetime] = Query(None, description="Start time filter"),
    end_time: Optional[datetime] = Query(None, description="End time filter"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Get analytics records by event type"""
    try:
        analytics_records = await service.get_analytics_by_event_type(
            event_type=event_type,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        
        return [ScheduleAnalyticsResponse.model_validate(record) for record in analytics_records]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get analytics by event type: {str(e)}"
        )


@router.get("/stats", response_model=ScheduleAnalyticsStats)
async def get_execution_stats(
    schedule_id: Optional[str] = Query(None, description="Filter by specific schedule ID"),
    hours_back: int = Query(24, ge=1, le=168, description="Hours back to analyze"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Get execution statistics"""
    try:
        stats = await service.get_execution_stats(
            schedule_id=schedule_id,
            hours_back=hours_back
        )
        return stats
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get execution stats: {str(e)}"
        )


@router.get("/errors/patterns", response_model=ErrorPatternsResponse)
async def get_error_patterns(
    hours_back: int = Query(24, ge=1, le=168, description="Hours back to analyze"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Get error patterns analysis"""
    try:
        error_patterns = await service.get_error_patterns(hours_back=hours_back)
        return error_patterns
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get error patterns: {str(e)}"
        )


@router.get("/performance/schedule/{schedule_id}", response_model=Dict[str, Any])
async def get_schedule_performance_summary(
    schedule_id: str,
    hours_back: int = Query(24, ge=1, le=168, description="Hours back to analyze"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Get performance summary for a specific schedule"""
    try:
        summary = await service.get_schedule_performance_summary(
            schedule_id=schedule_id,
            hours_back=hours_back
        )
        return summary
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schedule performance summary: {str(e)}"
        )


@router.get("/health/system", response_model=Dict[str, Any])
async def get_system_health_metrics(
    hours_back: int = Query(1, ge=1, le=24, description="Hours back for health check"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Get overall system health metrics"""
    try:
        health_metrics = await service.get_system_health_metrics(hours_back=hours_back)
        return health_metrics
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system health metrics: {str(e)}"
        )


@router.delete("/cleanup", response_model=Dict[str, Any])
async def cleanup_old_analytics(
    days_to_keep: int = Query(30, ge=1, le=365, description="Number of days to keep"),
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Clean up old analytics records"""
    try:
        deleted_count = await service.cleanup_old_analytics(days_to_keep=days_to_keep)
        
        return {
            "status": "success",
            "deleted_count": deleted_count,
            "days_kept": days_to_keep,
            "cleanup_time": datetime.utcnow()
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cleanup old analytics: {str(e)}"
        )


# Convenience endpoints for three-tier schedule types

@router.post("/mongo/execution", response_model=str, status_code=status.HTTP_201_CREATED)
async def record_mongo_execution(
    schedule_id: str,
    correlation_id: str,
    event_type: str,
    event_action: str,
    scheduled_at: datetime,
    started_processing_at: datetime,
    execution_result: str = "SUCCESS",
    error_message: Optional[str] = None,
    retry_count: int = 0,
    source_mongo_execution_id: Optional[str] = None,
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Record analytics for MongoDB schedule execution"""
    try:
        from ...models.schedule_analytics import ScheduleExecutionResult
        result = ScheduleExecutionResult(execution_result)
        
        analytics_id = await service.record_mongo_schedule_execution(
            schedule_id=schedule_id,
            correlation_id=correlation_id,
            event_type=event_type,
            event_action=event_action,
            scheduled_at=scheduled_at,
            started_processing_at=started_processing_at,
            execution_result=result,
            error_message=error_message,
            retry_count=retry_count,
            source_mongo_execution_id=source_mongo_execution_id
        )
        
        return analytics_id
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to record mongo execution: {str(e)}"
        )


@router.post("/clickhouse/transfer", response_model=str, status_code=status.HTTP_201_CREATED)
async def record_clickhouse_transfer(
    schedule_id: str,
    correlation_id: str,
    event_type: str,
    event_action: str,
    scheduled_at: datetime,
    started_processing_at: datetime,
    time_in_clickhouse_queue_ms: Optional[int] = None,
    execution_result: str = "SUCCESS",
    error_message: Optional[str] = None,
    source_clickhouse_id: Optional[str] = None,
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Record analytics for ClickHouse to Redis transfer"""
    try:
        from ...models.schedule_analytics import ScheduleExecutionResult
        result = ScheduleExecutionResult(execution_result)
        
        analytics_id = await service.record_clickhouse_transfer(
            schedule_id=schedule_id,
            correlation_id=correlation_id,
            event_type=event_type,
            event_action=event_action,
            scheduled_at=scheduled_at,
            started_processing_at=started_processing_at,
            time_in_clickhouse_queue_ms=time_in_clickhouse_queue_ms,
            execution_result=result,
            error_message=error_message,
            source_clickhouse_id=source_clickhouse_id
        )
        
        return analytics_id
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to record clickhouse transfer: {str(e)}"
        )


@router.post("/redis/processing", response_model=str, status_code=status.HTTP_201_CREATED)
async def record_redis_processing(
    schedule_id: str,
    correlation_id: str,
    event_type: str,
    event_action: str,
    scheduled_at: datetime,
    started_processing_at: datetime,
    kafka_topic: Optional[str] = None,
    kafka_partition: Optional[int] = None,
    kafka_offset: Optional[int] = None,
    kafka_publish_time_ms: Optional[int] = None,
    time_in_redis_queue_ms: Optional[int] = None,
    execution_result: str = "SUCCESS",
    error_message: Optional[str] = None,
    source_redis_key: Optional[str] = None,
    service = Depends(get_analytics_service),
    token: str = Depends(security)
):
    """Record analytics for Redis processing and Kafka publishing"""
    try:
        from ...models.schedule_analytics import ScheduleExecutionResult
        result = ScheduleExecutionResult(execution_result)
        
        analytics_id = await service.record_redis_processing(
            schedule_id=schedule_id,
            correlation_id=correlation_id,
            event_type=event_type,
            event_action=event_action,
            scheduled_at=scheduled_at,
            started_processing_at=started_processing_at,
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            kafka_publish_time_ms=kafka_publish_time_ms,
            time_in_redis_queue_ms=time_in_redis_queue_ms,
            execution_result=result,
            error_message=error_message,
            source_redis_key=source_redis_key
        )
        
        return analytics_id
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to record redis processing: {str(e)}"
        )
