"""
Schedule analytics service for collecting and analyzing performance metrics
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import uuid

from ..models.schedule_analytics import (
    ScheduleAnalytics, 
    AnalyticsLevel, 
    ScheduleExecutionResult,
    ScheduleType
)
from ..crud.crud_schedule_analytics import crud_schedule_analytics
from ..schemas.schedule_analytics import (
    ScheduleAnalyticsCreate, 
    ScheduleAnalyticsStats,
    ErrorPatternsResponse
)

logger = logging.getLogger(__name__)


class ScheduleAnalyticsService:
    """Service for managing schedule analytics and performance tracking"""
    
    def __init__(self):
        self.analytics_crud = crud_schedule_analytics

    async def record_analytics(
        self,
        schedule_type: ScheduleType,
        schedule_id: str,
        event_type: str,
        event_action: str,
        correlation_id: str,
        scheduled_at: datetime,
        started_processing_at: datetime,
        level: AnalyticsLevel = AnalyticsLevel.INFO,
        schedule_name: Optional[str] = None,
        event_entity_id: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        execution_result: ScheduleExecutionResult = ScheduleExecutionResult.SUCCESS,
        retry_count: int = 0,
        error_message: Optional[str] = None,
        time_in_mongo_queue_ms: Optional[int] = None,
        time_in_clickhouse_queue_ms: Optional[int] = None,
        time_in_redis_queue_ms: Optional[int] = None,
        total_processing_time_ms: Optional[int] = None,
        kafka_publish_time_ms: Optional[int] = None,
        processed_by_node: Optional[str] = None,
        kafka_topic: Optional[str] = None,
        kafka_partition: Optional[int] = None,
        kafka_offset: Optional[int] = None,
        source_mongo_execution_id: Optional[str] = None,
        source_clickhouse_id: Optional[str] = None,
        source_redis_key: Optional[str] = None,
    ) -> str:
        """Record analytics data for a schedule execution"""
        try:
            # Calculate timing if not provided
            if completed_at is None:
                completed_at = datetime.utcnow()
            
            if total_processing_time_ms is None:
                total_processing_time_ms = int((completed_at - started_processing_at).total_seconds() * 1000)
            
            analytics_data = ScheduleAnalyticsCreate(
                schedule_type=schedule_type,
                schedule_id=schedule_id,
                schedule_name=schedule_name,
                level=level,
                event_type=event_type,
                event_action=event_action,
                event_entity_id=event_entity_id,
                correlation_id=correlation_id,
                scheduled_at=scheduled_at,
                started_processing_at=started_processing_at,
                completed_at=completed_at,
                execution_result=execution_result,
                retry_count=retry_count,
                error_message=error_message,
                time_in_mongo_queue_ms=time_in_mongo_queue_ms,
                time_in_clickhouse_queue_ms=time_in_clickhouse_queue_ms,
                time_in_redis_queue_ms=time_in_redis_queue_ms,
                total_processing_time_ms=total_processing_time_ms,
                kafka_publish_time_ms=kafka_publish_time_ms,
                processed_by_node=processed_by_node,
                kafka_topic=kafka_topic,
                kafka_partition=kafka_partition,
                kafka_offset=kafka_offset,
                source_mongo_execution_id=source_mongo_execution_id,
                source_clickhouse_id=source_clickhouse_id,
                source_redis_key=source_redis_key
            )
            
            analytics_record = await self.analytics_crud.create(analytics_data)
            logger.debug(f"Recorded analytics for schedule {schedule_id}, correlation {correlation_id}")
            return analytics_record.analytics_id
            
        except Exception as e:
            logger.error(f"Failed to record analytics: {str(e)}")
            raise

    async def get_schedule_analytics(
        self,
        schedule_id: str,
        limit: int = 100,
        level: Optional[AnalyticsLevel] = None
    ) -> List[ScheduleAnalytics]:
        """Get analytics records for a specific schedule"""
        return await self.analytics_crud.get_by_schedule_id(
            schedule_id=schedule_id,
            limit=limit,
            level=level
        )

    async def get_analytics_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        schedule_id: Optional[str] = None,
        level: Optional[AnalyticsLevel] = None
    ) -> List[ScheduleAnalytics]:
        """Get analytics records within a time range"""
        return await self.analytics_crud.get_by_time_range(
            start_time=start_time,
            end_time=end_time,
            schedule_id=schedule_id,
            level=level
        )

    async def get_analytics_by_level(
        self,
        level: AnalyticsLevel,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[ScheduleAnalytics]:
        """Get analytics records by level"""
        return await self.analytics_crud.get_by_level(
            level=level,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

    async def get_analytics_by_event_type(
        self,
        event_type: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[ScheduleAnalytics]:
        """Get analytics records by event type"""
        return await self.analytics_crud.get_by_event_type(
            event_type=event_type,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

    async def get_execution_stats(
        self,
        schedule_id: Optional[str] = None,
        hours_back: int = 24
    ) -> ScheduleAnalyticsStats:
        """Get execution statistics"""
        stats_dict = await self.analytics_crud.get_execution_stats(
            schedule_id=schedule_id,
            hours_back=hours_back
        )
        
        return ScheduleAnalyticsStats(**stats_dict)

    async def get_error_patterns(
        self,
        hours_back: int = 24
    ) -> ErrorPatternsResponse:
        """Analyze error patterns"""
        patterns_dict = await self.analytics_crud.get_error_patterns(
            hours_back=hours_back
        )
        
        return ErrorPatternsResponse(**patterns_dict)

    async def cleanup_old_analytics(
        self,
        days_to_keep: int = 30
    ) -> int:
        """Clean up old analytics records"""
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        deleted_count = await self.analytics_crud.delete_before_date(cutoff_date)
        
        logger.info(f"Cleaned up {deleted_count} analytics records older than {cutoff_date}")
        return deleted_count

    # Convenience methods for common analytics scenarios
    
    async def record_mongo_schedule_execution(
        self,
        schedule_id: str,
        correlation_id: str,
        event_type: str,
        event_action: str,
        scheduled_at: datetime,
        started_processing_at: datetime,
        execution_result: ScheduleExecutionResult = ScheduleExecutionResult.SUCCESS,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        source_mongo_execution_id: Optional[str] = None
    ) -> str:
        """Record analytics for MongoDB schedule execution"""
        level = AnalyticsLevel.ERROR if execution_result == ScheduleExecutionResult.FAILURE else AnalyticsLevel.INFO
        
        return await self.record_analytics(
            schedule_type=ScheduleType.MONGO_PERIODIC,
            schedule_id=schedule_id,
            event_type=event_type,
            event_action=event_action,
            correlation_id=correlation_id,
            scheduled_at=scheduled_at,
            started_processing_at=started_processing_at,
            level=level,
            execution_result=execution_result,
            error_message=error_message,
            retry_count=retry_count,
            source_mongo_execution_id=source_mongo_execution_id
        )

    async def record_clickhouse_transfer(
        self,
        schedule_id: str,
        correlation_id: str,
        event_type: str,
        event_action: str,
        scheduled_at: datetime,
        started_processing_at: datetime,
        time_in_clickhouse_queue_ms: Optional[int] = None,
        execution_result: ScheduleExecutionResult = ScheduleExecutionResult.SUCCESS,
        error_message: Optional[str] = None,
        source_clickhouse_id: Optional[str] = None
    ) -> str:
        """Record analytics for ClickHouse to Redis transfer"""
        level = AnalyticsLevel.ERROR if execution_result == ScheduleExecutionResult.FAILURE else AnalyticsLevel.INFO
        
        return await self.record_analytics(
            schedule_type=ScheduleType.CLICKHOUSE_DELAYED,
            schedule_id=schedule_id,
            event_type=event_type,
            event_action=event_action,
            correlation_id=correlation_id,
            scheduled_at=scheduled_at,
            started_processing_at=started_processing_at,
            level=level,
            execution_result=execution_result,
            error_message=error_message,
            time_in_clickhouse_queue_ms=time_in_clickhouse_queue_ms,
            source_clickhouse_id=source_clickhouse_id
        )

    async def record_redis_processing(
        self,
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
        execution_result: ScheduleExecutionResult = ScheduleExecutionResult.SUCCESS,
        error_message: Optional[str] = None,
        source_redis_key: Optional[str] = None
    ) -> str:
        """Record analytics for Redis processing and Kafka publishing"""
        level = AnalyticsLevel.ERROR if execution_result == ScheduleExecutionResult.FAILURE else AnalyticsLevel.INFO
        
        return await self.record_analytics(
            schedule_type=ScheduleType.REDIS_IMMEDIATE,
            schedule_id=schedule_id,
            event_type=event_type,
            event_action=event_action,
            correlation_id=correlation_id,
            scheduled_at=scheduled_at,
            started_processing_at=started_processing_at,
            level=level,
            execution_result=execution_result,
            error_message=error_message,
            time_in_redis_queue_ms=time_in_redis_queue_ms,
            kafka_publish_time_ms=kafka_publish_time_ms,
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            source_redis_key=source_redis_key
        )


    async def record_warning(
        self,
        schedule_type: ScheduleType,
        schedule_id: str,
        event_type: str,
        event_action: str,
        correlation_id: str,
        scheduled_at: datetime,
        started_processing_at: datetime,
        message: str,
        schedule_name: Optional[str] = None,
        event_entity_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Record a warning-level analytics event"""
        return await self.record_analytics(
            schedule_type=schedule_type,
            schedule_id=schedule_id,
            event_type=event_type,
            event_action=event_action,
            correlation_id=correlation_id,
            scheduled_at=scheduled_at,
            started_processing_at=started_processing_at,
            level=AnalyticsLevel.WARNING,
            schedule_name=schedule_name,
            event_entity_id=event_entity_id,
            error_message=message,
            **kwargs
        )

    async def record_error(
        self,
        schedule_type: ScheduleType,
        schedule_id: str,
        event_type: str,
        event_action: str,
        correlation_id: str,
        scheduled_at: datetime,
        started_processing_at: datetime,
        error_message: str,
        schedule_name: Optional[str] = None,
        event_entity_id: Optional[str] = None,
        retry_count: int = 0,
        **kwargs
    ) -> str:
        """Record an error-level analytics event"""
        return await self.record_analytics(
            schedule_type=schedule_type,
            schedule_id=schedule_id,
            event_type=event_type,
            event_action=event_action,
            correlation_id=correlation_id,
            scheduled_at=scheduled_at,
            started_processing_at=started_processing_at,
            level=AnalyticsLevel.ERROR,
            schedule_name=schedule_name,
            event_entity_id=event_entity_id,
            execution_result=ScheduleExecutionResult.FAILURE,
            retry_count=retry_count,
            error_message=error_message,
            **kwargs
        )

    # Performance monitoring methods
    
    async def get_schedule_performance_summary(
        self,
        schedule_id: str,
        hours_back: int = 24
    ) -> Dict[str, Any]:
        """Get a performance summary for a specific schedule"""
        try:
            stats = await self.get_execution_stats(
                schedule_id=schedule_id,
                hours_back=hours_back
            )
            
            recent_errors = await self.get_analytics_by_level(
                level=AnalyticsLevel.ERROR,
                start_time=datetime.utcnow() - timedelta(hours=hours_back),
                limit=10
            )
            
            return {
                "schedule_id": schedule_id,
                "period_hours": hours_back,
                "statistics": stats,
                "recent_errors": [
                    {
                        "timestamp": error.started_processing_at,
                        "error_message": error.error_message,
                        "retry_count": error.retry_count
                    }
                    for error in recent_errors
                    if error.schedule_id == schedule_id
                ]
            }
            
        except Exception as e:
            logger.error(f"Failed to get performance summary for schedule {schedule_id}: {str(e)}")
            return {}

    async def get_system_health_metrics(
        self,
        hours_back: int = 1
    ) -> Dict[str, Any]:
        """Get overall system health metrics"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours_back)
            
            # Get overall stats
            overall_stats = await self.get_execution_stats(hours_back=hours_back)
            
            # Get error patterns
            error_patterns = await self.get_error_patterns(hours_back=hours_back)
            
            # Get stats by schedule type
            type_stats = {}
            for schedule_type in ScheduleType:
                type_analytics = await self.get_analytics_by_time_range(
                    start_time=start_time,
                    end_time=end_time
                )
                type_filtered = [a for a in type_analytics if a.schedule_type == schedule_type]
                
                if type_filtered:
                    type_stats[schedule_type.value] = {
                        "total_executions": len(type_filtered),
                        "success_count": len([a for a in type_filtered if a.execution_result == ScheduleExecutionResult.SUCCESS]),
                        "failure_count": len([a for a in type_filtered if a.execution_result == ScheduleExecutionResult.FAILURE]),
                        "avg_processing_time": sum([a.total_processing_time_ms or 0 for a in type_filtered]) / len(type_filtered) if type_filtered else 0
                    }
            
            return {
                "timestamp": datetime.utcnow(),
                "period_hours": hours_back,
                "overall_statistics": overall_stats,
                "error_patterns": error_patterns,
                "schedule_type_breakdown": type_stats
            }
            
        except Exception as e:
            logger.error(f"Failed to get system health metrics: {str(e)}")
            return {}


# Create service instance
analytics_service = ScheduleAnalyticsService()
