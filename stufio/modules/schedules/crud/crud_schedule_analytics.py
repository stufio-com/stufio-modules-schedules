"""
CRUD operations for schedule analytics
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from stufio.crud.clickhouse_base import CRUDClickhouse
from ..models.schedule_analytics import ScheduleAnalytics, AnalyticsLevel, ScheduleExecutionResult
from ..schemas.schedule_analytics import ScheduleAnalyticsCreate, ScheduleAnalyticsUpdate


class CRUDScheduleAnalytics(CRUDClickhouse[ScheduleAnalytics, ScheduleAnalyticsCreate, ScheduleAnalyticsUpdate]):
    """CRUD operations for schedule analytics"""
    
    async def get_by_schedule_id(
        self, 
        schedule_id: str, 
        limit: int = 100,
        level: Optional[AnalyticsLevel] = None
    ) -> List[ScheduleAnalytics]:
        """Get analytics records for a specific schedule"""
        where_clauses = [f"schedule_id = '{schedule_id}'"]
        
        if level:
            where_clauses.append(f"level = '{level.value}'")
        
        query = f"""
            SELECT * FROM {self.model.get_table_name()}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY completed_at DESC
            LIMIT {limit}
        """
        
        client = await self.client
        result = await client.query(query)
        return [self.model(**row) for row in result.named_results()]
    
    async def get_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        schedule_id: Optional[str] = None,
        level: Optional[AnalyticsLevel] = None
    ) -> List[ScheduleAnalytics]:
        """Get analytics records within a time range"""
        where_clauses = [
            f"completed_at >= '{start_time.isoformat()}'",
            f"completed_at <= '{end_time.isoformat()}'"
        ]
        
        if schedule_id:
            where_clauses.append(f"schedule_id = '{schedule_id}'")
            
        if level:
            where_clauses.append(f"level = '{level.value}'")
        
        query = f"""
            SELECT * FROM {self.model.get_table_name()}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY completed_at DESC
        """
        
        client = await self.client
        result = await client.query(query)
        return [self.model(**row) for row in result.named_results()]
    
    async def get_by_level(
        self,
        level: AnalyticsLevel,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[ScheduleAnalytics]:
        """Get analytics records by level with optional time filter"""
        where_clauses = [f"level = '{level.value}'"]
        
        if start_time and end_time:
            where_clauses.extend([
                f"completed_at >= '{start_time.isoformat()}'",
                f"completed_at <= '{end_time.isoformat()}'"
            ])
        elif start_time:
            where_clauses.append(f"completed_at >= '{start_time.isoformat()}'")
        elif end_time:
            where_clauses.append(f"completed_at <= '{end_time.isoformat()}'")
        
        query = f"""
            SELECT * FROM {self.model.get_table_name()}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY completed_at DESC
            LIMIT {limit}
        """
        
        client = await self.client
        result = await client.query(query)
        return [self.model(**row) for row in result.named_results()]
    
    async def get_by_event_type(
        self,
        event_type: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[ScheduleAnalytics]:
        """Get analytics records by event type"""
        where_clauses = [f"event_type = '{event_type}'"]
        
        if start_time and end_time:
            where_clauses.extend([
                f"completed_at >= '{start_time.isoformat()}'",
                f"completed_at <= '{end_time.isoformat()}'"
            ])
        
        query = f"""
            SELECT * FROM {self.model.get_table_name()}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY completed_at DESC
            LIMIT {limit}
        """
        
        client = await self.client
        result = await client.query(query)
        return [self.model(**row) for row in result.named_results()]
    
    async def get_with_queue_times(
        self,
        start_time: datetime,
        end_time: datetime,
        source: Optional[str] = None
    ) -> List[ScheduleAnalytics]:
        """Get analytics records that have queue time data"""
        where_clauses = [
            f"completed_at >= '{start_time.isoformat()}'",
            f"completed_at <= '{end_time.isoformat()}'",
            "total_processing_time_ms IS NOT NULL"
        ]
        
        query = f"""
            SELECT * FROM {self.model.get_table_name()}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY completed_at DESC
        """
        
        client = await self.client
        result = await client.query(query)
        return [self.model(**row) for row in result.named_results()]
    
    async def get_execution_stats(
        self,
        schedule_id: Optional[str] = None,
        hours_back: int = 24
    ) -> Dict[str, Any]:
        """Get execution statistics"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)
        
        # Get records using the time range query
        records = await self.get_by_time_range(start_time, end_time, schedule_id)
        
        stats: Dict[str, Any] = {
            "total_records": len(records),
            "info_count": len([r for r in records if r.level == AnalyticsLevel.INFO]),
            "warning_count": len([r for r in records if r.level == AnalyticsLevel.WARNING]),
            "error_count": len([r for r in records if r.level == AnalyticsLevel.ERROR]),
        }
        
        # Calculate averages - need to extract numeric values safely
        execution_times = []
        queue_times = []
        
        for r in records:
            if r.total_processing_time_ms is not None:
                execution_times.append(float(r.total_processing_time_ms))
            
            # Collect all queue times
            if r.time_in_mongo_queue_ms is not None:
                queue_times.append(float(r.time_in_mongo_queue_ms))
            if r.time_in_clickhouse_queue_ms is not None:
                queue_times.append(float(r.time_in_clickhouse_queue_ms))
            if r.time_in_redis_queue_ms is not None:
                queue_times.append(float(r.time_in_redis_queue_ms))
        
        stats["avg_execution_time"] = sum(execution_times) / len(execution_times) if execution_times else 0.0
        stats["avg_queue_time"] = sum(queue_times) / len(queue_times) if queue_times else 0.0
        
        # Calculate success rate
        success_count = len([r for r in records if r.execution_result == ScheduleExecutionResult.SUCCESS])
        stats["success_rate"] = (success_count / len(records)) if records else 0.0
        
        return stats
    
    async def delete_before_date(self, cutoff_date: datetime) -> int:
        """Delete analytics records before a specific date"""
        # ClickHouse uses lightweight deletes via mutations
        count_query = f"""
            SELECT count() as total FROM {self.model.get_table_name()}
            WHERE completed_at < '{cutoff_date.isoformat()}'
        """
        
        # Get count first
        client = await self.client
        result = await client.query(count_query)
        rows = list(result.named_results())
        count = int(rows[0]["total"]) if rows else 0
        
        # Delete records using ALTER TABLE DELETE
        if count > 0:
            delete_query = f"""
                ALTER TABLE {self.model.get_table_name()}
                DELETE WHERE completed_at < '{cutoff_date.isoformat()}'
            """
            await client.query(delete_query)
        
        return count
    
    async def get_error_patterns(
        self,
        hours_back: int = 24
    ) -> Dict[str, Any]:
        """Analyze error patterns"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)
        
        error_records = await self.get_by_level(
            AnalyticsLevel.ERROR,
            start_time=start_time,
            end_time=end_time
        )
        
        # Analyze patterns
        error_types = {}
        schedule_errors = {}
        
        for record in error_records:
            # Count by event type
            event_type = record.event_type
            error_types[event_type] = error_types.get(event_type, 0) + 1
            
            # Count by schedule
            if record.schedule_id:
                schedule_id = str(record.schedule_id)
                schedule_errors[schedule_id] = schedule_errors.get(schedule_id, 0) + 1
        
        return {
            "total_errors": len(error_records),
            "error_types": error_types,
            "schedule_errors": schedule_errors,
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            }
        }


# Create the instance
crud_schedule_analytics = CRUDScheduleAnalytics(ScheduleAnalytics)
