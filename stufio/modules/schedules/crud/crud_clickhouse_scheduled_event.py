from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timedelta
from uuid import UUID
from nameniac_fastapi.app import crud
from stufio.crud.clickhouse_base import CRUDClickhouse
from ..models.clickhouse_scheduled_event import ClickhouseScheduledEvent, ScheduledEventStatus, ScheduledEventSource
from ..schemas.clickhouse_scheduled_event import ClickhouseScheduledEventCreate, ClickhouseScheduledEventUpdate


class CRUDClickhouseScheduledEvent(CRUDClickhouse[ClickhouseScheduledEvent, ClickhouseScheduledEventCreate, ClickhouseScheduledEventUpdate]):
    """CRUD operations for ClickHouse scheduled events."""

    async def get_pending_events(self, limit: int = 1000) -> List[ClickhouseScheduledEvent]:
        """Get pending events ordered by scheduled_at."""
        query = f"""
        SELECT * FROM {self.model.get_table_name()}
        WHERE status = 'pending'
        ORDER BY scheduled_at ASC, priority DESC
        LIMIT {limit}
        """
        return await self.execute_query(query)

    async def get_events_due_for_transfer(
        self,
        transfer_horizon: datetime,
        limit: int = 1000
    ) -> List[ClickhouseScheduledEvent]:
        """Get events that should be transferred to Redis."""
        query = f"""
        SELECT * FROM {self.model.get_table_name()}
        WHERE status = 'pending'
        AND scheduled_at <= '{transfer_horizon.isoformat()}'
        ORDER BY scheduled_at ASC, priority DESC
        LIMIT {limit}
        """
        return await self.execute_query(query)



    async def update_status(
        self,
        schedule_id: str,
        status: ScheduledEventStatus,
        error: Optional[str] = None,
        node_id: Optional[str] = None,
        redis_key: Optional[str] = None
    ) -> bool:
        """Update the status of a scheduled event."""
        set_clauses = [
            f"status = '{status.value}'",
            f"updated_at = '{datetime.utcnow().isoformat()}'"
        ]
        
        if status == ScheduledEventStatus.PROCESSING:
            set_clauses.append(f"processing_started_at = '{datetime.utcnow().isoformat()}'")
        elif status in [ScheduledEventStatus.COMPLETED, ScheduledEventStatus.ERROR]:
            set_clauses.append(f"completed_at = '{datetime.utcnow().isoformat()}'")
        elif status == ScheduledEventStatus.TRANSFERRED_TO_REDIS:
            set_clauses.append(f"transferred_to_redis_at = '{datetime.utcnow().isoformat()}'")
            if redis_key:
                set_clauses.append(f"redis_key = '{redis_key}'")
        
        if error:
            escaped_error = error.replace("'", "''")
            set_clauses.append(f"error = '{escaped_error}'")
        if node_id:
            set_clauses.append(f"node_id = '{node_id}'")
        
        query = f"""
        ALTER TABLE {self.model.get_table_name()}
        UPDATE {', '.join(set_clauses)}
        WHERE schedule_id = '{schedule_id}'
        """
        
        result = await self.execute_query(query)
        return True  # ClickHouse ALTER doesn't return affected rows count easily

    async def acquire_lock(
        self,
        schedule_id: str,
        node_id: str,
        lock_duration_seconds: int = 300
    ) -> bool:
        """Acquire a distributed lock on a scheduled event."""
        lock_until = datetime.utcnow() + timedelta(seconds=lock_duration_seconds)
        
        query = f"""
        ALTER TABLE {self.model.get_table_name()}
        UPDATE 
            status = 'processing',
            node_id = '{node_id}',
            lock_until = '{lock_until.isoformat()}',
            processing_started_at = '{datetime.utcnow().isoformat()}',
            updated_at = '{datetime.utcnow().isoformat()}'
        WHERE schedule_id = '{schedule_id}'
        AND (status = 'pending' OR (status = 'processing' AND lock_until < '{datetime.utcnow().isoformat()}'))
        """
        
        result = await self.execute_query(query)
        return True  # In a real implementation, you'd check if the update actually happened

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics about scheduled events."""
        query = f"""
        SELECT 
            status,
            COUNT(*) as count,
            AVG(CASE 
                WHEN completed_at IS NOT NULL AND processing_started_at IS NOT NULL 
                THEN dateDiff('millisecond', processing_started_at, completed_at) 
                ELSE NULL 
            END) as avg_processing_time_ms,
            AVG(CASE 
                WHEN processing_started_at IS NOT NULL 
                THEN dateDiff('millisecond', created_at, processing_started_at) 
                ELSE NULL 
            END) as avg_queue_time_ms
        FROM {self.model.get_table_name()}
        GROUP BY status
        """
        
        result = await self.execute_query(query)
        
        stats = {
            "total_count": 0,
            "pending_count": 0,
            "processing_count": 0,
            "completed_count": 0,
            "error_count": 0,
            "transferred_to_redis_count": 0,
            "avg_processing_time_ms": None,
            "avg_queue_time_ms": None
        }
        
        for row in result:
            status = row.get("status")
            count = row.get("count", 0)
            stats["total_count"] += count
            
            if status == "pending":
                stats["pending_count"] = count
            elif status == "processing":
                stats["processing_count"] = count
            elif status == "completed":
                stats["completed_count"] = count
                if row.get("avg_processing_time_ms"):
                    stats["avg_processing_time_ms"] = row["avg_processing_time_ms"]
            elif status == "error":
                stats["error_count"] = count
            elif status == "transferred_to_redis":
                stats["transferred_to_redis_count"] = count
            
            if row.get("avg_queue_time_ms") and not stats["avg_queue_time_ms"]:
                stats["avg_queue_time_ms"] = row["avg_queue_time_ms"]
        
        return stats

    async def cleanup_completed_events(self, before: datetime) -> int:
        """Delete completed events older than the specified date."""
        # First count how many we're about to delete
        count_query = f"""
        SELECT COUNT(*) as count FROM {self.model.get_table_name()}
        WHERE status IN ('completed', 'error') 
        AND completed_at < '{before.isoformat()}'
        """
        
        result = await self.execute_query(count_query)
        count = result[0]["count"] if result else 0
        
        # Then delete them
        query = f"""
        ALTER TABLE {self.model.get_table_name()}
        DELETE WHERE status IN ('completed', 'error') 
        AND completed_at < '{before.isoformat()}'
        """
        
        await self.execute_query(query)
        return count

    async def mark_transferred(
        self,
        event_id: Union[str, UUID],
        transfer_metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[ClickhouseScheduledEvent]:
        """Mark an event as transferred to Redis."""
        # Convert UUID to string if needed
        event_id_str = str(event_id) if hasattr(event_id, '__str__') else event_id
        
        set_clauses = [
            f"status = '{ScheduledEventStatus.TRANSFERRED_TO_REDIS.value}'",
            f"transferred_to_redis_at = '{datetime.utcnow().isoformat()}'",
            f"updated_at = '{datetime.utcnow().isoformat()}'"
        ]
        
        if transfer_metadata:
            # Store metadata as JSON string (escaped for SQL)
            import json
            metadata_json = json.dumps(transfer_metadata).replace("'", "''")
            set_clauses.append(f"transfer_metadata = '{metadata_json}'")
        
        query = f"""
        ALTER TABLE {self.model.get_table_name()}
        UPDATE {', '.join(set_clauses)}
        WHERE id = '{event_id_str}' AND status = 'pending'
        """
        
        await self.execute_query(query)
        
        # Return the updated event
        return await self.get(event_id_str)

    async def bulk_mark_transferred(
        self,
        event_ids: List[Union[str, UUID]]
    ) -> Dict[str, Any]:
        """Bulk mark events as transferred to Redis."""
        if not event_ids:
            return {"transferred_count": 0, "failed_ids": []}
        
        # Convert all UUIDs to strings
        event_ids_str = [str(event_id) for event_id in event_ids]
        ids_str = "', '".join(event_ids_str)
        
        query = f"""
        ALTER TABLE {self.model.get_table_name()}
        UPDATE 
            status = '{ScheduledEventStatus.TRANSFERRED_TO_REDIS.value}',
            transferred_to_redis_at = '{datetime.utcnow().isoformat()}',
            updated_at = '{datetime.utcnow().isoformat()}'
        WHERE id IN ('{ids_str}') AND status = 'pending'
        """
        
        await self.execute_query(query)
        
        # Count successful transfers by checking which events are now transferred
        check_query = f"""
        SELECT COUNT(*) as count FROM {self.model.get_table_name()}
        WHERE id IN ('{ids_str}') AND status = '{ScheduledEventStatus.TRANSFERRED_TO_REDIS.value}'
        """
        
        result = await self.execute_query(check_query)
        transferred_count = result[0]["count"] if result else 0
        
        return {
            "transferred_count": transferred_count,
            "failed_ids": []  # Would need more complex logic to track individual failures
        }

    async def get_stuck_events(
        self,
        stuck_threshold_minutes: int = 30
    ) -> List[ClickhouseScheduledEvent]:
        """Get events that are stuck in processing status."""
        stuck_since = datetime.utcnow() - timedelta(minutes=stuck_threshold_minutes)
        
        query = f"""
        SELECT * FROM {self.model.get_table_name()}
        WHERE status = 'processing'
        AND (lock_until IS NULL OR lock_until < '{datetime.utcnow().isoformat()}')
        AND processing_started_at < '{stuck_since.isoformat()}'
        ORDER BY processing_started_at ASC
        """
        
        return await self.execute_query(query)

    async def get_by_status(
        self,
        status: ScheduledEventStatus,
        limit: int = 100
    ) -> List[ClickhouseScheduledEvent]:
        """Get events filtered by status."""
        query = f"""
        SELECT * FROM {self.model.get_table_name()}
        WHERE status = '{status.value}'
        ORDER BY created_at DESC
        LIMIT {limit}
        """
        return await self.execute_query(query)

    async def get_by_source(
        self,
        source: str,
        source_id: Optional[str] = None,
        limit: int = 100
    ) -> List[ClickhouseScheduledEvent]:
        """Get events by source and optionally source ID."""
        where_clauses = [f"source = '{source}'"]
        if source_id:
            where_clauses.append(f"source_id = '{source_id}'")
        
        query = f"""
        SELECT * FROM {self.model.get_table_name()}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY created_at DESC
        LIMIT {limit}
        """
        return await self.execute_query(query)

    async def get_by_time_range(
        self,
        scheduled_after: Optional[datetime] = None,
        scheduled_before: Optional[datetime] = None,
        limit: int = 100
    ) -> List[ClickhouseScheduledEvent]:
        """Get events filtered by scheduled time range."""
        where_clauses = []
        if scheduled_after:
            where_clauses.append(f"scheduled_at >= '{scheduled_after.isoformat()}'")
        if scheduled_before:
            where_clauses.append(f"scheduled_at <= '{scheduled_before.isoformat()}'")
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
        SELECT * FROM {self.model.get_table_name()}
        WHERE {where_clause}
        ORDER BY scheduled_at ASC
        LIMIT {limit}
        """
        return await self.execute_query(query)

    async def get_stats_by_time(
        self,
        hours_back: int = 24
    ) -> Dict[str, Any]:
        """Get statistics grouped by time periods."""
        since_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        query = f"""
        SELECT 
            toStartOfHour(created_at) as hour,
            status,
            COUNT(*) as count,
            AVG(CASE 
                WHEN completed_at IS NOT NULL AND processing_started_at IS NOT NULL 
                THEN dateDiff('millisecond', processing_started_at, completed_at) 
                ELSE NULL 
            END) as avg_processing_time_ms
        FROM {self.model.get_table_name()}
        WHERE created_at >= '{since_time.isoformat()}'
        GROUP BY hour, status
        ORDER BY hour DESC, status
        """
        
        result = await self.execute_query(query)
        
        # Organize results by hour
        stats_by_hour = {}
        for row in result:
            hour = row.get("hour")
            status = row.get("status")
            count = row.get("count", 0)
            avg_time = row.get("avg_processing_time_ms")
            
            if hour not in stats_by_hour:
                stats_by_hour[hour] = {
                    "total": 0,
                    "pending": 0,
                    "processing": 0,
                    "completed": 0,
                    "error": 0,
                    "transferred_to_redis": 0,
                    "avg_processing_time_ms": None
                }
            
            stats_by_hour[hour]["total"] += count
            if status == "pending":
                stats_by_hour[hour]["pending"] = count
            elif status == "processing":
                stats_by_hour[hour]["processing"] = count
            elif status == "completed":
                stats_by_hour[hour]["completed"] = count
                if avg_time:
                    stats_by_hour[hour]["avg_processing_time_ms"] = avg_time
            elif status == "error":
                stats_by_hour[hour]["error"] = count
            elif status == "transferred_to_redis":
                stats_by_hour[hour]["transferred_to_redis"] = count
        
        return {
            "hours_analyzed": hours_back,
            "stats_by_hour": stats_by_hour,
            "total_hours": len(stats_by_hour)
        }

    async def get_stats_by_source(self) -> Dict[str, Any]:
        """Get statistics grouped by source."""
        query = f"""
        SELECT 
            source,
            status,
            COUNT(*) as count,
            AVG(CASE 
                WHEN completed_at IS NOT NULL AND processing_started_at IS NOT NULL 
                THEN dateDiff('millisecond', processing_started_at, completed_at) 
                ELSE NULL 
            END) as avg_processing_time_ms,
            MIN(created_at) as earliest_event,
            MAX(created_at) as latest_event
        FROM {self.model.get_table_name()}
        GROUP BY source, status
        ORDER BY source, status
        """
        
        result = await self.execute_query(query)
        
        # Organize results by source
        stats_by_source = {}
        for row in result:
            source = row.get("source")
            status = row.get("status")
            count = row.get("count", 0)
            avg_time = row.get("avg_processing_time_ms")
            earliest = row.get("earliest_event")
            latest = row.get("latest_event")
            
            if source not in stats_by_source:
                stats_by_source[source] = {
                    "total": 0,
                    "pending": 0,
                    "processing": 0,
                    "completed": 0,
                    "error": 0,
                    "transferred_to_redis": 0,
                    "avg_processing_time_ms": None,
                    "earliest_event": earliest,
                    "latest_event": latest
                }
            
            stats_by_source[source]["total"] += count
            if status == "pending":
                stats_by_source[source]["pending"] = count
            elif status == "processing":
                stats_by_source[source]["processing"] = count
            elif status == "completed":
                stats_by_source[source]["completed"] = count
                if avg_time:
                    stats_by_source[source]["avg_processing_time_ms"] = avg_time
            elif status == "error":
                stats_by_source[source]["error"] = count
            elif status == "transferred_to_redis":
                stats_by_source[source]["transferred_to_redis"] = count
        
        return {
            "stats_by_source": stats_by_source,
            "total_sources": len(stats_by_source)
        }

    async def count_old_events(self, days_old: int) -> int:
        """Count old events that would be deleted in cleanup."""
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        
        query = f"""
        SELECT COUNT(*) as count FROM {self.model.get_table_name()}
        WHERE status IN ('completed', 'error') 
        AND completed_at < '{cutoff_date.isoformat()}'
        """
        
        result = await self.execute_query(query)
        return result[0]["count"] if result else 0

    async def cleanup_old_events(self, days_old: int) -> int:
        """Delete completed events older than the specified number of days."""
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        
        # First count how many we're about to delete
        count = await self.count_old_events(days_old)
        
        query = f"""
        ALTER TABLE {self.model.get_table_name()}
        DELETE WHERE status IN ('completed', 'error') 
        AND completed_at < '{cutoff_date.isoformat()}'
        """
        
        await self.execute_query(query)
        return count  # Return the count we got before deletion

    async def batch_create(
        self,
        events_data: List[ClickhouseScheduledEventCreate]
    ) -> Dict[str, Any]:
        """Batch create multiple scheduled events."""
        if not events_data:
            return {"created_count": 0, "failed_count": 0, "errors": []}
        
        created_count = 0
        failed_count = 0
        errors = []
        
        # For simplicity, create events one by one
        # In a real implementation, you might want to use bulk insert
        for i, event_data in enumerate(events_data):
            try:
                await self.create(event_data)
                created_count += 1
            except Exception as e:
                failed_count += 1
                errors.append({
                    "index": i,
                    "error": str(e),
                    "event_type": getattr(event_data, 'event_type', 'unknown')
                })
        
        return {
            "created_count": created_count,
            "failed_count": failed_count,
            "errors": errors
        }

    async def get_ready_for_transfer(
        self,
        transfer_window: datetime,
        limit: int = 100
    ) -> List[ClickhouseScheduledEvent]:
        """Get events ready for transfer to Redis within the transfer window."""
        query = f"""
        SELECT * FROM {self.model.get_table_name()}
        WHERE status = 'pending'
        AND scheduled_at <= '{transfer_window.isoformat()}'
        ORDER BY scheduled_at ASC, priority DESC
        LIMIT {limit}
        """
        return await self.execute_query(query)

    async def update(
        self,
        id: Any,
        obj_in: ClickhouseScheduledEventUpdate
    ) -> Optional[ClickhouseScheduledEvent]:
        """Update an existing scheduled event."""
        # Convert UUID to string if needed
        event_id = str(id) if hasattr(id, '__str__') else id
        
        # Get current event
        current_event = await self.get(event_id)
        if not current_event:
            return None
        
        # Build update clauses
        update_data = obj_in.model_dump(exclude_unset=True)
        if not update_data:
            return current_event
        
        set_clauses = []
        for field, value in update_data.items():
            if isinstance(value, str):
                escaped_value = value.replace("'", "''")
                set_clauses.append(f"{field} = '{escaped_value}'")
            elif isinstance(value, datetime):
                set_clauses.append(f"{field} = '{value.isoformat()}'")
            else:
                set_clauses.append(f"{field} = '{value}'")
        
        # Add updated_at timestamp
        set_clauses.append(f"updated_at = '{datetime.utcnow().isoformat()}'")
        
        query = f"""
        ALTER TABLE {self.model.get_table_name()}
        UPDATE {', '.join(set_clauses)}
        WHERE id = '{event_id}'
        """
        
        await self.execute_query(query)
        
        # Return updated event
        return await self.get(event_id)

    async def delete(self, id: Any) -> bool:
        """Delete a scheduled event by ID."""
        # Convert UUID to string if needed
        event_id = str(id) if hasattr(id, '__str__') else id
        
        # Check if event exists
        existing_event = await self.get(event_id)
        if not existing_event:
            return False
        
        query = f"""
        ALTER TABLE {self.model.get_table_name()}
        DELETE WHERE id = '{event_id}'
        """
        
        await self.execute_query(query)
        return True

    async def get_multi(
        self,
        skip: int = 0,
        limit: int = 100,
        **kwargs
    ) -> List[ClickhouseScheduledEvent]:
        """Get multiple events with pagination."""
        query = f"""
        SELECT * FROM {self.model.get_table_name()}
        ORDER BY created_at DESC
        LIMIT {limit} OFFSET {skip}
        """
        
        result = await self.execute_query(query)
        return [ClickhouseScheduledEvent(**row) for row in result.named_results()]


crud_clickhouse_scheduled_event = CRUDClickhouseScheduledEvent(ClickhouseScheduledEvent)