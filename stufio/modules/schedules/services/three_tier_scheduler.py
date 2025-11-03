"""
Three-tier scheduling service orchestrator

This service coordinates the flow between:
1. MongoDB (periodic cron-based schedules) -> ClickHouse (delayed events)
2. ClickHouse -> Redis (immediate scheduling up to 1h)  
3. Redis -> Kafka (event publishing)
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from uuid import UUID

import redis
from croniter import croniter

from ..models.mongo_schedule import MongoSchedule, ScheduleStatus
from ..models.clickhouse_scheduled_event import (
    ClickhouseScheduledEvent,
    ScheduledEventStatus,
    ScheduledEventSource,
)
from ..models.redis_scheduled_event import RedisScheduledEvent
from ..models.schedule_analytics import ScheduleAnalytics, AnalyticsLevel, ScheduleType

from ..crud.crud_mongo_schedule import CRUDMongoSchedule
from ..crud.crud_clickhouse_scheduled_event import CRUDClickhouseScheduledEvent
from ..crud.crud_redis_scheduled_event import CRUDRedisScheduledEvent

from ..schemas.clickhouse_scheduled_event import ClickhouseScheduledEventCreate
from ..schemas.redis_scheduled_event import RedisScheduledEventCreate
from ..schemas.schedule_analytics import ScheduleAnalyticsCreate

logger = logging.getLogger(__name__)


class ThreeTierSchedulerService:
    """
    Main orchestrator for the three-tier scheduling system
    """
    
    def __init__(
        self,
        mongo_crud: CRUDMongoSchedule,
        clickhouse_crud: CRUDClickhouseScheduledEvent,
        redis_crud: CRUDRedisScheduledEvent,
        kafka_producer,  # Would be actual Kafka producer
        redis_client: redis.Redis,
        analytics_service=None
    ):
        self.mongo_crud = mongo_crud
        self.clickhouse_crud = clickhouse_crud
        self.redis_crud = redis_crud
        self.kafka_producer = kafka_producer
        self.redis_client = redis_client
        self.analytics_service = analytics_service
        
        # Configuration
        self.mongo_check_interval = 60  # Check MongoDB schedules every minute
        self.clickhouse_transfer_interval = 30  # Transfer to Redis every 30 seconds
        self.redis_process_interval = 1  # Process Redis events every second
        self.transfer_window_hours = 1  # Transfer events scheduled within next hour
        
        # State tracking
        self._running = False
        self._tasks = []
    
    async def start(self):
        """Start the three-tier scheduler service"""
        if self._running:
            return
        
        self._running = True
        logger.info("Starting three-tier scheduler service")
        
        # Start the three main processing loops
        self._tasks = [
            asyncio.create_task(self._mongo_schedule_processor()),
            asyncio.create_task(self._clickhouse_to_redis_transfer()),
            asyncio.create_task(self._redis_event_processor()),
            asyncio.create_task(self._health_monitor())
        ]
        
        await asyncio.gather(*self._tasks, return_exceptions=True)
    
    async def stop(self):
        """Stop the scheduler service"""
        logger.info("Stopping three-tier scheduler service")
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
    
    async def _mongo_schedule_processor(self):
        """
        Process MongoDB schedules and create ClickHouse scheduled events
        """
        logger.info("Starting MongoDB schedule processor")
        
        while self._running:
            try:
                start_time = datetime.now(timezone.utc)
                
                # Get active schedules that are due for execution
                due_schedules = await self.mongo_crud.get_schedules_due_for_execution(start_time)
                
                for schedule in due_schedules:
                    try:
                        await self._process_mongo_schedule(schedule, start_time)
                    except Exception as e:
                        logger.error(f"Error processing schedule {schedule.id}: {str(e)}")
                        await self._record_analytics(
                            schedule_id=str(schedule.id),
                            level=AnalyticsLevel.ERROR,
                            event_type="mongo_schedule_processing",
                            data={"error": str(e)}
                        )
                
                # Record processing metrics
                processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                await self._record_analytics(
                    level=AnalyticsLevel.INFO,
                    event_type="mongo_processor_cycle",
                    data={
                        "processed_schedules": len(due_schedules),
                        "processing_time_ms": processing_time * 1000
                    }
                )
                
            except Exception as e:
                logger.error(f"Error in MongoDB schedule processor: {str(e)}")
            
            await asyncio.sleep(self.mongo_check_interval)
    
    async def _process_mongo_schedule(self, schedule: MongoSchedule, current_time: datetime):
        """Process a single MongoDB schedule"""
        logger.debug(f"Processing schedule: {schedule.name}")
        
        # Calculate next execution time using cron expression
        cron = croniter(schedule.cron_expression, current_time)
        next_execution = cron.get_next(datetime)
        
        # Create ClickHouse scheduled event
        event_data = ClickhouseScheduledEventCreate(
            topic=schedule.event_type,  # Use event_type as topic
            entity_type=schedule.event_type,
            action=schedule.event_action,
            entity_id=schedule.event_entity_id,
            actor_type=schedule.actor_type,
            actor_id=schedule.actor_id,
            payload=str(schedule.event_payload) if schedule.event_payload else "{}",
            scheduled_at=next_execution,
            source=ScheduledEventSource.MONGO_SCHEDULE,
            source_id=str(schedule.id),
            priority=0,  # Default priority as schedule doesn't have this field
            correlation_id=str(uuid.uuid4())
        )
        
        # Create the event in ClickHouse
        created_event = await self.clickhouse_crud.create(event_data)
        
        # Record execution in MongoDB
        await self.mongo_crud.record_execution(
            str(schedule.id),
            next_execution,
            success=True,
            clickhouse_event_id=created_event.schedule_id
        )
        
        # Update schedule's next execution time
        await self.mongo_crud.update_next_execution(str(schedule.id), next_execution)
        
        logger.info(f"Created ClickHouse event {created_event.schedule_id} for schedule {schedule.name}")
    
    async def _clickhouse_to_redis_transfer(self):
        """
        Transfer events from ClickHouse to Redis when they're within 1 hour of execution
        """
        logger.info("Starting ClickHouse to Redis transfer processor")
        
        while self._running:
            try:
                start_time = datetime.now(timezone.utc)
                transfer_cutoff = start_time + timedelta(hours=self.transfer_window_hours)
                
                # Get events ready for transfer
                ready_events = await self.clickhouse_crud.get_ready_for_transfer(transfer_cutoff)
                
                transferred_count = 0
                for event in ready_events:
                    try:
                        await self._transfer_to_redis(event, start_time)
                        transferred_count += 1
                    except Exception as e:
                        logger.error(f"Error transferring event {event.schedule_id}: {str(e)}")
                        await self._record_analytics(
                            schedule_id=event.source_id,
                            level=AnalyticsLevel.ERROR,
                            event_type="clickhouse_to_redis_transfer",
                            data={"event_id": str(event.schedule_id), "error": str(e)}
                        )
                
                # Record transfer metrics
                processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                await self._record_analytics(
                    level=AnalyticsLevel.INFO,
                    event_type="clickhouse_transfer_cycle",
                    data={
                        "transferred_events": transferred_count,
                        "processing_time_ms": processing_time * 1000
                    }
                )
                
            except Exception as e:
                logger.error(f"Error in ClickHouse to Redis transfer: {str(e)}")
            
            await asyncio.sleep(self.clickhouse_transfer_interval)
    
    async def _transfer_to_redis(self, event: ClickhouseScheduledEvent, transfer_time: datetime):
        """Transfer a single event from ClickHouse to Redis"""
        
        # Calculate queue time in ClickHouse
        queue_time_ms = (transfer_time - event.created_at).total_seconds() * 1000
        
        # Create Redis event
        redis_event_data = RedisScheduledEventCreate(
            topic=event.topic,
            entity_type=event.entity_type,
            action=event.action,
            entity_id=event.entity_id,
            actor_type=event.actor_type,
            actor_id=event.actor_id,
            payload={"queue_time_ms": queue_time_ms, "transferred_at": transfer_time.isoformat()},
            scheduled_at=event.scheduled_at,
            source=event.source,
            source_id=event.source_id,
            clickhouse_schedule_id=event.schedule_id,
            priority=event.priority,
            correlation_id=event.correlation_id
        )
        
        # Create in Redis
        redis_event = self.redis_crud.create(redis_event_data)
        
        # Mark as transferred in ClickHouse
        await self.clickhouse_crud.mark_transferred(
            event.schedule_id,
            {"redis_transfer_time": transfer_time.isoformat()}
        )
        
        logger.debug(f"Transferred event {event.schedule_id} to Redis")
    
    async def _redis_event_processor(self):
        """
        Process Redis events and publish to Kafka
        """
        logger.info("Starting Redis event processor")
        
        while self._running:
            try:
                start_time = datetime.now(timezone.utc)
                
                # Get events ready for processing
                ready_events = self.redis_crud.get_ready_events(limit=100)
                
                processed_count = 0
                for event in ready_events:
                    try:
                        success = await self._process_redis_event(event, start_time)
                        if success:
                            processed_count += 1
                    except Exception as e:
                        logger.error(f"Error processing Redis event {event.event_id}: {str(e)}")
                        # Mark as failed
                        self.redis_crud.mark_processed(event.event_id, success=False, error_message=str(e))
                
                # Record processing metrics
                processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                await self._record_analytics(
                    level=AnalyticsLevel.INFO,
                    event_type="redis_processor_cycle",
                    data={
                        "processed_events": processed_count,
                        "processing_time_ms": processing_time * 1000
                    }
                )
                
            except Exception as e:
                logger.error(f"Error in Redis event processor: {str(e)}")
            
            await asyncio.sleep(self.redis_process_interval)
    
    async def _process_redis_event(self, event, process_time: datetime) -> bool:
        """Process a single Redis event"""
        
        # Claim the event for processing
        processor_id = f"scheduler-{id(self)}"
        claimed = self.redis_crud.claim_for_processing(event.event_id, processor_id)
        
        if not claimed:
            return False  # Already being processed
        
        try:
            # Calculate queue times
            redis_queue_time_ms = (process_time - event.created_at).total_seconds() * 1000
            total_queue_time_ms = redis_queue_time_ms
            
            # Check if we have clickhouse queue time in payload
            if event.payload and 'queue_time_ms' in event.payload:
                total_queue_time_ms += event.payload['queue_time_ms']
            
            # Prepare Kafka message
            kafka_message = {
                "event_id": event.event_id,
                "event_type": event.entity_type,
                "event_action": event.action,
                "event_data": event.payload,
                "scheduled_at": event.scheduled_at.isoformat(),
                "published_at": process_time.isoformat(),
                "source": event.source,
                "source_id": event.source_id,
                "priority": event.priority,
                "queue_time_ms": total_queue_time_ms,
                "metadata": event.headers
            }
            
            # Publish to Kafka
            await self._publish_to_kafka(kafka_message)
            
            # Mark as processed
            self.redis_crud.mark_processed(event.event_id, success=True)
            
            # Record analytics
            await self._record_analytics(
                schedule_id=event.source_id,
                level=AnalyticsLevel.INFO,
                event_type="event_published",
                data={
                    "event_id": event.event_id,
                    "queue_time_ms": total_queue_time_ms,
                    "redis_queue_time_ms": redis_queue_time_ms
                }
            )
            
            logger.debug(f"Published event {event.event_id} to Kafka")
            return True
            
        except Exception as e:
            # Mark as failed
            self.redis_crud.mark_processed(event.event_id, success=False, error_message=str(e))
            raise
    
    async def _publish_to_kafka(self, message: Dict[str, Any]):
        """Publish message to Kafka"""
        if self.kafka_producer:
            # Add schedule_id to headers for analytics
            headers = {}
            if message.get('source_id'):
                headers['schedule_id'] = message['source_id']
            
            await self.kafka_producer.send(
                topic="scheduled_events",
                value=message,
                headers=headers
            )
        else:
            logger.warning("No Kafka producer configured - message not sent")
    
    async def _health_monitor(self):
        """Monitor system health and performance"""
        logger.info("Starting health monitor")
        
        while self._running:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Check Redis queue health
                redis_health = self.redis_crud.get_queue_health()
                
                if redis_health.get('health_status') == 'unhealthy':
                    logger.warning(f"Redis queue unhealthy: {redis_health}")
                    await self._record_analytics(
                        level=AnalyticsLevel.WARNING,
                        event_type="redis_queue_unhealthy",
                        data=redis_health
                    )
                
                # Check for stuck events in ClickHouse
                stuck_events = await self.clickhouse_crud.get_stuck_events()
                if stuck_events:
                    logger.warning(f"Found {len(stuck_events)} stuck events in ClickHouse")
                    await self._record_analytics(
                        level=AnalyticsLevel.WARNING,
                        event_type="clickhouse_stuck_events",
                        data={"count": len(stuck_events)}
                    )
                
                # Cleanup expired Redis events
                cleaned_count = self.redis_crud.cleanup_expired()
                if cleaned_count > 0:
                    logger.info(f"Cleaned up {cleaned_count} expired Redis events")
                
            except Exception as e:
                logger.error(f"Error in health monitor: {str(e)}")
    
    async def _record_analytics(
        self,
        level: AnalyticsLevel,
        event_type: str,
        data: Dict[str, Any],
        schedule_id: Optional[str] = None
    ):
        """Record analytics data"""
        if not self.analytics_service:
            return
        
        try:
            # Create analytics record with required fields from the schema
            analytics_data = ScheduleAnalyticsCreate(
                schedule_type=ScheduleType.MONGO_PERIODIC if schedule_id else ScheduleType.REDIS_IMMEDIATE,
                schedule_id=schedule_id or "system",
                level=level,
                event_type=event_type,
                event_action="process",
                correlation_id=str(uuid.uuid4()),
                scheduled_at=datetime.now(timezone.utc),
                started_processing_at=datetime.now(timezone.utc),
            )
            
            await self.analytics_service.create_analytics_record(analytics_data)
            
        except Exception as e:
            logger.error(f"Failed to record analytics: {str(e)}")
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        try:
            # MongoDB schedules status
            active_schedules_count = len(await self.mongo_crud.get_active_schedules())
            
            # ClickHouse status
            clickhouse_stats = await self.clickhouse_crud.get_stats()
            
            # Redis status
            redis_stats = self.redis_crud.get_stats()
            redis_health = self.redis_crud.get_queue_health()
            
            return {
                "service_running": self._running,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "mongo_schedules": {
                    "active_count": active_schedules_count
                },
                "clickhouse_events": clickhouse_stats,
                "redis_events": {
                    **redis_stats,
                    "health": redis_health
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {str(e)}")
            return {"error": str(e)}
    
    async def trigger_manual_transfer(self, event_id: UUID) -> bool:
        """Manually trigger transfer of specific event from ClickHouse to Redis"""
        try:
            event = await self.clickhouse_crud.get(event_id)
            if not event:
                return False
            
            if event.status != ScheduledEventStatus.PENDING:
                return False
            
            await self._transfer_to_redis(event, datetime.now(timezone.utc))
            return True
            
        except Exception as e:
            logger.error(f"Error in manual transfer: {str(e)}")
            return False


# Dependency for FastAPI
async def get_scheduler_service() -> ThreeTierSchedulerService:
    """Get the scheduler service instance"""
    # This would be injected with actual dependencies
    mongo_crud = CRUDMongoSchedule(MongoSchedule)
    clickhouse_crud = CRUDClickhouseScheduledEvent(ClickhouseScheduledEvent)
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    redis_crud = CRUDRedisScheduledEvent(redis_client)
    
    return ThreeTierSchedulerService(
        mongo_crud=mongo_crud,
        clickhouse_crud=clickhouse_crud,
        redis_crud=redis_crud,
        kafka_producer=None,  # Would be actual Kafka producer
        redis_client=redis_client
    )
