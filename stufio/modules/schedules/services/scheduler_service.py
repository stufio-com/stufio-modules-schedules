"""
Simplified scheduler service that coordinates the three-tier system

This is a working replacement for the complex three_tier_scheduler.py
that focuses on core functionality with correct field mappings.
"""

import asyncio
import logging
import redis
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from uuid import uuid4

from ..models.mongo_schedule import MongoSchedule, ScheduleStatus
from ..models.clickhouse_scheduled_event import ClickhouseScheduledEvent, ScheduledEventStatus
from ..models.redis_scheduled_event import RedisScheduledEvent, RedisScheduleStatus

from ..crud.crud_mongo_schedule import CRUDMongoSchedule
from ..crud.crud_clickhouse_scheduled_event import CRUDClickhouseScheduledEvent  
from ..crud.crud_redis_scheduled_event import CRUDRedisScheduledEvent

from ..schemas.clickhouse_scheduled_event import ClickhouseScheduledEventCreate
from ..schemas.redis_scheduled_event import RedisScheduledEventCreate

logger = logging.getLogger(__name__)


class HybridSchedulerService:
    """
    Simplified scheduler service for the three-tier scheduling system
    """
    
    def __init__(self):
        self.mongo_crud = CRUDMongoSchedule(MongoSchedule)
        self.clickhouse_crud = CRUDClickhouseScheduledEvent(ClickhouseScheduledEvent)
        
        # Initialize Redis client - in production this would come from config
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=False)
        self.redis_crud = CRUDRedisScheduledEvent(self.redis_client)
        
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        # Configuration
        self.mongo_check_interval = 300  # 5 minutes
        self.clickhouse_sync_interval = 60  # 1 minute
        self.redis_processing_interval = 5  # 5 seconds
        self.transfer_window_hours = 1  # Transfer events within 1 hour to Redis
        
    async def initialize(self) -> None:
        """Initialize the scheduler service."""
        logger.info("Initializing HybridSchedulerService")
        # Any initialization logic here
        
    async def start(self) -> None:
        """Start all background processing tasks."""
        if self._running:
            return
            
        self._running = True
        logger.info("Starting HybridSchedulerService")
        
        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._mongo_schedule_processor()),
            asyncio.create_task(self._clickhouse_to_redis_sync()),
            asyncio.create_task(self._redis_event_processor()),
        ]
        
    async def stop(self) -> None:
        """Stop all background processing tasks."""
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Wait for tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        self._tasks = []
        logger.info("HybridSchedulerService stopped")
        
    async def schedule_event(
        self,
        topic: str,
        entity_type: str, 
        action: str,
        body: str,
        scheduled_at: datetime,
        entity_id: Optional[str] = None,
        actor_type: str = "system",
        actor_id: str = "scheduler",
        correlation_id: Optional[str] = None,
        priority: int = 0,
        max_retries: int = 3
    ) -> str:
        """
        Schedule an event in the appropriate tier based on timing.
        
        Returns the schedule ID.
        """
        if not correlation_id:
            correlation_id = str(uuid4())
            
        now = datetime.now(timezone.utc)
        time_until_execution = scheduled_at - now
        
        # Decide which tier to use based on timing
        if time_until_execution <= timedelta(hours=self.transfer_window_hours):
            # Use Redis for immediate scheduling
            return await self._schedule_in_redis(
                topic=topic,
                entity_type=entity_type,
                action=action,
                body=body,
                scheduled_at=scheduled_at,
                entity_id=entity_id,
                actor_type=actor_type,
                actor_id=actor_id,
                correlation_id=correlation_id,
                priority=priority,
                max_retries=max_retries
            )
        else:
            # Use ClickHouse for long-term scheduling
            return await self._schedule_in_clickhouse(
                topic=topic,
                entity_type=entity_type,
                action=action,
                body=body,
                scheduled_at=scheduled_at,
                entity_id=entity_id,
                actor_type=actor_type,
                actor_id=actor_id,
                correlation_id=correlation_id,
                priority=priority,
                max_retries=max_retries
            )
    
    async def _schedule_in_redis(self, **kwargs) -> str:
        """Schedule an event directly in Redis."""
        event_data = RedisScheduledEventCreate(**kwargs)
        event = self.redis_crud.create(event_data)
        logger.info(f"Scheduled immediate event in Redis: {event.event_id}")
        return event.event_id
        
    async def _schedule_in_clickhouse(self, **kwargs) -> str:
        """Schedule an event in ClickHouse for long-term storage."""
        # Map to ClickHouse schema - uses payload instead of body
        ch_kwargs = {**kwargs}
        ch_kwargs['payload'] = ch_kwargs.pop('body', '{}')
        
        event_data = ClickhouseScheduledEventCreate(**ch_kwargs)
        event = await self.clickhouse_crud.create(event_data)
        logger.info(f"Scheduled long-term event in ClickHouse: {event.schedule_id}")
        return event.schedule_id
        
    async def _mongo_schedule_processor(self):
        """Process MongoDB schedules and create ClickHouse events."""
        logger.info("Starting MongoDB schedule processor")
        
        while self._running:
            try:
                # Get active schedules that might be due
                current_time = datetime.now(timezone.utc)
                due_schedules = await self.mongo_crud.get_schedules_due_for_execution(current_time)
                
                for schedule in due_schedules:
                    try:
                        await self._process_mongo_schedule(schedule)
                    except Exception as e:
                        logger.error(f"Error processing schedule {schedule.id}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error in MongoDB schedule processor: {str(e)}")
                
            await asyncio.sleep(self.mongo_check_interval)
            
    async def _process_mongo_schedule(self, schedule: MongoSchedule):
        """Process a single MongoDB schedule by creating a ClickHouse event."""
        # For now, create a simple next execution time (would use croniter in production)
        next_execution = datetime.now(timezone.utc) + timedelta(hours=24)  # Simple daily repeat
        
        # Create ClickHouse event using the schedule's event information
        event_data = ClickhouseScheduledEventCreate(
            topic=f"{schedule.event_type}.{schedule.event_action}",
            entity_type=schedule.event_type,
            action=schedule.event_action,
            entity_id=schedule.event_entity_id,
            actor_type=schedule.actor_type,
            actor_id=schedule.actor_id,
            payload=str(schedule.event_payload) if schedule.event_payload else '{}',
            scheduled_at=next_execution,
            priority=schedule.priority,
            source_id=str(schedule.id),
            max_retries=schedule.max_retries
        )
        
        # Create the event
        created_event = await self.clickhouse_crud.create(event_data)
        
        # Record execution in MongoDB
        await self.mongo_crud.record_execution(
            str(schedule.id),
            next_execution,
            success=True,
            clickhouse_event_id=created_event.schedule_id
        )
        
        # Update next execution time
        await self.mongo_crud.update_next_execution(str(schedule.id), next_execution)
        
        logger.info(f"Created ClickHouse event for schedule {schedule.name}")
        
    async def _clickhouse_to_redis_sync(self):
        """Transfer events from ClickHouse to Redis when they're ready."""
        logger.info("Starting ClickHouse to Redis sync")
        
        while self._running:
            try:
                current_time = datetime.now(timezone.utc)
                transfer_cutoff = current_time + timedelta(hours=self.transfer_window_hours)
                
                # Get events ready for transfer
                ready_events = await self.clickhouse_crud.get_ready_for_transfer(transfer_cutoff)
                
                for event in ready_events:
                    try:
                        await self._transfer_to_redis(event)
                    except Exception as e:
                        logger.error(f"Error transferring event {event.schedule_id}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error in ClickHouse to Redis sync: {str(e)}")
                
            await asyncio.sleep(self.clickhouse_sync_interval)
            
    async def _transfer_to_redis(self, event: ClickhouseScheduledEvent):
        """Transfer a single event from ClickHouse to Redis."""
        # Create Redis event from ClickHouse event
        redis_data = RedisScheduledEventCreate(
            topic=event.topic,
            entity_type=event.entity_type,
            action=event.action,
            entity_id=event.entity_id,
            actor_type=event.actor_type,
            actor_id=event.actor_id,
            payload={'data': event.payload},  # Redis expects dict payload
            scheduled_at=event.scheduled_at,
            priority=event.priority,
            clickhouse_schedule_id=event.schedule_id,
            max_retries=event.max_retries
        )
        
        # Create in Redis
        redis_event = self.redis_crud.create(redis_data)
        
        # Mark as transferred in ClickHouse
        await self.clickhouse_crud.mark_transferred(
            event.schedule_id,
            {"redis_event_id": redis_event.event_id}
        )
        
        logger.debug(f"Transferred event {event.schedule_id} to Redis")
        
    async def _redis_event_processor(self):
        """Process events from Redis when they're due."""
        logger.info("Starting Redis event processor")
        
        while self._running:
            try:
                current_time = datetime.now(timezone.utc)
                
                # Get due events from Redis
                due_events = self.redis_crud.get_ready_events(limit=100)
                
                for event in due_events:
                    try:
                        await self._process_redis_event(event)
                    except Exception as e:
                        logger.error(f"Error processing Redis event {event.event_id}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error in Redis event processor: {str(e)}")
                
            await asyncio.sleep(self.redis_processing_interval)
            
    async def _process_redis_event(self, event) -> bool:
        """Process a single Redis event by publishing to Kafka."""
        try:
            # Here you would publish to Kafka/event bus
            # For now, just log the event processing
            logger.info(f"Processing event: {event.topic} for {event.entity_type}/{event.action}")
            
            # Mark as processed in Redis
            self.redis_crud.mark_processed(
                event.event_id,
                success=True
            )
            
            return True
            
        except Exception as e:
            # Mark as failed in Redis
            self.redis_crud.mark_processed(
                event.event_id,
                success=False,
                error_message=str(e)
            )
            raise
            
    async def get_stats(self) -> Dict[str, Any]:
        """Get system statistics."""
        try:
            # Get counts from each tier
            mongo_stats = await self.mongo_crud.get_multi(limit=1)  # Just to test connection
            clickhouse_stats = await self.clickhouse_crud.get_stats()
            redis_stats = self.redis_crud.get_stats()
            
            return {
                "mongo_connection": "healthy" if mongo_stats is not None else "error",
                "clickhouse_stats": clickhouse_stats,
                "redis_stats": redis_stats,
                "service_status": "running" if self._running else "stopped"
            }
            
        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            return {
                "error": str(e),
                "service_status": "running" if self._running else "stopped"
            }


# Create singleton instance
scheduler_service = HybridSchedulerService()
