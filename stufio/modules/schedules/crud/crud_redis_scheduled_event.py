"""
CRUD operations for Redis scheduled events
"""
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, cast

import redis
import json
from redis.lock import Lock
from pydantic import ValidationError

from ..models.redis_scheduled_event import RedisScheduleStatus, RedisScheduledEvent
from ..schemas.redis_scheduled_event import (
    RedisScheduledEventCreate,
    RedisScheduledEventUpdate,
    RedisScheduledEventResponse,
)


class CRUDRedisScheduledEvent:
    """CRUD operations for Redis scheduled events"""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.key_prefix = "scheduled_event"
        self.index_key = "scheduled_events_index"
        self.stats_key = "scheduled_events_stats"
        self.lock_timeout = 30  # seconds

    def _make_key(self, event_id: str) -> str:
        """Generate Redis key for scheduled event"""
        return f"{self.key_prefix}:{event_id}"

    def _make_lock_key(self, event_id: str) -> str:
        """Generate Redis lock key for scheduled event"""
        return f"{self.key_prefix}:lock:{event_id}"

    def _serialize_event(self, event: RedisScheduledEventResponse) -> str:
        """Serialize event to JSON string"""
        event_dict = event.model_dump()
        # Convert datetime objects to ISO strings
        for field in ['scheduled_at', 'created_at', 'updated_at', 'processed_at']:
            if event_dict.get(field):
                event_dict[field] = event_dict[field].isoformat()
        return json.dumps(event_dict)

    def _deserialize_event(self, data: str) -> RedisScheduledEventResponse:
        """Deserialize event from JSON string"""
        event_dict = json.loads(data)
        # Convert ISO strings back to datetime objects
        for field in ['scheduled_at', 'created_at', 'updated_at', 'processed_at']:
            if event_dict.get(field):
                event_dict[field] = datetime.fromisoformat(event_dict[field])
        return RedisScheduledEventResponse(**event_dict)

    def create(
        self, event_create: RedisScheduledEventCreate
    ) -> RedisScheduledEventResponse:
        """Create a new Redis scheduled event"""
        now = datetime.now(timezone.utc)

        # Validate scheduling constraints
        if event_create.scheduled_at <= now:
            raise ValueError("Scheduled time must be in the future")

        max_schedule_time = now + timedelta(hours=1)
        if event_create.scheduled_at > max_schedule_time:
            raise ValueError("Redis events can only be scheduled up to 1 hour in advance")

        event = RedisScheduledEventResponse(
            **event_create.model_dump(), created_at=now
        )

        # Store event in Redis
        key = self._make_key(event.event_id)
        pipe = self.redis.pipeline()

        # Set event data with TTL (2 hours to allow for processing)
        ttl_seconds = int((event.scheduled_at - now).total_seconds()) + 7200  # +2 hours buffer
        pipe.setex(key, ttl_seconds, self._serialize_event(event))

        # Add to sorted set for time-based queries (score = timestamp)
        pipe.zadd(self.index_key, {event.event_id: event.scheduled_at.timestamp()})

        # Update stats
        pipe.hincrby(self.stats_key, "total_created", 1)
        pipe.hincrby(self.stats_key, f"created_{event.source}", 1)

        pipe.execute()

        return event

    def get(self, event_id: str) -> Optional[RedisScheduledEventResponse]:
        """Get Redis scheduled event by ID"""
        key = self._make_key(event_id)
        data = self.redis.get(key)

        if not data:
            return None

        try:
            # Type cast to ensure we have bytes
            data_bytes = data if isinstance(data, bytes) else str(data).encode('utf-8')
            return self._deserialize_event(data_bytes.decode('utf-8'))
        except (json.JSONDecodeError, ValidationError):
            # Clean up corrupted data
            self.redis.delete(key)
            self.redis.zrem(self.index_key, event_id)
            return None

    def update(self, event_id: str, event_update: RedisScheduledEventUpdate) -> Optional[RedisScheduledEventResponse]:
        """Update Redis scheduled event"""
        key = self._make_key(event_id)
        lock_key = self._make_lock_key(event_id)

        with Lock(self.redis, lock_key, timeout=self.lock_timeout):
            event = self.get(event_id)
            if not event:
                return None

            # Update fields
            update_data = event_update.model_dump(exclude_unset=True)

            # Validate scheduling constraints if scheduled_at is being updated
            if 'scheduled_at' in update_data:
                now = datetime.now(timezone.utc)
                new_scheduled_at = update_data['scheduled_at']

                if new_scheduled_at <= now:
                    raise ValueError("Scheduled time must be in the future")

                max_schedule_time = now + timedelta(hours=1)
                if new_scheduled_at > max_schedule_time:
                    raise ValueError("Redis events can only be scheduled up to 1 hour in advance")

            # Apply updates
            for field, value in update_data.items():
                setattr(event, field, value)

            event.updated_at = datetime.now(timezone.utc)

            # Save updated event
            pipe = self.redis.pipeline()

            # Update event data
            ttl_seconds = int((event.scheduled_at - datetime.now(timezone.utc)).total_seconds()) + 7200
            pipe.setex(key, ttl_seconds, self._serialize_event(event))

            # Update index if scheduled_at changed
            if 'scheduled_at' in update_data:
                pipe.zadd(self.index_key, {event.event_id: event.scheduled_at.timestamp()})

            pipe.execute()

            return event

    def delete(self, event_id: str) -> bool:
        """Delete Redis scheduled event"""
        key = self._make_key(event_id)
        lock_key = self._make_lock_key(event_id)

        with Lock(self.redis, lock_key, timeout=self.lock_timeout):
            pipe = self.redis.pipeline()
            pipe.delete(key)
            pipe.zrem(self.index_key, event_id)

            results = pipe.execute()
            deleted = results[0] > 0

            if deleted:
                self.redis.hincrby(self.stats_key, "total_deleted", 1)

            return deleted

    def get_ready_events(self, limit: int = 100) -> List[RedisScheduledEventResponse]:
        """Get events ready for processing (scheduled_at <= now)"""
        now = datetime.now(timezone.utc)

        # Get event IDs from sorted set where score <= current timestamp
        event_ids = cast(List[bytes], self.redis.zrangebyscore(
            self.index_key, 
            0, 
            now.timestamp(), 
            start=0, 
            num=limit
        ))

        if not event_ids:
            return []

        # Get event data
        events = []
        for event_id in event_ids:
            event = self.get(event_id.decode('utf-8'))
            if event and event.status == RedisScheduleStatus.PENDING:
                events.append(event)

        return events

    def claim_for_processing(self, event_id: str, processor_id: str) -> bool:
        """Claim an event for processing with distributed locking"""
        lock_key = self._make_lock_key(event_id)

        try:
            with Lock(self.redis, lock_key, timeout=self.lock_timeout):
                event = self.get(event_id)
                if not event or event.status != RedisScheduleStatus.PENDING:
                    return False

                # Update status to processing
                event.status = RedisScheduleStatus.RESERVED
                event.updated_at = datetime.now(timezone.utc)

                # Store processor info in headers
                if not event.headers:
                    event.headers = {}
                event.headers['processor_id'] = processor_id
                event.headers['claimed_at'] = datetime.now(timezone.utc).isoformat()

                # Save updated event
                key = self._make_key(event_id)
                ttl_seconds = int((event.scheduled_at - datetime.now(timezone.utc)).total_seconds()) + 7200
                self.redis.setex(key, ttl_seconds, self._serialize_event(event))

                return True

        except Exception:
            return False

    def mark_processed(self, event_id: str, success: bool = True, error_message: Optional[str] = None) -> bool:
        """Mark event as processed"""
        lock_key = self._make_lock_key(event_id)

        with Lock(self.redis, lock_key, timeout=self.lock_timeout):
            event = self.get(event_id)
            if not event:
                return False

            # Update status
            from ..models.redis_scheduled_event import RedisScheduleStatus
            event.status = RedisScheduleStatus.COMPLETED if success else RedisScheduleStatus.ERROR
            event.processed_at = datetime.now(timezone.utc)
            event.updated_at = datetime.now(timezone.utc)

            if error_message:
                if not event.headers:
                    event.headers = {}
                event.headers['error_message'] = error_message

            # Save updated event
            key = self._make_key(event_id)
            pipe = self.redis.pipeline()
            # Keep completed events for a short time for analytics
            ttl_seconds = 3600  # 1 hour
            pipe.setex(key, ttl_seconds, self._serialize_event(event))

            # Remove from processing index
            pipe.zrem(self.index_key, event_id)

            # Update stats
            pipe.hincrby(self.stats_key, f"total_{event.status}", 1)

            pipe.execute()

            return True

    def get_by_source(self, source: str, limit: int = 100) -> List[RedisScheduledEventResponse]:
        """Get events by source"""
        # This is a simple implementation - in production you might want to maintain source indexes
        all_event_ids = cast(List[bytes], self.redis.zrange(self.index_key, 0, -1))

        events = []
        count = 0
        for event_id in all_event_ids:
            if count >= limit:
                break

            event = self.get(event_id.decode('utf-8'))
            if event and event.source == source:
                events.append(event)
                count += 1

        return events

    def cleanup_expired(self) -> int:
        """Clean up expired events from index"""
        now = datetime.now(timezone.utc)

        # Get expired event IDs (events that should have been processed but are still in index)
        expired_cutoff = (now - timedelta(minutes=5)).timestamp()  # 5 minutes grace period
        expired_ids = cast(List[bytes], self.redis.zrangebyscore(self.index_key, 0, expired_cutoff))

        if not expired_ids:
            return 0

        # Remove expired events
        pipe = self.redis.pipeline()
        for event_id in expired_ids:
            event_id_str = event_id.decode('utf-8')
            event = self.get(event_id_str)

            # Only remove if event doesn't exist or is still pending (stuck)
            if not event or event.status == RedisScheduleStatus.PENDING:
                pipe.delete(self._make_key(event_id_str))
                pipe.zrem(self.index_key, event_id_str)

        results = pipe.execute()
        cleaned_count = sum(1 for i in range(0, len(results), 2) if results[i] > 0)

        if cleaned_count > 0:
            self.redis.hincrby(self.stats_key, "total_expired_cleaned", cleaned_count)

        return cleaned_count

    def get_stats(self) -> Dict[str, Any]:
        """Get Redis scheduled events statistics"""
        stats = cast(Dict[bytes, bytes], self.redis.hgetall(self.stats_key))

        # Convert byte keys/values to strings/ints
        converted_stats = {}
        for key, value in stats.items():
            try:
                converted_stats[key.decode('utf-8')] = int(value.decode('utf-8'))
            except (ValueError, AttributeError):
                converted_stats[key.decode('utf-8')] = value.decode('utf-8') if isinstance(value, bytes) else value

        # Add current queue stats
        now = datetime.now(timezone.utc)
        pending_count = cast(int, self.redis.zcount(self.index_key, 0, '+inf'))
        ready_count = cast(int, self.redis.zcount(self.index_key, 0, now.timestamp()))

        converted_stats.update({
            'pending_events': pending_count,
            'ready_events': ready_count,
            'queue_size': pending_count
        })

        return converted_stats

    def get_queue_health(self) -> Dict[str, Any]:
        """Get queue health metrics"""
        now = datetime.now(timezone.utc)

        # Count events by time ranges
        one_minute_ago = (now - timedelta(minutes=1)).timestamp()
        five_minutes_ago = (now - timedelta(minutes=5)).timestamp()

        overdue_count = cast(int, self.redis.zcount(self.index_key, 0, five_minutes_ago))
        ready_count = cast(int, self.redis.zcount(self.index_key, five_minutes_ago, now.timestamp()))
        future_count = cast(int, self.redis.zcount(self.index_key, now.timestamp(), '+inf'))

        return {
            'overdue_events': overdue_count,
            'ready_events': ready_count,
            'future_events': future_count,
            'total_events': overdue_count + ready_count + future_count,
            'health_status': 'unhealthy' if overdue_count > 10 else 'healthy'
        }


# Dependency to get CRUD instance
def get_redis_scheduled_event_crud(redis_client: redis.Redis) -> CRUDRedisScheduledEvent:
    """Get Redis scheduled event CRUD instance"""
    return CRUDRedisScheduledEvent(redis_client)
