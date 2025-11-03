"""
Delayed Events Consumer for Three-Tier Schedules Module.
Consumes delayed messages from KAFKA_DELAYED_TOPIC_NAME and creates ClickHouse scheduled events.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

from faststream.kafka.fastapi import KafkaMessage, Logger
from stufio.modules.events.consumers.asyncapi import stufio_subscriber
from stufio.modules.events.consumers import get_kafka_broker
from stufio.modules.events.helpers import extract_headers_safely
from stufio.core.config import get_settings
from stufio.modules.schedules.models import ScheduledEventSource

from ..crud.crud_clickhouse_scheduled_event import crud_clickhouse_scheduled_event
from ..schemas.clickhouse_scheduled_event import ClickhouseScheduledEventCreate
from ..models.schedule_analytics import AnalyticsLevel
from ..config import settings as schedules_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Get Kafka broker and router
kafka_broker = get_kafka_broker()


@stufio_subscriber(
    settings.events_KAFKA_DELAYED_TOPIC_NAME,
    group_id=f"{settings.events_KAFKA_GROUP_ID}.schedules.delayed_events",
    include_in_schema=False,
    max_poll_interval_ms=300000,
    session_timeout_ms=60000,
    heartbeat_interval_ms=20000,
    auto_offset_reset="earliest",
)
async def handle_delayed_events(msg: KafkaMessage, logger: Logger) -> None:
    """
    Handle delayed events from Kafka and create ClickHouse scheduled events.
    
    Expected message headers:
    - delivery_time: Unix timestamp when message should be delivered
    - original_topic: The topic where the message should be republished
    - correlation_id: Correlation ID for tracking
    - entity_type: Entity type from the original event
    - action: Action from the original event
    """
    try:
        # Extract headers using the existing helper function
        headers = extract_headers_safely(msg)

        # Validate required headers
        delivery_time = headers.get('delivery_time')
        original_topic = headers.get('original_topic')
        correlation_id = headers.get('correlation_id') or f"delayed_{datetime.now(timezone.utc).timestamp()}"
        entity_type = headers.get('entity_type', 'unknown')
        action = headers.get('action', 'unknown')
        priority = headers.get('priority', 1)  # Default priority if not set
        try:
            priority = int(priority)
        except (ValueError, TypeError):
            priority = 1
        max_delay_seconds = headers.get('max_delay_seconds', 86400)  # Default to 24 hours if not set
        try:
            max_delay_seconds = int(max_delay_seconds)
        except (ValueError, TypeError):
            max_delay_seconds = 86400

        if not delivery_time or not original_topic:
            logger.error(f"Missing required headers: delivery_time={delivery_time}, original_topic={original_topic}")
            return

        # Parse delivery time
        try:
            scheduled_at = datetime.fromtimestamp(float(delivery_time), tz=timezone.utc)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid delivery_time format: {delivery_time}, error: {e}")
            return

        # Get message body
        body = msg._decoded_body if hasattr(msg, '_decoded_body') else msg.body
        if isinstance(body, dict):
            event_data = body
        else:
            try:
                if isinstance(body, (bytes, bytearray)):
                    body_str = body.decode("utf-8")
                elif isinstance(body, str):
                    body_str = body
                else:
                    body_str = str(body)
                event_data = json.loads(body_str) if body else {}
            except json.JSONDecodeError:
                event_data = {"raw_body": str(body)}

        # Calculate delay from now
        now = datetime.now(timezone.utc)
        delay_seconds = (scheduled_at - now).total_seconds()

        logger.info(f"Processing delayed event: correlation_id={correlation_id}, "
                   f"scheduled_at={scheduled_at}, delay={delay_seconds:.2f}s")

        # Create ClickHouse scheduled event
        event_create = ClickhouseScheduledEventCreate(
            topic=original_topic,
            scheduled_at=scheduled_at,
            entity_type=entity_type,
            action=action,
            payload=json.dumps(event_data),
            source=ScheduledEventSource.KAFKA_DELAYED,
            source_id=correlation_id,
            priority=priority,
            headers=headers,
            max_delay_seconds=max_delay_seconds,
            correlation_id=correlation_id,
        )

        # Save to ClickHouse
        created_event = await crud_clickhouse_scheduled_event.create(event_create)

        logger.info(
            f"Created ClickHouse scheduled event {created_event.schedule_id} for delayed event {correlation_id}"
        )

        # Record analytics if service is available
        try:
            analytics_data = {
                "event_id": getattr(created_event, "id", None),
                "correlation_id": correlation_id,
                "delay_seconds": delay_seconds,
                "source_topic": original_topic,
                "kafka_processing_time_ms": (datetime.now(timezone.utc) - now).total_seconds() * 1000
            }

            # This would be injected analytics service in real implementation
            logger.debug(f"Would record analytics: {analytics_data}")

        except Exception as analytics_error:
            logger.warning(f"Failed to record analytics: {analytics_error}")

    except Exception as e:
        logger.error(f"Error processing delayed event: {e}")
        # Could add dead letter queue handling here
