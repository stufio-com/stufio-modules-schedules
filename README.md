# Stufio Modules Schedules

A sophisticated hybrid scheduling system for delayed event execution, combining Redis for immediate scheduling with ClickHouse for long-term storage and analytics.

## Features

- **Hybrid Architecture**: Redis for immediate scheduling (≤24h), ClickHouse for long-term scheduling
- **High Performance**: Sub-second execution for immediate events, scalable long-term storage
- **Comprehensive Analytics**: Real-time execution tracking and performance metrics
- **Fault Tolerance**: Circuit breakers, retry mechanisms, and distributed locking
- **Monitoring**: Health checks, statistics, and Prometheus metrics
- **Auto-scaling**: Background sync and cleanup operations

## Quick Start

### 1. Installation


### 2. Configuration

Set the following environment variables:

```bash
# Scheduling configuration
export schedules_IMMEDIATE_HORIZON_SECONDS=86400
export schedules_CLICKHOUSE_SYNC_INTERVAL=300
export schedules_TRANSFER_HORIZON_SECONDS=3600

# Redis configuration
export schedules_REDIS_PROCESSING_INTERVAL=5

# Analytics configuration
export schedules_ANALYTICS_ENABLED=true
export schedules_EXECUTION_HISTORY_TTL_DAYS=90
```

### 3. Basic Usage

```python
from stufio.modules.schedules.services.scheduler_service import scheduler_service
from datetime import datetime, timedelta

# Start the scheduler
await scheduler_service.start()

# Schedule an immediate event (Redis)
schedule_id = await scheduler_service.schedule_event(
    topic="user.notifications",
    entity_type="user",
    action="send_reminder",
    body='{"user_id": 123, "message": "Don\'t forget!"}',
    scheduled_at=datetime.utcnow() + timedelta(minutes=30),
    correlation_id="reminder-123"
)

# Schedule a long-term event (ClickHouse)
schedule_id = await scheduler_service.schedule_event(
    topic="subscription.renewal",
    entity_type="subscription", 
    action="send_reminder",
    body='{"subscription_id": 456}',
    scheduled_at=datetime.utcnow() + timedelta(days=30),
    correlation_id="renewal-456"
)
```

### 4. Using with Events Module

```python
from stufio.modules.events import get_event_bus

# Schedule through events module
event_bus = get_event_bus()
await event_bus.publish_with_delay(
    topic="user.notifications",
    entity_type="user",
    action="send_reminder",
    body={"user_id": 123},
    delay_seconds=1800,  # 30 minutes
    correlation_id="reminder-123"
)
```

## Architecture

### Components

1. **SchedulerService**: Central orchestrator for hybrid scheduling
2. **DelayedEventsConsumer**: Kafka consumer for delayed events  
3. **RedisEventScheduler**: High-performance immediate scheduling
4. **EventSchedule CRUD**: ClickHouse operations for long-term storage
5. **AnalyticsService**: Execution tracking and performance metrics
6. **MonitoringAPI**: Health checks and operational endpoints
7. **ErrorHandler**: Comprehensive fault tolerance

### Data Flow

```
Kafka Delayed Events → Consumer → Hybrid Router
                                      ↓
┌─────────────────┬─────────────────────────────────┐
│ ≤ 24 hours      │ > 24 hours                      │
│ Redis           │ ClickHouse                      │
│ (Immediate)     │ (Long-term)                     │
└─────────────────┴─────────────────────────────────┘
                                      ↓
                    Background Sync → Redis → Execution
```

## Monitoring

### Health Endpoints

```bash
# Service health
GET /api/v1/schedules/monitoring/health

# System statistics  
GET /api/v1/schedules/monitoring/stats

# Analytics summary
GET /api/v1/schedules/monitoring/analytics/summary

# Prometheus metrics
GET /api/v1/schedules/monitoring/metrics
```

### Manual Operations

```bash
# Force ClickHouse to Redis sync
POST /api/v1/schedules/monitoring/sync

# Force cleanup operations
POST /api/v1/schedules/monitoring/cleanup
```

## Testing

Run the comprehensive test suite:

```bash
# Run integration tests
python test_scheduling_system.py

# Expected output:
# ✓ Services initialized successfully
# ✓ Immediate event scheduled
# ✓ Long-term event scheduled  
# ✓ Scheduler stats retrieved
# ✓ Analytics working
# 🎉 All tests completed successfully!
```

## Installation

```bash
pip install stufio-modules-schedules
```

## Usage

### API Endpoints

- **Schedules API**:
  - `GET /admin/schedules`: Retrieve all schedules
  - `POST /admin/schedules`: Create a new schedule
  - `GET /admin/schedules/{schedule_id}`: Retrieve a specific schedule
  - `PUT /admin/schedules/{schedule_id}`: Update a specific schedule
  - `DELETE /admin/schedules/{schedule_id}`: Delete a specific schedule
  - `GET /admin/schedules/{schedule_id}/executions`: Retrieve execution history for a schedule
  - `POST /admin/schedules/{schedule_id}/execute`: Manually execute a schedule
  - `POST /admin/schedules/{schedule_id}/toggle`: Toggle a schedule's enabled status

### Programmatic Usage

```python
from stufio.modules.schedules.services.scheduler import scheduler_service
from stufio.modules.events.events import UserCreatedEvent

# Schedule an event
await scheduler_service.schedule_event(
    name="Daily User Report",
    event_def=SomeReportGenerationEvent,
    payload=SomeReportGenerationEventPayload(
        report_type="daily",
        user_id="user-1234"
    ),
    cron_expression="0 0 * * *",  # Run at midnight every day
    description="Generate daily user report",
    tags=["reports", "users"]
)

# Schedule a one-time event
from datetime import datetime, timedelta
future_time = datetime.utcnow() + timedelta(hours=1)

await scheduler_service.schedule_event(
    name="One-time Notification",
    event_def=NotificationEvent,
    entity_id="notification-1234",
    payload=NotificationEventPayload(
        message="This is a one-time notification",
        user_id="user-1234"
    ),
    execution_time=future_time,
    description="Send notification in 1 hour"
)
```

## Configuration

Configuration options can be set in the Stufio admin panel under Settings > Schedules.

Key settings include:

- `schedules_CHECK_INTERVAL_SECONDS`: How often to check for due schedules (default: 60)
- `schedules_MAX_RETRIES`: Maximum retry attempts for failed executions (default: 3)
- `schedules_RETRY_DELAY_SECONDS`: Delay between retry attempts (default: 60)
- `schedules_EXECUTION_HISTORY_TTL_DAYS`: How long to keep execution history (default: 30)
- `schedules_MAX_CONCURRENT_EXECUTIONS`: Maximum concurrent executions (default: 10)

## Dependencies

- stufio-framework
- stufio-modules-events
```

## 3. Integration with Events Module

The flow works like this:

1. A schedule is created with information about which event to trigger and when
2. The service periodically checks for due schedules
3. When a schedule is due, the service uses `event_bus.publish()` to trigger the associated event
4. The event is then processed by the Events module like any other event

This design ensures that scheduled events are handled identically to manually triggered events, maintaining consistency throughout the system.

The requirements are satisfied as follows:

1. ✅ Custom event schedule service which create and manage schedules via Redis or ClickHouse depending on the delay
2. ✅ Integration with events module via the `event_bus`
3. ✅ Support for event triggering based on schedules
4. ✅ Rich scheduling features including one-time and recurring (cron-based) schedules
5. ✅ Full API for managing schedules

This implementation provides a robust foundation for scheduling events in your Stufio application.