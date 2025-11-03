# Hybrid Scheduling System Documentation

## Overview

The stufio.modules.schedules module implements a sophisticated hybrid scheduling system that efficiently handles both immediate and long-term delayed event execution. The system combines Redis for high-performance short-term scheduling with ClickHouse for scalable long-term storage and analytics.

## Architecture

### Core Components

1. **Hybrid Scheduler Service** (`scheduler_service.py`)
   - Central orchestrator for event scheduling
   - Routes events to Redis or ClickHouse based on delay duration
   - Manages background sync and cleanup operations

2. **Delayed Events Consumer** (`delayed_events.py`)
   - Kafka consumer for `KAFKA_DELAYED_TOPIC_NAME`
   - Routes incoming delayed events to appropriate storage
   - Handles event correlation and headers safely

3. **Redis Event Scheduler** (`redis_event_scheduler.py`)
   - High-performance immediate scheduling (≤ 86400 seconds)
   - In-memory processing for minimal latency
   - Integrated error handling and retry mechanisms

4. **ClickHouse CRUD Operations** (`crud_event_schedule.py`)
   - Comprehensive database operations for long-term storage
   - Optimized queries with proper indexing and partitioning
   - Status management and distributed locking

5. **Analytics Service** (`analytics_service.py`)
   - Execution result tracking and performance metrics
   - Real-time analytics with materialized views
   - Automatic data aggregation and retention

6. **Monitoring API** (`monitoring.py`)
   - Health checks and system statistics
   - Manual sync and cleanup operations
   - Prometheus metrics integration

7. **Error Handling System** (`error_handler.py`)
   - Circuit breakers and retry strategies
   - Error classification and automatic recovery
   - Comprehensive fault tolerance

## Configuration

### Key Settings

```python
# Scheduling thresholds
schedules_IMMEDIATE_HORIZON_SECONDS = 86400  # 24 hours
schedules_TRANSFER_HORIZON_SECONDS = 3600    # 1 hour
schedules_CLICKHOUSE_SYNC_INTERVAL = 300     # 5 minutes

# Processing intervals
schedules_REDIS_PROCESSING_INTERVAL = 5      # 5 seconds

# Retention policies
schedules_EXECUTION_HISTORY_TTL_DAYS = 90    # 3 months

# Analytics settings
schedules_ANALYTICS_ENABLED = True
schedules_ANALYTICS_BATCH_SIZE = 1000
```

## Data Flow

### 1. Event Scheduling

```
Event Received → Delay Analysis → Storage Decision
                                ↓
┌─────────────────┬─────────────────────────────────┐
│ ≤ 86400 seconds │ > 86400 seconds                 │
│ (Redis)         │ (ClickHouse)                    │
└─────────────────┴─────────────────────────────────┘
```

### 2. Event Execution

```
ClickHouse Events → Background Sync → Redis → Execution
                   (Every 5 min)            (Real-time)
```

### 3. Analytics Pipeline

```
Execution Results → ClickHouse Tables → Materialized Views → Metrics
                                     → Automatic Aggregation
                                     → TTL Management
```

## Database Schema

### EventSchedule Table (ClickHouse)

```sql
CREATE TABLE event_schedules (
    schedule_id String,
    topic String,
    entity_type String,
    action String,
    body String,
    correlation_id String,
    headers String,  -- JSON
    scheduled_at DateTime64(6),
    max_delay_seconds UInt32 DEFAULT 86400,
    priority Int32 DEFAULT 0,
    status Enum8(...),
    created_at DateTime64(6),
    updated_at DateTime64(6),
    -- ... additional fields
) 
ENGINE = MergeTree()
ORDER BY (status, scheduled_at, priority)
PARTITION BY toYYYYMMDD(scheduled_at)
```

### Analytics Tables

#### Execution Results
```sql
CREATE TABLE schedule_execution_results (
    execution_id String,
    schedule_id String,
    correlation_id String,
    topic String,
    entity_type String,
    action String,
    scheduled_at DateTime64(6),
    executed_at DateTime64(6),
    delay_seconds Float64,
    status Enum8('success', 'error', 'timeout', 'skipped'),
    error_message Nullable(String),
    retry_count UInt8,
    processing_time_ms UInt32,
    node_id String,
    created_at DateTime64(6)
)
ENGINE = MergeTree()
ORDER BY (toYYYYMMDD(executed_at), status, entity_type, action)
PARTITION BY toYYYYMMDD(executed_at)
TTL executed_at + INTERVAL 90 DAY DELETE
```

#### Performance Metrics (Aggregated)
```sql
CREATE TABLE schedule_performance_metrics (
    date Date,
    hour UInt8,
    entity_type String,
    action String,
    topic String,
    total_executions UInt64,
    successful_executions UInt64,
    failed_executions UInt64,
    skipped_executions UInt64,
    avg_delay_seconds Float64,
    avg_processing_time_ms Float64,
    p50_delay_seconds Float64,
    p95_delay_seconds Float64,
    p99_delay_seconds Float64,
    -- ... percentile metrics
)
ENGINE = SummingMergeTree()
ORDER BY (date, hour, entity_type, action, topic)
PARTITION BY toYYYYMM(date)
TTL date + INTERVAL 1 YEAR DELETE
```

## API Usage

### Basic Scheduling

```python
from stufio.modules.schedules.services.scheduler_service import scheduler_service
from datetime import datetime, timedelta

# Initialize
await scheduler_service.start()

# Schedule immediate event (Redis)
schedule_id = await scheduler_service.schedule_event(
    topic="user.notifications",
    entity_type="user",
    action="send_reminder",
    body='{"user_id": 123, "message": "Don\'t forget!"}',
    scheduled_at=datetime.utcnow() + timedelta(minutes=30),
    correlation_id="reminder-123",
    headers={"priority": "high"},
    priority=1
)

# Schedule long-term event (ClickHouse)
schedule_id = await scheduler_service.schedule_event(
    topic="user.subscription",
    entity_type="subscription",
    action="renewal_reminder",
    body='{"subscription_id": 456}',
    scheduled_at=datetime.utcnow() + timedelta(days=30),
    correlation_id="renewal-456",
    priority=0
)

# Cancel event
cancelled = await scheduler_service.cancel_event(schedule_id)
```

### Using the Events Module Integration

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

### Monitoring and Analytics

```python
# Get system statistics
stats = await scheduler_service.get_stats()
print(f"Pending events: {stats['pending']}")
print(f"Redis events: {stats['redis_pending']}")

# Manual operations
sync_count = await scheduler_service.sync_clickhouse_to_redis()
cleanup_count = await scheduler_service.cleanup_old_schedules()

# Analytics
from stufio.modules.schedules.services.analytics_service import ScheduleAnalyticsService

analytics = ScheduleAnalyticsService()
await analytics.initialize()

# Get performance metrics
metrics = await analytics.get_performance_metrics(
    start_date=datetime.utcnow() - timedelta(days=7),
    end_date=datetime.utcnow(),
    entity_type="user"
)
```

## Monitoring Endpoints

### Health Check
```http
GET /api/v1/schedules/monitoring/health
```

### Statistics
```http
GET /api/v1/schedules/monitoring/stats
```

### Manual Operations
```http
POST /api/v1/schedules/monitoring/sync
POST /api/v1/schedules/monitoring/cleanup
```

### Analytics
```http
GET /api/v1/schedules/monitoring/analytics/summary
GET /api/v1/schedules/monitoring/analytics/performance
```

### Prometheus Metrics
```http
GET /api/v1/schedules/monitoring/metrics
```

## Error Handling

The system includes comprehensive error handling:

### Circuit Breakers
- Automatic failure detection
- Service degradation prevention
- Gradual recovery mechanisms

### Retry Strategies
- Exponential backoff
- Maximum retry limits
- Error classification

### Fault Tolerance
- Distributed locking
- Node failure recovery
- Data consistency guarantees

## Performance Considerations

### Redis Optimization
- Immediate execution for sub-second delays
- Memory-efficient data structures
- Configurable processing intervals

### ClickHouse Optimization
- Partitioned tables by date
- Optimized indexing strategies
- Automatic data retention (TTL)
- Materialized views for aggregation

### Scalability
- Horizontal scaling support
- Load balancing across nodes
- Background task distribution

## Migration and Deployment

### Deployment Steps
1. Run database migrations
2. Configure environment variables
3. Start scheduler service
4. Deploy Kafka consumers
5. Monitor health endpoints

## Best Practices

### Event Design
- Use meaningful correlation IDs
- Include necessary context in headers
- Design idempotent event handlers
- Implement proper error handling

### Performance Tuning
- Monitor Redis memory usage
- Optimize ClickHouse queries
- Adjust sync intervals based on load
- Use appropriate event priorities

### Monitoring
- Set up alerts for failed executions
- Monitor queue depths
- Track processing latencies
- Review analytics regularly

## Troubleshooting

### Common Issues

1. **Events not executing**
   - Check Redis connectivity
   - Verify scheduler service is running
   - Review error logs

2. **High memory usage**
   - Monitor Redis queue depth
   - Adjust sync intervals
   - Check for stuck events

3. **Slow performance**
   - Optimize ClickHouse queries
   - Review partition strategy
   - Scale Redis instances

### Debug Tools
- Health check endpoints
- System statistics API
- Error logs and metrics
- Manual sync/cleanup operations

## Security Considerations

- Secure Kafka topic access
- Encrypt sensitive event data
- Implement proper authentication
- Monitor for unusual patterns
- Regular security audits

## Future Enhancements

- Multi-region support
- Enhanced analytics dashboards
- Machine learning for optimization
- Advanced scheduling patterns
- Integration with external schedulers
