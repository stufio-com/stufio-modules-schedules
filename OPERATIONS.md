# Operational Runbook - Hybrid Scheduling System

## Table of Contents
1. [System Overview](#system-overview)
2. [Health Monitoring](#health-monitoring)
3. [Performance Monitoring](#performance-monitoring)
4. [Common Issues](#common-issues)
5. [Emergency Procedures](#emergency-procedures)
6. [Maintenance Tasks](#maintenance-tasks)
7. [Troubleshooting Guide](#troubleshooting-guide)

## System Overview

The hybrid scheduling system consists of:
- **Redis**: Immediate scheduling (≤24 hours)
- **ClickHouse**: Long-term scheduling (>24 hours)
- **Kafka**: Event message transport
- **Background Tasks**: Sync and cleanup operations

### Key Components Status Check
```bash
# Check if scheduler service is running
curl -f http://localhost:8000/api/v1/schedules/monitoring/health

# Get system statistics
curl http://localhost:8000/api/v1/schedules/monitoring/stats
```

## Health Monitoring

### Primary Health Indicators

1. **Scheduler Service Health**
   ```bash
   # Endpoint: GET /api/v1/schedules/monitoring/health
   # Expected: HTTP 200 with status "healthy"
   ```

2. **Queue Depths**
   - Redis pending events: < 10,000 (normal), < 50,000 (warning), > 100,000 (critical)
   - ClickHouse pending events: < 100,000 (normal), < 500,000 (warning), > 1,000,000 (critical)

3. **Processing Latency**
   - Event execution delay: < 30s (normal), < 120s (warning), > 300s (critical)
   - Sync operation time: < 60s (normal), < 180s (warning), > 300s (critical)

### Monitoring Commands

```bash
# Get comprehensive system stats
curl -s http://localhost:8000/api/v1/schedules/monitoring/stats | jq '.'

# Check recent execution metrics
curl -s "http://localhost:8000/api/v1/schedules/monitoring/analytics/summary?hours=1" | jq '.'

# Monitor Prometheus metrics
curl -s http://localhost:8000/api/v1/schedules/monitoring/metrics
```

## Performance Monitoring

### Key Metrics to Track

1. **Throughput Metrics**
   - Events scheduled per minute
   - Events executed per minute
   - Transfer rate (ClickHouse → Redis)

2. **Latency Metrics**
   - Schedule-to-execution delay
   - Processing time per event
   - Sync operation duration

3. **Error Metrics**
   - Failed executions percentage
   - Retry attempts
   - Circuit breaker activations

### Performance Queries

```sql
-- ClickHouse: Check recent execution performance
SELECT 
    toStartOfHour(executed_at) as hour,
    count() as total_executions,
    countIf(status = 'success') as successful,
    countIf(status = 'error') as failed,
    avg(processing_time_ms) as avg_processing_ms,
    quantile(0.95)(delay_seconds) as p95_delay
FROM schedule_execution_results 
WHERE executed_at >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;

-- Check pending schedules by status
SELECT 
    status,
    count() as count,
    min(scheduled_at) as earliest,
    max(scheduled_at) as latest
FROM event_schedules 
WHERE status IN ('pending', 'processing')
GROUP BY status;
```

## Common Issues

### Issue 1: High Redis Memory Usage

**Symptoms:**
- Redis memory usage > 80%
- Slow event processing
- Connection timeouts

**Investigation:**
```bash
# Check Redis memory info
redis-cli info memory

# Check queue depths
curl -s http://localhost:8000/api/v1/schedules/monitoring/stats | jq '.redis_pending'

# Check for stuck events
redis-cli zrange scheduled_events 0 10 WITHSCORES
```

**Resolution:**
```bash
# Force manual sync to transfer events to ClickHouse
curl -X POST http://localhost:8000/api/v1/schedules/monitoring/sync

# If critical, scale Redis or clear old events
# WARNING: Only in emergency, may lose data
redis-cli eval "return redis.call('zremrangebyscore', 'scheduled_events', 0, $(date -d '1 hour ago' +%s))" 0
```

### Issue 2: Events Not Executing

**Symptoms:**
- Events stuck in pending status
- No recent executions in analytics
- Queue depths increasing

**Investigation:**
```bash
# Check scheduler service status
curl -f http://localhost:8000/api/v1/schedules/monitoring/health

# Check background task status
curl -s http://localhost:8000/api/v1/schedules/monitoring/stats | jq '.running'

# Check recent errors
tail -f /var/log/scheduler/scheduler.log | grep ERROR
```

**Resolution:**
```bash
# Restart scheduler service
systemctl restart scheduler-service

# Force manual cleanup of stuck processing events
curl -X POST http://localhost:8000/api/v1/schedules/monitoring/cleanup

# Check ClickHouse connectivity
clickhouse-client --query "SELECT count() FROM event_schedules WHERE status = 'pending'"
```

### Issue 3: High Execution Failures

**Symptoms:**
- Error rate > 5%
- Increasing retry counts
- Circuit breaker activations

**Investigation:**
```sql
-- Check error patterns
SELECT 
    entity_type,
    action,
    count() as error_count,
    groupArray(error_message) as errors
FROM schedule_execution_results 
WHERE status = 'error' 
  AND executed_at >= now() - INTERVAL 1 HOUR
GROUP BY entity_type, action
ORDER BY error_count DESC;
```

**Resolution:**
1. Identify failing event types
2. Check downstream service health
3. Review event payload validity
4. Consider circuit breaker tuning

### Issue 4: Sync Operation Delays

**Symptoms:**
- Long sync operation times
- Events not transferring to Redis
- ClickHouse query timeouts

**Investigation:**
```sql
-- Check ClickHouse query performance
SELECT 
    count() as pending_transfer,
    min(scheduled_at) as earliest_pending
FROM event_schedules 
WHERE status = 'pending' 
  AND scheduled_at <= now() + INTERVAL 1 HOUR;

-- Check for table locks
SELECT * FROM system.processes WHERE query LIKE '%event_schedules%';
```

**Resolution:**
```bash
# Force manual sync
curl -X POST http://localhost:8000/api/v1/schedules/monitoring/sync

# Optimize ClickHouse if needed
clickhouse-client --query "OPTIMIZE TABLE event_schedules"

# Check ClickHouse resource usage
clickhouse-client --query "SELECT * FROM system.metrics WHERE metric LIKE '%Memory%'"
```

## Emergency Procedures

### Complete Service Outage

1. **Immediate Actions:**
   ```bash
   # Check service status
   systemctl status scheduler-service
   
   # Check dependencies
   systemctl status redis
   systemctl status clickhouse-server
   systemctl status kafka
   
   # Check logs
   journalctl -u scheduler-service -f --lines=100
   ```

2. **Recovery Steps:**
   ```bash
   # Restart in order
   systemctl restart redis
   systemctl restart clickhouse-server
   systemctl restart kafka
   systemctl restart scheduler-service
   
   # Verify health
   curl -f http://localhost:8000/api/v1/schedules/monitoring/health
   ```

### Data Corruption

1. **ClickHouse Table Issues:**
   ```sql
   -- Check table integrity
   CHECK TABLE event_schedules;
   
   -- Repair if needed
   DETACH TABLE event_schedules;
   ATTACH TABLE event_schedules;
   ```

2. **Redis Data Issues:**
   ```bash
   # Check Redis consistency
   redis-cli info persistence
   
   # Force save if needed
   redis-cli bgsave
   ```

### High Load Emergency

1. **Scale Up:**
   ```bash
   # Increase sync frequency temporarily
   # Edit config: schedules_CLICKHOUSE_SYNC_INTERVAL = 60
   
   # Add Redis instances
   # Configure Redis clustering if needed
   ```

2. **Load Shedding:**
   ```bash
   # Pause low-priority events
   # Implement priority-based filtering
   
   # Increase horizon to move more events to ClickHouse
   # Edit config: schedules_IMMEDIATE_HORIZON_SECONDS = 3600
   ```

## Maintenance Tasks

### Daily Tasks

1. **Health Check:**
   ```bash
   #!/bin/bash
   # daily_health_check.sh
   
   echo "=== Daily Health Check $(date) ==="
   
   # Service health
   curl -f http://localhost:8000/api/v1/schedules/monitoring/health || echo "❌ Service unhealthy"
   
   # Queue depths
   STATS=$(curl -s http://localhost:8000/api/v1/schedules/monitoring/stats)
   REDIS_PENDING=$(echo $STATS | jq -r '.redis_pending // 0')
   CH_PENDING=$(echo $STATS | jq -r '.pending // 0')
   
   echo "Redis pending: $REDIS_PENDING"
   echo "ClickHouse pending: $CH_PENDING"
   
   if [ "$REDIS_PENDING" -gt 50000 ]; then
       echo "⚠️  High Redis queue depth"
   fi
   
   if [ "$CH_PENDING" -gt 500000 ]; then
       echo "⚠️  High ClickHouse queue depth"
   fi
   ```

2. **Performance Report:**
   ```bash
   #!/bin/bash
   # daily_performance_report.sh
   
   echo "=== Performance Report $(date) ==="
   
   # Get 24h summary
   curl -s "http://localhost:8000/api/v1/schedules/monitoring/analytics/summary?hours=24" | jq '.'
   
   # Get execution metrics
   curl -s "http://localhost:8000/api/v1/schedules/monitoring/analytics/performance?hours=24" | jq '.'
   ```

### Weekly Tasks

1. **Cleanup Operations:**
   ```bash
   #!/bin/bash
   # weekly_cleanup.sh
   
   echo "=== Weekly Cleanup $(date) ==="
   
   # Force cleanup
   CLEANED=$(curl -X POST -s http://localhost:8000/api/v1/schedules/monitoring/cleanup)
   echo "Cleaned items: $CLEANED"
   
   # ClickHouse optimization
   clickhouse-client --query "OPTIMIZE TABLE event_schedules FINAL"
   clickhouse-client --query "OPTIMIZE TABLE schedule_execution_results FINAL"
   ```

2. **Performance Analysis:**
   ```sql
   -- Weekly performance review
   SELECT 
       toStartOfWeek(executed_at) as week,
       count() as total_executions,
       countIf(status = 'success') / count() * 100 as success_rate,
       avg(processing_time_ms) as avg_processing_ms,
       quantile(0.95)(delay_seconds) as p95_delay_seconds
   FROM schedule_execution_results 
   WHERE executed_at >= now() - INTERVAL 4 WEEK
   GROUP BY week
   ORDER BY week DESC;
   ```

### Monthly Tasks

1. **Capacity Planning:**
   ```bash
   # Check growth trends
   # Analyze resource usage
   # Plan scaling needs
   ```

2. **Security Review:**
   ```bash
   # Check access logs
   # Review error patterns
   # Update credentials if needed
   ```

## Troubleshooting Guide

### Debug Tools

1. **Log Analysis:**
   ```bash
   # Scheduler service logs
   tail -f /var/log/scheduler/scheduler.log
   
   # Error pattern analysis
   grep -E "(ERROR|CRITICAL)" /var/log/scheduler/scheduler.log | tail -20
   
   # Performance analysis
   grep "sync.*completed" /var/log/scheduler/scheduler.log | tail -10
   ```

2. **Database Queries:**
   ```sql
   -- Find stuck events
   SELECT *
   FROM event_schedules 
   WHERE status = 'processing' 
     AND processing_started_at < now() - INTERVAL 1 HOUR;
   
   -- Check error distribution
   SELECT 
       error,
       count() as count
   FROM event_schedules 
   WHERE status = 'error'
   GROUP BY error
   ORDER BY count DESC;
   ```

3. **Redis Debugging:**
   ```bash
   # Check Redis keys
   redis-cli keys "*scheduled*"
   
   # Monitor Redis commands
   redis-cli monitor
   
   # Check memory usage by key
   redis-cli --bigkeys
   ```

### Performance Tuning

1. **ClickHouse Optimization:**
   ```sql
   -- Check query performance
   SELECT 
       query,
       elapsed,
       memory_usage
   FROM system.query_log 
   WHERE query LIKE '%event_schedules%'
   ORDER BY elapsed DESC
   LIMIT 10;
   
   -- Optimize tables
   OPTIMIZE TABLE event_schedules;
   ```

2. **Redis Optimization:**
   ```bash
   # Tune Redis configuration
   # redis.conf adjustments:
   # maxmemory-policy allkeys-lru
   # save 900 1
   # timeout 300
   ```

### Escalation Procedures

1. **Level 1: Automatic Recovery**
   - Circuit breakers
   - Retry mechanisms
   - Self-healing processes

2. **Level 2: Operational Response**
   - Manual sync/cleanup
   - Configuration adjustments
   - Service restarts

3. **Level 3: Engineering Support**
   - Code-level debugging
   - Architecture changes
   - Emergency patches

## Contact Information

- **On-call Engineer**: [Contact details]
- **System Admin**: [Contact details]
- **Database Admin**: [Contact details]
- **Emergency Escalation**: [Contact details]

## Useful Commands Reference

```bash
# Quick health check
curl -f http://localhost:8000/api/v1/schedules/monitoring/health

# Get statistics
curl -s http://localhost:8000/api/v1/schedules/monitoring/stats | jq '.'

# Force sync
curl -X POST http://localhost:8000/api/v1/schedules/monitoring/sync

# Force cleanup
curl -X POST http://localhost:8000/api/v1/schedules/monitoring/cleanup

# Check logs
tail -f /var/log/scheduler/scheduler.log

# ClickHouse status
clickhouse-client --query "SELECT count() FROM event_schedules WHERE status = 'pending'"

# Redis status
redis-cli info stats
```
