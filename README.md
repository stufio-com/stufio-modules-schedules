# Stufio Schedules Module

Schedule-based event trigger module for Stufio framework.

## Features

- Schedule events to be triggered at specific times
- Support for one-time and recurring (cron-based) schedules
- Integration with the Stufio Events module
- Execution history tracking
- Admin interface for schedule management

## Installation

```bash
pip install stufio-modules-schedules
```

## Usage

### API Endpoints

- **Schedules API**:
  - `GET /schedules`: Retrieve all schedules
  - `POST /schedules`: Create a new schedule
  - `GET /schedules/{schedule_id}`: Retrieve a specific schedule
  - `PUT /schedules/{schedule_id}`: Update a specific schedule
  - `DELETE /schedules/{schedule_id}`: Delete a specific schedule
  - `GET /schedules/{schedule_id}/executions`: Retrieve execution history for a schedule
  - `POST /schedules/{schedule_id}/execute`: Manually execute a schedule
  - `POST /schedules/{schedule_id}/toggle`: Toggle a schedule's enabled status

### Programmatic Usage

```python
from stufio.modules.schedules.services.scheduler import scheduler_service
from stufio.modules.events.events import UserCreatedEvent

# Initialize the scheduler service (done automatically during app startup)
await scheduler_service.initialize()

# Get the broker
broker = scheduler_service.get_broker()

# Schedule an event
await broker.schedule_event(
    name="Daily User Report",
    event_def=UserCreatedEvent,
    payload={"report_type": "daily"},
    cron_expression="0 0 * * *",  # Run at midnight every day
    description="Generate daily user report",
    tags=["reports", "users"]
)

# Schedule a one-time event
from datetime import datetime, timedelta
future_time = datetime.utcnow() + timedelta(hours=1)

await broker.schedule_event(
    name="One-time Notification",
    event_def=NotificationEvent,
    entity_id="notification-1234",
    payload={"message": "Your scheduled task is ready"},
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
- taskiq
- croniter
- pytz
```

## 3. Integration with Events Module

The key integration between the Schedules module and the Events module happens in the `StufioBroker` class. This broker extends TaskIQ's `AsyncBroker` but relies on Stufio's `EventBus` to trigger events instead of using TaskIQ's task execution mechanisms.

The flow works like this:

1. A schedule is created with information about which event to trigger and when
2. The `StufioBroker` periodically checks for due schedules
3. When a schedule is due, the broker uses `event_bus.publish()` to trigger the associated event
4. The event is then processed by the Events module like any other event

This design ensures that scheduled events are handled identically to manually triggered events, maintaining consistency throughout the system.

The requirements are satisfied as follows:

1. ✅ Custom broker (`StufioBroker`) that extends TaskIQ's `AsyncBroker`
2. ✅ Integration with events module via the `event_bus`
3. ✅ Support for event triggering based on schedules
4. ✅ Rich scheduling features including one-time and recurring (cron-based) schedules
5. ✅ Full API for managing schedules

This implementation provides a robust foundation for scheduling events in your Stufio application.## 3. Integration with Events Module

The key integration between the Schedules module and the Events module happens in the `StufioBroker` class. This broker extends TaskIQ's `AsyncBroker` but relies on Stufio's `EventBus` to trigger events instead of using TaskIQ's task execution mechanisms.

The flow works like this:

1. A schedule is created with information about which event to trigger and when
2. The `StufioBroker` periodically checks for due schedules
3. When a schedule is due, the broker uses `event_bus.publish()` to trigger the associated event
4. The event is then processed by the Events module like any other event

This design ensures that scheduled events are handled identically to manually triggered events, maintaining consistency throughout the system.

The requirements are satisfied as follows:

1. ✅ Custom broker (`StufioBroker`) that extends TaskIQ's `AsyncBroker`
2. ✅ Integration with events module via the `event_bus`
3. ✅ Support for event triggering based on schedules
4. ✅ Rich scheduling features including one-time and recurring (cron-based) schedules
5. ✅ Full API for managing schedules

This implementation provides a robust foundation for scheduling events in your Stufio application.