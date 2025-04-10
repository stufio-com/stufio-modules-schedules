import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional, Type, Union
from uuid import uuid4
from datetime import datetime, timedelta

from taskiq.abc.broker import AsyncBroker
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage
from taskiq.result import TaskiqResult
from taskiq.task import AsyncTaskiqTask
from taskiq.scheduler.scheduler import TaskiqScheduler
from taskiq.scheduler.scheduled_task import ScheduledTask

from stufio.core.config import get_settings
from stufio.modules.events import get_event_bus, EventDefinition, ActorType

from ..models.schedule import Schedule
from ..crud.crud_schedule import crud_schedule

logger = logging.getLogger(__name__)
settings = get_settings()


class StufioBroker(AsyncBroker):
    """Custom broker for Stufio that integrates with the events module.

    This broker uses the EventBus from the events module to trigger
    events according to schedules defined in the database.
    """

    def __init__(self, scheduler_interval: int = 60):
        """Initialize the StufioBroker.

        Args:
            scheduler_interval: Interval in seconds to check for due schedules
        """
        super().__init__()
        self.event_bus = None
        self.scheduler_interval = scheduler_interval
        self.scheduler_task = None
        self.running = False
        self.scheduler = TaskiqScheduler(self, [])  # Pass empty list as sources

    async def startup(self) -> None:
        """Start the broker and initialize the event bus connection."""
        await super().startup()

        logger.info("Starting StufioBroker")

        self.event_bus = get_event_bus()

        # Start the scheduler task
        self.running = True
        self.scheduler_task = asyncio.create_task(self._run_scheduler())

        logger.info("StufioBroker started successfully")

    async def shutdown(self) -> None:
        """Shutdown the broker and stop the scheduler."""
        logger.info("Shutting down StufioBroker")

        self.running = False
        if self.scheduler_task and not self.scheduler_task.done():
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass

        await super().shutdown()
        logger.info("StufioBroker shut down successfully")

    async def _run_scheduler(self) -> None:
        """Run the scheduler loop to process scheduled events."""
        logger.info(
            f"Starting scheduler loop with interval: {self.scheduler_interval}s"
        )

        while self.running:
            try:
                await self._process_due_schedules()
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)

            # Sleep until next check
            await asyncio.sleep(self.scheduler_interval)

    async def _process_due_schedules(self) -> None:
        """Process all schedules that are due for execution."""
        now = datetime.utcnow()
        logger.debug(f"Checking for due schedules at {now}")

        # Get all due schedules
        due_schedules = await crud_schedule.get_due_schedules()
        if not due_schedules:
            return

        logger.info(f"Found {len(due_schedules)} schedules due for execution")

        # Process each due schedule
        for schedule in due_schedules:
            try:
                await self._execute_schedule(schedule)
            except Exception as e:
                logger.error(
                    f"Error executing schedule {schedule.id}: {e}", exc_info=True
                )

                # Update the schedule with error information
                await crud_schedule.update_execution_status(
                    schedule_id=str(schedule.id), status="error", error=str(e)
                )

    async def _execute_schedule(self, schedule: Schedule) -> None:
        """Execute a single schedule by triggering its associated event."""
        if not self.event_bus:
            logger.error("Event bus not initialized")
            return

        logger.info(f"Executing schedule '{schedule.name}' ({schedule.id})")
        start_time = datetime.utcnow()
        event_id = None

        try:
            # Trigger the event
            event_message = await self.event_bus.publish(
                entity_type=schedule.event_type,
                entity_id=schedule.event_entity_id or str(uuid4()),
                action=schedule.event_action,
                actor_type=schedule.actor_type,
                actor_id=schedule.actor_id,
                payload=schedule.event_payload,
                correlation_id=str(uuid4()),
                metrics={"source": "scheduler", "schedule_id": str(schedule.id)},
            )

            event_id = str(event_message.event_id)
            status = "success"
            error = None

        except Exception as e:
            logger.error(
                f"Error triggering event for schedule {schedule.id}: {e}", exc_info=True
            )
            status = "error"
            error = str(e)

        # Calculate duration
        end_time = datetime.utcnow()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        # Update the schedule status
        await crud_schedule.update_execution_status(
            schedule_id=str(schedule.id),
            status=status,
            event_id=event_id,
            error=error,
            duration_ms=duration_ms,
        )

        logger.info(f"Schedule '{schedule.name}' executed with status: {status}")

    async def schedule_event(
        self,
        name: str,
        event_def: Type[EventDefinition],
        entity_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        cron_expression: Optional[str] = None,
        execution_time: Optional[datetime] = None,
        enabled: bool = True,
        actor_type: str = "system",
        actor_id: str = "scheduler",
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        created_by: Optional[str] = None,
    ) -> Schedule:
        """Schedule an event to be triggered at a specific time or on a recurring schedule.

        Args:
            name: Unique name for the schedule
            event_def: Event definition class
            entity_id: Optional entity ID for the event
            payload: Optional payload for the event
            cron_expression: Cron expression for recurring schedules
            execution_time: Execution time for one-time schedules
            enabled: Whether the schedule is enabled
            actor_type: Type of actor to use when triggering the event
            actor_id: ID of actor to use when triggering the event
            description: Optional description of the schedule
            tags: Optional tags for filtering
            created_by: Optional ID of user who created the schedule

        Returns:
            The created schedule
        """
        if not cron_expression and not execution_time:
            raise ValueError(
                "Either cron_expression or execution_time must be provided"
            )

        # Create the schedule
        schedule_data = {
            "name": name,
            "description": description,
            "enabled": enabled,
            "event_type": event_def.entity_type,
            "event_action": event_def.action,
            "event_entity_id": entity_id,
            "event_payload": payload,
            "actor_type": actor_type,
            "actor_id": actor_id,
            "cron_expression": cron_expression,
            "one_time": execution_time is not None,
            "execution_time": execution_time,
            "tags": tags or [],
            "created_by": created_by,
        }

        # Calculate next execution time
        if execution_time:
            schedule_data["next_execution"] = execution_time
        elif cron_expression:
            # Calculate next execution from cron expression
            from croniter import croniter

            now = datetime.utcnow()
            schedule_data["next_execution"] = croniter(cron_expression, now).get_next(
                datetime
            )

        # Create the schedule
        schedule = await crud_schedule.create(schedule_data)
        logger.info(f"Created schedule '{name}' ({schedule.id})")

        return schedule

    async def kick(self, message: BrokerMessage) -> None:
        """Kick a task to be executed.

        This is not used in StufioBroker as we rely on the event bus
        for event triggering.
        """
        logger.warning("StufioBroker does not support direct task kicking")

    async def listen(self) -> AsyncGenerator[Union[bytes, AckableMessage], None]:
        """Listen for tasks.

        This is not used in StufioBroker as we don't listen for tasks.
        """
        logger.warning("StufioBroker does not support listening for tasks")
        while True:
            yield b""  # This will never be reached
