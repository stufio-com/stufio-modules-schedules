from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from uuid import UUID
import logging 

from odmantic import ObjectId
from stufio.crud.mongo_base import CRUDMongo
from stufio.core.config import get_settings

from ..models.schedule import Schedule, ScheduleExecution
from ..schemas.schedule import ScheduleCreate, ScheduleUpdate, ScheduleExecutionCreate

settings = get_settings()
logger = logging.getLogger(__name__)


class CRUDSchedule(CRUDMongo[Schedule, ScheduleCreate, ScheduleUpdate]):
    """CRUD operations for schedules."""

    async def get_by_name(self, name: str) -> Optional[Schedule]:
        """Get a schedule by name."""
        return await self.get_by_field("name", name)

    async def get_due_schedules(self, limit: int = 100) -> List[Schedule]:
        """Get all schedules that are due for execution."""
        now = datetime.utcnow()

        # Find schedules where:
        # 1. The schedule is enabled
        # 2. The next_execution is in the past or now
        # 3. For one-time schedules, check if it has been executed already
        filter_expr = (
            (self.model.enabled == True)
            & (self.model.next_execution <= now)
            & (
                (self.model.one_time == False)
                | ((self.model.one_time == True) & (self.model.last_execution == None))
            )
        )

        return await self.get_multi(
            filter_expression=filter_expr, sort=self.model.next_execution, limit=limit
        )

    async def update_execution_status(
        self,
        schedule_id: str,
        status: str,
        event_id: Optional[str] = None,
        error: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> Schedule:
        """Update a schedule's execution status."""
        # Get the schedule
        schedule = await self.get(schedule_id)
        if not schedule:
            raise ValueError(f"Schedule not found: {schedule_id}")

        # Create execution history entry
        execution_data = ScheduleExecutionCreate(
            schedule_id=schedule_id,
            status=status,
            event_id=event_id,
            error=error,
            duration_ms=duration_ms,
        )

        # Get MongoDB connection
        db = self.engine.get_database()
        await db.schedule_executions.insert_one(execution_data.dict())

        # Update the schedule
        now = datetime.utcnow()
        update_data = {
            "last_execution": now,
            "last_status": status,
            "updated_at": now,
            "execution_count": schedule.execution_count + 1,
        }

        # Update next execution time if it's a recurring schedule
        if not schedule.one_time and schedule.cron_expression:
            from croniter import croniter

            update_data["next_execution"] = croniter(
                schedule.cron_expression, now
            ).get_next(datetime)
        else:
            # One-time schedule has been executed, no next execution
            update_data["next_execution"] = None

        # Update error info if applicable
        if status == "error":
            update_data["error_count"] = schedule.error_count + 1
            update_data["last_error"] = error

        # Update the schedule
        return await self.update(schedule, update_data)

    async def get_by_tags(
        self, tags: List[str], enabled_only: bool = True
    ) -> List[Schedule]:
        """Get schedules by tags."""
        filter_expr = self.model.tags.in_(tags)
        if enabled_only:
            filter_expr = filter_expr & (self.model.enabled == True)

        return await self.get_multi(filter_expression=filter_expr)

    async def get_by_event(
        self, event_type: str, event_action: str, enabled_only: bool = True
    ) -> List[Schedule]:
        """Get schedules by event type and action."""
        filter_expr = (self.model.event_type == event_type) & (
            self.model.event_action == event_action
        )

        if enabled_only:
            filter_expr = filter_expr & (self.model.enabled == True)

        return await self.get_multi(filter_expression=filter_expr)


class CRUDScheduleExecution(CRUDMongo[ScheduleExecution, ScheduleExecutionCreate, Any]):
    """CRUD operations for schedule executions."""

    async def get_for_schedule(
        self, schedule_id: str, limit: int = 50, skip: int = 0
    ) -> List[ScheduleExecution]:
        """Get execution history for a schedule."""
        filter_expr = self.model.schedule_id == schedule_id

        return await self.get_multi(
            filter_expression=filter_expr,
            sort=-self.model.execution_time,
            limit=limit,
            skip=skip,
        )

    async def get_recent_executions(
        self, limit: int = 50, skip: int = 0, status: Optional[str] = None
    ) -> List[ScheduleExecution]:
        """Get recent schedule executions."""
        filter_expr = None
        if status:
            filter_expr = self.model.status == status

        return await self.get_multi(
            filter_expression=filter_expr,
            sort=-self.model.execution_time,
            limit=limit,
            skip=skip,
        )


# Create singleton instances
crud_schedule = CRUDSchedule(Schedule)
crud_schedule_execution = CRUDScheduleExecution(ScheduleExecution)
