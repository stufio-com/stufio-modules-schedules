from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from datetime import datetime
from uuid import UUID

from stufio.api import deps
from stufio.models import User
from stufio.modules.events.schemas.event_definition import EventDefinition

from ..schemas.schedule import (
    ScheduleCreate,
    ScheduleUpdate,
    ScheduleResponse,
    ScheduleExecutionResponse,
)
from ..crud.crud_schedule import crud_schedule, crud_schedule_execution
from ..services.scheduler import scheduler_service

router = APIRouter()


@router.post("/", response_model=ScheduleResponse)
async def create_schedule(
    *,
    schedule_in: ScheduleCreate,
    current_user: User = Depends(deps.get_current_active_user),
) -> ScheduleResponse:
    """Create a new schedule."""
    # Check if a schedule with this name already exists
    existing = await crud_schedule.get_by_name(schedule_in.name)
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Schedule with name '{schedule_in.name}' already exists",
        )

    # Create the schedule
    schedule_data = schedule_in.dict()
    schedule_data["created_by"] = str(current_user.id)

    # Calculate next execution time
    if schedule_in.one_time and schedule_in.execution_time:
        schedule_data["next_execution"] = schedule_in.execution_time
    elif schedule_in.cron_expression:
        # Calculate next execution from cron expression
        from croniter import croniter

        now = datetime.utcnow()
        schedule_data["next_execution"] = croniter(
            schedule_in.cron_expression, now
        ).get_next(datetime)

    schedule = await crud_schedule.create(schedule_data)
    return schedule


@router.get("/", response_model=List[ScheduleResponse])
async def get_schedules(
    *,
    skip: int = 0,
    limit: int = 100,
    enabled: Optional[bool] = None,
    tag: Optional[str] = None,
    event_type: Optional[str] = None,
    event_action: Optional[str] = None,
    current_user: User = Depends(deps.get_current_active_user),
) -> List[ScheduleResponse]:
    """Get schedules with optional filtering."""
    # Build filter expression
    filter_dict = {}
    if enabled is not None:
        filter_dict["enabled"] = enabled
    if event_type:
        filter_dict["event_type"] = event_type
    if event_action:
        filter_dict["event_action"] = event_action

    # Handle tag filtering separately
    if tag:
        return await crud_schedule.get_by_tags([tag], enabled_only=(enabled is True))

    # Get schedules
    schedules = await crud_schedule.get_multi(
        skip=skip, limit=limit, filters=filter_dict
    )

    return schedules


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    *,
    schedule_id: str = Path(..., title="The ID of the schedule to get"),
    current_user: User = Depends(deps.get_current_active_user),
) -> ScheduleResponse:
    """Get a schedule by ID."""
    schedule = await crud_schedule.get(schedule_id)
    if not schedule:
        raise HTTPException(
            status_code=404, detail=f"Schedule not found: {schedule_id}"
        )

    return schedule


@router.put("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    *,
    schedule_id: str = Path(..., title="The ID of the schedule to update"),
    schedule_in: ScheduleUpdate,
    current_user: User = Depends(deps.get_current_active_user),
) -> ScheduleResponse:
    """Update a schedule."""
    schedule = await crud_schedule.get(schedule_id)
    if not schedule:
        raise HTTPException(
            status_code=404, detail=f"Schedule not found: {schedule_id}"
        )

    # If name is being changed, check for duplicates
    if schedule_in.name and schedule_in.name != schedule.name:
        existing = await crud_schedule.get_by_name(schedule_in.name)
        if existing and str(existing.id) != schedule_id:
            raise HTTPException(
                status_code=400,
                detail=f"Schedule with name '{schedule_in.name}' already exists",
            )

    # If cron expression or execution time changing, update next_execution
    update_data = schedule_in.dict(exclude_unset=True)
    if (
        "cron_expression" in update_data
        or "one_time" in update_data
        or "execution_time" in update_data
    ):
        # Get the updated values, falling back to existing values
        one_time = update_data.get("one_time", schedule.one_time)
        cron_expression = update_data.get("cron_expression", schedule.cron_expression)
        execution_time = update_data.get("execution_time", schedule.execution_time)

        # Calculate next execution time
        if one_time and execution_time:
            update_data["next_execution"] = execution_time
        elif not one_time and cron_expression:
            # Calculate next execution from cron expression
            from croniter import croniter

            now = datetime.utcnow()
            update_data["next_execution"] = croniter(cron_expression, now).get_next(
                datetime
            )

    # Update the schedule
    schedule = await crud_schedule.update(schedule, update_data)
    return schedule


@router.delete("/{schedule_id}")
async def delete_schedule(
    *,
    schedule_id: str = Path(..., title="The ID of the schedule to delete"),
    current_user: User = Depends(deps.get_current_active_user),
) -> dict:
    """Delete a schedule."""
    schedule = await crud_schedule.get(schedule_id)
    if not schedule:
        raise HTTPException(
            status_code=404, detail=f"Schedule not found: {schedule_id}"
        )

    await crud_schedule.remove(schedule_id)
    return {"message": f"Schedule '{schedule.name}' deleted successfully"}


@router.get("/{schedule_id}/executions", response_model=List[ScheduleExecutionResponse])
async def get_schedule_executions(
    *,
    schedule_id: str = Path(..., title="The ID of the schedule"),
    skip: int = 0,
    limit: int = 50,
    current_user: User = Depends(deps.get_current_active_user),
) -> List[ScheduleExecutionResponse]:
    """Get execution history for a schedule."""
    schedule = await crud_schedule.get(schedule_id)
    if not schedule:
        raise HTTPException(
            status_code=404, detail=f"Schedule not found: {schedule_id}"
        )

    executions = await crud_schedule_execution.get_for_schedule(
        schedule_id=schedule_id, limit=limit, skip=skip
    )

    return executions


@router.post("/{schedule_id}/execute", response_model=ScheduleResponse)
async def execute_schedule_now(
    *,
    schedule_id: str = Path(..., title="The ID of the schedule to execute"),
    current_user: User = Depends(deps.get_current_active_superuser),
) -> ScheduleResponse:
    """Manually execute a schedule now."""
    schedule = await crud_schedule.get(schedule_id)
    if not schedule:
        raise HTTPException(
            status_code=404, detail=f"Schedule not found: {schedule_id}"
        )

    # Get the broker
    broker = scheduler_service.get_broker()

    # Execute the schedule
    try:
        await broker._execute_schedule(schedule)
        # Refresh the schedule to get updated status
        schedule = await crud_schedule.get(schedule_id)
        return schedule
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error executing schedule: {str(e)}"
        )


@router.post("/{schedule_id}/toggle", response_model=ScheduleResponse)
async def toggle_schedule(
    *,
    schedule_id: str = Path(..., title="The ID of the schedule to toggle"),
    current_user: User = Depends(deps.get_current_active_user),
) -> ScheduleResponse:
    """Toggle a schedule's enabled status."""
    schedule = await crud_schedule.get(schedule_id)
    if not schedule:
        raise HTTPException(
            status_code=404, detail=f"Schedule not found: {schedule_id}"
        )

    # Toggle the enabled status
    update_data = {"enabled": not schedule.enabled}
    schedule = await crud_schedule.update(schedule, update_data)
    return schedule
