"""
API routes for MongoDB schedule management
"""
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPBearer

from ...crud.crud_mongo_schedule import crud_mongo_schedule
from ...models.mongo_schedule import MongoSchedule, ScheduleStatus
from ...schemas.mongo_schedule import (
    MongoScheduleCreate,
    MongoScheduleUpdate,
    MongoScheduleResponse,
    MongoScheduleExecutionResponse,
    MongoScheduleStats
)

router = APIRouter(prefix="/admin/schedules", tags=["MongoDB Schedules"])
security = HTTPBearer()


@router.post("/", response_model=MongoScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(
    schedule_data: MongoScheduleCreate,
    token: str = Depends(security)
):
    """Create a new MongoDB schedule"""
    try:
        # Check if schedule with same name already exists
        existing = await crud_mongo_schedule.get_by_name(schedule_data.name)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Schedule with name '{schedule_data.name}' already exists"
            )

        schedule = await crud_mongo_schedule.create(schedule_data)
        return MongoScheduleResponse.model_validate(schedule)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create schedule: {str(e)}"
        )


@router.get("/", response_model=List[MongoScheduleResponse])
async def list_schedules(
    status_filter: Optional[ScheduleStatus] = Query(None, description="Filter by schedule status"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    event_action: Optional[str] = Query(None, description="Filter by event action"),
    skip: int = Query(0, ge=0, description="Number of schedules to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of schedules to return"),
    token: str = Depends(security)
):
    """List MongoDB schedules with optional filters"""
    try:
        if status_filter:
            schedules = await crud_mongo_schedule.get_schedules_by_status(status_filter)
        elif event_type:
            schedules = await crud_mongo_schedule.get_schedules_by_event_type(event_type, event_action)
        else:
            schedules = await crud_mongo_schedule.get_multi(skip=skip, limit=limit)

        return [MongoScheduleResponse.model_validate(schedule) for schedule in schedules]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list schedules: {str(e)}"
        )


@router.get("/{schedule_id}", response_model=MongoScheduleResponse)
async def get_schedule(
    schedule_id: UUID,
    token: str = Depends(security)
):
    """Get MongoDB schedule by ID"""
    schedule = await crud_mongo_schedule.get(str(schedule_id))
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schedule not found"
        )
    
    return MongoScheduleResponse.model_validate(schedule)


@router.put("/{schedule_id}", response_model=MongoScheduleResponse)
async def update_schedule(
    schedule_id: UUID,
    schedule_update: MongoScheduleUpdate,
    token: str = Depends(security)
):
    """Update MongoDB schedule"""
    try:
        # Get the existing schedule first
        existing_schedule = await crud_mongo_schedule.get(str(schedule_id))
        if not existing_schedule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found"
            )
        
        # Update with the model instance and update data
        schedule = await crud_mongo_schedule.update(existing_schedule, schedule_update.model_dump(exclude_unset=True))
        if not schedule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found"
            )
        
        return MongoScheduleResponse.model_validate(schedule)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update schedule: {str(e)}"
        )


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(
    schedule_id: UUID,
    token: str = Depends(security)
):
    """Delete MongoDB schedule"""
    deleted = await crud_mongo_schedule.remove(str(schedule_id))
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Schedule not found"
        )


@router.post("/{schedule_id}/activate", response_model=MongoScheduleResponse)
async def activate_schedule(
    schedule_id: UUID,
    token: str = Depends(security)
):
    """Activate a MongoDB schedule"""
    try:
        schedule = await crud_mongo_schedule.update_status(schedule_id, ScheduleStatus.ACTIVE)
        if not schedule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found"
            )
        
        return MongoScheduleResponse.model_validate(schedule)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to activate schedule: {str(e)}"
        )


@router.post("/{schedule_id}/deactivate", response_model=MongoScheduleResponse)
async def deactivate_schedule(
    schedule_id: UUID,
    token: str = Depends(security)
):
    """Deactivate a MongoDB schedule"""
    try:
        schedule = await crud_mongo_schedule.update_status(schedule_id, ScheduleStatus.PAUSED)
        if not schedule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found"
            )
        
        return MongoScheduleResponse.model_validate(schedule)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to deactivate schedule: {str(e)}"
        )


@router.get("/{schedule_id}/executions", response_model=List[MongoScheduleExecutionResponse])
async def get_schedule_executions(
    schedule_id: UUID,
    limit: int = Query(50, ge=1, le=500, description="Number of executions to return"),
    token: str = Depends(security)
):
    """Get execution history for a MongoDB schedule"""
    try:
        executions = await crud_mongo_schedule.get_recent_executions(schedule_id, limit)
        return [MongoScheduleExecutionResponse.model_validate(execution) for execution in executions]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get executions: {str(e)}"
        )


@router.get("/{schedule_id}/stats", response_model=MongoScheduleStats)
async def get_schedule_stats(
    schedule_id: UUID,
    token: str = Depends(security)
):
    """Get statistics for a MongoDB schedule"""
    try:
        stats = await crud_mongo_schedule.get_schedule_stats(schedule_id)
        if not stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found"
            )
        
        return MongoScheduleStats(**stats)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schedule stats: {str(e)}"
        )


@router.get("/active/due", response_model=List[MongoScheduleResponse])
async def get_due_schedules(
    token: str = Depends(security)
):
    """Get schedules that are due for execution"""
    try:
        now = datetime.utcnow()
        schedules = await crud_mongo_schedule.get_schedules_due_for_execution(now)
        return [MongoScheduleResponse.model_validate(schedule) for schedule in schedules]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get due schedules: {str(e)}"
        )


@router.post("/bulk/activate", response_model=Dict[str, Any])
async def bulk_activate_schedules(
    schedule_ids: List[UUID],
    token: str = Depends(security)
):
    """Bulk activate multiple schedules"""
    try:
        results = await crud_mongo_schedule.bulk_update_status([str(sid) for sid in schedule_ids], ScheduleStatus.ACTIVE)
        return {
            "matched_count": results["matched_count"],
            "modified_count": results["modified_count"]
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to bulk activate schedules: {str(e)}"
        )


@router.post("/bulk/deactivate", response_model=Dict[str, Any])
async def bulk_deactivate_schedules(
    schedule_ids: List[UUID],
    token: str = Depends(security)
):
    """Bulk deactivate multiple schedules"""
    try:
        results = await crud_mongo_schedule.bulk_update_status([str(sid) for sid in schedule_ids], ScheduleStatus.PAUSED)
        return {
            "matched_count": results["matched_count"],
            "modified_count": results["modified_count"]
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to bulk deactivate schedules: {str(e)}"
        )
