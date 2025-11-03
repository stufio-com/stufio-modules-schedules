"""
API routes for ClickHouse scheduled events management
"""
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPBearer

from ...crud.crud_clickhouse_scheduled_event import crud_clickhouse_scheduled_event
from ...models.clickhouse_scheduled_event import ScheduledEventStatus
from ...schemas.clickhouse_scheduled_event import (
    ClickhouseScheduledEventCreate,
    ClickhouseScheduledEventUpdate,
    ClickhouseScheduledEventResponse,
    ClickhouseScheduledEventStats
)

router = APIRouter(prefix="/admin/scheduled", tags=["ClickHouse Scheduled Events"])
security = HTTPBearer()


@router.post("/", response_model=ClickhouseScheduledEventResponse, status_code=status.HTTP_201_CREATED)
async def create_scheduled_event(
    event_data: ClickhouseScheduledEventCreate,
    token: str = Depends(security)
):
    """Create a new ClickHouse scheduled event"""
    try:
        event = await crud_clickhouse_scheduled_event.create(event_data)
        return ClickhouseScheduledEventResponse.model_validate(event)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create scheduled event: {str(e)}"
        )


@router.get("/", response_model=List[ClickhouseScheduledEventResponse])
async def list_scheduled_events(
    status_filter: Optional[ScheduledEventStatus] = Query(None, description="Filter by event status"),
    source: Optional[str] = Query(None, description="Filter by source"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    scheduled_after: Optional[datetime] = Query(None, description="Filter events scheduled after this time"),
    scheduled_before: Optional[datetime] = Query(None, description="Filter events scheduled before this time"),
    skip: int = Query(0, ge=0, description="Number of events to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of events to return"),
    token: str = Depends(security)
):
    """List ClickHouse scheduled events with optional filters"""
    try:
        if status_filter:
            events = await crud_clickhouse_scheduled_event.get_by_status(status_filter, limit)
        elif source:
            events = await crud_clickhouse_scheduled_event.get_by_source(source, limit=limit)
        elif scheduled_after or scheduled_before:
            events = await crud_clickhouse_scheduled_event.get_by_time_range(scheduled_after, scheduled_before, limit)
        else:
            events = await crud_clickhouse_scheduled_event.get_multi(skip, limit)

        return [ClickhouseScheduledEventResponse.model_validate(event) for event in events]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list scheduled events: {str(e)}"
        )


@router.get("/{event_id}", response_model=ClickhouseScheduledEventResponse)
async def get_scheduled_event(
    event_id: UUID,
    token: str = Depends(security)
):
    """Get ClickHouse scheduled event by ID"""
    event = await crud_clickhouse_scheduled_event.get(event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Scheduled event not found"
        )
    
    return ClickhouseScheduledEventResponse.model_validate(event)


@router.put("/{event_id}", response_model=ClickhouseScheduledEventResponse)
async def update_scheduled_event(
    event_id: UUID,
    event_update: ClickhouseScheduledEventUpdate,
    token: str = Depends(security)
):
    """Update ClickHouse scheduled event"""
    try:
        event = await crud_clickhouse_scheduled_event.update(event_id, event_update)
        if not event:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Scheduled event not found"
            )
        
        return ClickhouseScheduledEventResponse.model_validate(event)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update scheduled event: {str(e)}"
        )


@router.delete("/{event_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_scheduled_event(
    event_id: UUID,
    token: str = Depends(security)
):
    """Delete ClickHouse scheduled event"""
    deleted = await crud_clickhouse_scheduled_event.delete(event_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Scheduled event not found"
        )


@router.get("/ready/transfer", response_model=List[ClickhouseScheduledEventResponse])
async def get_events_ready_for_transfer(
    limit: int = Query(100, ge=1, le=1000, description="Number of events to return"),
    token: str = Depends(security)
):
    """Get events ready for transfer to Redis (scheduled within next hour)"""
    try:
        now = datetime.utcnow()
        transfer_window = now + timedelta(hours=1)
        events = await crud_clickhouse_scheduled_event.get_ready_for_transfer(transfer_window, limit)
        return [ClickhouseScheduledEventResponse.model_validate(event) for event in events]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get events ready for transfer: {str(e)}"
        )


@router.post("/{event_id}/transfer", response_model=ClickhouseScheduledEventResponse)
async def mark_event_transferred(
    event_id: UUID,
    transfer_metadata: Optional[Dict[str, Any]] = None,
    token: str = Depends(security)
):
    """Mark event as transferred to Redis"""
    try:
        event = await crud_clickhouse_scheduled_event.mark_transferred(event_id, transfer_metadata)
        if not event:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Scheduled event not found"
            )
        
        return ClickhouseScheduledEventResponse.model_validate(event)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to mark event as transferred: {str(e)}"
        )


@router.post("/bulk/transfer", response_model=Dict[str, Any])
async def bulk_transfer_events(
    event_ids: List[str | UUID],
    token: str = Depends(security)
):
    """Bulk mark events as transferred to Redis"""
    try:
        results = await crud_clickhouse_scheduled_event.bulk_mark_transferred(event_ids)
        return {
            "transferred_count": results["transferred_count"],
            "failed_ids": results["failed_ids"]
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to bulk transfer events: {str(e)}"
        )


@router.get("/stats/overview", response_model=ClickhouseScheduledEventStats)
async def get_scheduled_events_stats(
    token: str = Depends(security)
):
    """Get ClickHouse scheduled events statistics"""
    try:
        stats = await crud_clickhouse_scheduled_event.get_stats()
        return ClickhouseScheduledEventStats(**stats)
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get scheduled events stats: {str(e)}"
        )


@router.get("/stats/by-source", response_model=Dict[str, Any])
async def get_stats_by_source(
    token: str = Depends(security)
):
    """Get statistics grouped by source"""
    try:
        stats = await crud_clickhouse_scheduled_event.get_stats_by_source()
        return stats
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stats by source: {str(e)}"
        )


@router.get("/stats/by-time", response_model=Dict[str, Any])
async def get_stats_by_time(
    hours_back: int = Query(24, ge=1, le=168, description="Hours back to analyze"),
    token: str = Depends(security)
):
    """Get statistics grouped by time periods"""
    try:
        stats = await crud_clickhouse_scheduled_event.get_stats_by_time(hours_back)
        return stats
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stats by time: {str(e)}"
        )


@router.delete("/cleanup/old", response_model=Dict[str, Any])
async def cleanup_old_events(
    days_old: int = Query(30, ge=1, le=365, description="Delete events older than this many days"),
    dry_run: bool = Query(True, description="Perform dry run without actual deletion"),
    token: str = Depends(security)
):
    """Clean up old processed events"""
    try:
        if dry_run:
            count = await crud_clickhouse_scheduled_event.count_old_events(days_old)
            return {"would_delete": count, "dry_run": True}
        else:
            deleted_count = await crud_clickhouse_scheduled_event.cleanup_old_events(days_old)
            return {"deleted_count": deleted_count, "dry_run": False}
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cleanup old events: {str(e)}"
        )


@router.post("/batch/create", response_model=Dict[str, Any])
async def batch_create_events(
    events_data: List[ClickhouseScheduledEventCreate],
    token: str = Depends(security)
):
    """Batch create multiple scheduled events"""
    try:
        if len(events_data) > 1000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot create more than 1000 events in a single batch"
            )

        results = await crud_clickhouse_scheduled_event.batch_create(events_data)
        return {
            "created_count": results["created_count"],
            "failed_count": results["failed_count"],
            "errors": results.get("errors", [])
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to batch create events: {str(e)}"
        )
