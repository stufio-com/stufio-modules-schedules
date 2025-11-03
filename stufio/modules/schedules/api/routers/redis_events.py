"""
API routes for Redis scheduled events management
"""
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPBearer
import redis

from ...crud.crud_redis_scheduled_event import CRUDRedisScheduledEvent, get_redis_scheduled_event_crud
from ...models.redis_scheduled_event import RedisScheduledEvent, RedisScheduleStatus
from ...schemas.redis_scheduled_event import (
    RedisScheduledEventCreate,
    RedisScheduledEventUpdate,
    RedisScheduledEventResponse
)

router = APIRouter(prefix="/admin/redis-events", tags=["Redis Scheduled Events"])
security = HTTPBearer()


async def get_redis_client() -> redis.Redis:
    """Dependency to get Redis client"""
    # This would be injected with actual Redis client configuration
    return redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)


@router.post("/", response_model=RedisScheduledEventResponse, status_code=status.HTTP_201_CREATED)
async def create_redis_event(
    event_data: RedisScheduledEventCreate,
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Create a new Redis scheduled event"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        event = crud.create(event_data)
        return RedisScheduledEventResponse.model_validate(event)
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create Redis event: {str(e)}"
        )


@router.get("/", response_model=List[RedisScheduledEventResponse])
async def list_redis_events(
    source: Optional[str] = Query(None, description="Filter by source"),
    limit: int = Query(100, ge=1, le=1000, description="Number of events to return"),
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """List Redis scheduled events with optional filters"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        
        if source:
            events = crud.get_by_source(source, limit)
        else:
            # Get all pending events from the index using CRUD method
            events = crud.get_ready_events(limit)
        
        return [RedisScheduledEventResponse.model_validate(event) for event in events]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list Redis events: {str(e)}"
        )


@router.get("/{event_id}", response_model=RedisScheduledEventResponse)
async def get_redis_event(
    event_id: str,
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Get Redis scheduled event by ID"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        event = crud.get(event_id)
        if not event:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Redis event not found"
            )
        
        return RedisScheduledEventResponse.model_validate(event)
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get Redis event: {str(e)}"
        )


@router.put("/{event_id}", response_model=RedisScheduledEventResponse)
async def update_redis_event(
    event_id: str,
    event_update: RedisScheduledEventUpdate,
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Update Redis scheduled event"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        event = crud.update(event_id, event_update)
        if not event:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Redis event not found"
            )
        
        return RedisScheduledEventResponse.model_validate(event)
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update Redis event: {str(e)}"
        )


@router.delete("/{event_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_redis_event(
    event_id: str,
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Delete Redis scheduled event"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        deleted = crud.delete(event_id)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Redis event not found"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete Redis event: {str(e)}"
        )


@router.get("/ready/processing", response_model=List[RedisScheduledEventResponse])
async def get_ready_events(
    limit: int = Query(100, ge=1, le=1000, description="Number of events to return"),
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Get events ready for processing (scheduled_at <= now)"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        events = crud.get_ready_events(limit)
        return [RedisScheduledEventResponse.model_validate(event) for event in events]
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get ready events: {str(e)}"
        )


@router.post("/{event_id}/claim", response_model=Dict[str, Any])
async def claim_event_for_processing(
    event_id: str,
    processor_id: str = Query(..., description="ID of the processor claiming the event"),
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Claim an event for processing with distributed locking"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        claimed = crud.claim_for_processing(event_id, processor_id)
        
        if not claimed:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Event could not be claimed (already processing or not found)"
            )
        
        return {"claimed": True, "processor_id": processor_id}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to claim event: {str(e)}"
        )


@router.post("/{event_id}/complete", response_model=Dict[str, Any])
async def mark_event_processed(
    event_id: str,
    success: bool = Query(True, description="Whether processing was successful"),
    error_message: Optional[str] = Query(None, description="Error message if processing failed"),
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Mark event as processed"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        marked = crud.mark_processed(event_id, success, error_message)
        
        if not marked:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Event not found"
            )
        
        return {
            "processed": True,
            "status": "completed" if success else "failed",
            "error_message": error_message
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to mark event as processed: {str(e)}"
        )


@router.get("/stats/overview", response_model=Dict[str, Any])
async def get_redis_events_stats(
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Get Redis scheduled events statistics"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        stats = crud.get_stats()
        return stats
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get Redis events stats: {str(e)}"
        )


@router.get("/stats/health", response_model=Dict[str, Any])
async def get_queue_health(
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Get Redis queue health metrics"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        health = crud.get_queue_health()
        return health
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get queue health: {str(e)}"
        )


@router.post("/maintenance/cleanup", response_model=Dict[str, Any])
async def cleanup_expired_events(
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Clean up expired events from Redis"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        cleaned_count = crud.cleanup_expired()
        return {"cleaned_count": cleaned_count}
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cleanup expired events: {str(e)}"
        )


@router.get("/monitoring/dashboard", response_model=Dict[str, Any])
async def get_monitoring_dashboard(
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Get comprehensive monitoring dashboard data for Redis events"""
    try:
        crud = get_redis_scheduled_event_crud(redis_client)
        
        stats = crud.get_stats()
        health = crud.get_queue_health()
        ready_count = len(crud.get_ready_events(limit=1))
        
        return {
            "stats": stats,
            "health": health,
            "ready_events_sample": ready_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get monitoring dashboard data: {str(e)}"
        )


@router.post("/batch/transfer-from-clickhouse", response_model=Dict[str, Any])
async def batch_transfer_from_clickhouse(
    events_data: List[RedisScheduledEventCreate],
    redis_client: redis.Redis = Depends(get_redis_client),
    token: str = Depends(security)
):
    """Batch transfer events from ClickHouse to Redis"""
    try:
        if len(events_data) > 500:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot transfer more than 500 events in a single batch"
            )
        
        crud = get_redis_scheduled_event_crud(redis_client)
        created_count = 0
        failed_count = 0
        errors = []
        
        for i, event_data in enumerate(events_data):
            try:
                crud.create(event_data)
                created_count += 1
            except Exception as e:
                failed_count += 1
                errors.append(f"Event #{i+1}: {str(e)}")
        
        return {
            "created_count": created_count,
            "failed_count": failed_count,
            "errors": errors[:10]  # Limit error messages
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to batch transfer events: {str(e)}"
        )
