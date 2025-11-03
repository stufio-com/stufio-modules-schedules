"""
Monitoring API endpoints for the three-tier schedules system.
"""
import logging
from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ...crud.crud_clickhouse_scheduled_event import CRUDClickhouseScheduledEvent
from ...models.clickhouse_scheduled_event import ClickhouseScheduledEvent
from ...services.analytics_service import ScheduleAnalyticsService

logger = logging.getLogger(__name__)

router = APIRouter()

# Create CRUD and service instances
crud_clickhouse_scheduled_event = CRUDClickhouseScheduledEvent(ClickhouseScheduledEvent)
schedule_analytics = ScheduleAnalyticsService()


class SystemHealthResponse(BaseModel):
    """Response model for system health check."""
    status: str
    timestamp: datetime
    services: Dict[str, Dict[str, Any]]
    metrics: Dict[str, Any]


class ScheduleStatsResponse(BaseModel):
    """Response model for schedule statistics."""
    total_schedules: int
    pending_schedules: int
    processing_schedules: int
    completed_schedules: int
    error_schedules: int
    transferred_to_redis_schedules: int


@router.get("/health", response_model=SystemHealthResponse)
async def get_system_health():
    """Get comprehensive system health status."""
    try:
        timestamp = datetime.utcnow()
        services = {}
        metrics = {}

        # Check ClickHouse service
        try:
            stats = await crud_clickhouse_scheduled_event.get_stats()
            services["clickhouse"] = {
                "status": "healthy",
                "total_events": stats["total_count"]
            }
            metrics["schedules"] = stats
        except Exception as e:
            services["clickhouse"] = {"status": "error", "error": str(e)}
            metrics["schedules"] = {"error": str(e)}

        # Check analytics service
        try:
            services["analytics"] = {
                "status": "healthy"
            }
        except Exception as e:
            services["analytics"] = {"status": "error", "error": str(e)}

        # Overall system status
        overall_status = "healthy"
        for service_name, service_info in services.items():
            if service_info.get("status") != "healthy":
                overall_status = "degraded"
                break

        return SystemHealthResponse(
            status=overall_status,
            timestamp=timestamp,
            services=services,
            metrics=metrics
        )

    except Exception as e:
        logger.error(f"Error getting system health: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting system health: {str(e)}")


@router.get("/stats", response_model=ScheduleStatsResponse)
async def get_schedule_stats():
    """Get schedule statistics from ClickHouse."""
    try:
        stats = await crud_clickhouse_scheduled_event.get_stats()

        return ScheduleStatsResponse(
            total_schedules=stats["total_count"],
            pending_schedules=stats["pending_count"],
            processing_schedules=stats["processing_count"],
            completed_schedules=stats["completed_count"],
            error_schedules=stats["error_count"],
            transferred_to_redis_schedules=stats["transferred_to_redis_count"]
        )

    except Exception as e:
        logger.error(f"Error getting schedule stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting schedule stats: {str(e)}")


@router.get("/metrics/prometheus")
async def get_prometheus_metrics():
    """Get metrics in Prometheus format for monitoring integration."""
    try:
        # Get basic stats
        stats_response = await get_schedule_stats()
        
        # Format as Prometheus metrics
        metrics_lines = [
            "# HELP stufio_schedules_total Total number of schedules",
            "# TYPE stufio_schedules_total gauge",
            f"stufio_schedules_total {stats_response.total_schedules}",
            "",
            "# HELP stufio_schedules_pending Number of pending schedules",
            "# TYPE stufio_schedules_pending gauge",
            f"stufio_schedules_pending {stats_response.pending_schedules}",
            "",
            "# HELP stufio_schedules_processing Number of processing schedules",
            "# TYPE stufio_schedules_processing gauge",
            f"stufio_schedules_processing {stats_response.processing_schedules}",
            "",
            "# HELP stufio_schedules_completed Number of completed schedules",
            "# TYPE stufio_schedules_completed gauge",
            f"stufio_schedules_completed {stats_response.completed_schedules}",
            "",
            "# HELP stufio_schedules_error Number of error schedules",
            "# TYPE stufio_schedules_error gauge",
            f"stufio_schedules_error {stats_response.error_schedules}",
            "",
            "# HELP stufio_schedules_transferred_to_redis Number of schedules transferred to Redis",
            "# TYPE stufio_schedules_transferred_to_redis gauge",
            f"stufio_schedules_transferred_to_redis {stats_response.transferred_to_redis_schedules}",
            ""
        ]

        return "\n".join(metrics_lines)

    except Exception as e:
        logger.error(f"Error getting Prometheus metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting metrics: {str(e)}")
