"""
API routers for schedules module
"""

from .mongo_schedules import router as mongo_schedules_router
from .clickhouse_events import router as clickhouse_events_router  
from .redis_events import router as redis_events_router
from .analytics import router as analytics_router
from .monitoring import router as monitoring_router

__all__ = [
    "mongo_schedules_router",
    "clickhouse_events_router", 
    "redis_events_router",
    "analytics_router",
    "monitoring_router"
]
