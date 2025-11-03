from fastapi import APIRouter

from .routers.monitoring import router as monitoring_router
from .routers.mongo_schedules import router as mongo_schedules_router
from .routers.clickhouse_events import router as clickhouse_events_router
from .routers.redis_events import router as redis_events_router
from .routers.analytics import router as analytics_router

router = APIRouter()

# Monitoring routes
router.include_router(monitoring_router, prefix="/monitoring", tags=["monitoring"])

# Three-tier scheduling system routes
router.include_router(mongo_schedules_router, tags=["schedules"])
router.include_router(clickhouse_events_router, tags=["schedules"])
router.include_router(redis_events_router, tags=["schedules"])
router.include_router(analytics_router, tags=["schedules"])


__all__ = ["router"]