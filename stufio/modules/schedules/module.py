from typing import List, Any, Tuple
import logging

from stufio.core.module_registry import ModuleInterface
from stufio.core.stufioapi import StufioAPI
from .api import router
from .models import Schedule, ScheduleExecution
from .services.scheduler import scheduler_service
from .__version__ import __version__

logger = logging.getLogger(__name__)


class SchedulesModule(ModuleInterface):
    """Module for scheduling events."""

    version = __version__

    def register_routes(self, app: StufioAPI) -> None:
        """Register this module's routes with the FastAPI app."""
        app.include_router(router, prefix=self.routes_prefix)

    def get_middlewares(self) -> List[Tuple]:
        """Return middleware classes for this module."""
        return []

    async def on_startup(self, app: StufioAPI) -> None:
        """Initialize module on application startup."""
        try:
            # Initialize the scheduler service
            await scheduler_service.initialize()
            logger.info("Schedules module started successfully")
        except Exception as e:
            logger.error(f"Error initializing schedules module: {e}", exc_info=True)

    async def on_shutdown(self, app: StufioAPI) -> None:
        """Shutdown module."""
        try:
            # Shutdown the scheduler service
            await scheduler_service.shutdown()
            logger.info("Schedules module shut down successfully")
        except Exception as e:
            logger.error(f"Error shutting down schedules module: {e}", exc_info=True)
