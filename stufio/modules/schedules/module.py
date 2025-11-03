from typing import List, Any, Tuple
import logging

from stufio.core.module_registry import ModuleInterface
from stufio.core.stufioapi import StufioAPI
from .api import router
from .services.analytics_service import analytics_service
from .__version__ import __version__

logger = logging.getLogger(__name__)


class SchedulesModule(ModuleInterface):
    """Module for three-tier scheduling events."""

    version = __version__

    def register_routes(self, app: StufioAPI) -> None:
        """Register this module's routes with the FastAPI app."""
        app.include_router(router, prefix=self.routes_prefix)

    def get_middlewares(self) -> List[Tuple]:
        """Return middleware classes for this module."""
        return []

    async def on_startup(self, app: StufioAPI) -> None:
        """Initialize module on application startup."""
        import os

        # Check if we're in testing mode and skip complex shutdown
        is_testing = os.getenv("TESTING") == "1" or os.getenv("TESTING") == "true"

        if is_testing:
            logger.info("Running in testing mode - skipping schedules module shutdown")
            return
        
        try:
            # Initialize analytics service (if needed)
            logger.info("Schedules module started successfully")
            logger.info("Three-tier scheduler service available via API endpoints")
        except Exception as e:
            logger.error(f"Error initializing schedules module: {e}", exc_info=True)

    async def on_shutdown(self, app: StufioAPI) -> None:
        """Shutdown module."""
        import os

        # Check if we're in testing mode and skip complex shutdown
        is_testing = os.getenv("TESTING") == "1" or os.getenv("TESTING") == "true"

        if is_testing:
            logger.info("Running in testing mode - skipping schedules module shutdown")
            return

        try:
            logger.info("Schedules module shut down successfully")
        except Exception as e:
            logger.error(f"Error shutting down schedules module: {e}", exc_info=True)
