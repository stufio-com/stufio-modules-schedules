import logging
import asyncio

from stufio.core.config import get_settings
from .scheduler_service import scheduler_service as hybrid_scheduler_service, HybridSchedulerService
from .analytics_service import analytics_service

logger = logging.getLogger(__name__)
settings = get_settings()


class SchedulerService:
    """Service for managing the complete schedule system."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SchedulerService, cls).__new__(cls)
            cls._instance.hybrid_scheduler = None
            cls._instance.initialized = False
        return cls._instance

    async def initialize(self) -> None:
        """Initialize the scheduler service."""
        if self.initialized:
            return

        logger.info("Initializing complete SchedulerService")

        try:
            # Initialize hybrid scheduler service 
            self.hybrid_scheduler = hybrid_scheduler_service
            await self.hybrid_scheduler.initialize()
            await self.hybrid_scheduler.start()

            self.initialized = True
            logger.info("SchedulerService initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize SchedulerService: {e}", exc_info=True)
            await self.shutdown()
            raise

    async def shutdown(self) -> None:
        """Shutdown the scheduler service."""
        if not self.initialized:
            return

        logger.info("Shutting down SchedulerService")

        if self.hybrid_scheduler:
            try:
                await self.hybrid_scheduler.stop()
            except Exception as e:
                logger.error(f"Error stopping hybrid scheduler: {e}")

        self.initialized = False
        logger.info("SchedulerService shut down successfully")

    def get_hybrid_scheduler(self) -> HybridSchedulerService:
        """Get the HybridSchedulerService instance."""
        if not self.initialized:
            raise RuntimeError("SchedulerService not initialized")
        return self.hybrid_scheduler


# Create a singleton instance
scheduler_service = SchedulerService()
