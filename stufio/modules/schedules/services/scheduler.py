import logging

from stufio.core.config import get_settings
from ..brokers.stufio_broker import StufioBroker

logger = logging.getLogger(__name__)
settings = get_settings()


class SchedulerService:
    """Service for managing the schedule broker and execution."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SchedulerService, cls).__new__(cls)
            cls._instance.broker = None
            cls._instance.initialized = False
        return cls._instance

    async def initialize(self) -> None:
        """Initialize the scheduler service."""
        if self.initialized:
            return

        logger.info("Initializing SchedulerService")

        # Create and start the broker
        self.broker = StufioBroker(
            scheduler_interval=settings.schedules_CHECK_INTERVAL_SECONDS
        )
        await self.broker.startup()

        self.initialized = True
        logger.info("SchedulerService initialized successfully")

    async def shutdown(self) -> None:
        """Shutdown the scheduler service."""
        if not self.initialized:
            return

        logger.info("Shutting down SchedulerService")

        if self.broker:
            await self.broker.shutdown()

        self.initialized = False
        logger.info("SchedulerService shut down successfully")

    def get_broker(self) -> StufioBroker:
        """Get the StufioBroker instance."""
        if not self.initialized:
            raise RuntimeError("SchedulerService not initialized")
        return self.broker


# Create a singleton instance
scheduler_service = SchedulerService()
