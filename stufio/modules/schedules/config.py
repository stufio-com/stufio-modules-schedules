from pydantic import BaseModel
from stufio.core.settings import ModuleSettings, get_settings

settings = get_settings()


class SchedulesSettings(ModuleSettings, BaseModel):
    """Settings for the schedules module."""

    # Scheduler settings
    CHECK_INTERVAL_SECONDS: int = 60  # How often to check for due schedules
    MAX_RETRIES: int = 3  # Default maximum number of retries for failed executions
    RETRY_DELAY_SECONDS: int = 60  # Default delay between retries

    # Execution history
    EXECUTION_HISTORY_TTL_DAYS: int = 30  # How long to keep execution history

    # Advanced settings
    MAX_CONCURRENT_EXECUTIONS: int = 10  # Maximum number of concurrent executions
    TIMEZONE: str = "UTC"  # Default timezone for schedules


# Register settings with the core
settings.register_module_settings("schedules", SchedulesSettings)
