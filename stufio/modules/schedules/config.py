from stufio.core.settings import ModuleSettings
from stufio.core.config import get_settings

settings = get_settings()


class SchedulesSettings(ModuleSettings):
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
    
    # Event Scheduler settings
    USE_HYBRID_SCHEDULER: bool = True  # Whether to use hybrid Redis+ClickHouse scheduler
    REDIS_PROCESSING_INTERVAL: int = 1  # How often to check Redis for due messages (seconds)
    CLICKHOUSE_SYNC_INTERVAL: int = 300  # How often to sync ClickHouse to Redis (seconds)
    IMMEDIATE_HORIZON_SECONDS: int = 86400  # Events within this time go directly to Redis (24 hours)
    TRANSFER_HORIZON_SECONDS: int = 172800  # Events within this time are transferred to Redis (48 hours)
    
    # Analytics settings
    ANALYTICS_ENABLED: bool = True  # Whether to store execution results in analytics tables
    ANALYTICS_RETENTION_DAYS: int = 90  # How long to keep analytics data


# Register settings with the core
settings.register_module_settings("schedules", SchedulesSettings)
