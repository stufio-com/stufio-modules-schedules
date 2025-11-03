from typing import Dict, Any, Optional, Type, ClassVar, Generic, TypeVar
import logging
from datetime import datetime
from pydantic import BaseModel, Field
from stufio.modules.events.schemas.event_definition import EventDefinition
from stufio.modules.events.schemas.payloads import BaseEventPayload

# Import status enum from models
from enum import Enum

class ScheduledEventDefinitionStatus(str, Enum):
    """Status of a scheduled event definition."""
    ACTIVE = "active"
    DISABLED = "disabled"
    DRAFT = "draft"

# Type variable for payload typing
P = TypeVar('P', bound='BaseEventPayload')

logger = logging.getLogger(__name__)


# CRUD Schemas for database operations
class ScheduledEventDefinitionBase(BaseModel):
    """Base schema for scheduled event definitions."""
    event_class_name: str
    event_name: str
    entity_type: Optional[str] = None
    action: Optional[str] = None
    cron_expression: Optional[str] = None
    timezone: str = "UTC"
    description: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    actor_type: str = "system"
    actor_id: str = "scheduler"
    module_name: str
    max_retries: int = 3
    retry_delay_seconds: int = 60
    status: ScheduledEventDefinitionStatus = ScheduledEventDefinitionStatus.ACTIVE


class ScheduledEventDefinitionCreate(ScheduledEventDefinitionBase):
    """Schema for creating a scheduled event definition."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_sync_at: datetime = Field(default_factory=datetime.utcnow)


class ScheduledEventDefinitionUpdate(BaseModel):
    """Schema for updating a scheduled event definition."""
    event_class_name: Optional[str] = None
    event_name: Optional[str] = None
    entity_type: Optional[str] = None
    action: Optional[str] = None
    cron_expression: Optional[str] = None
    timezone: Optional[str] = None
    description: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    actor_type: Optional[str] = None
    actor_id: Optional[str] = None
    module_name: Optional[str] = None
    max_retries: Optional[int] = None
    retry_delay_seconds: Optional[int] = None
    status: Optional[ScheduledEventDefinitionStatus] = None
    last_execution_at: Optional[datetime] = None
    next_execution_at: Optional[datetime] = None
    execution_count: Optional[int] = None
    error_count: Optional[int] = None
    last_error: Optional[str] = None
    manual_cron_override: Optional[bool] = None
    manual_payload_override: Optional[bool] = None
    manual_status_override: Optional[bool] = None
    last_sync_at: Optional[datetime] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ScheduledEventDefinitionMeta(type):
    """Metaclass for ScheduledEventDefinition to handle class attribute setup."""

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)

        # Skip processing for the base ScheduledEventDefinition class itself
        if name == "ScheduledEventDefinition":
            return cls

        # Initialize scheduled event attributes dictionary
        setattr(cls, '_scheduled_attrs', {})
        scheduled_attrs = getattr(cls, '_scheduled_attrs')

        # Extract class attributes into _scheduled_attrs
        for key, value in namespace.items():
            if not key.startswith("__") and not callable(value):
                scheduled_attrs[key] = value

        # Ensure required attributes are present
        required_attrs = ["entity_type", "action", "cron_expression"]
        for attr in required_attrs:
            if attr not in scheduled_attrs:
                logger.warning(f"ScheduledEventDefinition {name} missing required attribute: {attr}")

        # Generate event name if not provided
        if "name" not in scheduled_attrs:
            entity_type = scheduled_attrs.get("entity_type", "unknown")
            action = scheduled_attrs.get("action", "unknown")
            scheduled_attrs["name"] = f"{entity_type}.{action}"

        logger.info(f"Registered scheduled event definition: {name}")
        return cls


class ScheduledEventDefinition(Generic[P], metaclass=ScheduledEventDefinitionMeta):
    """
    Base class for defining scheduled events that are automatically registered and synced.
    
    Similar to EventDefinition but specifically for events that should be executed on a schedule.
    These events are automatically synced to MongoDB on application startup.
    
    Example:
        class FavoriteCleanScheduledEvent(ScheduledEventDefinition[BaseEventPayload]):
            '''Daily cleanup of favorites.'''
            entity_type = "favorite"
            action = "cleanup"
            description = "Daily cleanup of favorites"
            cron_expression = "0 0 * * *"  # Daily at midnight
            payload = {"cleanup_type": "daily"}
    """
    
    _scheduled_attrs: ClassVar[Dict[str, Any]] = {}

    # Required attributes (must be defined by subclasses)
    entity_type: ClassVar[str]
    action: ClassVar[str]
    cron_expression: ClassVar[str]  # Cron expression for scheduling

    # Optional attributes
    name: ClassVar[Optional[str]] = None  # Auto-generated if not provided
    description: ClassVar[Optional[str]] = None
    payload: ClassVar[Dict[str, Any]] = {}
    actor_type: ClassVar[str] = "system"
    actor_id: ClassVar[str] = "scheduler"
    timezone: ClassVar[str] = "UTC"
    max_retries: ClassVar[int] = 3
    retry_delay_seconds: ClassVar[int] = 60

    @classmethod
    def get_scheduled_attrs(cls) -> Dict[str, Any]:
        """Get all scheduled event attributes."""
        return getattr(cls, '_scheduled_attrs', {}).copy()

    @classmethod
    def get_name(cls) -> str:
        """Get the event name."""
        scheduled_attrs = getattr(cls, '_scheduled_attrs', {})
        return scheduled_attrs.get("name", f"{cls.entity_type}.{cls.action}")

    @classmethod
    def get_entity_type(cls) -> str:
        """Get the entity type."""
        scheduled_attrs = getattr(cls, '_scheduled_attrs', {})
        return scheduled_attrs.get("entity_type", "")

    @classmethod
    def get_action(cls) -> str:
        """Get the action."""
        scheduled_attrs = getattr(cls, '_scheduled_attrs', {})
        return scheduled_attrs.get("action", "")

    @classmethod
    def get_cron_expression(cls) -> str:
        """Get the cron expression."""
        scheduled_attrs = getattr(cls, '_scheduled_attrs', {})
        return scheduled_attrs.get("cron_expression", "")

    @classmethod
    def get_description(cls) -> Optional[str]:
        """Get the description."""
        scheduled_attrs = getattr(cls, '_scheduled_attrs', {})
        return scheduled_attrs.get("description")

    @classmethod
    def get_payload(cls) -> Dict[str, Any]:
        """Get the default payload."""
        scheduled_attrs = getattr(cls, '_scheduled_attrs', {})
        return scheduled_attrs.get("payload", {})

    @classmethod
    def get_module_name(cls) -> str:
        """Get the module name where this event is defined."""
        return cls.__module__.split('.')[0] if '.' in cls.__module__ else cls.__module__


# List to track all scheduled event definitions
ALL_SCHEDULED_EVENTS: list[Type[ScheduledEventDefinition]] = []


def get_all_scheduled_events() -> list[Type[ScheduledEventDefinition]]:
    """Get all registered scheduled event definitions."""
    return ALL_SCHEDULED_EVENTS.copy()


def register_scheduled_event(event_class: Type[ScheduledEventDefinition]) -> None:
    """Register a scheduled event definition."""
    if event_class not in ALL_SCHEDULED_EVENTS:
        ALL_SCHEDULED_EVENTS.append(event_class)
        logger.info(f"Registered scheduled event: {event_class.__name__}")
