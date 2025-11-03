from datetime import datetime
from typing import Dict, Optional, Any
from uuid import uuid4
from odmantic import Field, Index
from stufio.db.mongo_base import MongoBase, datetime_now_sec
from enum import Enum


class ScheduledEventDefinitionStatus(str, Enum):
    """Status of a scheduled event definition."""
    ACTIVE = "active"
    DISABLED = "disabled"
    DRAFT = "draft"


class ScheduledEventDefinition(MongoBase):
    """MongoDB model for storing scheduled event definitions that are auto-registered on startup."""

    # Event identification (derived from EventDefinition class)
    event_class_name: str = Field(index=True, unique=True)  # e.g., "FavoriteCleanScheduledEvent"
    event_name: str = Field(index=True)  # e.g., "favorite.cleanup"
    entity_type: str  # e.g., "favorite"
    action: str  # e.g., "cleanup"
    
    # Schedule configuration
    cron_expression: str  # e.g., "0 0 * * *"
    timezone: str = "UTC"
    
    # Event details
    description: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)  # Default payload
    actor_type: str = "system"
    actor_id: str = "scheduler"
    
    # System fields
    status: ScheduledEventDefinitionStatus = ScheduledEventDefinitionStatus.ACTIVE
    module_name: str  # Which module this event belongs to
    
    # Execution configuration
    max_retries: int = 3
    retry_delay_seconds: int = 60
    
    # Analytics and monitoring
    last_sync_at: Optional[datetime] = None  # Last time this was synced from code
    last_execution_at: Optional[datetime] = None
    next_execution_at: Optional[datetime] = None
    execution_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime_now_sec)
    updated_at: datetime = Field(default_factory=datetime_now_sec)
    
    # Manual overrides (can be set by admins)
    manual_payload_override: Optional[Dict[str, Any]] = None
    manual_cron_override: Optional[str] = None
    manual_status_override: Optional[ScheduledEventDefinitionStatus] = None

    model_config = {
        "collection": "scheduled_event_definitions",
        "indexes": lambda: [
            Index("event_class_name", unique=True),
            Index("event_name", unique=True),
            Index("entity_type", "action"),
            Index("module_name"),
            Index("status", "next_execution_at"),
        ],
    }
