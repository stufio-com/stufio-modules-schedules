from typing import List, Optional, Dict, Any
from datetime import datetime
from stufio.crud.mongo_base import CRUDMongo
from ..models.scheduled_event_definition import ScheduledEventDefinition, ScheduledEventDefinitionStatus
from ..schemas.scheduled_event_definition import (
    ScheduledEventDefinitionCreate,
    ScheduledEventDefinitionUpdate
)


class CRUDScheduledEventDefinition(CRUDMongo[ScheduledEventDefinition, ScheduledEventDefinitionCreate, ScheduledEventDefinitionUpdate]):
    """CRUD operations for scheduled event definitions."""

    async def get_by_class_name(self, event_class_name: str) -> Optional[ScheduledEventDefinition]:
        """Get a scheduled event definition by class name."""
        return await self.get_by_field("event_class_name", event_class_name)

    async def get_by_event_name(self, event_name: str) -> Optional[ScheduledEventDefinition]:
        """Get a scheduled event definition by event name."""
        return await self.get_by_field("event_name", event_name)

    async def get_active_definitions(self) -> List[ScheduledEventDefinition]:
        """Get all active scheduled event definitions."""
        return await self.get_multi(filters={"status": ScheduledEventDefinitionStatus.ACTIVE})

    async def get_by_module(self, module_name: str) -> List[ScheduledEventDefinition]:
        """Get scheduled event definitions by module."""
        return await self.get_multi(filters={"module_name": module_name})

    async def get_definitions_due_for_execution(self, before: datetime) -> List[ScheduledEventDefinition]:
        """Get definitions that are due for execution."""
        # Use filters dict instead of filter_expression to handle None comparison properly
        filters = {
            "status": ScheduledEventDefinitionStatus.ACTIVE,
            "next_execution_at": {"$ne": None, "$lte": before}
        }
        return await self.get_multi(filters=filters)

    async def upsert_from_class(
        self,
        event_class,
        module_name: str
    ) -> ScheduledEventDefinition:
        """Create or update a scheduled event definition from a class definition."""
        from ..schemas.scheduled_event_definition import ScheduledEventDefinition as ScheduledEventDefSchema

        # Get attributes from the class
        attrs = event_class.get_scheduled_attrs()

        # Prepare data for upsert
        definition_data = {
            "event_class_name": event_class.__name__,
            "event_name": attrs.get("name", f"{attrs.get('entity_type', 'unknown')}.{attrs.get('action', 'unknown')}"),
            "entity_type": attrs.get("entity_type"),
            "action": attrs.get("action"),
            "cron_expression": attrs.get("cron_expression"),
            "timezone": attrs.get("timezone", "UTC"),
            "description": attrs.get("description"),
            "payload": attrs.get("payload", {}),
            "actor_type": attrs.get("actor_type", "system"),
            "actor_id": attrs.get("actor_id", "scheduler"),
            "module_name": module_name,
            "max_retries": attrs.get("max_retries", 3),
            "retry_delay_seconds": attrs.get("retry_delay_seconds", 60),
            "last_sync_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

        # Check if it exists
        existing = await self.get_by_class_name(event_class.__name__)

        if existing:
            # Update only system fields, preserve manual overrides
            update_data = {
                "entity_type": definition_data["entity_type"],
                "action": definition_data["action"],
                "description": definition_data["description"],
                "module_name": definition_data["module_name"],
                "last_sync_at": definition_data["last_sync_at"],
                "updated_at": definition_data["updated_at"],
            }

            # Only update these if there are no manual overrides
            if not existing.manual_cron_override:
                update_data["cron_expression"] = definition_data["cron_expression"]
            if not existing.manual_payload_override:
                update_data["payload"] = definition_data["payload"]
            if not existing.manual_status_override:
                update_data["status"] = ScheduledEventDefinitionStatus.ACTIVE

            # Fetch the ScheduledEventDefinition instance by id
            db_obj = await self.get(existing.id)
            if db_obj is not None:
                update_schema = ScheduledEventDefinitionUpdate(**update_data)
                return await self.update(db_obj, update_schema.model_dump(exclude_unset=True))
        
        # Create new
        create_schema = ScheduledEventDefinitionCreate(**definition_data)
        return await self.create(create_schema)

    async def update_execution_info(
        self,
        definition_id: str,
        last_execution_at: datetime,
        next_execution_at: Optional[datetime] = None,
        success: bool = True,
        error: Optional[str] = None
    ) -> Optional[ScheduledEventDefinition]:
        """Update execution information for a definition."""
        # Fetch the ScheduledEventDefinition instance by id first
        db_obj = await self.get(definition_id)
        if db_obj is None:
            return None

        update_data = {
            "last_execution_at": last_execution_at,
            "execution_count": db_obj.execution_count + 1,
            "updated_at": datetime.utcnow(),
        }

        if next_execution_at:
            update_data["next_execution_at"] = next_execution_at

        if success:
            update_data["last_error"] = None
        else:
            update_data["error_count"] = db_obj.error_count + 1
            update_data["last_error"] = error

        update_schema = ScheduledEventDefinitionUpdate(**update_data)
        return await self.update(db_obj, update_schema.model_dump(exclude_unset=True))

    async def sync_definitions_from_classes(
        self,
        event_classes: List,
        module_name: str
    ) -> List[ScheduledEventDefinition]:
        """Sync multiple event definitions from classes."""
        results = []
        for event_class in event_classes:
            try:
                definition = await self.upsert_from_class(event_class, module_name)
                results.append(definition)
            except Exception as e:
                # Log error but continue with other definitions
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error syncing scheduled event definition {event_class.__name__}: {e}")

        return results


# Create instance
crud_scheduled_event_definition = CRUDScheduledEventDefinition(ScheduledEventDefinition)
