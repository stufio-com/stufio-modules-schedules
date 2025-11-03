from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from uuid import UUID
from nameniac_fastapi.app import crud
from stufio.crud.mongo_base import CRUDMongo
from ..models.mongo_schedule import MongoSchedule, MongoScheduleExecution, ScheduleStatus, ExecutionStatus
from ..schemas.mongo_schedule import (
    MongoScheduleCreate, 
    MongoScheduleUpdate,
    MongoScheduleExecutionCreate,
    MongoScheduleExecutionUpdate
)


class CRUDMongoSchedule(CRUDMongo[MongoSchedule, MongoScheduleCreate, MongoScheduleUpdate]):
    """CRUD operations for MongoDB schedules."""

    async def get_by_name(self, name: str) -> Optional[MongoSchedule]:
        """Get a schedule by name."""
        return await self.model.find_one(self.model.name == name)

    async def get_active_schedules(self) -> List[MongoSchedule]:
        """Get all active schedules."""
        return await self.model.find(self.model.status == ScheduleStatus.ACTIVE).to_list()

    async def get_schedules_by_status(self, status: ScheduleStatus) -> List[MongoSchedule]:
        """Get schedules by status."""
        return await self.model.find(self.model.status == status).to_list()

    async def get_schedules_by_event_type(self, event_type: str, event_action: Optional[str] = None) -> List[MongoSchedule]:
        """Get schedules by event type and optionally action."""
        query = self.model.event_type == event_type
        if event_action:
            query = query & (self.model.event_action == event_action)
        return await self.model.find(query).to_list()

    async def get_schedules_due_for_execution(self, before: datetime) -> List[MongoSchedule]:
        """Get schedules that are due for execution."""
        # Use execute_query for complex queries with null checking
        schedules = await self.execute_query(
            lambda collection: collection.find({
                "status": ScheduleStatus.ACTIVE.value,
                "next_execution": {"$ne": None, "$lte": before}
            }).to_list(None)
        )
        # Convert to model instances
        return [MongoSchedule.model_validate(doc) for doc in schedules]

    async def update_execution_info(
        self,
        schedule_id: str,
        last_execution: datetime,
        next_execution: Optional[datetime],
        status: Optional[str] = None,
        error: Optional[str] = None
    ) -> Optional[MongoSchedule]:
        """Update execution information for a schedule."""
        update_data: Dict[str, Any] = {
            "last_execution": last_execution,
            "updated_at": datetime.utcnow(),
            "execution_count": self.model.execution_count + 1
        }
        
        if next_execution:
            update_data["next_execution"] = next_execution
        if status:
            update_data["last_status"] = status
        if error:
            update_data["last_error"] = error
            update_data["error_count"] = self.model.error_count + 1

        db_obj = await self.get(schedule_id)
        if not db_obj:
            return None
        
        return await self.update(db_obj, update_data)

    async def get_schedules_by_tags(self, tags: List[str]) -> List[MongoSchedule]:
        """Get schedules that have any of the specified tags."""
        # Use execute_query for $in operator on array fields
        schedules = await self.execute_query(
            lambda collection: collection.find({"tags": {"$in": tags}}).to_list(None)
        )
        # Convert to model instances
        return [MongoSchedule.model_validate(doc) for doc in schedules]

    async def record_execution(
        self,
        schedule_id: str,
        execution_time: datetime,
        success: bool = True,
        clickhouse_event_id: Optional[str] = None,
        error: Optional[str] = None
    ) -> None:
        """Record a schedule execution."""
        execution_data = MongoScheduleExecutionCreate(
            schedule_id=schedule_id,
            schedule_name="",  # Will be filled by the execution model
            status=ExecutionStatus.SUCCESS if success else ExecutionStatus.FAILURE,
            clickhouse_schedule_id=clickhouse_event_id,
            error=error,
            execution_time=execution_time
        )
        
        # Create execution record
        await CRUDMongoScheduleExecution(MongoScheduleExecution).create(execution_data)
        
        # Update schedule statistics
        schedule = await self.get(schedule_id)
        if schedule:
            update_data = {
                "last_execution": execution_time,
                "execution_count": schedule.execution_count + 1,
                "updated_at": datetime.utcnow()
            }
            
            if not success:
                update_data["error_count"] = schedule.error_count + 1
                update_data["last_error"] = error
                update_data["last_status"] = "failed"
            else:
                update_data["last_status"] = "success"
                
            await self.update(schedule, update_data)

    async def update_next_execution(
        self,
        schedule_id: str,
        next_execution: datetime
    ) -> Optional[MongoSchedule]:
        """Update the next execution time for a schedule."""
        schedule = await self.get(schedule_id)
        if not schedule:
            return None
            
        return await self.update(schedule, {
            "next_execution": next_execution,
            "updated_at": datetime.utcnow()
        })

    async def update_status(self, schedule_id: Union[str, UUID], status: ScheduleStatus) -> Optional[MongoSchedule]:
        """Update the status of a schedule."""
        db_obj = await self.get(str(schedule_id))
        if not db_obj:
            return None
            
        return await self.update(db_obj, {"status": status, "updated_at": datetime.utcnow()})
        
    async def bulk_update_status(self, schedule_ids: List[Union[str, UUID]], status: ScheduleStatus) -> Dict[str, Any]:
        """Update the status of multiple schedules."""
        str_ids = [str(id) for id in schedule_ids]
        
        result = await self.execute_query(
            lambda collection: collection.update_many(
                {"_id": {"$in": str_ids}},
                {"$set": {"status": status.value, "updated_at": datetime.utcnow()}}
            )
        )
        
        return {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count
        }

    async def get_schedule_stats(self, schedule_id: UUID) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific schedule."""
        schedule_id_str = str(schedule_id)
        
        # Get execution statistics using execute_query for aggregation operations
        total_executions = await self.execute_query(
            lambda collection: collection.count_documents(
                {"schedule_id": schedule_id_str}
            )
        )
        
        successful_executions = await self.execute_query(
            lambda collection: collection.count_documents(
                {"schedule_id": schedule_id_str, "status": "success"}
            )
        )
        
        failed_executions = await self.execute_query(
            lambda collection: collection.count_documents(
                {"schedule_id": schedule_id_str, "status": "failure"}
            )
        )
        
        # Get recent executions for timing stats
        recent_executions = await self.get_multi(
            filter_expression=self.model.schedule_id == schedule_id_str,
            sort=[("execution_time", -1)],
            limit=100
        )
        
        # Calculate averages
        avg_duration = 0
        if recent_executions:
            durations = [ex.duration_ms for ex in recent_executions if ex.duration_ms]
            if durations:
                avg_duration = sum(durations) / len(durations)
        
        # Get last execution
        last_execution = None
        if recent_executions:
            last_execution = recent_executions[0]
        
        return {
            "schedule_id": schedule_id_str,
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "success_rate": successful_executions / total_executions if total_executions > 0 else 0,
            "avg_duration_ms": avg_duration,
            "last_execution": {
                "execution_time": last_execution.execution_time,
                "status": last_execution.status,
                "duration_ms": last_execution.duration_ms
            } if last_execution else None
        }

    async def get_recent_executions(self, schedule_id: Union[str, UUID], limit: int = 100) -> List[MongoScheduleExecution]:
        """Get recent executions for a schedule."""
        execution_crud = CRUDMongoScheduleExecution(MongoScheduleExecution)
        # Convert to UUID if it's a string
        if isinstance(schedule_id, str):
            from uuid import UUID
            schedule_id = UUID(schedule_id)
        return await execution_crud.get_recent_executions_for_schedule(schedule_id, limit)


class CRUDMongoScheduleExecution(CRUDMongo[MongoScheduleExecution, MongoScheduleExecutionCreate, MongoScheduleExecutionUpdate]):
    """CRUD operations for MongoDB schedule executions."""

    async def get_executions_by_schedule(
        self,
        schedule_id: str,
        limit: int = 100,
        skip: int = 0
    ) -> List[MongoScheduleExecution]:
        """Get executions for a specific schedule."""
        return await self.get_multi(
            filter_expression=self.model.schedule_id == schedule_id,
            sort=[("execution_time", -1)],
            skip=skip,
            limit=limit
        )

    async def get_recent_executions(
        self,
        since: datetime,
        limit: int = 100
    ) -> List[MongoScheduleExecution]:
        """Get recent executions since a specific time."""
        return await self.get_multi(
            filter_expression=self.model.execution_time >= since,
            sort=[("execution_time", -1)],
            limit=limit
        )

    async def get_failed_executions(
        self,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[MongoScheduleExecution]:
        """Get failed executions."""
        filter_expr = self.model.status == "failure"
        if since:
            filter_expr = filter_expr & (self.model.execution_time >= since)
        
        return await self.get_multi(
            filter_expression=filter_expr,
            sort=[("execution_time", -1)],
            limit=limit
        )

    async def cleanup_old_executions(self, before: datetime) -> int:
        """Delete execution records older than the specified date."""
        # Use the execute_query method to access raw MongoDB operations
        result = await self.execute_query(
            lambda collection: collection.delete_many(
                {"execution_time": {"$lt": before}}
            )
        )
        return result.deleted_count

    async def get_recent_executions_for_schedule(
        self,
        schedule_id: UUID,
        limit: int = 100
    ) -> List[MongoScheduleExecution]:
        """Get recent executions for a specific schedule."""
        return await self.get_multi(
            filter_expression=self.model.schedule_id == str(schedule_id),
            sort=[("execution_time", -1)],
            limit=limit
        )

    async def get_schedule_stats(self, schedule_id: UUID) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific schedule."""
        schedule_id_str = str(schedule_id)
        
        # Get execution statistics using execute_query for aggregation operations
        total_executions = await self.execute_query(
            lambda collection: collection.count_documents(
                {"schedule_id": schedule_id_str}
            )
        )
        
        successful_executions = await self.execute_query(
            lambda collection: collection.count_documents(
                {"schedule_id": schedule_id_str, "status": "success"}
            )
        )
        
        failed_executions = await self.execute_query(
            lambda collection: collection.count_documents(
                {"schedule_id": schedule_id_str, "status": "failure"}
            )
        )
        
        # Get recent executions for timing stats
        recent_executions = await self.get_multi(
            filter_expression=self.model.schedule_id == schedule_id_str,
            sort=[("execution_time", -1)],
            limit=100
        )
        
        # Calculate averages
        avg_duration = 0
        if recent_executions:
            durations = [ex.duration_ms for ex in recent_executions if ex.duration_ms]
            if durations:
                avg_duration = sum(durations) / len(durations)
        
        # Get last execution
        last_execution = None
        if recent_executions:
            last_execution = recent_executions[0]
        
        return {
            "schedule_id": schedule_id_str,
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "success_rate": successful_executions / total_executions if total_executions > 0 else 0,
            "avg_duration_ms": avg_duration,
            "last_execution": {
                "execution_time": last_execution.execution_time,
                "status": last_execution.status,
                "duration_ms": last_execution.duration_ms
            } if last_execution else None
        }

crud_mongo_schedule = CRUDMongoSchedule(MongoSchedule)