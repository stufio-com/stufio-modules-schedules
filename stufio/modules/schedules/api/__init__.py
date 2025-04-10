from fastapi import APIRouter

from .schedules import router as schedules_router

router = APIRouter()

router.include_router(schedules_router, prefix="/schedules", tags=["schedules"])


__all__ = ["router"]