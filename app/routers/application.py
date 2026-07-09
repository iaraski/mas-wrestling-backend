from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from app.applications.dependencies import (
    resolve_application_details_write_access,
    resolve_create_my_application_user_id,
)
from app.applications.service import (
    create_application as create_application_service,
    create_my_application as create_my_application_service,
    get_application_details as get_application_details_service,
    list_applications as list_applications_service,
)
from app.schemas.competition import Application, ApplicationCreate

router = APIRouter(prefix="/applications", tags=["applications"])

@router.get("/", response_model=List[Application])
async def get_applications(competition_id: Optional[UUID] = None):
    try:
        return await list_applications_service(competition_id)
    except Exception as e:
        import traceback

        print(f"[Applications] CRITICAL ERROR: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{app_id}/")
@router.get("/{app_id}")
async def get_application_details(
    app_id: UUID,
    write: bool = Depends(resolve_application_details_write_access),
):
    try:
        return await get_application_details_service(app_id, write=write)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"[Applications] Error fetching details: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", response_model=Application)
async def create_application(app_in: ApplicationCreate):
    return await create_application_service(app_in)

@router.post("/me")
async def create_my_application(
    category_id: str,
    resolved_user_id: str = Depends(resolve_create_my_application_user_id),
):
    return await create_my_application_service(category_id, resolved_user_id)


