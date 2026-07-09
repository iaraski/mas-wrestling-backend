from uuid import UUID

from fastapi import APIRouter, Depends

from app.applications.service import (
    update_application_status as update_application_status_service,
    verify_passport as verify_passport_service,
)
from app.authorization import require_staff_user_id
from app.schemas.competition import Application, ApplicationUpdate, PassportVerifyUpdate


router = APIRouter(prefix="/applications", tags=["applications"])


@router.patch("/passport/{passport_id}/verify/")
@router.patch("/passport/{passport_id}/verify")
async def verify_passport(
    passport_id: UUID,
    payload: PassportVerifyUpdate,
    _: str = Depends(require_staff_user_id),
):
    return await verify_passport_service(passport_id, is_verified=payload.is_verified)


@router.patch("/{app_id}/", response_model=Application)
@router.patch("/{app_id}", response_model=Application)
async def update_application_status(
    app_id: UUID,
    app_update: ApplicationUpdate,
    _: str = Depends(require_staff_user_id),
):
    return await update_application_status_service(app_id, app_update)
