from uuid import UUID

from fastapi import APIRouter, Depends

from app.applications.service import (
    admin_apply_athlete_to_category as admin_apply_athlete_to_category_service,
    admin_create_athlete_and_application as admin_create_athlete_and_application_service,
    admin_update_athlete_profile as admin_update_athlete_profile_service,
)
from app.authorization import require_staff_user_id
from app.schemas.competition import (
    AdminApplyAthleteToCategory,
    AdminCreateAthleteApplication,
    AdminUpdateAthleteProfile,
)


router = APIRouter(prefix="/applications", tags=["applications"])


@router.post("/admin-create")
async def admin_create_athlete_and_application(
    body: AdminCreateAthleteApplication,
    _: str = Depends(require_staff_user_id),
):
    return await admin_create_athlete_and_application_service(body)


@router.post("/admin-apply")
async def admin_apply_athlete_to_category(
    body: AdminApplyAthleteToCategory,
    _: str = Depends(require_staff_user_id),
):
    return await admin_apply_athlete_to_category_service(body)


@router.put("/{app_id}/athlete-profile")
async def admin_update_athlete_profile(
    app_id: UUID,
    body: AdminUpdateAthleteProfile,
    _: str = Depends(require_staff_user_id),
):
    return await admin_update_athlete_profile_service(app_id, body)
