from fastapi import Header, HTTPException

from app.authorization.access import get_role_codes_safe, is_staff_role
from app.core.local_auth import get_user_id_from_bearer


async def require_authenticated_user_id(authorization: str | None = Header(default=None)) -> str:
    return await get_user_id_from_bearer(authorization)


async def require_staff_user_id(authorization: str | None = Header(default=None)) -> str:
    user_id = await get_user_id_from_bearer(authorization)
    role_codes = await get_role_codes_safe(user_id)
    if not is_staff_role(role_codes):
        raise HTTPException(status_code=403, detail="Forbidden")
    return user_id
