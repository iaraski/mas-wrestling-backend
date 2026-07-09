import hashlib
import time

from fastapi import Header, HTTPException

from app.authorization import get_role_codes_safe, is_staff_role
from app.core.local_auth import extract_token_from_authorization, get_user_id_from_auth as _local_get_user_id_from_auth


_me_cache: dict[str, tuple[float, str]] = {}


async def get_cached_user_id_from_auth(authorization: str | None) -> str:
    token = extract_token_from_authorization(authorization)

    cache_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
    cached = _me_cache.get(cache_key)
    if cached and cached[0] > time.time():
        return cached[1]

    user_id = await _local_get_user_id_from_auth(authorization)
    _me_cache[cache_key] = (time.time() + 30.0, user_id)
    return user_id


async def get_cached_user_id_from_bearer(authorization: str | None) -> str:
    return await get_cached_user_id_from_auth(authorization)


async def resolve_application_details_write_access(authorization: str | None = Header(default=None)) -> bool:
    if not authorization:
        return False
    try:
        requester_id = await get_cached_user_id_from_auth(authorization)
        codes = await get_role_codes_safe(requester_id)
        return is_staff_role(codes)
    except Exception:
        return False


async def resolve_create_my_application_user_id(
    authorization: str | None = Header(default=None),
    user_id: str | None = None,
) -> str:
    try:
        from app.core.ratelimit import allow as rl_allow

        uid_for_rl = user_id or "anon"
        if not rl_allow(f"apply:{uid_for_rl}", rate_per_minute=15.0, burst=30.0):
            raise HTTPException(status_code=429, detail="Too many application requests, please try again shortly")
    except HTTPException:
        raise
    except Exception:
        pass

    if authorization:
        user_id = await get_cached_user_id_from_auth(authorization)
    if not user_id:
        raise HTTPException(status_code=401, detail="Missing authentication")
    return user_id
