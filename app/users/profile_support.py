import hashlib
import time

import anyio
from fastapi import HTTPException

from app.authorization import get_role_codes_safe, is_staff_role
from app.core.local_auth import extract_token_from_authorization, get_user_id_from_auth as _local_get_user_id_from_auth
from app.core.rest import rest_get
from app.core.supabase import admin_supabase


_me_cache: dict[str, tuple[float, str]] = {}


def ensure_service_role_configured() -> None:
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")


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


async def is_profile_locked(user_id: str) -> bool:
    try:
        resp = await rest_get(
            "registrations",
            {"select": "stage", "user_id": f"eq.{user_id}", "limit": "1"},
            write=True,
        )
        rows = resp.json()
        row = rows[0] if isinstance(rows, list) and rows else {}
        return bool(isinstance(row, dict) and str(row.get("stage") or "") == "complete")
    except Exception:
        return False


async def require_can_edit_self(user_id: str) -> None:
    role_codes = await get_role_codes_safe(user_id)
    if is_staff_role(role_codes):
        return
    if await is_profile_locked(user_id):
        raise HTTPException(status_code=403, detail="Profile is locked")


async def execute_supabase(query, *, retries: int = 4):
    for attempt in range(retries + 1):
        try:
            if hasattr(query, "execute_async"):
                res = await query.execute_async()
            else:
                res = await anyio.to_thread.run_sync(query.execute)
            if res is None:
                raise HTTPException(status_code=503, detail="Supabase temporarily unavailable")
            if hasattr(res, "error") and getattr(res, "error"):
                raise HTTPException(status_code=503, detail=str(getattr(res, "error")))
            return res
        except HTTPException:
            raise
        except Exception as e:
            if e.__class__.__name__ == "APIError":
                msg = getattr(e, "message", None) or repr(e)
                code = getattr(e, "code", None)
                details = getattr(e, "details", None)
                hint = getattr(e, "hint", None)
                extra = []
                if code:
                    extra.append(f"code={code}")
                if details:
                    extra.append(f"details={details}")
                if hint:
                    extra.append(f"hint={hint}")
                tail = f" ({', '.join(extra)})" if extra else ""
                raise HTTPException(status_code=400, detail=f"Supabase API error: {msg}{tail}")
            if attempt >= retries:
                raise HTTPException(status_code=503, detail=f"Supabase temporarily unavailable: {type(e).__name__}")
            await anyio.sleep(0.35 * (attempt + 1))


def safe_supabase_data(res):
    if res is None:
        raise HTTPException(status_code=503, detail="Supabase request failed (no response)")
    if hasattr(res, "error") and getattr(res, "error"):
        raise HTTPException(status_code=503, detail=str(getattr(res, "error")))
    if not hasattr(res, "data"):
        raise HTTPException(status_code=503, detail="Supabase response missing data")
    return res.data


async def get_location_path_v2(location_id: str) -> dict:
    resp = await rest_get(
        "locations",
        {"select": "id,type,parent_id", "id": f"eq.{location_id}", "limit": "1"},
        write=False,
    )
    rows = resp.json()
    loc = rows[0] if isinstance(rows, list) and rows else None
    if not isinstance(loc, dict) or not loc.get("id"):
        return {"country_id": None, "district_id": None, "region_id": None}

    loc_type = str(loc.get("type") or "")
    region_id: str | None = None
    district_id: str | None = None
    country_id: str | None = None

    if loc_type == "region":
        region_id = str(loc.get("id"))
        parent_id = loc.get("parent_id")
        if parent_id:
            district_id = str(parent_id)
            d_resp = await rest_get(
                "locations",
                {"select": "id,parent_id", "id": f"eq.{district_id}", "limit": "1"},
                write=False,
            )
            d_rows = d_resp.json()
            dloc = d_rows[0] if isinstance(d_rows, list) and d_rows else None
            if isinstance(dloc, dict) and dloc.get("parent_id"):
                country_id = str(dloc["parent_id"])
    elif loc_type == "district":
        district_id = str(loc.get("id"))
        parent_id = loc.get("parent_id")
        if parent_id:
            country_id = str(parent_id)
    elif loc_type == "country":
        country_id = str(loc.get("id"))

    return {"country_id": country_id, "district_id": district_id, "region_id": region_id}
