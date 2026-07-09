from fastapi import APIRouter, HTTPException, Header, Request, Response
from pydantic import BaseModel
import hashlib
import time
from sqlalchemy import select as _select

from app.core.local_auth import (
    access_cookie_max_age,
    auth_access_cookie_name,
    auth_csrf_cookie_name,
    auth_cookie_domain,
    auth_cookie_path,
    auth_cookie_samesite,
    auth_cookie_secure,
    auth_refresh_cookie_name,
    new_csrf_token,
    issue_access_token,
    issue_refresh_token,
    refresh_cookie_max_age,
    revoke_refresh_token,
    store_refresh_token,
    verify_session_access_token,
    verify_user_password,
    ensure_refresh_token_active,
    extract_token_from_authorization,
)
from app.competitions import get_allowed_competition_scales, get_user_competition_access_context
from app.core.db import SessionLocal, tables
from app.core.rest import rest_get
from app.core.roles import get_role_codes

router = APIRouter(prefix="/auth", tags=["auth"])

_me_cache: dict[str, tuple[float, dict]] = {}


def _norm_email(email: str) -> str:
    return email.strip().lower()


def _set_auth_cookies(response: Response, *, access_token: str, refresh_token: str, csrf_token: str) -> None:
    cookie_kwargs = {
        "httponly": True,
        "secure": auth_cookie_secure(),
        "samesite": auth_cookie_samesite(),
        "path": auth_cookie_path(),
        "domain": auth_cookie_domain(),
    }
    response.set_cookie(
        key=auth_access_cookie_name(),
        value=access_token,
        max_age=access_cookie_max_age(),
        **cookie_kwargs,
    )
    response.set_cookie(
        key=auth_refresh_cookie_name(),
        value=refresh_token,
        max_age=refresh_cookie_max_age(),
        **cookie_kwargs,
    )
    response.set_cookie(
        key=auth_csrf_cookie_name(),
        value=csrf_token,
        max_age=refresh_cookie_max_age(),
        httponly=False,
        secure=auth_cookie_secure(),
        samesite=auth_cookie_samesite(),
        path=auth_cookie_path(),
        domain=auth_cookie_domain(),
    )


def _clear_auth_cookies(response: Response) -> None:
    cookie_kwargs = {
        "path": auth_cookie_path(),
        "domain": auth_cookie_domain(),
    }
    response.delete_cookie(key=auth_access_cookie_name(), **cookie_kwargs)
    response.delete_cookie(key=auth_refresh_cookie_name(), **cookie_kwargs)
    response.delete_cookie(key=auth_csrf_cookie_name(), **cookie_kwargs)


@router.get("/me")
async def get_me(authorization: str | None = Header(default=None)):
    token = extract_token_from_authorization(authorization)

    cache_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
    cached = _me_cache.get(cache_key)
    if cached and cached[0] > time.time():
        return cached[1]

    payload = verify_session_access_token(authorization)
    user_id = str(payload.get("sub") or "")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    async with SessionLocal() as session:
        email = str(payload.get("email") or "").strip() or None
        if not email:
            users_t = tables.get("users")
            if users_t is not None:
                user_row = (
                    await session.execute(
                        _select(users_t.c.email).where(users_t.c.id == user_id).limit(1)
                    )
                ).mappings().first()
                email = str(user_row.get("email")) if user_row and user_row.get("email") else None

        role_codes = await get_role_codes(user_id, session=session)
        is_admin = any(
            c in ["admin", "founder", "country_admin", "region_admin", "country_secretary", "region_secretary"]
            for c in role_codes
        )
        is_secretary = any(c in ["secretary", "country_secretary", "region_secretary"] for c in role_codes)

        primary_role = "athlete"
        if is_admin:
            primary_role = "admin"
        elif is_secretary:
            primary_role = "secretary"

        competition_ctx = await get_user_competition_access_context(user_id, role_codes=role_codes, session=session)
    result = {
        "user_id": user_id,
        "email": email,
        "role_codes": role_codes,
        "role": primary_role,
        "competition_scope": competition_ctx.scope_kind,
        "staff_location_id": competition_ctx.staff_location_id,
        "staff_location_path": competition_ctx.staff_location_path,
        "profile_location_id": competition_ctx.profile_location_id,
        "profile_location_path": competition_ctx.profile_location_path,
        "allowed_competition_scales": get_allowed_competition_scales(competition_ctx),
    }
    _me_cache[cache_key] = (time.time() + 300.0, result)
    return result


class LoginBody(BaseModel):
    email: str
    password: str


@router.post("/login")
async def login(body: LoginBody, response: Response):
    email = _norm_email(body.email)
    password = str(body.password or "")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 8 символов")

    async with SessionLocal() as session:
        users_t = tables.get("users")
        if users_t is None:
            raise HTTPException(status_code=500, detail="Users table is not initialized")

        row = (
            await session.execute(
                _select(users_t.c.id, users_t.c.email).where(users_t.c.email == email).limit(1)
            )
        ).mappings().first()
        if row is None or not row.get("id"):
            raise HTTPException(status_code=401, detail="Invalid login credentials")

        user_id = str(row["id"])
        resolved_email = str(row.get("email") or email)
        migrated = False
        ok_local = await verify_user_password(user_id, password, session=session)
        if not ok_local:
            raise HTTPException(status_code=401, detail="Invalid login credentials")

        role_codes = await get_role_codes(user_id, session=session)
        is_admin = any(c in ["admin", "founder", "country_admin", "region_admin", "country_secretary", "region_secretary"] for c in role_codes)
        is_secretary = any(c in ["secretary", "country_secretary", "region_secretary"] for c in role_codes)
        primary_role = "athlete"
        if is_admin:
            primary_role = "admin"
        elif is_secretary:
            primary_role = "secretary"

        competition_ctx = await get_user_competition_access_context(user_id, role_codes=role_codes, session=session)
    access_token = issue_access_token(user_id=user_id, email=resolved_email)
    refresh_token = issue_refresh_token(user_id=user_id, email=resolved_email)
    await store_refresh_token(user_id=user_id, refresh_token=refresh_token)
    _set_auth_cookies(
        response,
        access_token=access_token,
        refresh_token=refresh_token,
        csrf_token=new_csrf_token(),
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user_id": user_id,
        "email": resolved_email,
        "role_codes": role_codes,
        "role": primary_role,
        "competition_scope": competition_ctx.scope_kind,
        "staff_location_id": competition_ctx.staff_location_id,
        "staff_location_path": competition_ctx.staff_location_path,
        "profile_location_id": competition_ctx.profile_location_id,
        "profile_location_path": competition_ctx.profile_location_path,
        "allowed_competition_scales": get_allowed_competition_scales(competition_ctx),
        "migrated": migrated,
    }


@router.post("/refresh")
async def refresh_session(request: Request, response: Response):
    refresh_token = request.cookies.get(auth_refresh_cookie_name())
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Missing refresh token")

    payload = await ensure_refresh_token_active(refresh_token)
    user_id = str(payload.get("sub") or "")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    resp = await rest_get("users", {"select": "id,email", "id": f"eq.{user_id}", "limit": "1"}, write=True)
    rows = resp.json()
    user_row = rows[0] if isinstance(rows, list) and rows else {}
    email = user_row.get("email") if isinstance(user_row, dict) else payload.get("email")

    access_token = issue_access_token(user_id=user_id, email=email)
    new_refresh_token = issue_refresh_token(user_id=user_id, email=email)
    await store_refresh_token(user_id=user_id, refresh_token=new_refresh_token)
    await revoke_refresh_token(refresh_token, replaced_by_token=new_refresh_token)
    _set_auth_cookies(
        response,
        access_token=access_token,
        refresh_token=new_refresh_token,
        csrf_token=new_csrf_token(),
    )
    return {"ok": True}


@router.post("/logout")
async def logout(request: Request, response: Response):
    refresh_token = request.cookies.get(auth_refresh_cookie_name())
    if refresh_token:
        await revoke_refresh_token(refresh_token)
    _clear_auth_cookies(response)
    return {"ok": True}

@router.post("/debug/clear-cache")
async def debug_clear_cache():
    _me_cache.clear()
    return {"ok": True}
