from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
import hashlib
import time

from app.core.local_auth import issue_access_token, verify_access_token, verify_user_password
from app.core.rest import rest_get
from app.core.roles import get_role_codes

router = APIRouter(prefix="/auth", tags=["auth"])

_me_cache: dict[str, tuple[float, dict]] = {}


def _norm_email(email: str) -> str:
    return email.strip().lower()


@router.get("/me")
async def get_me(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    cache_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
    cached = _me_cache.get(cache_key)
    if cached and cached[0] > time.time():
        return cached[1]

    payload = verify_access_token(token)
    user_id = str(payload.get("sub") or "")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    resp = await rest_get("users", {"select": "id,email", "id": f"eq.{user_id}", "limit": "1"}, write=True)
    rows = resp.json()
    user_row = rows[0] if isinstance(rows, list) and rows else {}
    email = user_row.get("email") if isinstance(user_row, dict) else payload.get("email")

    role_codes = await get_role_codes(user_id)
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

    result = {"user_id": user_id, "email": email, "role_codes": role_codes, "role": primary_role}
    _me_cache[cache_key] = (time.time() + 300.0, result)
    return result


class LoginBody(BaseModel):
    email: str
    password: str


@router.post("/login")
async def login(body: LoginBody):
    email = _norm_email(body.email)
    password = str(body.password or "")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 8 символов")

    resp = await rest_get(
        "users",
        {"select": "id,email", "email": f"eq.{email}", "limit": "1"},
        write=True,
    )
    rows = resp.json()
    row = rows[0] if isinstance(rows, list) and rows else None
    if not isinstance(row, dict) or not row.get("id"):
        raise HTTPException(status_code=401, detail="Invalid login credentials")

    user_id = str(row["id"])

    migrated = False
    ok_local = await verify_user_password(user_id, password)
    if not ok_local:
        raise HTTPException(status_code=401, detail="Invalid login credentials")

    role_codes = await get_role_codes(user_id)
    is_admin = any(c in ["admin", "founder", "country_admin", "region_admin", "country_secretary", "region_secretary"] for c in role_codes)
    is_secretary = any(c in ["secretary", "country_secretary", "region_secretary"] for c in role_codes)
    primary_role = "athlete"
    if is_admin:
        primary_role = "admin"
    elif is_secretary:
        primary_role = "secretary"

    access_token = issue_access_token(user_id=user_id, email=email)
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user_id": user_id,
        "email": email,
        "role_codes": role_codes,
        "role": primary_role,
        "migrated": migrated,
    }

@router.post("/debug/clear-cache")
async def debug_clear_cache():
    _me_cache.clear()
    return {"ok": True}
