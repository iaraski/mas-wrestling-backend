from fastapi import APIRouter, HTTPException, Header, UploadFile, File, Form
from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID
from uuid import uuid4
import httpx
import os
import anyio
import hashlib
import time
from datetime import date
from app.core.supabase import supabase, admin_supabase, SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_ROLE_KEY
from app.core.rest import rest_upsert, rest_get, rest_delete, rest_patch, rest_post
from app.core.ratelimit import allow as rl_allow
from app.core.local_auth import get_user_id_from_bearer as _local_get_user_id_from_bearer
from app.schemas.user import Role, UserProfile, RoleAssign, AdminCreate, ProfileResponse, ProfileCreate, PassportResponse, PassportBase, AthleteResponse

router = APIRouter(prefix="/users", tags=["users"])

_me_cache: dict[str, tuple[float, str]] = {}


class AthleteDetailsUpdate(BaseModel):
    birth_date: Optional[date] = None
    rank: Optional[str] = None
    photo_url: Optional[str] = None
    gender: Optional[str] = None


class AdminAthleteUpdate(BaseModel):
    full_name: Optional[str] = None
    phone: Optional[str] = None
    city: Optional[str] = None
    location_id: Optional[UUID] = None
    coach_name: Optional[str] = None
    birth_date: Optional[date] = None
    gender: Optional[str] = None
    rank: Optional[str] = None
    photo_url: Optional[str] = None


class EditableUpdate(BaseModel):
    editable: bool


def _is_staff_role(codes: list[str]) -> bool:
    allowed = {
        "admin",
        "founder",
        "country_admin",
        "region_admin",
        "secretary",
        "country_secretary",
        "region_secretary",
    }
    return any(c in allowed for c in codes)


async def _get_my_role_codes(user_id: str) -> list[str]:
    if not admin_supabase:
        return []
    try:
        res = await _execute(
            admin_supabase.table("user_roles")
            .select("roles(code)")
            .eq("user_id", user_id)
        )
        data = _safe_data(res) or []
        out: list[str] = []
        for r in data:
            role = r.get("roles")
            if isinstance(role, list):
                role = role[0] if role else None
            code = role.get("code") if isinstance(role, dict) else None
            if code:
                out.append(str(code))
        return out
    except Exception:
        return []


async def _is_profile_locked(user_id: str) -> bool:
    if not admin_supabase:
        return False
    try:
        res = await _execute(
            admin_supabase.table("registrations")
            .select("stage")
            .eq("user_id", user_id)
            .maybe_single()
        )
        data = _safe_data(res)
        return bool(data and str(data.get("stage") or "") == "complete")
    except Exception:
        return False


async def _require_can_edit_self(user_id: str, authorization: str | None):
    role_codes = await _get_my_role_codes(user_id)
    if _is_staff_role(role_codes):
        return
    if await _is_profile_locked(user_id):
        raise HTTPException(status_code=403, detail="Profile is locked")


async def _require_staff(authorization: str | None) -> str:
    user_id = await _get_user_id_from_bearer(authorization)
    role_codes = await _get_my_role_codes(user_id)
    if not _is_staff_role(role_codes):
        raise HTTPException(status_code=403, detail="Forbidden")
    return user_id

async def _execute(query, *, retries: int = 4):
    for attempt in range(retries + 1):
        try:
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


def _chunk(items: list[str], size: int) -> list[list[str]]:
    out: list[list[str]] = []
    for i in range(0, len(items), size):
        out.append(items[i : i + size])
    return out


def _pg_in(ids: list[str]) -> str:
    return f"in.({','.join(ids)})"


async def _get_seed_user_ids(limit: int, *, email_ilike: str | None = None, include_null_email: bool = True) -> list[str]:
    filters: dict[str, str] = {
        "select": "id",
        "telegram_id": "is.null",
        "limit": str(limit),
    }
    if include_null_email and email_ilike:
        filters["or"] = f"(email.is.null,email.ilike.{email_ilike})"
    elif include_null_email:
        filters["email"] = "is.null"
    elif email_ilike:
        filters["email"] = f"ilike.{email_ilike}"
    else:
        filters["email"] = "is.null"
    resp = await rest_get(
        "users",
        filters,
        write=True,
    )
    rows = resp.json()
    if not isinstance(rows, list):
        return []
    ids: list[str] = []
    for r in rows:
        if isinstance(r, dict) and r.get("id"):
            ids.append(str(r["id"]))
    return ids


async def _filter_referenced_user_ids(candidate_ids: list[str]) -> set[str]:
    referenced: set[str] = set()
    if not candidate_ids:
        return referenced
    tables = [
        ("profiles", "user_id"),
        ("athletes", "user_id"),
        ("registrations", "user_id"),
        ("user_roles", "user_id"),
        ("staff_locations", "user_id"),
    ]
    for table, col in tables:
        for batch in _chunk(candidate_ids, 100):
            try:
                resp = await rest_get(
                    table,
                    {
                        "select": col,
                        col: _pg_in(batch),
                        "limit": "10000",
                    },
                    write=True,
                )
                rows = resp.json()
                if isinstance(rows, list):
                    for r in rows:
                        if isinstance(r, dict) and r.get(col):
                            referenced.add(str(r[col]))
            except Exception:
                continue
    return referenced


@router.get("/admin/seed-users")
async def list_seed_users(
    limit: int = 2000,
    email_ilike: str = "seed_%",
    include_null_email: bool = True,
    authorization: str | None = Header(default=None),
):
    await _require_staff(authorization)
    candidates = await _get_seed_user_ids(limit, email_ilike=email_ilike, include_null_email=include_null_email)
    referenced = await _filter_referenced_user_ids(candidates)
    deletable = [uid for uid in candidates if uid not in referenced]
    return {
        "candidate_count": len(candidates),
        "deletable_count": len(deletable),
        "deletable_sample": deletable[:50],
        "email_ilike": email_ilike,
        "include_null_email": include_null_email,
    }


@router.post("/admin/seed-users/cleanup")
async def cleanup_seed_users(
    dry_run: bool = True,
    limit: int = 2000,
    email_ilike: str = "seed_%",
    include_null_email: bool = True,
    authorization: str | None = Header(default=None),
):
    await _require_staff(authorization)
    candidates = await _get_seed_user_ids(limit, email_ilike=email_ilike, include_null_email=include_null_email)
    referenced = await _filter_referenced_user_ids(candidates)
    deletable = [uid for uid in candidates if uid not in referenced]
    if dry_run:
        return {
            "dry_run": True,
            "candidate_count": len(candidates),
            "deletable_count": len(deletable),
            "deletable_sample": deletable[:50],
            "email_ilike": email_ilike,
            "include_null_email": include_null_email,
        }
    deleted: list[str] = []
    for batch in _chunk(deletable, 100):
        resp = await rest_delete("users", {"id": _pg_in(batch)})
        if resp.status_code not in (200, 204):
            raise HTTPException(status_code=400, detail=f"Delete failed: {resp.status_code} {resp.text}")
        deleted.extend(batch)
    return {"dry_run": False, "deleted_count": len(deleted), "deleted_sample": deleted[:50]}


@router.post("/debug/seed-users/cleanup")
async def debug_cleanup_seed_users(
    dry_run: bool = True,
    limit: int = 2000,
    email_ilike: str = "seed_%",
    include_null_email: bool = True,
):
    if os.getenv("APP_DEBUG") != "1":
        raise HTTPException(status_code=404, detail="Not Found")
    candidates = await _get_seed_user_ids(limit, email_ilike=email_ilike, include_null_email=include_null_email)
    referenced = await _filter_referenced_user_ids(candidates)
    deletable = [uid for uid in candidates if uid not in referenced]
    if dry_run:
        return {
            "dry_run": True,
            "candidate_count": len(candidates),
            "deletable_count": len(deletable),
            "deletable_sample": deletable[:50],
            "email_ilike": email_ilike,
            "include_null_email": include_null_email,
        }
    deleted: list[str] = []
    for batch in _chunk(deletable, 100):
        resp = await rest_delete("users", {"id": _pg_in(batch)})
        if resp.status_code not in (200, 204):
            raise HTTPException(status_code=400, detail=f"Delete failed: {resp.status_code} {resp.text}")
        deleted.extend(batch)
    return {"dry_run": False, "deleted_count": len(deleted), "deleted_sample": deleted[:50]}

async def _rest_select_ids(table: str, id_col: str, where: dict[str, str]) -> list[str]:
    resp = await rest_get(
        table,
        {"select": id_col, **where, "limit": "10000"},
        write=True,
    )
    rows = resp.json()
    if not isinstance(rows, list):
        return []
    out: list[str] = []
    for r in rows:
        if isinstance(r, dict) and r.get(id_col):
            out.append(str(r[id_col]))
    return out


async def _rest_select_rows(table: str, select: str, where: dict[str, str]) -> list[dict]:
    resp = await rest_get(
        table,
        {"select": select, **where, "limit": "10000"},
        write=True,
    )
    rows = resp.json()
    return rows if isinstance(rows, list) else []


async def _rest_delete_in(table: str, col: str, ids: list[str], *, chunk_size: int = 100) -> int:
    if not ids:
        return 0
    deleted = 0
    for batch in _chunk(ids, chunk_size):
        resp = await rest_delete(table, {col: _pg_in(batch)})
        if resp.status_code not in (200, 204):
            raise HTTPException(status_code=400, detail=f"Delete {table} failed: {resp.status_code} {resp.text}")
        deleted += len(batch)
    return deleted


@router.post("/debug/seed-users/force-cleanup")
async def debug_force_cleanup_seed_users(
    dry_run: bool = True,
    limit: int = 5000,
    email_ilike: str = "seed_%@example.com",
    include_null_email: bool = False,
):
    if os.getenv("APP_DEBUG") != "1":
        raise HTTPException(status_code=404, detail="Not Found")

    user_ids = await _get_seed_user_ids(limit, email_ilike=email_ilike, include_null_email=include_null_email)
    if not user_ids:
        return {"ok": True, "deleted": False, "users": 0, "email_ilike": email_ilike}

    athlete_rows = await _rest_select_rows("athletes", "id,user_id", {"user_id": _pg_in(user_ids)})
    athlete_ids = [str(r["id"]) for r in athlete_rows if isinstance(r, dict) and r.get("id")]

    app_rows = []
    app_ids: list[str] = []
    if athlete_ids:
        app_rows = await _rest_select_rows("applications", "id,athlete_id", {"athlete_id": _pg_in(athlete_ids)})
        app_ids = [str(r["id"]) for r in app_rows if isinstance(r, dict) and r.get("id")]

    competition_bout_rows: list[dict] = []
    if athlete_ids:
        for batch in _chunk(athlete_ids, 40):
            where = {
                "or": f"(athlete_red_id.{_pg_in(batch)},athlete_blue_id.{_pg_in(batch)},winner_athlete_id.{_pg_in(batch)})"
            }
            competition_bout_rows.extend(
                await _rest_select_rows("competition_bouts", "id,competition_id", where)
            )
    comp_bout_ids = [str(r["id"]) for r in competition_bout_rows if isinstance(r, dict) and r.get("id")]
    comp_bout_ids = list(dict.fromkeys(comp_bout_ids))

    comp_to_bouts: dict[str, list[str]] = {}
    for r in competition_bout_rows:
        if not isinstance(r, dict):
            continue
        bid = r.get("id")
        cid = r.get("competition_id")
        if bid and cid:
            comp_to_bouts.setdefault(str(cid), []).append(str(bid))

    bout_ids: list[str] = []
    if app_ids:
        for batch in _chunk(app_ids, 60):
            where = {"or": f"(red_athlete_id.{_pg_in(batch)},blue_athlete_id.{_pg_in(batch)},winner_id.{_pg_in(batch)})"}
            bout_ids.extend(await _rest_select_ids("bouts", "id", where))
    bout_ids = list(dict.fromkeys(bout_ids))

    counts = {
        "users": len(user_ids),
        "athletes": len(athlete_ids),
        "applications": len(app_ids),
        "competition_bouts": len(comp_bout_ids),
        "bouts": len(bout_ids),
        "email_ilike": email_ilike,
        "include_null_email": include_null_email,
    }

    if dry_run:
        return {"ok": True, "deleted": False, **counts, "sample_user_ids": user_ids[:20]}

    # 1) Clear current bout pointers, then delete competition_bouts
    for comp_id, ids in comp_to_bouts.items():
        for batch in _chunk(list(dict.fromkeys(ids)), 100):
            try:
                await rest_patch(
                    "competition_mats",
                    {"competition_id": f"eq.{comp_id}", "current_bout_id": _pg_in(batch)},
                    {"current_bout_id": None},
                    prefer="return=minimal",
                )
            except Exception:
                pass
    await _rest_delete_in("competition_bouts", "id", comp_bout_ids, chunk_size=200)

    # 2) Delete legacy bouts (based on applications)
    await _rest_delete_in("bouts", "id", bout_ids, chunk_size=200)

    # 3) Delete applications
    await _rest_delete_in("applications", "id", app_ids, chunk_size=200)

    # 4) Delete passports and athletes
    await _rest_delete_in("passports", "athlete_id", athlete_ids, chunk_size=200)
    await _rest_delete_in("athletes", "id", athlete_ids, chunk_size=200)

    # 5) Delete user-linked rows and users
    await _rest_delete_in("profiles", "user_id", user_ids, chunk_size=200)
    await _rest_delete_in("registrations", "user_id", user_ids, chunk_size=200)
    await _rest_delete_in("user_roles", "user_id", user_ids, chunk_size=200)
    await _rest_delete_in("staff_locations", "user_id", user_ids, chunk_size=200)
    await _rest_delete_in("competition_secretaries", "user_id", user_ids, chunk_size=200)
    await _rest_delete_in("users", "id", user_ids, chunk_size=200)

    return {"ok": True, "deleted": True, **counts}


class BootstrapAdminBody(BaseModel):
    email: str
    password: str
    full_name: str = "Admin"
    role_codes: list[str] = ["founder"]


def _normalize_email_local(email: str) -> str:
    e = str(email or "").strip().lower()
    if not e or "@" not in e or "." not in e.split("@")[-1]:
        raise HTTPException(status_code=400, detail="Некорректный email")
    if len(e) > 320:
        raise HTTPException(status_code=400, detail="Некорректный email")
    return e


@router.post("/debug/bootstrap-admin")
async def debug_bootstrap_admin(
    body: BootstrapAdminBody,
    x_bootstrap_secret: str | None = Header(default=None),
):
    if os.getenv("APP_DEBUG") != "1":
        raise HTTPException(status_code=404, detail="Not Found")
    secret = (os.getenv("BOOTSTRAP_ADMIN_SECRET") or "").strip()
    if not secret:
        raise HTTPException(status_code=500, detail="BOOTSTRAP_ADMIN_SECRET is not configured")
    if not x_bootstrap_secret or x_bootstrap_secret.strip() != secret:
        raise HTTPException(status_code=403, detail="Forbidden")

    email = _normalize_email_local(body.email)
    password = str(body.password or "")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 8 символов")

    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_SERVICE_ROLE_KEY:
        raise HTTPException(status_code=500, detail="Supabase keys missing")

    async with httpx.AsyncClient(timeout=25.0, http2=False) as client:
        created = await client.post(
            f"{SUPABASE_URL}/auth/v1/admin/users",
            json={"email": email, "password": password, "email_confirm": True},
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "apikey": SUPABASE_SERVICE_ROLE_KEY,
                "Content-Type": "application/json",
            },
        )
        if created.status_code not in (200, 201):
            raise HTTPException(status_code=400, detail=f"Failed to create auth user: {created.text}")
        payload = created.json() if created.content else {}
        user_id = payload.get("id") if isinstance(payload, dict) else None
        if not user_id:
            raise HTTPException(status_code=400, detail="Auth user id missing in response")

    await rest_upsert("users", {"id": user_id, "email": email}, on_conflict="id")
    await rest_upsert("profiles", {"user_id": user_id, "full_name": body.full_name}, on_conflict="user_id")

    role_codes = [str(c) for c in (body.role_codes or []) if str(c).strip()]
    if not role_codes:
        role_codes = ["founder"]
    roles_resp = await rest_get(
        "roles",
        {"select": "id,code", "code": _pg_in(role_codes), "limit": "1000"},
        write=True,
    )
    roles_rows = roles_resp.json()
    if not isinstance(roles_rows, list) or not roles_rows:
        raise HTTPException(status_code=400, detail="Roles not found for provided role_codes")

    await rest_delete("user_roles", {"user_id": f"eq.{user_id}"})
    to_insert = [{"user_id": str(user_id), "role_id": str(r["id"])} for r in roles_rows if r.get("id")]
    if to_insert:
        resp = await rest_post("user_roles", {}, to_insert, prefer="return=minimal")
        if resp.status_code not in (200, 201, 204):
            raise HTTPException(status_code=400, detail=f"Failed to insert user_roles: {resp.status_code} {resp.text}")

    return {"ok": True, "user_id": user_id, "email": email, "role_codes": role_codes}

def _safe_data(res):
    if res is None:
        raise HTTPException(status_code=503, detail="Supabase request failed (no response)")
    if hasattr(res, "error") and getattr(res, "error"):
        raise HTTPException(status_code=503, detail=str(getattr(res, "error")))
    if not hasattr(res, "data"):
        raise HTTPException(status_code=503, detail="Supabase response missing data")
    return res.data

async def _get_user_id_from_bearer(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    cache_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
    cached = _me_cache.get(cache_key)
    if cached and cached[0] > time.time():
        return cached[1]
    user_id = await _local_get_user_id_from_bearer(authorization)
    _me_cache[cache_key] = (time.time() + 30.0, user_id)
    return user_id

@router.get("/me/profile")
async def get_my_profile(authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    try:
        resp = await rest_get(
            "profiles",
            {
                "select": "id,user_id,full_name,phone,city,location_id,created_at",
                "user_id": f"eq.{user_id}",
                "limit": "1",
            },
            write=True,
        )
        rows = resp.json()
        if isinstance(rows, list) and rows:
            return rows[0]
    except Exception:
        pass

    q = (
        admin_supabase.table("profiles")
        .select("id,user_id,full_name,phone,city,location_id,created_at")
        .eq("user_id", user_id)
        .maybe_single()
    )
    try:
        res = await _execute(q)
        data = _safe_data(res)
        if not data:
            return {"user_id": user_id, "full_name": "", "phone": "", "city": "", "location_id": None}
        return data
    except HTTPException as e:
        if e.status_code in (400, 503):
            return {"user_id": user_id, "full_name": "", "phone": "", "city": "", "location_id": None}
        raise


async def _get_location_path_v2(location_id: str) -> dict:
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

@router.put("/me/profile", response_model=ProfileResponse)
async def update_my_profile(profile: ProfileCreate, authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    await _require_can_edit_self(user_id, authorization)
    payload: dict[str, object] = {"user_id": user_id}
    if profile.full_name and profile.full_name.strip():
        payload["full_name"] = profile.full_name.strip()
    if profile.phone and profile.phone.strip():
        payload["phone"] = profile.phone.strip()
    if profile.city and profile.city.strip():
        payload["city"] = profile.city.strip()
    if profile.location_id is not None:
        payload["location_id"] = str(profile.location_id)
    q = admin_supabase.table("profiles").upsert(
        payload,
        on_conflict="user_id",
    )
    res = await _execute(q)
    data = _safe_data(res)
    return (data[0] if isinstance(data, list) and data else data) or {"user_id": user_id}

@router.get("/me/athlete")
async def get_my_athlete(authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    try:
        resp = await rest_get(
            "athletes",
            {"select": "id,user_id,coach_name", "user_id": f"eq.{user_id}", "limit": "1"},
            write=True,
        )
        rows = resp.json()
        if isinstance(rows, list) and rows:
            a = rows[0]
            athlete_id = str(a.get("id") or "")
            passports = []
            if athlete_id:
                p_resp = await rest_get(
                    "passports",
                    {"select": "*", "athlete_id": f"eq.{athlete_id}", "limit": "100"},
                    write=True,
                )
                p_rows = p_resp.json()
                passports = p_rows if isinstance(p_rows, list) else []
            a["passports"] = passports
            return a
    except Exception:
        pass

    try:
        q = admin_supabase.table("athletes").select("*, passports(*)").eq("user_id", user_id).maybe_single()
        res = await _execute(q)
        data = _safe_data(res)
        if data:
            return data
    except Exception:
        pass

    return {
        "id": "00000000-0000-0000-0000-000000000000",
        "user_id": user_id,
        "coach_name": "",
        "passports": [],
    }

@router.put("/me/athlete")
async def update_my_athlete(coach_name: Optional[str] = None, authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    await _require_can_edit_self(user_id, authorization)
    q = admin_supabase.table("athletes").upsert(
        {
            "user_id": user_id,
            "coach_name": coach_name,
        },
        on_conflict="user_id",
    )
    res = await _execute(q)
    data = _safe_data(res)
    return (data[0] if isinstance(data, list) and data else data) or {"user_id": user_id}

@router.get("/me/details")
async def get_my_details(authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    try:
        a_resp = await rest_get(
            "athletes",
            {"select": "id", "user_id": f"eq.{user_id}", "limit": "1"},
            write=True,
        )
        a_rows = a_resp.json()
        if not isinstance(a_rows, list) or not a_rows or not a_rows[0].get("id"):
            return {"birth_date": None, "rank": None, "photo_url": None, "gender": None}
        athlete_id = str(a_rows[0]["id"])

        p_resp = await rest_get(
            "passports",
            {
                "select": "birth_date,rank,photo_url,gender",
                "athlete_id": f"eq.{athlete_id}",
                "limit": "1",
            },
            write=True,
        )
        p_rows = p_resp.json()
        p = p_rows[0] if isinstance(p_rows, list) and p_rows else {}
        return {
            "birth_date": p.get("birth_date") if isinstance(p, dict) else None,
            "rank": p.get("rank") if isinstance(p, dict) else None,
            "photo_url": p.get("photo_url") if isinstance(p, dict) else None,
            "gender": p.get("gender") if isinstance(p, dict) else None,
        }
    except Exception:
        pass

    try:
        athlete_res = await _execute(
            admin_supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single()
        )
        athlete_data = _safe_data(athlete_res)
        if not athlete_data:
            return {"birth_date": None, "rank": None, "photo_url": None, "gender": None}
        athlete_id = str(athlete_data["id"])
        p_res = await _execute(
            admin_supabase.table("passports")
            .select("birth_date,rank,photo_url,gender")
            .eq("athlete_id", athlete_id)
            .maybe_single()
        )
        p = _safe_data(p_res) or {}
        return {
            "birth_date": p.get("birth_date"),
            "rank": p.get("rank"),
            "photo_url": p.get("photo_url"),
            "gender": p.get("gender"),
        }
    except Exception:
        return {"birth_date": None, "rank": None, "photo_url": None, "gender": None}


@router.get("/me/dashboard")
async def get_my_dashboard(authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)

    async def _fetch_registration() -> dict:
        try:
            r = await rest_get(
                "registrations",
                {"select": "stage,consent_accepted,updated_at", "user_id": f"eq.{user_id}", "limit": "1"},
                write=True,
            )
            rows = r.json()
            row = rows[0] if isinstance(rows, list) and rows else {}
            stage = row.get("stage") if isinstance(row, dict) else None
            if not stage:
                stage = "start"
            return {"user_id": user_id, "stage": stage, "locked": bool(stage == "complete")}
        except Exception:
            return {"user_id": user_id, "stage": "start", "locked": False}

    async def _fetch_profile() -> dict:
        try:
            r = await rest_get(
                "profiles",
                {
                    "select": "id,user_id,full_name,phone,city,location_id,created_at",
                    "user_id": f"eq.{user_id}",
                    "limit": "1",
                },
                write=True,
            )
            rows = r.json()
            if isinstance(rows, list) and rows:
                return rows[0]
        except Exception:
            pass
        return {"user_id": user_id, "full_name": "", "phone": "", "city": "", "location_id": None}

    async def _fetch_athlete() -> dict:
        try:
            r = await rest_get(
                "athletes",
                {"select": "id,user_id,coach_name", "user_id": f"eq.{user_id}", "limit": "1"},
                write=True,
            )
            rows = r.json()
            if isinstance(rows, list) and rows:
                return rows[0]
        except Exception:
            pass
        return {"id": None, "user_id": user_id, "coach_name": ""}

    async def _fetch_details(athlete_id: str | None) -> dict:
        if not athlete_id:
            return {"birth_date": None, "rank": None, "photo_url": None, "gender": None}
        try:
            r = await rest_get(
                "passports",
                {
                    "select": "birth_date,rank,photo_url,gender",
                    "athlete_id": f"eq.{athlete_id}",
                    "limit": "1",
                },
                write=True,
            )
            rows = r.json()
            p = rows[0] if isinstance(rows, list) and rows else {}
            return {
                "birth_date": p.get("birth_date") if isinstance(p, dict) else None,
                "rank": p.get("rank") if isinstance(p, dict) else None,
                "photo_url": p.get("photo_url") if isinstance(p, dict) else None,
                "gender": p.get("gender") if isinstance(p, dict) else None,
            }
        except Exception:
            return {"birth_date": None, "rank": None, "photo_url": None, "gender": None}

    registration: dict = {}
    profile: dict = {}
    athlete: dict = {}

    async with anyio.create_task_group() as tg:
        async def _run_reg():
            nonlocal registration
            registration = await _fetch_registration()

        async def _run_profile():
            nonlocal profile
            profile = await _fetch_profile()

        async def _run_athlete():
            nonlocal athlete
            athlete = await _fetch_athlete()

        tg.start_soon(_run_reg)
        tg.start_soon(_run_profile)
        tg.start_soon(_run_athlete)

    athlete_id = str(athlete.get("id") or "") if isinstance(athlete, dict) else ""
    details = await _fetch_details(athlete_id or None)

    location_path = None
    loc_id = profile.get("location_id") if isinstance(profile, dict) else None
    if loc_id:
        try:
            location_path = await _get_location_path_v2(str(loc_id))
        except Exception:
            location_path = None

    return {
        "registration": registration,
        "profile": profile,
        "athlete": athlete,
        "details": details,
        "location_path": location_path,
    }


@router.put("/me/details")
async def update_my_details(body: AthleteDetailsUpdate, authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    await _require_can_edit_self(user_id, authorization)
    athlete_q = admin_supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single()
    athlete_res = await _execute(athlete_q)
    athlete_data = _safe_data(athlete_res)
    if not athlete_data:
        raise HTTPException(status_code=404, detail="Athlete profile must be created first")
    athlete_id = str(athlete_data["id"])
    payload: dict[str, object] = {"athlete_id": athlete_id}
    if body.birth_date is not None:
        payload["birth_date"] = str(body.birth_date)
    if body.rank is not None:
        payload["rank"] = body.rank
    if body.photo_url is not None:
        payload["photo_url"] = body.photo_url
    if body.gender is not None:
        payload["gender"] = body.gender
    await _execute(
        admin_supabase.table("passports").upsert(
            payload,
            on_conflict="athlete_id",
        )
    )
    return {"ok": True}


@router.post("/me/profile/submit")
async def submit_my_profile(
    full_name: str = Form(default=""),
    phone: str | None = Form(default=None),
    city: str | None = Form(default=None),
    location_id: str | None = Form(default=None),
    coach_name: str | None = Form(default=None),
    birth_date: str | None = Form(default=None),
    gender: str | None = Form(default=None),
    rank: str | None = Form(default=None),
    photo_url: str | None = Form(default=None),
    photo: UploadFile | None = File(default=None),
    authorization: str | None = Header(default=None),
):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    await _require_can_edit_self(user_id, authorization)
    if not rl_allow(f"profile_submit:{user_id}", rate_per_minute=12.0, burst=24.0):
        raise HTTPException(status_code=429, detail="Too many profile submissions, please try again shortly")

    profile_payload: dict[str, object] = {"user_id": user_id}
    if full_name and full_name.strip():
        profile_payload["full_name"] = full_name.strip()
    if phone is not None:
        digits = "".join(ch for ch in str(phone) if ch.isdigit())
        profile_payload["phone"] = digits or None
    if city is not None:
        profile_payload["city"] = str(city).strip()
    if location_id is not None:
        loc = str(location_id).strip()
        profile_payload["location_id"] = loc or None

    await _execute(admin_supabase.table("profiles").upsert(profile_payload, on_conflict="user_id"))
    try:
        await rest_upsert("profiles", profile_payload, on_conflict="user_id")
    except Exception:
        pass

    ath_payload = {"user_id": user_id, "coach_name": coach_name}
    try:
        await rest_upsert("athletes", ath_payload, on_conflict="user_id")
        ath_data = ath_payload | {"id": None}
    except Exception:
        ath_res = await _execute(
            admin_supabase.table("athletes").upsert(
                {"user_id": user_id, "coach_name": coach_name},
                on_conflict="user_id",
            )
        )
        ath_data = _safe_data(ath_res)
    athlete_id = None
    if isinstance(ath_data, list) and ath_data:
        athlete_id = str(ath_data[0].get("id")) if isinstance(ath_data[0], dict) else None
    elif isinstance(ath_data, dict):
        athlete_id = str(ath_data.get("id")) if ath_data.get("id") else None
    if not athlete_id or athlete_id == "None":
        athlete_q = admin_supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single()
        athlete_res = await _execute(athlete_q)
        athlete_row = _safe_data(athlete_res)
        if not athlete_row:
            raise HTTPException(status_code=500, detail="Athlete upsert failed")
        athlete_id = str(athlete_row["id"])

    final_photo_url = str(photo_url).strip() if photo_url else None
    if photo:
        if not photo.content_type or not photo.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="Only image files are supported")
        content = await photo.read()
        if not content:
            raise HTTPException(status_code=400, detail="Empty file")
        if len(content) > 10 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="File too large (max 10MB)")

        ext = os.path.splitext(photo.filename or "")[1].lower()
        if not ext:
            if photo.content_type == "image/png":
                ext = ".png"
            elif photo.content_type == "image/webp":
                ext = ".webp"
            else:
                ext = ".jpg"

        object_path = f"documents/{user_id}/{uuid4().hex}{ext}"
        bucket = "avatars"
        def _sync_upload():
            admin_supabase.storage.from_(bucket).upload(
                object_path,
                content,
                file_options={"content-type": photo.content_type or "application/octet-stream"},
            )

        upload_exc = None
        for attempt in range(3):
            try:
                await anyio.to_thread.run_sync(_sync_upload)
                upload_exc = None
                break
            except Exception as e:
                upload_exc = e
                if attempt >= 2:
                    break
                await anyio.sleep(0.35 * (attempt + 1))
        if upload_exc:
            raise HTTPException(
                status_code=503,
                detail=f"Supabase storage unavailable: {type(upload_exc).__name__}",
            )
        final_photo_url = object_path

    passport_payload: dict[str, object] = {"athlete_id": athlete_id}
    if birth_date is not None:
        passport_payload["birth_date"] = str(birth_date).strip() or None
    if gender is not None:
        passport_payload["gender"] = str(gender).strip() or None
    if rank is not None:
        passport_payload["rank"] = str(rank).strip() or None
    if final_photo_url is not None:
        passport_payload["photo_url"] = final_photo_url

    if not SUPABASE_SERVICE_ROLE_KEY or not SUPABASE_KEY or not SUPABASE_URL:
        raise HTTPException(status_code=500, detail="Supabase keys not configured")

    try:
        await rest_upsert("passports", passport_payload, on_conflict="athlete_id")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to upsert passports: {repr(e)}")

    return {"ok": True, "photo_url": final_photo_url}


@router.post("/me/complete")
async def complete_my_profile(authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    role_codes = await _get_my_role_codes(user_id)
    if _is_staff_role(role_codes):
        raise HTTPException(status_code=400, detail="Staff accounts cannot be completed here")

    prof_res = await _execute(admin_supabase.table("profiles").select("full_name,city,location_id").eq("user_id", user_id).maybe_single())
    prof = _safe_data(prof_res) or {}
    ath_res = await _execute(admin_supabase.table("athletes").select("id,coach_name").eq("user_id", user_id).maybe_single())
    ath = _safe_data(ath_res) or {}
    athlete_id = str(ath.get("id") or "")
    p_res = await _execute(
        admin_supabase.table("passports")
        .select("birth_date,rank,photo_url,gender")
        .eq("athlete_id", athlete_id)
        .maybe_single()
    )
    p = _safe_data(p_res) or {}

    if not prof.get("full_name") or not prof.get("city") or not prof.get("location_id"):
        raise HTTPException(status_code=400, detail="Fill full_name, city and region")
    if not ath.get("coach_name"):
        raise HTTPException(status_code=400, detail="Fill coach name")
    if not p.get("birth_date") or not p.get("rank") or not p.get("photo_url") or not p.get("gender"):
        raise HTTPException(status_code=400, detail="Fill birth_date, gender, rank and upload photo")

    await _execute(
        admin_supabase.table("registrations").upsert(
            {"user_id": user_id, "stage": "complete", "consent_accepted": True},
            on_conflict="user_id",
        )
    )
    return {"ok": True, "locked": True}


@router.get("/me/registration")
async def get_my_registration(authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    try:
        res = await _execute(
            admin_supabase.table("registrations")
            .select("stage,consent_accepted,updated_at")
            .eq("user_id", user_id)
            .maybe_single()
        )
        data = _safe_data(res)
        stage = (data or {}).get("stage") if isinstance(data, dict) else None
        if not stage:
            stage = "start"
        return {"user_id": user_id, "stage": stage, "locked": bool(stage == "complete")}
    except Exception:
        return {"user_id": user_id, "stage": "start", "locked": False}

@router.get("/me/applications")
async def get_my_applications(authorization: str | None = Header(default=None)):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    user_id = await _get_user_id_from_bearer(authorization)
    try:
        athlete_resp = await rest_get(
            "athletes",
            {"select": "id", "user_id": f"eq.{user_id}", "limit": "1"},
            write=True,
        )
        athlete_rows = athlete_resp.json()
        if not isinstance(athlete_rows, list) or not athlete_rows or not athlete_rows[0].get("id"):
            return []
        athlete_id = str(athlete_rows[0]["id"])

        apps_resp = await rest_get(
            "applications",
            {
                "select": "id,status,draw_number,created_at,category_id,competitions(id,name,start_date),competition_categories(gender,age_min,age_max,weight_min,weight_max)",
                "athlete_id": f"eq.{athlete_id}",
                "order": "created_at.desc",
                "limit": "1000",
            },
            write=True,
        )
        rows = apps_resp.json()
        return rows if isinstance(rows, list) else []
    except Exception:
        return []

@router.post("/admin-create/", response_model=UserProfile)
@router.post("/admin-create", response_model=UserProfile)
async def create_admin_user(payload: AdminCreate):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="SUPABASE_SERVICE_ROLE_KEY is not set")

    is_admin = any("admin" in code for code in payload.role_codes)
    is_secretary = any("secretary" in code for code in payload.role_codes)
    if is_admin and is_secretary:
        raise HTTPException(status_code=400, detail="Администратор не может быть секретарем")

    if (is_admin or is_secretary) and not payload.location_id:
        raise HTTPException(status_code=400, detail="Для админов/секретарей нужна привязка к локации")

    try:
        auth_res = admin_supabase.auth.admin.create_user(
            {
                "email": payload.email,
                "password": payload.password,
                "email_confirm": True,
            }
        )
    except Exception as e:
        print(f"Error creating user in Supabase Auth: {e}")
        raise HTTPException(status_code=400, detail=f"Ошибка создания пользователя в Auth: {str(e)}")

    auth_user = None
    if isinstance(auth_res, dict):
        auth_user = auth_res.get("user")
    else:
        auth_user = getattr(auth_res, "user", None)

    if not auth_user or not getattr(auth_user, "id", None):
        raise HTTPException(status_code=400, detail="Failed to create auth user")

    user_id = getattr(auth_user, "id")

    try:
        await rest_upsert("users", {"id": user_id, "email": payload.email}, on_conflict="id")
        await rest_upsert(
            "profiles",
            {
                "user_id": user_id,
                "full_name": payload.full_name,
                "phone": payload.phone,
                "location_id": str(payload.location_id) if payload.location_id else None,
            },
            on_conflict="user_id",
        )

        roles_resp = await rest_get(
            "roles",
            {"select": "id,code", "code": _pg_in([str(c) for c in payload.role_codes]), "limit": "1000"},
            write=True,
        )
        roles_rows = roles_resp.json()
        if not isinstance(roles_rows, list) or not roles_rows:
            raise HTTPException(status_code=400, detail="Invalid role codes")

        await rest_delete("user_roles", {"user_id": f"eq.{user_id}"})
        to_insert_roles = [{"user_id": str(user_id), "role_id": str(r["id"])} for r in roles_rows if r.get("id")]
        if to_insert_roles:
            await rest_post("user_roles", {}, to_insert_roles, prefer="return=minimal")

        await rest_delete("staff_locations", {"user_id": f"eq.{user_id}"})
        if payload.location_id and (is_admin or is_secretary):
            to_insert_staff = [
                {"user_id": str(user_id), "location_id": str(payload.location_id), "role_id": r["id"]}
                for r in roles_rows
                if ("admin" in str(r.get("code") or "") or "secretary" in str(r.get("code") or ""))
            ]
            if to_insert_staff:
                await rest_post("staff_locations", {}, to_insert_staff, prefer="return=minimal")
    except Exception as e:
        print(f"Error inserting user details to public tables: {e}")
        # Если произошла ошибка при добавлении в публичные таблицы, стоит удалить пользователя из Auth
        try:
            admin_supabase.auth.admin.delete_user(str(user_id))
        except:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка сохранения данных: {str(e)}")

    loc_name = None
    if payload.location_id:
        try:
            loc_resp = await rest_get(
                "locations",
                {"select": "name", "id": f"eq.{str(payload.location_id)}", "limit": "1"},
                write=False,
            )
            loc_rows = loc_resp.json()
            if isinstance(loc_rows, list) and loc_rows and isinstance(loc_rows[0], dict):
                loc_name = loc_rows[0].get("name")
        except Exception:
            loc_name = None

    return UserProfile(
        user_id=str(user_id),
        full_name=payload.full_name,
        phone=payload.phone,
        email=payload.email,
        roles=[str(c) for c in payload.role_codes],
        location_id=str(payload.location_id) if payload.location_id else None,
        location_name=loc_name,
    )


@router.get("/athletes")
async def list_athletes(
    authorization: str | None = Header(default=None),
    query: str | None = None,
    limit: int = 200,
):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    await _require_staff(authorization)

    limit = max(1, min(int(limit), 500))

    user_ids: list[str] | None = None
    if query:
        q = await _execute(
            admin_supabase.table("profiles")
            .select("user_id")
            .ilike("full_name", f"%{query}%")
            .limit(500)
        )
        data = _safe_data(q) or []
        user_ids = [str(p.get("user_id")) for p in data if p.get("user_id")]
        if not user_ids:
            return []

    athletes_q = (
        admin_supabase.table("athletes")
        .select(
            "id,user_id,coach_name,"
            "users:users!athletes_user_id_fkey(email,profiles(full_name,phone,city,location_id)),"
            "passports(birth_date,gender,rank,photo_url)"
        )
        .limit(limit)
    )
    if user_ids is not None:
        athletes_q = athletes_q.in_("user_id", user_ids)

    res = await _execute(athletes_q)
    rows = _safe_data(res) or []
    out: list[dict] = []
    for r in rows:
        users_data = r.get("users")
        if isinstance(users_data, list):
            users_data = users_data[0] if users_data else None
        profiles_data = users_data.get("profiles") if isinstance(users_data, dict) else None
        if isinstance(profiles_data, list):
            profiles_data = profiles_data[0] if profiles_data else None
        passports_data = r.get("passports")
        if isinstance(passports_data, list):
            passports_data = passports_data[0] if passports_data else None

        out.append(
            {
                "athlete_id": r.get("id"),
                "user_id": r.get("user_id"),
                "full_name": (profiles_data or {}).get("full_name"),
                "phone": (profiles_data or {}).get("phone"),
                "city": (profiles_data or {}).get("city"),
                "location_id": (profiles_data or {}).get("location_id"),
                "email": (users_data or {}).get("email") if isinstance(users_data, dict) else None,
                "coach_name": r.get("coach_name"),
                "birth_date": (passports_data or {}).get("birth_date"),
                "gender": (passports_data or {}).get("gender"),
                "rank": (passports_data or {}).get("rank"),
                "photo_url": (passports_data or {}).get("photo_url"),
            }
        )
    return out


@router.get("/athletes/{athlete_id}")
async def get_athlete_details(
    athlete_id: UUID,
    authorization: str | None = Header(default=None),
):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    await _require_staff(authorization)

    ath_res = await _execute(
        admin_supabase.table("athletes")
        .select("id,user_id,coach_name")
        .eq("id", str(athlete_id))
        .maybe_single()
    )
    athlete = _safe_data(ath_res)
    if not athlete:
        raise HTTPException(status_code=404, detail="Athlete not found")

    user_id = str(athlete.get("user_id"))
    prof_res = await _execute(
        admin_supabase.table("profiles")
        .select("user_id,full_name,phone,city,location_id")
        .eq("user_id", user_id)
        .maybe_single()
    )
    prof = _safe_data(prof_res) or {}

    p_res = await _execute(
        admin_supabase.table("passports")
        .select("birth_date,gender,rank,photo_url")
        .eq("athlete_id", str(athlete_id))
        .maybe_single()
    )
    p = _safe_data(p_res) or {}

    reg_res = await _execute(
        admin_supabase.table("registrations")
        .select("stage")
        .eq("user_id", user_id)
        .maybe_single()
    )
    reg = _safe_data(reg_res) or {}
    stage = str(reg.get("stage") or "start")

    return {
        "athlete_id": str(athlete_id),
        "user_id": user_id,
        "full_name": prof.get("full_name"),
        "phone": prof.get("phone"),
        "city": prof.get("city"),
        "location_id": prof.get("location_id"),
        "coach_name": athlete.get("coach_name"),
        "birth_date": p.get("birth_date"),
        "gender": p.get("gender"),
        "rank": p.get("rank"),
        "photo_url": p.get("photo_url"),
        "stage": stage,
        "locked": bool(stage == "complete"),
    }


@router.put("/athletes/{athlete_id}")
async def update_athlete_details(
    athlete_id: UUID,
    body: AdminAthleteUpdate,
    authorization: str | None = Header(default=None),
):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    await _require_staff(authorization)

    ath_res = await _execute(
        admin_supabase.table("athletes")
        .select("id,user_id")
        .eq("id", str(athlete_id))
        .maybe_single()
    )
    athlete = _safe_data(ath_res)
    if not athlete:
        raise HTTPException(status_code=404, detail="Athlete not found")
    user_id = str(athlete.get("user_id"))

    prof_payload: dict[str, object] = {"user_id": user_id}
    if body.full_name is not None:
        prof_payload["full_name"] = body.full_name
    if body.phone is not None:
        prof_payload["phone"] = body.phone
    if body.city is not None:
        prof_payload["city"] = body.city
    if body.location_id is not None:
        prof_payload["location_id"] = str(body.location_id)
    await _execute(admin_supabase.table("profiles").upsert(prof_payload, on_conflict="user_id"))

    if body.coach_name is not None:
        await _execute(admin_supabase.table("athletes").update({"coach_name": body.coach_name}).eq("id", str(athlete_id)))

    pass_payload: dict[str, object] = {"athlete_id": str(athlete_id)}
    if body.birth_date is not None:
        pass_payload["birth_date"] = str(body.birth_date)
    if body.gender is not None:
        pass_payload["gender"] = body.gender
    if body.rank is not None:
        pass_payload["rank"] = body.rank
    if body.photo_url is not None:
        pass_payload["photo_url"] = body.photo_url
    await _execute(admin_supabase.table("passports").upsert(pass_payload, on_conflict="athlete_id"))

    return {"ok": True}


@router.post("/athletes/{athlete_id}/editable")
async def set_athlete_editable(
    athlete_id: UUID,
    body: EditableUpdate,
    authorization: str | None = Header(default=None),
):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    await _require_staff(authorization)

    ath_res = await _execute(
        admin_supabase.table("athletes")
        .select("user_id")
        .eq("id", str(athlete_id))
        .maybe_single()
    )
    athlete = _safe_data(ath_res)
    if not athlete or not athlete.get("user_id"):
        raise HTTPException(status_code=404, detail="Athlete not found")

    user_id = str(athlete.get("user_id"))
    stage = "start" if body.editable else "complete"
    await _execute(
        admin_supabase.table("registrations").upsert({"user_id": user_id, "stage": stage}, on_conflict="user_id")
    )
    return {"ok": True, "stage": stage, "locked": bool(stage == "complete")}

@router.get("/roles", response_model=List[Role])
async def get_roles():
    resp = await rest_get("roles", {"select": "*", "limit": "1000"}, write=True)
    rows = resp.json()
    return rows if isinstance(rows, list) else []

@router.get("/search", response_model=List[UserProfile])
async def search_users(query: str):
    q = str(query or "").strip()
    if not q:
        return []

    user_ids: set[str] = set()

    if q.isdigit():
        u_resp = await rest_get(
            "users",
            {"select": "id", "telegram_id": f"eq.{q}", "limit": "200"},
            write=True,
        )
        u_rows = u_resp.json()
        if isinstance(u_rows, list):
            for r in u_rows:
                if isinstance(r, dict) and r.get("id"):
                    user_ids.add(str(r["id"]))

    u2_resp = await rest_get(
        "users",
        {"select": "id", "email": f"ilike.*{q}*", "limit": "200"},
        write=True,
    )
    u2_rows = u2_resp.json()
    if isinstance(u2_rows, list):
        for r in u2_rows:
            if isinstance(r, dict) and r.get("id"):
                user_ids.add(str(r["id"]))

    p_resp = await rest_get(
        "profiles",
        {"select": "user_id", "full_name": f"ilike.*{q}*", "limit": "500"},
        write=True,
    )
    p_rows = p_resp.json()
    if isinstance(p_rows, list):
        for r in p_rows:
            if isinstance(r, dict) and r.get("user_id"):
                user_ids.add(str(r["user_id"]))

    all_user_ids = list(user_ids)
    if not all_user_ids:
        return []

    final_resp = await rest_get(
        "users",
        {
            "select": "id,email,profiles(full_name,phone),user_roles(roles(code)),staff_locations(location_id,locations(name))",
            "id": _pg_in(all_user_ids[:1000]),
            "limit": "1000",
        },
        write=True,
    )
    final_rows = final_resp.json()
    if not isinstance(final_rows, list):
        final_rows = []

    users = []
    for u in final_rows:
        profile = u.get("profiles")
        if isinstance(profile, list):
            profile = profile[0] if profile else None

        roles = [ur["roles"]["code"] for ur in u.get("user_roles", []) if ur.get("roles")]

        staff = u.get("staff_locations")
        if isinstance(staff, list):
            staff = staff[0] if staff else None

        loc_id = staff.get("location_id") if staff else None
        loc_name = staff.get("locations", {}).get("name") if staff and staff.get("locations") else None
            
        users.append(UserProfile(
            user_id=u["id"],
            full_name=profile.get("full_name") if profile else None,
            phone=profile.get("phone") if profile else None,
            email=u.get("email"),
            roles=roles,
            location_id=loc_id,
            location_name=loc_name
        ))
        
    return users

@router.post("/{user_id}/roles", response_model=UserProfile)
async def assign_roles(user_id: UUID, role_in: RoleAssign):
    is_admin = any("admin" in code for code in role_in.role_codes)
    is_secretary = any("secretary" in code for code in role_in.role_codes)
    
    if is_admin and is_secretary:
        raise HTTPException(status_code=400, detail="Администратор не может быть секретарем")
        
    roles_resp = await rest_get(
        "roles",
        {"select": "id,code", "code": _pg_in([str(c) for c in role_in.role_codes]), "limit": "1000"},
        write=True,
    )
    roles_rows = roles_resp.json()
    if not isinstance(roles_rows, list) or not roles_rows:
        raise HTTPException(status_code=400, detail="Invalid role codes")
        
    await rest_delete("user_roles", {"user_id": f"eq.{str(user_id)}"})
    to_insert_roles = [{"user_id": str(user_id), "role_id": str(r["id"])} for r in roles_rows if r.get("id")]
    if to_insert_roles:
        await rest_post("user_roles", {}, to_insert_roles, prefer="return=minimal")
    
    await rest_delete("staff_locations", {"user_id": f"eq.{str(user_id)}"})
    if role_in.location_id and (is_admin or is_secretary):
        to_insert_staff = [
            {"user_id": str(user_id), "location_id": str(role_in.location_id), "role_id": r["id"]} 
            for r in roles_rows
            if ("admin" in str(r.get("code") or "") or "secretary" in str(r.get("code") or ""))
        ]
        if to_insert_staff:
            await rest_post("staff_locations", {}, to_insert_staff, prefer="return=minimal")
    
    res = await rest_get(
        "users",
        {
            "select": "id,email,profiles(full_name,phone),user_roles(roles(code)),staff_locations(location_id,locations(name))",
            "id": f"eq.{str(user_id)}",
            "limit": "1",
        },
        write=True,
    )
    u_rows = res.json()
    u = u_rows[0] if isinstance(u_rows, list) and u_rows else None
    if not isinstance(u, dict):
        raise HTTPException(status_code=404, detail="User not found")
    profile = u.get("profiles")
    if isinstance(profile, list):
        profile = profile[0] if profile else None

    roles = [ur["roles"]["code"] for ur in u.get("user_roles", []) if ur.get("roles")]
    staff = u.get("staff_locations")
    if isinstance(staff, list):
        staff = staff[0] if staff else None

    loc_id = staff.get("location_id") if staff else None
    loc_name = staff.get("locations", {}).get("name") if staff and staff.get("locations") else None
    
    return UserProfile(
        user_id=u["id"],
        full_name=profile.get("full_name") if profile else None,
        phone=profile.get("phone") if profile else None,
        email=u.get("email"),
        roles=roles,
        location_id=loc_id,
        location_name=loc_name
    )

@router.get("/secretaries", response_model=List[UserProfile])
async def get_secretaries(location_id: Optional[UUID] = None):
    roles_resp = await rest_get(
        "roles",
        {"select": "id,code", "code": "ilike.*secretary*", "limit": "1000"},
        write=True,
    )
    role_rows = roles_resp.json()
    role_ids = [str(r["id"]) for r in role_rows if isinstance(r, dict) and r.get("id")]
    if not role_ids:
        return []

    params: dict[str, str] = {
        "select": "user_id,location_id,locations(name),roles(code),users(email,profiles(full_name,phone))",
        "role_id": _pg_in(role_ids),
        "limit": "10000",
    }
    if location_id:
        params["location_id"] = f"eq.{str(location_id)}"

    res = await rest_get("staff_locations", params, write=True)
    rows = res.json()
    if not isinstance(rows, list):
        return []

    users_dict: dict[str, dict] = {}
    for r in rows:
        if not isinstance(r, dict) or not r.get("user_id"):
            continue
        uid = str(r["user_id"])
        user = r.get("users") or {}
        profile = user.get("profiles") if isinstance(user, dict) else None
        if isinstance(profile, list):
            profile = profile[0] if profile else None
        roles = r.get("roles")
        if isinstance(roles, list):
            roles = roles[0] if roles else None
        role_code = roles.get("code") if isinstance(roles, dict) else None

        if uid not in users_dict:
            loc = r.get("locations") if isinstance(r.get("locations"), dict) else None
            users_dict[uid] = {
                "user_id": uid,
                "full_name": profile.get("full_name") if isinstance(profile, dict) else None,
                "phone": profile.get("phone") if isinstance(profile, dict) else None,
                "email": user.get("email") if isinstance(user, dict) else None,
                "location_id": r.get("location_id"),
                "location_name": loc.get("name") if loc else None,
                "roles": [],
            }
        if role_code:
            users_dict[uid]["roles"].append(str(role_code))

    return [UserProfile(**u) for u in users_dict.values()]


@router.delete("/{user_id}/")
@router.delete("/{user_id}")
async def delete_user(user_id: UUID):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="SUPABASE_SERVICE_ROLE_KEY is not set")
    
    try:
        # Delete from Supabase Auth
        # This usually cascades to public.users if FK is set up correctly
        res = admin_supabase.auth.admin.delete_user(str(user_id))
        
        # Also explicitly try to delete from public.users just in case cascade is missing
        # or if we want to be sure.
        # However, if cascade is ON, this second delete might find nothing, which is fine.
        await rest_delete("users", {"id": f"eq.{str(user_id)}"})
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
        
    return {"message": "User deleted successfully"}

@router.get("/admins", response_model=List[UserProfile])
async def get_admins():
    admin_codes = ["founder", "world_admin", "world_secretary", "country_admin", "country_secretary", "region_admin", "region_secretary"]

    roles_resp = await rest_get(
        "roles",
        {"select": "id,code", "code": _pg_in([str(c) for c in admin_codes]), "limit": "1000"},
        write=True,
    )
    role_rows = roles_resp.json()
    role_ids = [str(r["id"]) for r in role_rows if isinstance(r, dict) and r.get("id")]
    if not role_ids:
        return []

    res = await rest_get(
        "staff_locations",
        {
            "select": "user_id,location_id,locations(name),roles(code),users(email,profiles(full_name,phone))",
            "role_id": _pg_in(role_ids),
            "limit": "10000",
        },
        write=True,
    )
    rows = res.json()
    if not isinstance(rows, list):
        return []

    users_dict: dict[str, dict] = {}
    for r in rows:
        if not isinstance(r, dict) or not r.get("user_id"):
            continue
        uid = str(r["user_id"])
        user = r.get("users") or {}
        profile = user.get("profiles") if isinstance(user, dict) else None
        if isinstance(profile, list):
            profile = profile[0] if profile else None
        roles = r.get("roles")
        if isinstance(roles, list):
            roles = roles[0] if roles else None
        role_code = roles.get("code") if isinstance(roles, dict) else None

        if uid not in users_dict:
            loc = r.get("locations") if isinstance(r.get("locations"), dict) else None
            users_dict[uid] = {
                "user_id": uid,
                "full_name": profile.get("full_name") if isinstance(profile, dict) else None,
                "phone": profile.get("phone") if isinstance(profile, dict) else None,
                "email": user.get("email") if isinstance(user, dict) else None,
                "location_id": r.get("location_id"),
                "location_name": loc.get("name") if loc else None,
                "roles": [],
            }
        if role_code:
            users_dict[uid]["roles"].append(str(role_code))

    return [UserProfile(**u) for u in users_dict.values()]
