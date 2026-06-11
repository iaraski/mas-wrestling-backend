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
from app.core.supabase import admin_supabase
from app.core.rest import rest_upsert, rest_get, rest_delete, rest_patch, rest_post
from app.core.ratelimit import allow as rl_allow
from app.core.local_auth import get_user_id_from_bearer as _local_get_user_id_from_bearer, set_user_password
from app.core.roles import get_role_codes
from app.schemas.user import Role, UserProfile, RoleAssign, AdminCreate, ProfileResponse, ProfileCreate, PassportResponse, PassportBase, AthleteResponse

router = APIRouter(prefix="/users", tags=["users"])

_me_cache: dict[str, tuple[float, str]] = {}
_staff_list_cache: dict[str, tuple[float, list[UserProfile]]] = {}
_admins_cache: tuple[float, list[UserProfile]] | None = None


@router.post("/uploads/photo")
async def upload_photo_for_staff(
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
):
    await _require_staff(authorization)
    if not file.content_type or not str(file.content_type).startswith("image/"):
        raise HTTPException(status_code=400, detail="Only image files are supported")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 10MB)")

    filename = file.filename or "photo"
    ext = os.path.splitext(filename)[1].lower()
    if not ext:
        ct = str(file.content_type or "").lower()
        if ct == "image/png":
            ext = ".png"
        elif ct == "image/webp":
            ext = ".webp"
        else:
            ext = ".jpg"

    key = f"documents/admin/{uuid4().hex}{ext}"
    from app.core.minio import put_object

    await put_object(key, content, content_type=file.content_type or "application/octet-stream")
    return {"photo_url": key}


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
    series: Optional[str] = None
    number: Optional[str] = None
    issued_by: Optional[str] = None
    issue_date: Optional[date] = None
    rank: Optional[str] = None
    photo_url: Optional[str] = None
    passport_scan_url: Optional[str] = None


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
    try:
        return await get_role_codes(user_id)
    except Exception:
        return []


async def _is_profile_locked(user_id: str) -> bool:
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


def _chunk(items: list[str], size: int) -> list[list[str]]:
    out: list[list[str]] = []
    for i in range(0, len(items), size):
        out.append(items[i : i + size])
    return out


def _pg_in(ids: list[str]) -> str:
    return f"in.({','.join(ids)})"


def _unwrap_rel(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value if isinstance(value, dict) else None


async def _list_user_profiles_by_role_ids(
    role_ids: list[str],
    *,
    location_id: str | None = None,
) -> list[UserProfile]:
    if not role_ids:
        return []

    from uuid import UUID as _UUID

    role_uuid_ids: list[object] = []
    role_id_strs: list[str] = []
    for rid in role_ids:
        try:
            u = _UUID(str(rid))
            role_uuid_ids.append(u)
            role_id_strs.append(str(u))
        except Exception:
            continue
    if not role_uuid_ids:
        return []

    cache_key = f"{','.join(sorted(role_id_strs))}|{str(location_id or '')}"
    now = time.time()
    cached = _staff_list_cache.get(cache_key)
    if cached and cached[0] > now:
        return cached[1]

    from sqlalchemy import and_ as _and, or_ as _or, select as _select
    from app.core.db import SessionLocal, tables

    users_t = tables["users"]
    profiles_t = tables.get("profiles")
    user_roles_t = tables["user_roles"]
    roles_t = tables["roles"]
    staff_locations_t = tables.get("staff_locations")
    locations_t = tables.get("locations")
    if profiles_t is None or staff_locations_t is None or locations_t is None:
        return []

    loc_uuid = None
    if location_id:
        try:
            loc_uuid = _UUID(str(location_id))
        except Exception:
            loc_uuid = None

    prof_loc = locations_t.alias("prof_loc")
    staff_loc = locations_t.alias("staff_loc")

    j = (
        users_t.join(user_roles_t, user_roles_t.c.user_id == users_t.c.id)
        .join(roles_t, roles_t.c.id == user_roles_t.c.role_id)
        .outerjoin(profiles_t, profiles_t.c.user_id == users_t.c.id)
        .outerjoin(
            staff_locations_t,
            _and(
                staff_locations_t.c.user_id == users_t.c.id,
                staff_locations_t.c.role_id == user_roles_t.c.role_id,
            ),
        )
        .outerjoin(staff_loc, staff_loc.c.id == staff_locations_t.c.location_id)
        .outerjoin(prof_loc, prof_loc.c.id == profiles_t.c.location_id)
    )

    cols = [
        users_t.c.id.label("user_id"),
        users_t.c.email,
        profiles_t.c.full_name,
        profiles_t.c.phone,
        profiles_t.c.location_id.label("profile_location_id"),
        prof_loc.c.name.label("profile_location_name"),
        staff_locations_t.c.location_id.label("staff_location_id"),
        staff_loc.c.name.label("staff_location_name"),
        roles_t.c.code.label("role_code"),
    ]

    stmt = _select(*cols).select_from(j).where(roles_t.c.id.in_(role_uuid_ids)).limit(10000)
    if loc_uuid is not None:
        # Для секретарей (особенно глобальных соревнований) loc_uuid может быть передан как фильтр,
        # но если мы хотим видеть ВСЕХ глобальных секретарей, нужно убедиться, что они подтягиваются.
        # В данном случае фильтруем жестко по локации, если она передана
        stmt = stmt.where(
            _or_(
                staff_locations_t.c.location_id == loc_uuid,
                profiles_t.c.location_id == loc_uuid,
            )
        )

    async with SessionLocal() as session:
        res = await session.execute(stmt)
        rows = [dict(r) for r in res.mappings().all()]

    users_dict: dict[str, dict[str, object]] = {}
    for r in rows:
        uid = str(r.get("user_id") or "")
        if not uid:
            continue
        if uid not in users_dict:
            users_dict[uid] = {
                "user_id": uid,
                "email": r.get("email"),
                "full_name": r.get("full_name"),
                "phone": r.get("phone"),
                "roles": set(),
                "location_id": None,
                "location_name": None,
            }

        role_code = r.get("role_code")
        if role_code:
            users_dict[uid]["roles"].add(str(role_code))

        prof_loc_id = r.get("profile_location_id")
        prof_loc_name = r.get("profile_location_name")
        staff_loc_id = r.get("staff_location_id")
        staff_loc_name = r.get("staff_location_name")

        if prof_loc_id and prof_loc_name:
            users_dict[uid]["location_id"] = str(prof_loc_id)
            users_dict[uid]["location_name"] = str(prof_loc_name)
        elif staff_loc_id and staff_loc_name and not users_dict[uid].get("location_id"):
            users_dict[uid]["location_id"] = str(staff_loc_id)
            users_dict[uid]["location_name"] = str(staff_loc_name)

    result = [
        UserProfile(
            user_id=u["user_id"],
            full_name=u.get("full_name"),
            phone=u.get("phone"),
            email=u.get("email"),
            roles=sorted(list(u.get("roles") or [])),
            location_id=u.get("location_id"),
            location_name=u.get("location_name"),
        )
        for u in users_dict.values()
    ]

    result.sort(key=lambda item: ((item.full_name or "").lower(), (item.email or "").lower(), str(item.user_id)))
    _staff_list_cache[cache_key] = (now + 10.0, result)
    return result


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
    user_id = str(uuid4())

    await rest_upsert("users", {"id": user_id, "email": email}, on_conflict="id")
    await rest_upsert("profiles", {"user_id": user_id, "full_name": body.full_name}, on_conflict="user_id")
    ok_pwd = await set_user_password(user_id, password)
    if not ok_pwd:
        raise HTTPException(status_code=500, detail="Local auth is not configured (apply backend/sql/local_auth.sql)")

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
    try:
        await rest_upsert("profiles", payload, on_conflict="user_id")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to update profile: {repr(e)}")
    return payload | {"user_id": user_id}

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
    user_id = await _get_user_id_from_bearer(authorization)
    await _require_can_edit_self(user_id, authorization)
    try:
        await rest_upsert("athletes", {"user_id": user_id, "coach_name": coach_name}, on_conflict="user_id")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to update athlete: {repr(e)}")
    return {"user_id": user_id, "coach_name": coach_name}

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

    try:
        await _execute(admin_supabase.table("profiles").upsert(profile_payload, on_conflict="user_id"))
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"Profiles upsert failed: {e.detail}")
    try:
        await rest_upsert("profiles", profile_payload, on_conflict="user_id")
    except Exception:
        pass

    athlete_id = None
    athlete_q = admin_supabase.table("athletes").select("id,coach_name").eq("user_id", user_id).maybe_single()
    try:
        athlete_res = await _execute(athlete_q)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"Athlete lookup failed: {e.detail}")
    athlete_row = _safe_data(athlete_res)
    if isinstance(athlete_row, dict) and athlete_row.get("id"):
        athlete_id = str(athlete_row["id"])
        if coach_name is not None:
            try:
                await _execute(
                    admin_supabase.table("athletes")
                    .update({"coach_name": coach_name})
                    .eq("id", athlete_id)
                )
            except HTTPException as e:
                raise HTTPException(status_code=e.status_code, detail=f"Athletes update failed: {e.detail}")
    else:
        insert_payload = {"id": str(uuid4()), "user_id": user_id, "coach_name": coach_name}
        try:
            await _execute(admin_supabase.table("athletes").insert(insert_payload))
        except HTTPException as e:
            detail = str(e.detail)
            lowered = detail.lower()
            if "integrityerror" not in lowered and "duplicate" not in lowered and "unique" not in lowered:
                raise HTTPException(status_code=e.status_code, detail=f"Athletes insert failed: {e.detail}")
        athlete_q = admin_supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single()
        try:
            athlete_res = await _execute(athlete_q)
        except HTTPException as e:
            raise HTTPException(status_code=e.status_code, detail=f"Athlete lookup failed: {e.detail}")
        athlete_row = _safe_data(athlete_res)
        if not athlete_row:
            raise HTTPException(status_code=500, detail="Athlete create failed")
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
        from app.core.minio import put_object

        final_photo_url = await put_object(
            object_path,
            content,
            content_type=photo.content_type or "application/octet-stream",
        )

    passport_payload: dict[str, object] = {"athlete_id": athlete_id}
    if birth_date is not None:
        bd_str = str(birth_date).strip()
        if bd_str:
            try:
                import datetime
                dt = datetime.date.fromisoformat(bd_str)
                passport_payload["birth_date"] = dt
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid birth_date format, expected YYYY-MM-DD")
        else:
            passport_payload["birth_date"] = None
    if gender is not None:
        passport_payload["gender"] = str(gender).strip() or None
    if rank is not None:
        passport_payload["rank"] = str(rank).strip() or None
    if final_photo_url is not None:
        passport_payload["photo_url"] = final_photo_url

    try:
        await admin_supabase.table("passports").upsert(passport_payload, on_conflict="athlete_id").execute_async()
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
                "select": "id,status,draw_number,created_at,category_id,competition_id",
                "athlete_id": f"eq.{athlete_id}",
                "order": "created_at.desc",
                "limit": "1000",
            },
            write=True,
        )
        rows = apps_resp.json()
        if not isinstance(rows, list):
            return []

        cat_ids = {str(r.get("category_id")) for r in rows if r.get("category_id")}
        comp_ids = {str(r.get("competition_id")) for r in rows if r.get("competition_id")}

        cats_map = {}
        if cat_ids:
            from sqlalchemy import select as _select
            from app.core.db import SessionLocal, tables
            cats_t = tables.get("competition_categories")
            if cats_t is not None:
                async with SessionLocal() as session:
                    cat_res = await session.execute(_select(cats_t).where(cats_t.c.id.in_(list(cat_ids))))
                    for r in cat_res.mappings().all():
                        cats_map[str(r.get("id"))] = dict(r)

        comps_map = {}
        if comp_ids:
            from sqlalchemy import select as _select
            from app.core.db import SessionLocal, tables
            comps_t = tables.get("competitions")
            if comps_t is not None:
                async with SessionLocal() as session:
                    c_res = await session.execute(_select(comps_t.c.id, comps_t.c.name, comps_t.c.start_date).where(comps_t.c.id.in_(list(comp_ids))))
                    for r in c_res.mappings().all():
                        comps_map[str(r.get("id"))] = dict(r)

        for app in rows:
            cat_id = str(app.get("category_id")) if app.get("category_id") else ""
            comp_id = str(app.get("competition_id")) if app.get("competition_id") else ""
            app["competition_categories"] = cats_map.get(cat_id)
            app["competitions"] = comps_map.get(comp_id)

        return rows
    except Exception as e:
        print(f"Error in get_my_applications: {e}")
        return []

@router.post("/admin-create/", response_model=UserProfile)
@router.post("/admin-create", response_model=UserProfile)
async def create_admin_user(payload: AdminCreate):
    is_admin = any("admin" in code for code in payload.role_codes)
    is_secretary = any("secretary" in code for code in payload.role_codes)
    if is_admin and is_secretary:
        raise HTTPException(status_code=400, detail="Администратор не может быть секретарем")

    if (is_admin or is_secretary) and not payload.location_id:
        raise HTTPException(status_code=400, detail="Для админов/секретарей нужна привязка к локации")

    user_id = str(uuid4())

    try:
        await rest_upsert("users", {"id": user_id, "email": payload.email}, on_conflict="id")
        ok_pwd = await set_user_password(user_id, payload.password)
        if not ok_pwd:
            raise HTTPException(status_code=500, detail="Local auth is not configured (apply backend/sql/local_auth.sql)")
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
    await _require_staff(authorization)

    limit = max(1, min(int(limit), 500))
    from sqlalchemy import select as _select
    from app.core.db import SessionLocal, tables

    athletes_t = tables["athletes"]
    users_t = tables["users"]
    profiles_t = tables["profiles"]
    passports_t = tables.get("passports")

    async with SessionLocal() as session:
        j = athletes_t.join(users_t, athletes_t.c.user_id == users_t.c.id).outerjoin(
            profiles_t, profiles_t.c.user_id == users_t.c.id
        )
        if passports_t is not None:
            j = j.outerjoin(passports_t, passports_t.c.athlete_id == athletes_t.c.id)

        cols = [
            athletes_t.c.id.label("athlete_id"),
            athletes_t.c.user_id,
            athletes_t.c.coach_name,
            users_t.c.email,
            profiles_t.c.full_name,
            profiles_t.c.phone,
            profiles_t.c.city,
            profiles_t.c.location_id,
        ]
        if passports_t is not None:
            cols.extend(
                [
                    passports_t.c.birth_date,
                    passports_t.c.gender,
                    passports_t.c.rank,
                    passports_t.c.photo_url,
                ]
            )

        stmt = _select(*cols).select_from(j).limit(limit)
        if query:
            stmt = stmt.where(profiles_t.c.full_name.ilike(f"%{query}%"))

        res = await session.execute(stmt)
        rows = [dict(r) for r in res.mappings().all()]

    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "athlete_id": r.get("athlete_id"),
                "user_id": r.get("user_id"),
                "full_name": r.get("full_name"),
                "phone": r.get("phone"),
                "city": r.get("city"),
                "location_id": r.get("location_id"),
                "email": r.get("email"),
                "coach_name": r.get("coach_name"),
                "birth_date": r.get("birth_date"),
                "gender": r.get("gender"),
                "rank": r.get("rank"),
                "photo_url": r.get("photo_url"),
            }
        )
    return out


@router.get("/athletes/{athlete_id}")
async def get_athlete_details(
    athlete_id: UUID,
    authorization: str | None = Header(default=None),
):
    await _require_staff(authorization)
    from sqlalchemy import select as _select
    from app.core.db import SessionLocal, tables

    athletes_t = tables["athletes"]
    users_t = tables["users"]
    profiles_t = tables["profiles"]
    passports_t = tables.get("passports")
    registrations_t = tables.get("registrations")

    async with SessionLocal() as session:
        j = athletes_t.join(users_t, athletes_t.c.user_id == users_t.c.id).outerjoin(
            profiles_t, profiles_t.c.user_id == users_t.c.id
        )
        if passports_t is not None:
            j = j.outerjoin(passports_t, passports_t.c.athlete_id == athletes_t.c.id)
        if registrations_t is not None:
            j = j.outerjoin(registrations_t, registrations_t.c.user_id == users_t.c.id)

        cols = [
            athletes_t.c.id.label("athlete_id"),
            athletes_t.c.user_id,
            athletes_t.c.coach_name,
            profiles_t.c.full_name,
            profiles_t.c.phone,
            profiles_t.c.city,
            profiles_t.c.location_id,
        ]
        if passports_t is not None:
            cols.extend(
                [
                    passports_t.c.birth_date,
                    passports_t.c.gender,
                    passports_t.c.rank,
                    passports_t.c.photo_url,
                ]
            )
        if registrations_t is not None:
            cols.append(registrations_t.c.stage)

        stmt = _select(*cols).select_from(j).where(athletes_t.c.id == athlete_id).limit(1)
        res = await session.execute(stmt)
        row = res.mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Athlete not found")
    stage = str(row.get("stage") or "start")
    return {
        "athlete_id": str(row.get("athlete_id")),
        "user_id": str(row.get("user_id")),
        "full_name": row.get("full_name"),
        "phone": row.get("phone"),
        "city": row.get("city"),
        "location_id": row.get("location_id"),
        "coach_name": row.get("coach_name"),
        "birth_date": row.get("birth_date"),
        "gender": row.get("gender"),
        "rank": row.get("rank"),
        "photo_url": row.get("photo_url"),
        "stage": stage,
        "locked": bool(stage == "complete"),
    }


@router.put("/athletes/{athlete_id}")
async def update_athlete_details(
    athlete_id: UUID,
    body: AdminAthleteUpdate,
    authorization: str | None = Header(default=None),
):
    await _require_staff(authorization)
    from sqlalchemy import select as _select, update as _update
    from sqlalchemy.dialects.postgresql import insert as _pg_insert
    from app.core.db import SessionLocal, tables

    athletes_t = tables["athletes"]
    profiles_t = tables["profiles"]
    passports_t = tables.get("passports")

    async with SessionLocal() as session:
        res = await session.execute(
            _select(athletes_t.c.user_id).where(athletes_t.c.id == athlete_id).limit(1)
        )
        row = res.mappings().first()
        if not row or not row.get("user_id"):
            raise HTTPException(status_code=404, detail="Athlete not found")
        user_id = row["user_id"]

        prof_payload: dict[str, object] = {"user_id": user_id}
        if body.full_name is not None:
            prof_payload["full_name"] = body.full_name
        if body.phone is not None:
            prof_payload["phone"] = body.phone
        if body.city is not None:
            prof_payload["city"] = body.city
        if body.location_id is not None:
            prof_payload["location_id"] = body.location_id

        if len(prof_payload) > 1:
            stmt = _pg_insert(profiles_t).values(prof_payload)
            stmt = stmt.on_conflict_do_update(
                index_elements=[profiles_t.c.user_id],
                set_={k: stmt.excluded[k] for k in prof_payload.keys() if k != "user_id"},
            )
            await session.execute(stmt)

        if body.coach_name is not None:
            await session.execute(
                _update(athletes_t)
                .where(athletes_t.c.id == athlete_id)
                .values({"coach_name": body.coach_name})
            )

        if passports_t is not None:
            pass_payload: dict[str, object] = {"athlete_id": athlete_id}
            if body.birth_date is not None:
                pass_payload["birth_date"] = body.birth_date
            if body.gender is not None:
                pass_payload["gender"] = body.gender
            if body.series is not None:
                pass_payload["series"] = body.series
            if body.number is not None:
                pass_payload["number"] = body.number
            if body.issued_by is not None:
                pass_payload["issued_by"] = body.issued_by
            if body.issue_date is not None:
                pass_payload["issue_date"] = body.issue_date
            if body.rank is not None:
                pass_payload["rank"] = body.rank
            if body.photo_url is not None:
                pass_payload["photo_url"] = body.photo_url
            if body.passport_scan_url is not None:
                pass_payload["passport_scan_url"] = body.passport_scan_url

            if len(pass_payload) > 1:
                stmt = _pg_insert(passports_t).values(pass_payload)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[passports_t.c.athlete_id],
                    set_={k: stmt.excluded[k] for k in pass_payload.keys() if k != "athlete_id"},
                )
                await session.execute(stmt)

        await session.commit()

    return {"ok": True}


@router.post("/athletes/{athlete_id}/editable")
async def set_athlete_editable(
    athlete_id: UUID,
    body: EditableUpdate,
    authorization: str | None = Header(default=None),
):
    await _require_staff(authorization)
    from sqlalchemy import select as _select
    from sqlalchemy.dialects.postgresql import insert as _pg_insert
    from app.core.db import SessionLocal, tables

    athletes_t = tables["athletes"]
    registrations_t = tables.get("registrations")

    async with SessionLocal() as session:
        res = await session.execute(
            _select(athletes_t.c.user_id).where(athletes_t.c.id == athlete_id).limit(1)
        )
        row = res.mappings().first()
        if not row or not row.get("user_id"):
            raise HTTPException(status_code=404, detail="Athlete not found")
        user_id = row["user_id"]

        stage = "start" if body.editable else "complete"
        if registrations_t is not None:
            payload = {"user_id": user_id, "stage": stage}
            stmt = _pg_insert(registrations_t).values(payload)
            stmt = stmt.on_conflict_do_update(
                index_elements=[registrations_t.c.user_id],
                set_={"stage": stmt.excluded["stage"]},
            )
            await session.execute(stmt)
            await session.commit()

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
    return await _list_user_profiles_by_role_ids(
        role_ids,
        location_id=str(location_id) if location_id else None,
    )


@router.delete("/{user_id}/")
@router.delete("/{user_id}")
async def delete_user(user_id: UUID):
    try:
        uid = str(user_id)
        ath_resp = await rest_get(
            "athletes",
            {"select": "id", "user_id": f"eq.{uid}", "limit": "1000"},
            write=True,
        )
        ath_rows = ath_resp.json()
        athlete_ids = [str(r.get("id")) for r in ath_rows if isinstance(r, dict) and r.get("id")] if isinstance(ath_rows, list) else []

        if athlete_ids:
            await rest_delete("passports", {"athlete_id": _pg_in(athlete_ids)})
            await rest_delete("applications", {"athlete_id": _pg_in(athlete_ids)})
            await rest_delete("athlete_coaches", {"athlete_id": _pg_in(athlete_ids)})

        await rest_delete("staff_locations", {"user_id": f"eq.{uid}"})
        await rest_delete("user_roles", {"user_id": f"eq.{uid}"})
        await rest_delete("registrations", {"user_id": f"eq.{uid}"})
        await rest_delete("profiles", {"user_id": f"eq.{uid}"})
        await rest_delete("athletes", {"user_id": f"eq.{uid}"})
        await rest_delete("users", {"id": f"eq.{uid}"})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
        
    return {"message": "User deleted successfully"}

@router.get("/admins", response_model=List[UserProfile])
async def get_admins():
    global _admins_cache
    now = time.time()
    if _admins_cache and _admins_cache[0] > now:
        return _admins_cache[1]

    admin_codes = [
        "world_admin",
        "world_secretary",
        "country_admin",
        "country_secretary",
        "region_admin",
        "region_secretary",
    ]

    from sqlalchemy import Text as _Text, and_ as _and, cast as _cast, exists as _exists, func as _func, select as _select
    from app.core.db import SessionLocal, tables

    users_t = tables.get("users")
    profiles_t = tables.get("profiles")
    user_roles_t = tables.get("user_roles")
    roles_t = tables.get("roles")
    staff_locations_t = tables.get("staff_locations")
    locations_t = tables.get("locations")
    if (
        users_t is None
        or profiles_t is None
        or user_roles_t is None
        or roles_t is None
        or staff_locations_t is None
        or locations_t is None
    ):
        return []

    prof_loc = locations_t.alias("prof_loc")
    staff_loc = locations_t.alias("staff_loc")

    founder_exists = _exists(
        _select(1)
        .select_from(user_roles_t.join(roles_t, roles_t.c.id == user_roles_t.c.role_id))
        .where(
            _and(
                user_roles_t.c.user_id == users_t.c.id,
                roles_t.c.code == "founder",
            )
        )
    )

    staff_join = staff_locations_t.join(staff_loc, staff_loc.c.id == staff_locations_t.c.location_id)

    j = (
        users_t.join(user_roles_t, user_roles_t.c.user_id == users_t.c.id)
        .join(roles_t, roles_t.c.id == user_roles_t.c.role_id)
        .outerjoin(profiles_t, profiles_t.c.user_id == users_t.c.id)
        .outerjoin(prof_loc, prof_loc.c.id == profiles_t.c.location_id)
        .outerjoin(
            staff_join,
            _and(
                staff_locations_t.c.user_id == users_t.c.id,
                staff_locations_t.c.role_id == user_roles_t.c.role_id,
            ),
        )
    )

    staff_location_id = _func.max(_cast(staff_locations_t.c.location_id, _Text))
    staff_location_name = _func.max(staff_loc.c.name)

    stmt = (
        _select(
            users_t.c.id.label("user_id"),
            users_t.c.email,
            profiles_t.c.full_name,
            profiles_t.c.phone,
            _func.coalesce(_cast(profiles_t.c.location_id, _Text), staff_location_id).label("location_id"),
            _func.coalesce(prof_loc.c.name, staff_location_name).label("location_name"),
            _func.array_agg(_func.distinct(roles_t.c.code)).label("roles"),
        )
        .select_from(j)
        .where(roles_t.c.code.in_(admin_codes))
        .where(~founder_exists)
        .group_by(
            users_t.c.id,
            users_t.c.email,
            profiles_t.c.full_name,
            profiles_t.c.phone,
            profiles_t.c.location_id,
            prof_loc.c.name,
        )
        .limit(10000)
    )

    async with SessionLocal() as session:
        res = await session.execute(stmt)
        rows = [dict(r) for r in res.mappings().all()]

    out: list[UserProfile] = []
    for r in rows:
        roles = r.get("roles") or []
        role_list = [str(x) for x in roles] if isinstance(roles, list) else []
        out.append(
            UserProfile(
                user_id=str(r.get("user_id")),
                full_name=r.get("full_name"),
                phone=r.get("phone"),
                email=r.get("email"),
                roles=sorted(role_list),
                location_id=str(r.get("location_id")) if r.get("location_id") else None,
                location_name=r.get("location_name"),
            )
        )

    out.sort(key=lambda item: ((item.full_name or "").lower(), (item.email or "").lower(), str(item.user_id)))
    _admins_cache = (now + 10.0, out)
    return out
