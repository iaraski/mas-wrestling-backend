import time
from typing import List, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from app.authorization import require_staff_user_id
from app.core.rest import rest_delete, rest_get, rest_post
from app.schemas.user import Role, RoleAssign, UserProfile


router = APIRouter(prefix="/users", tags=["users"])

_staff_list_cache: dict[str, tuple[float, list[UserProfile]]] = {}


def _pg_in(ids: list[str]) -> str:
    return f"in.({','.join(ids)})"


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

        users.append(
            UserProfile(
                user_id=u["id"],
                full_name=profile.get("full_name") if profile else None,
                phone=profile.get("phone") if profile else None,
                email=u.get("email"),
                roles=roles,
                location_id=loc_id,
                location_name=loc_name,
            )
        )

    return users


@router.post("/{user_id}/roles", response_model=UserProfile)
async def assign_roles(user_id: UUID, role_in: RoleAssign, _: str = Depends(require_staff_user_id)):
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
        location_name=loc_name,
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


@router.get("/admins")
async def get_admins(
    query: str | None = None,
    role_code: str | None = None,
    location_id: Optional[UUID] = None,
    sort_by: Literal["full_name", "email", "location_name"] = "full_name",
    sort_dir: Literal["asc", "desc"] = "asc",
    limit: int = 20,
    offset: int = 0,
):
    query_norm = str(query or "").strip()
    role_code_norm = str(role_code or "").strip()
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    admin_codes = [
        "world_admin",
        "world_secretary",
        "country_admin",
        "country_secretary",
        "region_admin",
        "region_secretary",
    ]

    from sqlalchemy import (
        Text as _Text,
        and_ as _and,
        cast as _cast,
        exists as _exists,
        func as _func,
        or_ as _or,
        select as _select,
    )
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
        return {"items": [], "has_more": False, "next_offset": None}

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
    effective_location_id = _func.coalesce(_cast(profiles_t.c.location_id, _Text), staff_location_id)
    effective_location_name = _func.coalesce(prof_loc.c.name, staff_location_name)

    role_filter_exists = None
    if role_code_norm:
        role_filter_exists = _exists(
            _select(1)
            .select_from(user_roles_t.join(roles_t, roles_t.c.id == user_roles_t.c.role_id))
            .where(
                _and(
                    user_roles_t.c.user_id == users_t.c.id,
                    roles_t.c.code == role_code_norm,
                )
            )
        )

    stmt = (
        _select(
            users_t.c.id.label("user_id"),
            users_t.c.email,
            profiles_t.c.full_name,
            profiles_t.c.phone,
            effective_location_id.label("location_id"),
            effective_location_name.label("location_name"),
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
    )

    if query_norm:
        ilike_pattern = f"%{query_norm}%"
        stmt = stmt.where(
            _or_(
                profiles_t.c.full_name.ilike(ilike_pattern),
                users_t.c.email.ilike(ilike_pattern),
                profiles_t.c.phone.ilike(ilike_pattern),
            )
        )

    if role_filter_exists is not None:
        stmt = stmt.where(role_filter_exists)

    if location_id is not None:
        stmt = stmt.where(
            _or_(
                profiles_t.c.location_id == location_id,
                staff_locations_t.c.location_id == location_id,
            )
        )

    sort_column_map = {
        "full_name": _func.lower(_func.coalesce(profiles_t.c.full_name, users_t.c.email, "")),
        "email": _func.lower(_func.coalesce(users_t.c.email, "")),
        "location_name": _func.lower(_func.coalesce(effective_location_name, "")),
    }
    sort_expr = sort_column_map.get(sort_by, sort_column_map["full_name"])
    if sort_dir == "desc":
        stmt = stmt.order_by(sort_expr.desc(), users_t.c.id.desc())
    else:
        stmt = stmt.order_by(sort_expr.asc(), users_t.c.id.asc())

    stmt = stmt.offset(offset).limit(limit + 1)

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

    has_more = len(out) > limit
    items = out[:limit]

    return {
        "items": items,
        "has_more": has_more,
        "next_offset": (offset + limit) if has_more else None,
    }
