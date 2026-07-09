from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.authorization import get_role_codes_safe, is_staff_role
from app.core.db import SessionLocal, tables


GLOBAL_COMPETITION_ROLE_CODES = frozenset({"admin", "founder"})
COUNTRY_COMPETITION_ROLE_CODES = frozenset({"country_admin", "country_secretary"})
REGION_COMPETITION_ROLE_CODES = frozenset({"region_admin", "region_secretary"})


@dataclass
class CompetitionAccessContext:
    user_id: str
    role_codes: list[str]
    primary_role: str
    scope_kind: str
    is_staff: bool
    staff_location_id: str | None
    staff_location_path: dict[str, str | None]
    profile_location_id: str | None
    profile_location_path: dict[str, str | None]

    @property
    def country_id(self) -> str | None:
        return self.staff_location_path.get("country_id") or self.profile_location_path.get("country_id")

    @property
    def region_id(self) -> str | None:
        return self.staff_location_path.get("region_id") or self.profile_location_path.get("region_id")


async def _fetch_location_row(location_id: str | None, *, session: AsyncSession | None = None) -> dict[str, Any] | None:
    if not location_id:
        return None
    locations_t = tables.get("locations")
    if locations_t is None:
        return None

    from sqlalchemy import select as _select

    stmt = _select(locations_t.c.id, locations_t.c.type, locations_t.c.parent_id).where(locations_t.c.id == location_id)
    if session is not None:
        res = await session.execute(stmt)
        row = res.mappings().first()
    else:
        async with SessionLocal() as own_session:
            res = await own_session.execute(stmt)
            row = res.mappings().first()
    return dict(row) if row else None


async def resolve_competition_location_path(
    location_id: str | None,
    *,
    session: AsyncSession | None = None,
) -> dict[str, str | None]:
    if not location_id:
        return {"country_id": None, "district_id": None, "region_id": None}

    loc = await _fetch_location_row(location_id, session=session)
    if not loc:
        return {"country_id": None, "district_id": None, "region_id": None}

    loc_type = str(loc.get("type") or "")
    current_id = str(loc.get("id") or "")

    if loc_type == "country":
        return {"country_id": current_id or None, "district_id": None, "region_id": None}
    if loc_type == "district":
        parent_id = str(loc.get("parent_id")) if loc.get("parent_id") else None
        return {"country_id": parent_id, "district_id": current_id or None, "region_id": None}
    if loc_type != "region":
        return {"country_id": None, "district_id": None, "region_id": None}

    district_id = str(loc.get("parent_id")) if loc.get("parent_id") else None
    country_id = None
    if district_id:
        district = await _fetch_location_row(district_id, session=session)
        if district and district.get("parent_id"):
            country_id = str(district["parent_id"])
    return {"country_id": country_id, "district_id": district_id, "region_id": current_id or None}


async def _get_profile_location_id(user_id: str, *, session: AsyncSession | None = None) -> str | None:
    profiles_t = tables.get("profiles")
    if profiles_t is None:
        return None

    from sqlalchemy import select as _select

    stmt = _select(profiles_t.c.location_id).where(profiles_t.c.user_id == user_id).limit(1)
    if session is not None:
        res = await session.execute(stmt)
        row = res.mappings().first()
    else:
        async with SessionLocal() as own_session:
            res = await own_session.execute(stmt)
            row = res.mappings().first()
    if row and row.get("location_id"):
        return str(row["location_id"])
    return None


async def _get_staff_role_assignments(
    user_id: str,
    *,
    session: AsyncSession | None = None,
) -> list[dict[str, str | None]]:
    user_roles_t = tables.get("user_roles")
    roles_t = tables.get("roles")
    staff_locations_t = tables.get("staff_locations")
    if user_roles_t is None or roles_t is None or staff_locations_t is None:
        return []

    from sqlalchemy import and_ as _and, select as _select

    stmt = (
        _select(
            roles_t.c.code.label("role_code"),
            staff_locations_t.c.location_id.label("location_id"),
        )
        .select_from(
            user_roles_t.join(roles_t, roles_t.c.id == user_roles_t.c.role_id).outerjoin(
                staff_locations_t,
                _and(
                    staff_locations_t.c.user_id == user_roles_t.c.user_id,
                    staff_locations_t.c.role_id == user_roles_t.c.role_id,
                ),
            )
        )
        .where(user_roles_t.c.user_id == user_id)
        .limit(1000)
    )

    if session is not None:
        res = await session.execute(stmt)
        rows = res.mappings().all()
    else:
        async with SessionLocal() as own_session:
            res = await own_session.execute(stmt)
            rows = res.mappings().all()
    return [
        {
            "role_code": str(r.get("role_code") or ""),
            "location_id": str(r.get("location_id")) if r.get("location_id") else None,
        }
        for r in rows
    ]


def _pick_scope_assignment(assignments: list[dict[str, str | None]]) -> tuple[str | None, str]:
    if any(a.get("role_code") in GLOBAL_COMPETITION_ROLE_CODES for a in assignments):
        return None, "global"

    for role_code in COUNTRY_COMPETITION_ROLE_CODES:
        for assignment in assignments:
            if assignment.get("role_code") == role_code and assignment.get("location_id"):
                return assignment["location_id"], "country"

    for role_code in REGION_COMPETITION_ROLE_CODES:
        for assignment in assignments:
            if assignment.get("role_code") == role_code and assignment.get("location_id"):
                return assignment["location_id"], "region"

    for assignment in assignments:
        if assignment.get("location_id"):
            return assignment["location_id"], "region"

    return None, "athlete"


async def get_user_competition_access_context(
    user_id: str,
    *,
    role_codes: list[str] | None = None,
    session: AsyncSession | None = None,
) -> CompetitionAccessContext:
    role_codes = list(role_codes) if role_codes is not None else await get_role_codes_safe(user_id, session=session)
    is_staff = is_staff_role(role_codes)
    assignments: list[dict[str, str | None]] = []
    staff_location_id: str | None = None
    scope_kind = "athlete"

    if any(code in GLOBAL_COMPETITION_ROLE_CODES for code in role_codes):
        primary_role = "admin"
    elif any(code in COUNTRY_COMPETITION_ROLE_CODES or code in REGION_COMPETITION_ROLE_CODES for code in role_codes):
        primary_role = "staff"
    elif any("secretary" in str(code) for code in role_codes):
        primary_role = "secretary"
    else:
        primary_role = "athlete"

    if session is not None:
        working_session = session
        owns_session = False
    else:
        working_session = SessionLocal()
        owns_session = True

    try:
        if is_staff:
            assignments = await _get_staff_role_assignments(user_id, session=working_session)
            staff_location_id, scope_kind = _pick_scope_assignment(assignments)

        staff_location_path = await resolve_competition_location_path(staff_location_id, session=working_session)
        profile_location_id = await _get_profile_location_id(user_id, session=working_session)
        profile_location_path = await resolve_competition_location_path(profile_location_id, session=working_session)
    finally:
        if owns_session:
            await working_session.close()

    return CompetitionAccessContext(
        user_id=user_id,
        role_codes=role_codes,
        primary_role=primary_role,
        scope_kind=scope_kind,
        is_staff=is_staff,
        staff_location_id=staff_location_id,
        staff_location_path=staff_location_path,
        profile_location_id=profile_location_id,
        profile_location_path=profile_location_path,
    )


def get_allowed_competition_scales(ctx: CompetitionAccessContext) -> list[str]:
    if not ctx.is_staff:
        return []
    if ctx.scope_kind == "global":
        return ["world", "country", "region"]
    if ctx.scope_kind == "country":
        return ["country", "region"]
    if ctx.scope_kind == "region":
        return ["region"]
    return []


def _is_same_country(ctx: CompetitionAccessContext, competition_path: dict[str, str | None]) -> bool:
    return bool(ctx.country_id and competition_path.get("country_id") == ctx.country_id)


def _is_same_region(ctx: CompetitionAccessContext, competition_location_id: str | None) -> bool:
    return bool(ctx.region_id and competition_location_id and competition_location_id == ctx.region_id)


def _can_access_country_competition(
    ctx: CompetitionAccessContext,
    competition_location_id: str | None,
    competition_path: dict[str, str | None],
) -> bool:
    if _is_same_country(ctx, competition_path):
        return True
    if ctx.country_id and competition_location_id == ctx.country_id:
        return True
    # Backward compatibility for old country-level competitions created without a country location.
    if ctx.country_id and not competition_location_id:
        return True
    return False


async def _competition_path_cached(
    competition: dict[str, Any],
    *,
    path_cache: dict[str, dict[str, str | None]],
) -> dict[str, str | None]:
    location_id = str(competition.get("location_id") or "") or None
    if not location_id:
        return {"country_id": None, "district_id": None, "region_id": None}
    if location_id not in path_cache:
        path_cache[location_id] = await resolve_competition_location_path(location_id)
    return path_cache[location_id]


async def can_view_competition(
    competition: dict[str, Any],
    ctx: CompetitionAccessContext,
    *,
    path_cache: dict[str, dict[str, str | None]],
) -> bool:
    scale = str(competition.get("scale") or "")
    location_id = str(competition.get("location_id") or "") or None
    competition_path = await _competition_path_cached(competition, path_cache=path_cache)

    if ctx.scope_kind == "global":
        return True
    if scale == "world":
        return True
    if ctx.scope_kind == "country":
        if scale == "country":
            return _can_access_country_competition(ctx, location_id, competition_path)
        if scale == "region":
            return _is_same_country(ctx, competition_path)
        return False
    if ctx.scope_kind == "region":
        if scale == "country":
            return _can_access_country_competition(ctx, location_id, competition_path)
        if scale == "region":
            return _is_same_region(ctx, location_id)
        return False

    if scale == "country":
        return _can_access_country_competition(ctx, location_id, competition_path)
    if scale == "region":
        return _is_same_region(ctx, location_id)
    return False


async def can_edit_competition(
    competition: dict[str, Any],
    ctx: CompetitionAccessContext,
    *,
    path_cache: dict[str, dict[str, str | None]],
) -> bool:
    if not ctx.is_staff:
        return False

    scale = str(competition.get("scale") or "")
    location_id = str(competition.get("location_id") or "") or None
    competition_path = await _competition_path_cached(competition, path_cache=path_cache)

    if ctx.scope_kind == "global":
        return True
    if ctx.scope_kind == "country":
        if scale == "country":
            return _can_access_country_competition(ctx, location_id, competition_path)
        if scale == "region":
            return _is_same_country(ctx, competition_path)
        return False
    if ctx.scope_kind == "region":
        return scale == "region" and _is_same_region(ctx, location_id)
    return False


async def apply_competition_access(
    competition: dict[str, Any],
    ctx: CompetitionAccessContext,
    *,
    path_cache: dict[str, dict[str, str | None]] | None = None,
) -> dict[str, Any] | None:
    cache = path_cache if path_cache is not None else {}
    if not await can_view_competition(competition, ctx, path_cache=cache):
        return None

    can_edit = await can_edit_competition(competition, ctx, path_cache=cache)
    can_apply = not can_edit
    return {
        **competition,
        "can_edit": can_edit,
        "can_apply": can_apply,
        "access_scope": ctx.scope_kind,
    }


async def filter_competitions_for_user(
    competitions: list[dict[str, Any]],
    ctx: CompetitionAccessContext,
) -> list[dict[str, Any]]:
    path_cache: dict[str, dict[str, str | None]] = {}
    result: list[dict[str, Any]] = []
    for competition in competitions:
        decorated = await apply_competition_access(competition, ctx, path_cache=path_cache)
        if decorated is not None:
            result.append(decorated)
    return result


async def require_can_create_competition(ctx: CompetitionAccessContext, payload: dict[str, Any]) -> None:
    if not ctx.is_staff:
        raise HTTPException(status_code=403, detail="Forbidden")

    scale = str(payload.get("scale") or "")
    location_id = str(payload.get("location_id") or "") or None
    location_path = await resolve_competition_location_path(location_id)

    if ctx.scope_kind == "global":
        return
    if ctx.scope_kind == "country":
        if scale == "country" and _can_access_country_competition(ctx, location_id, location_path):
            return
        if scale == "region" and _is_same_country(ctx, location_path):
            return
        raise HTTPException(status_code=403, detail="Cannot create competition outside your country scope")
    if ctx.scope_kind == "region":
        if scale == "region" and _is_same_region(ctx, location_id):
            return
        raise HTTPException(status_code=403, detail="Regional staff can create only own region competitions")

    raise HTTPException(status_code=403, detail="Forbidden")


async def require_can_edit_competition(
    competition: dict[str, Any],
    ctx: CompetitionAccessContext,
) -> None:
    if not await can_edit_competition(competition, ctx, path_cache={}):
        raise HTTPException(status_code=403, detail="Forbidden")
