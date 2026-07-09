from typing import Optional
from uuid import UUID

from app.core.db import SessionLocal, tables
from app.core.rest import rest_delete, rest_get, rest_patch, rest_post, rest_upsert


async def list_application_rows(competition_id: Optional[UUID]) -> list[dict]:
    from sqlalchemy import select as _select

    apps_t = tables["applications"]
    async with SessionLocal() as session:
        stmt = _select(apps_t).order_by(apps_t.c.created_at.desc()).limit(10000)
        if competition_id:
            stmt = stmt.where(apps_t.c.competition_id == str(competition_id))
        res = await session.execute(stmt)
        return [dict(r) for r in res.mappings().all()]


async def get_competition_start_map(comp_ids: set[str]) -> dict[str, str | None]:
    from sqlalchemy import select as _select

    comps_t = tables.get("competitions")
    if comps_t is None or not comp_ids:
        return {}
    async with SessionLocal() as session:
        res = await session.execute(
            _select(comps_t.c.id, comps_t.c.start_date).where(comps_t.c.id.in_(list(comp_ids)))
        )
        return {str(r.get("id")): r.get("start_date") for r in res.mappings().all()}


async def get_category_map(cat_ids: set[str]) -> dict[str, dict]:
    from sqlalchemy import select as _select

    cats_t = tables.get("competition_categories")
    if cats_t is None or not cat_ids:
        return {}
    async with SessionLocal() as session:
        res = await session.execute(_select(cats_t).where(cats_t.c.id.in_(list(cat_ids))))
        return {str(r.get("id")): dict(r) for r in res.mappings().all()}


async def get_athlete_summary_map(athlete_ids: set[str]) -> dict[str, dict]:
    from sqlalchemy import select as _select

    athletes_t = tables.get("athletes")
    users_t = tables.get("users")
    profiles_t = tables.get("profiles")
    locs_t = tables.get("locations")
    if athletes_t is None or users_t is None or profiles_t is None or not athlete_ids:
        return {}

    async with SessionLocal() as session:
        joined = athletes_t.join(users_t, athletes_t.c.user_id == users_t.c.id).outerjoin(
            profiles_t, profiles_t.c.user_id == users_t.c.id
        )
        if locs_t is not None:
            joined = joined.outerjoin(locs_t, profiles_t.c.location_id == locs_t.c.id)

        cols = [
            athletes_t.c.id.label("athlete_id"),
            profiles_t.c.full_name,
            profiles_t.c.location_id,
        ]
        if locs_t is not None:
            cols.append(locs_t.c.name.label("region_name"))

        res = await session.execute(_select(*cols).select_from(joined).where(athletes_t.c.id.in_(list(athlete_ids))))
        return {str(r.get("athlete_id")): dict(r) for r in res.mappings().all()}


async def get_application(app_id: UUID, *, write: bool) -> dict | None:
    resp = await rest_get(
        "applications",
        {"select": "*", "id": f"eq.{str(app_id)}", "limit": "1"},
        write=write,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def get_athlete(athlete_id: str, *, write: bool) -> dict | None:
    resp = await rest_get(
        "athletes",
        {"select": "id,coach_name,user_id", "id": f"eq.{athlete_id}", "limit": "1"},
        write=write,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def get_user(user_id: str, *, write: bool) -> dict | None:
    resp = await rest_get(
        "users",
        {"select": "id,email,telegram_id", "id": f"eq.{user_id}", "limit": "1"},
        write=write,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def get_profile(user_id: str, *, write: bool) -> dict | None:
    resp = await rest_get(
        "profiles",
        {"select": "user_id,full_name,phone,city,location_id", "user_id": f"eq.{user_id}", "limit": "1"},
        write=write,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def get_category(category_id: str, *, write: bool) -> dict | None:
    resp = await rest_get(
        "competition_categories",
        {"select": "*", "id": f"eq.{category_id}", "limit": "1"},
        write=write,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def get_competition(competition_id: str, *, write: bool) -> dict | None:
    resp = await rest_get(
        "competitions",
        {"select": "id,name,start_date", "id": f"eq.{competition_id}", "limit": "1"},
        write=write,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def get_passport_by_athlete(athlete_id: str, *, write: bool, select: str = "*") -> dict | None:
    resp = await rest_get(
        "passports",
        {"select": select, "athlete_id": f"eq.{athlete_id}", "limit": "1"},
        write=write,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def find_competition_id_by_category(category_id: str) -> str | None:
    resp = await rest_get(
        "competition_categories",
        {"select": "competition_id", "id": f"eq.{category_id}", "limit": "1"},
        write=True,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows and rows[0].get("competition_id"):
        return str(rows[0]["competition_id"])
    return None


async def get_category_competition_row(category_id: str) -> dict | None:
    resp = await rest_get(
        "competition_categories",
        {"select": "id,competition_id,gender,age_min,age_max", "id": f"eq.{category_id}", "limit": "1"},
        write=True,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def get_competition_start_date(competition_id: str) -> str | None:
    resp = await rest_get(
        "competitions",
        {"select": "start_date", "id": f"eq.{competition_id}", "limit": "1"},
        write=True,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0].get("start_date")
    return None


async def find_athlete_by_user(user_id: str) -> dict | None:
    resp = await rest_get(
        "athletes",
        {"select": "id,coach_name", "user_id": f"eq.{user_id}", "limit": "1"},
        write=True,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def find_application_for_competition(athlete_id: str, competition_id: str) -> dict | None:
    resp = await rest_get(
        "applications",
        {
            "select": "id",
            "athlete_id": f"eq.{athlete_id}",
            "competition_id": f"eq.{competition_id}",
            "limit": "1",
        },
        write=True,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def find_application_for_competition_with_status(athlete_id: str, competition_id: str) -> dict | None:
    resp = await rest_get(
        "applications",
        {
            "select": "id,status,category_id",
            "athlete_id": f"eq.{athlete_id}",
            "competition_id": f"eq.{competition_id}",
            "limit": "1",
        },
        write=True,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def create_application(payload: dict) -> dict | None:
    res = await rest_post("applications", {}, payload, prefer="return=representation")
    rows = res.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def create_user_with_id(user_id: str) -> None:
    await rest_post("users", {}, {"id": user_id}, prefer="return=minimal")


async def create_profile(payload: dict) -> None:
    await rest_post("profiles", {}, payload, prefer="return=minimal")


async def create_athlete(payload: dict) -> dict | None:
    res = await rest_post("athletes", {}, payload, prefer="return=representation")
    rows = res.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def upsert_passport(payload: dict) -> None:
    await rest_upsert("passports", payload, on_conflict="athlete_id")


async def delete_application(application_id: str) -> int:
    res = await rest_delete("applications", {"id": f"eq.{application_id}"})
    return int(res.status_code)


async def get_application_athlete_id(app_id: str) -> str | None:
    resp = await rest_get(
        "applications",
        {"select": "athlete_id", "id": f"eq.{app_id}", "limit": "1"},
        write=True,
    )
    rows = resp.json()
    if isinstance(rows, list) and rows and rows[0].get("athlete_id"):
        return str(rows[0]["athlete_id"])
    return None


async def update_user_email(user_id: str, email: str) -> None:
    await rest_patch("users", {"id": f"eq.{user_id}"}, {"email": email}, prefer="return=minimal")


async def upsert_profile(payload: dict) -> None:
    await rest_upsert("profiles", payload, on_conflict="user_id")


async def update_athlete(athlete_id: str, payload: dict) -> None:
    await rest_patch("athletes", {"id": f"eq.{athlete_id}"}, payload, prefer="return=minimal")


async def update_application(app_id: UUID, payload: dict) -> dict | None:
    res = await rest_patch(
        "applications",
        {"id": f"eq.{str(app_id)}"},
        payload,
        prefer="return=representation",
    )
    rows = res.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def update_passport_verification(passport_id: UUID, *, is_verified: bool) -> dict | None:
    res = await rest_patch(
        "passports",
        {"id": f"eq.{str(passport_id)}"},
        {"is_verified": bool(is_verified)},
        prefer="return=representation",
    )
    rows = res.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


async def mark_registration_complete(user_id: str) -> None:
    await rest_upsert(
        "registrations",
        {"user_id": user_id, "stage": "complete", "consent_accepted": True},
        on_conflict="user_id",
    )
