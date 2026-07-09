import os
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from app.authorization import require_staff_user_id
from app.core.local_auth import set_user_password
from app.core.rest import rest_delete, rest_get, rest_patch, rest_post, rest_upsert


router = APIRouter(prefix="/users", tags=["users"])


async def _require_staff(authorization: str | None) -> str:
    return await require_staff_user_id(authorization)


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
    resp = await rest_get("users", filters, write=True)
    rows = resp.json()
    if not isinstance(rows, list):
        return []
    ids: list[str] = []
    for row in rows:
        if isinstance(row, dict) and row.get("id"):
            ids.append(str(row["id"]))
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
                    {"select": col, col: _pg_in(batch), "limit": "10000"},
                    write=True,
                )
                rows = resp.json()
                if isinstance(rows, list):
                    for row in rows:
                        if isinstance(row, dict) and row.get(col):
                            referenced.add(str(row[col]))
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
    resp = await rest_get(table, {"select": id_col, **where, "limit": "10000"}, write=True)
    rows = resp.json()
    if not isinstance(rows, list):
        return []
    out: list[str] = []
    for row in rows:
        if isinstance(row, dict) and row.get(id_col):
            out.append(str(row[id_col]))
    return out


async def _rest_select_rows(table: str, select: str, where: dict[str, str]) -> list[dict]:
    resp = await rest_get(table, {"select": select, **where, "limit": "10000"}, write=True)
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
    athlete_ids = [str(row["id"]) for row in athlete_rows if isinstance(row, dict) and row.get("id")]

    app_ids: list[str] = []
    if athlete_ids:
        app_rows = await _rest_select_rows("applications", "id,athlete_id", {"athlete_id": _pg_in(athlete_ids)})
        app_ids = [str(row["id"]) for row in app_rows if isinstance(row, dict) and row.get("id")]

    competition_bout_rows: list[dict] = []
    if athlete_ids:
        for batch in _chunk(athlete_ids, 40):
            where = {
                "or": f"(athlete_red_id.{_pg_in(batch)},athlete_blue_id.{_pg_in(batch)},winner_athlete_id.{_pg_in(batch)})"
            }
            competition_bout_rows.extend(await _rest_select_rows("competition_bouts", "id,competition_id", where))
    comp_bout_ids = [str(row["id"]) for row in competition_bout_rows if isinstance(row, dict) and row.get("id")]
    comp_bout_ids = list(dict.fromkeys(comp_bout_ids))

    comp_to_bouts: dict[str, list[str]] = {}
    for row in competition_bout_rows:
        if not isinstance(row, dict):
            continue
        bout_id = row.get("id")
        competition_id = row.get("competition_id")
        if bout_id and competition_id:
            comp_to_bouts.setdefault(str(competition_id), []).append(str(bout_id))

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

    for competition_id, ids in comp_to_bouts.items():
        for batch in _chunk(list(dict.fromkeys(ids)), 100):
            try:
                await rest_patch(
                    "competition_mats",
                    {"competition_id": f"eq.{competition_id}", "current_bout_id": _pg_in(batch)},
                    {"current_bout_id": None},
                    prefer="return=minimal",
                )
            except Exception:
                pass
    await _rest_delete_in("competition_bouts", "id", comp_bout_ids, chunk_size=200)
    await _rest_delete_in("bouts", "id", bout_ids, chunk_size=200)
    await _rest_delete_in("applications", "id", app_ids, chunk_size=200)
    await _rest_delete_in("passports", "athlete_id", athlete_ids, chunk_size=200)
    await _rest_delete_in("athletes", "id", athlete_ids, chunk_size=200)
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
    normalized = str(email or "").strip().lower()
    if not normalized or "@" not in normalized or "." not in normalized.split("@")[-1]:
        raise HTTPException(status_code=400, detail="Некорректный email")
    if len(normalized) > 320:
        raise HTTPException(status_code=400, detail="Некорректный email")
    return normalized


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

    role_codes = [str(code) for code in (body.role_codes or []) if str(code).strip()]
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
    to_insert = [{"user_id": str(user_id), "role_id": str(row["id"])} for row in roles_rows if row.get("id")]
    if to_insert:
        resp = await rest_post("user_roles", {}, to_insert, prefer="return=minimal")
        if resp.status_code not in (200, 201, 204):
            raise HTTPException(status_code=400, detail=f"Failed to insert user_roles: {resp.status_code} {resp.text}")

    return {"ok": True, "user_id": user_id, "email": email, "role_codes": role_codes}
