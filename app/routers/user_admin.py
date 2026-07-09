import os
from typing import Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from app.authorization import require_staff_user_id
from app.core.local_auth import set_user_password
from app.core.rest import rest_delete, rest_get, rest_post, rest_upsert
from app.schemas.user import AdminAthleteUpdate, AdminCreate, EditableUpdate, UserProfile


router = APIRouter(prefix="/users", tags=["users"])


def _pg_in(ids: list[str]) -> str:
    return f"in.({','.join(ids)})"


@router.post("/uploads/photo")
async def upload_photo_for_staff(
    file: UploadFile = File(...),
    _: str = Depends(require_staff_user_id),
):
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


@router.post("/admin-create/", response_model=UserProfile)
@router.post("/admin-create", response_model=UserProfile)
async def create_admin_user(payload: AdminCreate, _: str = Depends(require_staff_user_id)):
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
    query: str | None = None,
    gender: str | None = None,
    location_id: str | None = None,
    sort_by: Literal["full_name", "birth_date", "city", "coach_name"] = "full_name",
    sort_dir: Literal["asc", "desc"] = "asc",
    limit: int = 20,
    offset: int = 0,
    _: str = Depends(require_staff_user_id),
):
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    query_norm = str(query or "").strip()
    gender_norm = str(gender or "").strip().lower()
    location_norm = str(location_id or "").strip()
    from sqlalchemy import func as _func, or_ as _or, select as _select
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

        stmt = _select(*cols).select_from(j)
        if query_norm:
            ilike_pattern = f"%{query_norm}%"
            stmt = stmt.where(
                _or_(
                    profiles_t.c.full_name.ilike(ilike_pattern),
                    users_t.c.email.ilike(ilike_pattern),
                    profiles_t.c.phone.ilike(ilike_pattern),
                    profiles_t.c.city.ilike(ilike_pattern),
                    athletes_t.c.coach_name.ilike(ilike_pattern),
                )
            )
        if location_norm:
            stmt = stmt.where(profiles_t.c.location_id == location_norm)
        if gender_norm and passports_t is not None:
            if gender_norm in {"m", "male", "м"}:
                stmt = stmt.where(_func.lower(passports_t.c.gender).in_(["male", "m", "м"]))
            elif gender_norm in {"f", "female", "ж"}:
                stmt = stmt.where(_func.lower(passports_t.c.gender).in_(["female", "f", "ж"]))

        sort_column_map = {
            "full_name": _func.lower(_func.coalesce(profiles_t.c.full_name, users_t.c.email, "")),
            "birth_date": passports_t.c.birth_date if passports_t is not None else profiles_t.c.full_name,
            "city": _func.lower(_func.coalesce(profiles_t.c.city, "")),
            "coach_name": _func.lower(_func.coalesce(athletes_t.c.coach_name, "")),
        }
        sort_expr = sort_column_map.get(sort_by, sort_column_map["full_name"])
        if sort_dir == "desc":
            stmt = stmt.order_by(sort_expr.desc(), athletes_t.c.id.desc())
        else:
            stmt = stmt.order_by(sort_expr.asc(), athletes_t.c.id.asc())
        stmt = stmt.offset(offset).limit(limit + 1)

        res = await session.execute(stmt)
        rows = [dict(r) for r in res.mappings().all()]

    has_more = len(rows) > limit
    items: list[dict] = []
    for r in rows[:limit]:
        items.append(
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
    return {
        "items": items,
        "has_more": has_more,
        "next_offset": (offset + limit) if has_more else None,
    }


@router.get("/athletes/{athlete_id}")
async def get_athlete_details(
    athlete_id: UUID,
    _: str = Depends(require_staff_user_id),
):
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
    _: str = Depends(require_staff_user_id),
):
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
    _: str = Depends(require_staff_user_id),
):
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


@router.delete("/{user_id}/")
@router.delete("/{user_id}")
async def delete_user(user_id: UUID, _: str = Depends(require_staff_user_id)):
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
