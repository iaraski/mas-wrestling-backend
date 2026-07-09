import os
from uuid import uuid4

import anyio
from fastapi import APIRouter, File, Form, Header, HTTPException, UploadFile

from app.authorization import get_role_codes_safe, is_staff_role
from app.core.ratelimit import allow as rl_allow
from app.core.rest import rest_get, rest_upsert
from app.core.supabase import admin_supabase
from app.schemas.user import AthleteDetailsUpdate, ProfileCreate, ProfileResponse
from app.users import (
    ensure_service_role_configured,
    execute_supabase,
    get_cached_user_id_from_bearer,
    get_location_path_v2,
    require_can_edit_self,
    safe_supabase_data,
)


router = APIRouter(prefix="/users", tags=["users"])


async def _get_my_role_codes(user_id: str) -> list[str]:
    return await get_role_codes_safe(user_id)


async def _get_athlete_row_by_user_id(user_id: str) -> dict | None:
    athlete_q = admin_supabase.table("athletes").select("id,user_id,coach_name").eq("user_id", user_id).maybe_single()
    athlete_res = await execute_supabase(athlete_q)
    athlete_row = safe_supabase_data(athlete_res)
    return athlete_row if isinstance(athlete_row, dict) and athlete_row.get("id") else None


async def _upsert_athlete_profile(user_id: str, coach_name: str | None) -> str:
    existing = None
    try:
        existing = await _get_athlete_row_by_user_id(user_id)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"Athlete lookup failed: {e.detail}")

    if existing:
        try:
            await execute_supabase(
                admin_supabase.table("athletes")
                .update({"coach_name": coach_name})
                .eq("id", str(existing["id"]))
            )
            return str(existing["id"])
        except HTTPException as e:
            raise HTTPException(status_code=e.status_code, detail=f"Athlete update failed: {e.detail}")

    try:
        insert_res = await execute_supabase(
            admin_supabase.table("athletes").insert({"user_id": user_id, "coach_name": coach_name})
        )
        insert_data = safe_supabase_data(insert_res)
        if isinstance(insert_data, list) and insert_data and isinstance(insert_data[0], dict) and insert_data[0].get("id"):
            return str(insert_data[0]["id"])
        if isinstance(insert_data, dict) and insert_data.get("id"):
            return str(insert_data["id"])
    except HTTPException as e:
        # A concurrent insert may have won the race; re-read before failing.
        try:
            existing = await _get_athlete_row_by_user_id(user_id)
        except HTTPException as lookup_error:
            raise HTTPException(
                status_code=lookup_error.status_code,
                detail=f"Athletes insert failed and lookup retry failed: {lookup_error.detail}",
            )
        if existing:
            return str(existing["id"])
        raise HTTPException(status_code=e.status_code, detail=f"Athletes insert failed: {e.detail}")

    try:
        existing = await _get_athlete_row_by_user_id(user_id)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"Athlete lookup after insert failed: {e.detail}")
    if not existing:
        raise HTTPException(status_code=500, detail="Athlete upsert failed")
    return str(existing["id"])


@router.get("/me/profile")
async def get_my_profile(authorization: str | None = Header(default=None)):
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)
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
        res = await execute_supabase(q)
        data = safe_supabase_data(res)
        if not data:
            return {"user_id": user_id, "full_name": "", "phone": "", "city": "", "location_id": None}
        return data
    except HTTPException as e:
        if e.status_code in (400, 503):
            return {"user_id": user_id, "full_name": "", "phone": "", "city": "", "location_id": None}
        raise


@router.put("/me/profile", response_model=ProfileResponse)
async def update_my_profile(profile: ProfileCreate, authorization: str | None = Header(default=None)):
    user_id = await get_cached_user_id_from_bearer(authorization)
    await require_can_edit_self(user_id)
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
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)
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
        res = await execute_supabase(q)
        data = safe_supabase_data(res)
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
async def update_my_athlete(coach_name: str | None = None, authorization: str | None = Header(default=None)):
    user_id = await get_cached_user_id_from_bearer(authorization)
    await require_can_edit_self(user_id)
    try:
        await rest_upsert("athletes", {"user_id": user_id, "coach_name": coach_name}, on_conflict="user_id")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to update athlete: {repr(e)}")
    return {"user_id": user_id, "coach_name": coach_name}


@router.get("/me/details")
async def get_my_details(authorization: str | None = Header(default=None)):
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)
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
        athlete_res = await execute_supabase(
            admin_supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single()
        )
        athlete_data = safe_supabase_data(athlete_res)
        if not athlete_data:
            return {"birth_date": None, "rank": None, "photo_url": None, "gender": None}
        athlete_id = str(athlete_data["id"])
        p_res = await execute_supabase(
            admin_supabase.table("passports")
            .select("birth_date,rank,photo_url,gender")
            .eq("athlete_id", athlete_id)
            .maybe_single()
        )
        p = safe_supabase_data(p_res) or {}
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
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)

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
            location_path = await get_location_path_v2(str(loc_id))
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
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)
    await require_can_edit_self(user_id)
    athlete_q = admin_supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single()
    athlete_res = await execute_supabase(athlete_q)
    athlete_data = safe_supabase_data(athlete_res)
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
    await execute_supabase(
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
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)
    await require_can_edit_self(user_id)
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
        await execute_supabase(admin_supabase.table("profiles").upsert(profile_payload, on_conflict="user_id"))
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"Profiles upsert failed: {e.detail}")
    try:
        await rest_upsert("profiles", profile_payload, on_conflict="user_id")
    except Exception:
        pass

    athlete_id = await _upsert_athlete_profile(user_id, coach_name)

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
        await execute_supabase(
            admin_supabase.table("passports").upsert(passport_payload, on_conflict="athlete_id")
        )
    except Exception as e:
        if isinstance(e, HTTPException):
            raise HTTPException(status_code=e.status_code, detail=f"Failed to upsert passports: {e.detail}")
        raise HTTPException(status_code=400, detail=f"Failed to upsert passports: {repr(e)}")

    return {"ok": True, "photo_url": final_photo_url}


@router.post("/me/complete")
async def complete_my_profile(authorization: str | None = Header(default=None)):
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)
    role_codes = await _get_my_role_codes(user_id)
    if is_staff_role(role_codes):
        raise HTTPException(status_code=400, detail="Staff accounts cannot be completed here")

    prof_res = await execute_supabase(admin_supabase.table("profiles").select("full_name,city,location_id").eq("user_id", user_id).maybe_single())
    prof = safe_supabase_data(prof_res) or {}
    ath_res = await execute_supabase(admin_supabase.table("athletes").select("id,coach_name").eq("user_id", user_id).maybe_single())
    ath = safe_supabase_data(ath_res) or {}
    athlete_id = str(ath.get("id") or "")
    p_res = await execute_supabase(
        admin_supabase.table("passports")
        .select("birth_date,rank,photo_url,gender")
        .eq("athlete_id", athlete_id)
        .maybe_single()
    )
    p = safe_supabase_data(p_res) or {}

    if not prof.get("full_name") or not prof.get("city") or not prof.get("location_id"):
        raise HTTPException(status_code=400, detail="Fill full_name, city and region")
    if not ath.get("coach_name"):
        raise HTTPException(status_code=400, detail="Fill coach name")
    if not p.get("birth_date") or not p.get("rank") or not p.get("photo_url") or not p.get("gender"):
        raise HTTPException(status_code=400, detail="Fill birth_date, gender, rank and upload photo")

    await execute_supabase(
        admin_supabase.table("registrations").upsert(
            {"user_id": user_id, "stage": "complete", "consent_accepted": True},
            on_conflict="user_id",
        )
    )
    return {"ok": True, "locked": True}


@router.get("/me/registration")
async def get_my_registration(authorization: str | None = Header(default=None)):
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)
    try:
        res = await execute_supabase(
            admin_supabase.table("registrations")
            .select("stage,consent_accepted,updated_at")
            .eq("user_id", user_id)
            .maybe_single()
        )
        data = safe_supabase_data(res)
        stage = (data or {}).get("stage") if isinstance(data, dict) else None
        if not stage:
            stage = "start"
        return {"user_id": user_id, "stage": stage, "locked": bool(stage == "complete")}
    except Exception:
        return {"user_id": user_id, "stage": "start", "locked": False}


@router.get("/me/applications")
async def get_my_applications(authorization: str | None = Header(default=None)):
    ensure_service_role_configured()
    user_id = await get_cached_user_id_from_bearer(authorization)
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
