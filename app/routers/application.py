from fastapi import APIRouter, HTTPException, Response, Header
from pydantic import BaseModel
from typing import List, Optional
import random
import hashlib
import time
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4
import anyio
from app.core.supabase import supabase, admin_supabase
from app.core.rest import rest_get, rest_post, rest_upsert, rest_patch, rest_delete
from app.core.supabase import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from app.schemas.competition import Application, ApplicationCreate, ApplicationUpdate

from app.core.telegram import send_telegram_notification, get_telegram_file_url

router = APIRouter(prefix="/applications", tags=["applications"])

_me_cache: dict[str, tuple[float, str]] = {}
_MSK_TZ = timezone(timedelta(hours=3))
_APPLICATION_DEADLINE = datetime(2026, 4, 18, 0, 0, tzinfo=_MSK_TZ)

def _applications_open_now() -> bool:
    now = datetime.now(_MSK_TZ)
    return now < _APPLICATION_DEADLINE

def _category_group(gender: str | None, age_min: int | None, age_max: int | None) -> str:
    g = (gender or "").lower()
    is_male = g == "male" or g == "m"
    is_female = g == "female" or g == "f"
    if age_min == 18 and age_max == 21:
        return "Юниоры" if is_male else "Юниорки" if is_female else "Юниоры"
    if age_max is not None and age_max < 18:
        return "Юноши" if is_male else "Девушки" if is_female else "Юноши"
    return "Мужчины" if is_male else "Женщины" if is_female else "Мужчины"

def _weight_label(weight_min: float | int | None, weight_max: float | int | None) -> str:
    try:
        if weight_max is None or float(weight_max) >= 999:
            if not weight_min:
                return "абсолютная"
            return f"{int(float(weight_min))}+ кг"
        return f"до {weight_max} кг"
    except Exception:
        return "—"

def _birth_years_label(age_min: int | None, age_max: int | None, at_date: str | None) -> str | None:
    if age_min is None or age_max is None:
        return None
    try:
        year = datetime.fromisoformat(str(at_date).replace("Z", "+00:00")).year if at_date else datetime.now(_MSK_TZ).year
    except Exception:
        year = datetime.now(_MSK_TZ).year
    return f"{year - age_max}-{year - age_min} г.р."

def _format_category_label(cat: dict, at_date: str | None) -> str:
    try:
        group = _category_group(cat.get("gender"), cat.get("age_min"), cat.get("age_max"))
        years = _birth_years_label(cat.get("age_min"), cat.get("age_max"), at_date)
        weight = _weight_label(cat.get("weight_min"), cat.get("weight_max"))
        return f"{group} {years}, {weight}" if years else f"{group}, {weight}"
    except Exception:
        return "Неизвестная категория"

async def _get_role_codes(user_id: str) -> list[str]:
    try:
        res = await rest_get(
            "user_roles",
            {"select": "roles(code)", "user_id": f"eq.{user_id}", "limit": "1000"},
            write=True,
        )
        data = res.json() or []
        out: list[str] = []
        if not isinstance(data, list):
            return []
        for r in data:
            role = r.get("roles") if isinstance(r, dict) else None
            if isinstance(role, list):
                role = role[0] if role else None
            code = role.get("code") if isinstance(role, dict) else None
            if code:
                out.append(str(code))
        return out
    except Exception:
        return []

def _is_staff(codes: list[str]) -> bool:
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


class AdminCreateAthleteApplication(BaseModel):
    category_id: UUID
    full_name: str
    city: str
    location_id: UUID
    coach_name: str
    birth_date: str
    rank: str
    photo_url: str
    declared_weight: Optional[float] = None
    actual_weight: Optional[float] = None


class AdminUpdateAthleteProfile(BaseModel):
    full_name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    city: str
    location_id: UUID
    coach_name: str
    birth_date: str
    gender: Optional[str] = None
    rank: str
    photo_url: str


class AdminApplyAthleteToCategory(BaseModel):
    athlete_id: UUID
    category_id: UUID

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

    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")

    def _sync_get_user():
        return admin_supabase.auth.get_user(token)

    try:
        auth_res = await anyio.to_thread.run_sync(_sync_get_user)
    except Exception as e:
        msg = repr(e)
        lowered = msg.lower()
        if "jwt" in lowered or "token" in lowered or "401" in lowered or "403" in lowered:
            raise HTTPException(status_code=401, detail="Invalid token")
        raise HTTPException(status_code=503, detail=f"Supabase Auth unavailable: {msg}")

    user_obj = None
    if isinstance(auth_res, dict):
        user_obj = auth_res.get("user") or auth_res.get("data") or auth_res.get("user_data")
    else:
        user_obj = getattr(auth_res, "user", None) or getattr(auth_res, "data", None)

    user_id = None
    if hasattr(user_obj, "id"):
        user_id = getattr(user_obj, "id")
    elif isinstance(user_obj, dict):
        user_id = user_obj.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    _me_cache[cache_key] = (time.time() + 30.0, user_id)
    return user_id

@router.get("/", response_model=List[Application])
async def get_applications(competition_id: Optional[UUID] = None):
    try:
        params = {
            "select": "*,competition:competitions(start_date),athletes(users!athletes_user_id_fkey(profiles(full_name))),competition_categories(*)",
            "order": "created_at.desc",
            "limit": "10000",
        }
        if competition_id:
            params["competition_id"] = f"eq.{str(competition_id)}"
        response = await rest_get("applications", params, write=True)
        rows = response.json()
        if not isinstance(rows, list):
            rows = []
        
        # Преобразуем данные, чтобы вынести ФИО и описание категории
        apps = []
        for app in rows:
            full_name = "Unknown"
            try:
                # athlete -> users -> profiles -> full_name
                if (app.get("athletes") and 
                    app["athletes"].get("users") and 
                    app["athletes"]["users"].get("profiles")):
                    full_name = app["athletes"]["users"]["profiles"].get("full_name")
            except Exception as e:
                print(f"[Applications] Error parsing athlete name for app {app.get('id')}: {e}")
            
            # Данные категории
            category_desc = "Unknown"
            try:
                if app.get("competition_categories"):
                    cat = app["competition_categories"]
                    comp = app.get("competition") or {}
                    at_date = comp.get("start_date") if isinstance(comp, dict) else None
                    category_desc = _format_category_label(cat, at_date)
            except Exception as e:
                print(f"[Applications] Error parsing category for app {app.get('id')}: {e}")

            # Добавляем в объект
            app["athlete_name"] = full_name or "Unknown"
            app["category_description"] = category_desc or "Unknown"
            apps.append(app)
            
        return apps
    except Exception as e:
        import traceback
        print(f"[Applications] CRITICAL ERROR: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

import httpx
from fastapi.responses import Response

@router.get("/photo/{file_id}")
async def get_photo_proxy(file_id: str):
    """Проксирует картинку из Telegram, чтобы обойти CORS и избежать протухания ссылок"""
    real_url = await get_telegram_file_url(file_id)
    if not real_url:
        raise HTTPException(status_code=404, detail="Photo not found in Telegram")
        
    # Увеличиваем таймаут и отключаем http2 для надежности
    async with httpx.AsyncClient(timeout=15.0, http2=False) as client:
        try:
            response = await client.get(real_url)
            if response.status_code == 200:
                # Возвращаем само изображение с кешированием на час (чтобы браузер не грузил его каждый раз)
                headers = {
                    "Cache-Control": "public, max-age=3600"
                }
                return Response(
                    content=response.content, 
                    media_type=response.headers.get("Content-Type", "image/jpeg"),
                    headers=headers
                )
            else:
                raise HTTPException(status_code=response.status_code, detail="Failed to download photo")
        except Exception as e:
            import traceback
            print(f"[Photo Proxy] Error downloading: {e}")
            print(traceback.format_exc())
            raise HTTPException(status_code=500, detail="Error downloading photo")

@router.get("/{app_id}/")
@router.get("/{app_id}")
async def get_application_details(app_id: UUID, authorization: str | None = Header(default=None)):
    try:
        write = False
        if authorization:
            try:
                requester_id = await _get_user_id_from_bearer(authorization)
                codes = await _get_role_codes(requester_id)
                write = _is_staff(codes)
            except Exception:
                write = False

        # Получаем детальную информацию по заявке, включая паспорт
        resp = await rest_get(
            "applications",
            {
                "select": "*,athletes(coach_name,user_id,users!athletes_user_id_fkey(email,profiles(full_name,phone,city,location_id))),competition_categories(*),competition:competitions(start_date,name)",
                "id": f"eq.{str(app_id)}",
                "limit": "1",
            },
            write=write,
        )
        rows = resp.json()
        if not isinstance(rows, list) or not rows:
            raise HTTPException(status_code=404, detail="Application not found")
            
        app_data = rows[0]
        
        # Получаем паспорт отдельно, так как он привязан к athlete_id
        athlete_id = app_data.get("athlete_id")
        passport_data = None
        if athlete_id:
            pass_resp = await rest_get(
                "passports",
                {"select": "*", "athlete_id": f"eq.{str(athlete_id)}", "limit": "1"},
                write=write,
            )
            pass_rows = pass_resp.json()
            passport_data = pass_rows[0] if isinstance(pass_rows, list) and pass_rows else None
            
            # Если есть фото, возвращаем URL на наш собственный прокси-эндпоинт
            if passport_data and passport_data.get("photo_url"):
                file_id = passport_data["photo_url"]
                if isinstance(file_id, str) and not (
                    file_id.startswith("http")
                    or file_id.startswith("documents/")
                    or file_id.startswith("/")
                ):
                    passport_data["photo_url"] = f"/applications/photo/{file_id}"
            
        app_data["passport"] = passport_data
        
        # Вытаскиваем ФИО, телефон и email для удобства
        full_name = "Unknown"
        phone = "Не указан"
        email = "Не указан"
        try:
            athlete_data = app_data.get("athletes")
            if athlete_data:
                if isinstance(athlete_data, list):
                    athlete_data = athlete_data[0] if athlete_data else {}
                
                app_data["coach_name"] = athlete_data.get("coach_name", "Не указан")
                if athlete_data.get("user_id"):
                    app_data["athlete_user_id"] = str(athlete_data.get("user_id"))
                
                users_data = athlete_data.get("users")
                if users_data:
                    if isinstance(users_data, list):
                        users_data = users_data[0] if users_data else {}
                    
                    profiles_data = users_data.get("profiles")
                    if profiles_data:
                        if isinstance(profiles_data, list):
                            profiles_data = profiles_data[0] if profiles_data else {}
                        full_name = profiles_data.get("full_name", "Unknown")
                        phone = profiles_data.get("phone", "Не указан")
                        app_data["athlete_city"] = profiles_data.get("city")
                        app_data["athlete_location_id"] = profiles_data.get("location_id")
                        
                        # Email получаем из users, так как в profiles его нет
                        email = users_data.get("email", "Не указан")
        except Exception as e:
            print(f"Error parsing athlete name in details: {e}")
            
        app_data["athlete_name"] = full_name
        app_data["athlete_phone"] = phone
        app_data["athlete_email"] = email
        
        # Описание категории
        category_desc = "Не указана"
        try:
            cat = app_data.get("competition_categories")
            if cat:
                if isinstance(cat, list):
                    cat = cat[0] if cat else {}
                comp = app_data.get("competition") or app_data.get("competitions") or {}
                if isinstance(comp, list):
                    comp = comp[0] if comp else {}
                at_date = comp.get("start_date") if isinstance(comp, dict) else None
                category_desc = _format_category_label(cat, at_date)
        except Exception as e:
            print(f"Error parsing category in details: {e}")
        app_data["category_description"] = category_desc
        
        return app_data
    except Exception as e:
        import traceback
        print(f"[Applications] Error fetching details: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

class PassportVerifyUpdate(BaseModel):
    is_verified: bool

@router.patch("/passport/{passport_id}/verify/")
@router.patch("/passport/{passport_id}/verify")
async def verify_passport(passport_id: UUID, payload: PassportVerifyUpdate):
    try:
        res = await rest_patch(
            "passports",
            {"id": f"eq.{str(passport_id)}"},
            {"is_verified": bool(payload.is_verified)},
            prefer="return=representation",
        )
        rows = res.json()
        if not isinstance(rows, list) or not rows:
            raise HTTPException(status_code=404, detail="Passport not found")
        return rows[0]
    except Exception as e:
        import traceback
        print(f"[Applications] Error verifying passport: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", response_model=Application)
async def create_application(app_in: ApplicationCreate):
    # В реальном проекте здесь будет проверка на то, что это атлет или тренер за атлета
    
    # Проверка, нет ли уже заявки от этого атлета на это соревнование
    check_resp = await rest_get(
        "applications",
        {
            "select": "id",
            "athlete_id": f"eq.{str(app_in.athlete_id)}",
            "competition_id": f"eq.{str(app_in.competition_id)}",
            "limit": "1",
        },
        write=True,
    )
    check_rows = check_resp.json()
    if isinstance(check_rows, list) and check_rows:
        raise HTTPException(status_code=400, detail="Application already exists")
        
    res = await rest_post(
        "applications",
        {},
        app_in.model_dump(),
        prefer="return=representation",
    )
    rows = res.json()
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return rows[0]

@router.post("/me")
async def create_my_application(
    category_id: str,
    authorization: str | None = Header(default=None),
    user_id: str | None = None,
):
    try:
        from app.core.ratelimit import allow as rl_allow
        uid_for_rl = user_id or "anon"
        if not rl_allow(f"apply:{uid_for_rl}", rate_per_minute=15.0, burst=30.0):
            raise HTTPException(status_code=429, detail="Too many application requests, please try again shortly")
    except Exception:
        pass
    if authorization:
        user_id = await _get_user_id_from_bearer(authorization)
    if not user_id:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    if not _applications_open_now():
        raise HTTPException(status_code=403, detail="Application deadline has passed")

    cat_resp = await rest_get(
        "competition_categories",
        {"select": "competition_id", "id": f"eq.{category_id}", "limit": "1"},
        write=True,
    )
    cat_rows = cat_resp.json()
    if not isinstance(cat_rows, list) or not cat_rows or not cat_rows[0].get("competition_id"):
        raise HTTPException(status_code=404, detail="Category not found")
    competition_id = str(cat_rows[0]["competition_id"])

    athlete_resp = await rest_get(
        "athletes",
        {"select": "id,coach_name", "user_id": f"eq.{user_id}", "limit": "1"},
        write=True,
    )
    athlete_rows = athlete_resp.json()
    if not isinstance(athlete_rows, list) or not athlete_rows or not athlete_rows[0].get("id"):
        raise HTTPException(status_code=404, detail="Athlete not found")
    athlete_id = str(athlete_rows[0]["id"])
    coach = athlete_rows[0] if isinstance(athlete_rows[0], dict) else {}

    prof_resp = await rest_get(
        "profiles",
        {"select": "full_name,city,location_id", "user_id": f"eq.{user_id}", "limit": "1"},
        write=True,
    )
    prof_rows = prof_resp.json()
    prof = prof_rows[0] if isinstance(prof_rows, list) and prof_rows else {}

    pass_resp = await rest_get(
        "passports",
        {
            "select": "birth_date,rank,photo_url,gender",
            "athlete_id": f"eq.{athlete_id}",
            "limit": "1",
        },
        write=True,
    )
    pass_rows = pass_resp.json()
    passport = pass_rows[0] if isinstance(pass_rows, list) and pass_rows else {}

    if not prof.get("full_name") or not prof.get("city") or not prof.get("location_id"):
        raise HTTPException(status_code=400, detail="Fill full_name, city and region")
    if not coach.get("coach_name"):
        raise HTTPException(status_code=400, detail="Fill coach name")
    if not passport.get("birth_date") or not passport.get("rank") or not passport.get("photo_url") or not passport.get("gender"):
        raise HTTPException(status_code=400, detail="Fill birth_date, gender, rank and upload photo")
    
    # Check if already applied to this competition (only one application per competition)
    existing_resp = await rest_get(
        "applications",
        {
            "select": "id",
            "athlete_id": f"eq.{athlete_id}",
            "competition_id": f"eq.{competition_id}",
            "limit": "1",
        },
        write=True,
    )
    existing_rows = existing_resp.json()
    if isinstance(existing_rows, list) and existing_rows:
        raise HTTPException(status_code=400, detail="Already applied to this competition")
        
    res = await rest_post(
        "applications",
        {},
        {
        "competition_id": competition_id,
        "athlete_id": athlete_id,
        "category_id": category_id,
        "status": "pending"
        },
        prefer="return=representation",
    )
    created_rows = res.json()
    if not isinstance(created_rows, list) or not created_rows:
        raise HTTPException(status_code=400, detail="Failed to create application")

    try:
        await rest_upsert(
            "registrations",
            {"user_id": user_id, "stage": "complete", "consent_accepted": True},
            on_conflict="user_id",
        )
    except Exception:
        pass

    return created_rows[0]


@router.post("/admin-create")
async def admin_create_athlete_and_application(
    body: AdminCreateAthleteApplication,
    authorization: str | None = Header(default=None),
):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    requester_id = await _get_user_id_from_bearer(authorization)
    codes = await _get_role_codes(requester_id)
    if not _is_staff(codes):
        raise HTTPException(status_code=403, detail="Forbidden")

    cat_id = str(body.category_id)
    cat_resp = await rest_get(
        "competition_categories",
        {"select": "competition_id", "id": f"eq.{cat_id}", "limit": "1"},
        write=True,
    )
    cat_rows = cat_resp.json()
    if not isinstance(cat_rows, list) or not cat_rows or not cat_rows[0].get("competition_id"):
        raise HTTPException(status_code=404, detail="Category not found")
    competition_id = str(cat_rows[0]["competition_id"])

    user_id = str(uuid4())
    await rest_post("users", {}, {"id": user_id}, prefer="return=minimal")
    await rest_post(
        "profiles",
        {},
        {"user_id": user_id, "full_name": body.full_name, "city": body.city, "location_id": str(body.location_id)},
        prefer="return=minimal",
    )
    athlete_insert = await rest_post(
        "athletes",
        {},
        {"user_id": user_id, "coach_name": body.coach_name},
        prefer="return=representation",
    )
    athlete_rows = athlete_insert.json()
    if not isinstance(athlete_rows, list) or not athlete_rows or not athlete_rows[0].get("id"):
        raise HTTPException(status_code=400, detail="Failed to create athlete")
    athlete_id = str(athlete_rows[0]["id"])

    await rest_upsert(
        "passports",
        {"athlete_id": athlete_id, "birth_date": body.birth_date, "rank": body.rank, "photo_url": body.photo_url},
        on_conflict="athlete_id",
    )

    status = "weighed" if body.actual_weight is not None else "approved"
    app_payload = {
        "competition_id": competition_id,
        "athlete_id": athlete_id,
        "category_id": cat_id,
        "status": status,
        "declared_weight": body.declared_weight,
        "actual_weight": body.actual_weight,
    }
    app_insert = await rest_post("applications", {}, app_payload, prefer="return=representation")
    app_rows = app_insert.json()
    if not isinstance(app_rows, list) or not app_rows:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return {"ok": True, "user_id": user_id, "athlete_id": athlete_id, "application": app_rows[0]}


@router.post("/admin-apply")
async def admin_apply_athlete_to_category(
    body: AdminApplyAthleteToCategory,
    authorization: str | None = Header(default=None),
):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    requester_id = await _get_user_id_from_bearer(authorization)
    codes = await _get_role_codes(requester_id)
    if not _is_staff(codes):
        raise HTTPException(status_code=403, detail="Forbidden")

    cat_id = str(body.category_id)
    cat_resp = await rest_get(
        "competition_categories",
        {"select": "id,competition_id,gender,age_min,age_max", "id": f"eq.{cat_id}", "limit": "1"},
        write=True,
    )
    cat_rows = cat_resp.json()
    if not isinstance(cat_rows, list) or not cat_rows:
        raise HTTPException(status_code=404, detail="Category not found")
    category = cat_rows[0]
    competition_id = str(category["competition_id"])

    comp_resp = await rest_get(
        "competitions",
        {"select": "start_date", "id": f"eq.{competition_id}", "limit": "1"},
        write=True,
    )
    comp_rows = comp_resp.json()
    start_date_str = comp_rows[0].get("start_date") if isinstance(comp_rows, list) and comp_rows else None

    pass_resp = await rest_get(
        "passports",
        {"select": "birth_date,gender,photo_url", "athlete_id": f"eq.{str(body.athlete_id)}", "limit": "1"},
        write=True,
    )
    pass_rows = pass_resp.json()
    passport = pass_rows[0] if isinstance(pass_rows, list) and pass_rows else {}
    if not passport.get("birth_date") or not passport.get("gender"):
        raise HTTPException(status_code=400, detail="Athlete birth_date and gender are required")

    athlete_gender = str(passport.get("gender"))
    if str(category.get("gender")) != athlete_gender:
        raise HTTPException(status_code=400, detail="Athlete gender does not match category")

    def _age_at(birth_date: str, at_date: str | None) -> int:
        b = datetime.fromisoformat(str(birth_date)).date()
        if at_date:
            s = str(at_date).replace("Z", "+00:00")
            at = datetime.fromisoformat(s).date()
        else:
            at = datetime.now(_MSK_TZ).date()
        age = at.year - b.year
        if (at.month, at.day) < (b.month, b.day):
            age -= 1
        return age

    age = _age_at(str(passport.get("birth_date")), start_date_str)
    if age < int(category.get("age_min") or 0) or age > int(category.get("age_max") or 200):
        raise HTTPException(status_code=400, detail="Athlete age does not match category")

    existing_resp = await rest_get(
        "applications",
        {
            "select": "id,status,category_id",
            "athlete_id": f"eq.{str(body.athlete_id)}",
            "competition_id": f"eq.{competition_id}",
            "limit": "1",
        },
        write=True,
    )
    existing_rows = existing_resp.json()
    existing = existing_rows[0] if isinstance(existing_rows, list) and existing_rows else None
    if existing:
        existing_id = str(existing.get("id"))
        existing_status = str(existing.get("status") or "")
        if existing_status != "rejected":
            raise HTTPException(
                status_code=400,
                detail=f"Athlete already has an application for this competition (status: {existing_status})",
            )
        del_res = await rest_delete("applications", {"id": f"eq.{existing_id}"})
        if del_res.status_code not in (200, 204):
            raise HTTPException(status_code=400, detail="Failed to delete rejected application")

    try:
        app_res = await rest_post(
            "applications",
            {},
            {
                "competition_id": competition_id,
                "athlete_id": str(body.athlete_id),
                "category_id": cat_id,
                "status": "approved",
            },
            prefer="return=representation",
        )
    except Exception as e:
        code = getattr(e, "code", None)
        msg = str(e)
        if code == "23505" or "23505" in msg or "duplicate key value" in msg:
            raise HTTPException(status_code=400, detail="Athlete already has an application for this competition")
        raise
    app_rows = app_res.json()
    if not isinstance(app_rows, list) or not app_rows:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return {
        "ok": True,
        "application": app_rows[0],
        "replaced": bool(existing),
    }

@router.put("/{app_id}/athlete-profile")
async def admin_update_athlete_profile(
    app_id: UUID,
    body: AdminUpdateAthleteProfile,
    authorization: str | None = Header(default=None),
):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    requester_id = await _get_user_id_from_bearer(authorization)
    codes = await _get_role_codes(requester_id)
    if not _is_staff(codes):
        raise HTTPException(status_code=403, detail="Forbidden")

    app_resp = await rest_get(
        "applications",
        {"select": "athlete_id", "id": f"eq.{str(app_id)}", "limit": "1"},
        write=True,
    )
    app_rows = app_resp.json()
    if not isinstance(app_rows, list) or not app_rows or not app_rows[0].get("athlete_id"):
        raise HTTPException(status_code=404, detail="Application not found")
    athlete_id = str(app_rows[0]["athlete_id"])

    ath_resp = await rest_get(
        "athletes",
        {"select": "user_id", "id": f"eq.{athlete_id}", "limit": "1"},
        write=True,
    )
    ath_rows = ath_resp.json()
    if not isinstance(ath_rows, list) or not ath_rows or not ath_rows[0].get("user_id"):
        raise HTTPException(status_code=404, detail="Athlete not found")
    user_id = str(ath_rows[0]["user_id"])

    email = str(body.email or "").strip().lower() if body.email is not None else None
    if email:
        current_email = None
        try:
            cur = await rest_get(
                "users",
                {"select": "email", "id": f"eq.{user_id}", "limit": "1"},
                write=True,
            )
            cur_rows = cur.json()
            if isinstance(cur_rows, list) and cur_rows and isinstance(cur_rows[0], dict):
                current_email = str(cur_rows[0].get("email") or "").strip().lower() or None
        except Exception:
            current_email = None

        if current_email != email:
            await rest_patch("users", {"id": f"eq.{user_id}"}, {"email": email}, prefer="return=minimal")
        if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
            if current_email != email:
                async with httpx.AsyncClient(timeout=httpx.Timeout(20.0, connect=8.0)) as client:
                    upd = await client.patch(
                        f"{SUPABASE_URL}/auth/v1/admin/users/{user_id}",
                        json={"email": email, "email_confirm": True},
                        headers={
                            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                            "apikey": SUPABASE_SERVICE_ROLE_KEY,
                            "Content-Type": "application/json",
                        },
                    )
                    if upd.status_code in (200, 201, 204):
                        pass
                    elif upd.status_code == 404:
                        pass
                    else:
                        t = (upd.text or "").strip()
                        tl = t.lower()
                        if "user not found" in tl or "not found" in tl:
                            pass
                        else:
                            msg = t or f"status {upd.status_code}"
                            raise HTTPException(
                                status_code=400,
                                detail=f"Failed to update auth email ({upd.status_code}): {msg}",
                            )

    prof_payload: dict[str, object] = {
        "user_id": user_id,
        "full_name": body.full_name,
        "city": body.city,
        "location_id": str(body.location_id),
    }
    if body.phone is not None:
        prof_payload["phone"] = str(body.phone).strip() or None
    await rest_upsert("profiles", prof_payload, on_conflict="user_id")
    await rest_patch("athletes", {"id": f"eq.{athlete_id}"}, {"coach_name": body.coach_name}, prefer="return=minimal")
    await rest_upsert(
        "passports",
        {
            "athlete_id": athlete_id,
            "birth_date": body.birth_date,
            "gender": body.gender,
            "rank": body.rank,
            "photo_url": body.photo_url,
        },
        on_conflict="athlete_id",
    )

    return {"ok": True}

@router.patch("/{app_id}/", response_model=Application)
@router.patch("/{app_id}", response_model=Application)
async def update_application_status(app_id: UUID, app_update: ApplicationUpdate):
    # В реальном проекте здесь будет проверка на роль секретаря/админа
    
    # Сначала получим текущую заявку, чтобы знать telegram_id и название соревнования
    # Используем явное имя отношения для athletes -> users
    resp = await rest_get(
        "applications",
        {
            "select": "*,competition:competitions(name,start_date),athletes(users!athletes_user_id_fkey(telegram_id)),competition_categories(*)",
            "id": f"eq.{str(app_id)}",
            "limit": "1",
        },
        write=True,
    )
    rows = resp.json()
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=404, detail="Application not found")
        
    old_app = rows[0]
    
    # Обновляем статус
    update_data = app_update.model_dump(exclude_unset=True)
    if not update_data:
        return old_app
        
    # Сериализуем UUID в строки перед отправкой в Supabase
    for key, value in update_data.items():
        if isinstance(value, UUID):
            update_data[key] = str(value)
            
    upd = await rest_patch(
        "applications",
        {"id": f"eq.{str(app_id)}"},
        update_data,
        prefer="return=representation",
    )
    upd_rows = upd.json()
    if not isinstance(upd_rows, list) or not upd_rows:
        raise HTTPException(status_code=404, detail="Application not found")
        
    updated_app = upd_rows[0]
    
    # Если статус изменился, отправляем уведомление
    if "status" in update_data and updated_app["status"] != old_app["status"]:
        telegram_id = None
        try:
            # Debug: Print structure
            # print(f"[Applications] Old app structure keys: {old_app.keys()}")
            
            athlete_data = old_app.get("athletes")
            if athlete_data:
                # print(f"[Applications] Athlete data type: {type(athlete_data)}")
                if isinstance(athlete_data, list):
                    athlete_data = athlete_data[0] if athlete_data else {}
                
                users_data = athlete_data.get("users")
                if users_data:
                    if isinstance(users_data, list):
                        users_data = users_data[0] if users_data else {}
                        
                    telegram_id = users_data.get("telegram_id")
                    print(f"[Applications] Found telegram_id: {telegram_id}")
                else:
                    print(f"[Applications] No users data in athlete: {athlete_data}")
            else:
                print(f"[Applications] No athlete data in app: {old_app}")
                
        except Exception as e:
            print(f"[Applications] Error extracting telegram_id: {e}")
            import traceback
            traceback.print_exc()
            
        if telegram_id:
            comp_name = old_app.get("competition", {}).get("name", "соревнование")
            new_status = updated_app["status"]
            
            if new_status == "approved":
                message = f"✅ Ваша заявка на <b>{comp_name}</b> одобрена!\n\nОжидаем вас на мандатной комиссии."
            elif new_status == "weighed":
                # Запросим категорию из БД для детального уведомления
                cat_id = updated_app.get("category_id")
                category_str = "неизвестно"
                comp_day_str = ""
                
                if cat_id:
                    try:
                        cat_resp = await rest_get(
                            "competition_categories",
                            {"select": "*", "id": f"eq.{str(cat_id)}", "limit": "1"},
                            write=True,
                        )
                        cat_rows = cat_resp.json()
                        if isinstance(cat_rows, list) and cat_rows:
                            cat = cat_rows[0]
                            gender = "М" if cat.get("gender") == "male" else "Ж"
                            
                            if cat.get('weight_max') == 999:
                                weight_str = f"{int(cat.get('weight_min'))}+ кг"
                            else:
                                weight_str = f"до {cat.get('weight_max')} кг" if cat.get("weight_max") else f"свыше {cat.get('weight_min')} кг"
                                
                            category_str = f"{gender}, {cat.get('age_min')}-{cat.get('age_max')} лет, {weight_str}"
                            
                            # Если есть день выступления, добавляем
                            if cat.get("competition_day"):
                                # Парсим дату, если нужно, или просто выводим
                                comp_day_str = f"\n📅 <b>День выступления:</b> {cat.get('competition_day')}"
                    except Exception as e:
                        print(f"Error fetching category for notification: {e}")

                message = f"⚖️ Вы успешно прошли мандатную комиссию!\n\n🏆 <b>Соревнование:</b> {comp_name}\n👥 <b>Допущены в категории:</b> {category_str}{comp_day_str}\n\nЖелаем удачи на соревнованиях!"
            else:
                message = f"❌ Ваша заявка на <b>{comp_name}</b> отклонена."
            
            if updated_app.get("comment"):
                message += f"\n\n💬 Комментарий: {updated_app['comment']}"
                
            await send_telegram_notification(telegram_id, message)
        else:
            print("[Applications] Warning: No telegram_id found for notification")
            
    return updated_app
