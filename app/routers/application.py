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
from app.schemas.competition import Application, ApplicationCreate, ApplicationUpdate

from app.core.telegram import send_telegram_notification, get_telegram_file_url

router = APIRouter(prefix="/applications", tags=["applications"])

_me_cache: dict[str, tuple[float, str]] = {}
_MSK_TZ = timezone(timedelta(hours=3))
_APPLICATION_DEADLINE = datetime(2026, 4, 18, 0, 0, tzinfo=_MSK_TZ)

def _applications_open_now() -> bool:
    now = datetime.now(_MSK_TZ)
    return now < _APPLICATION_DEADLINE

async def _get_role_codes(user_id: str) -> list[str]:
    if not admin_supabase:
        return []
    try:
        res = admin_supabase.table("user_roles").select("roles(code)").eq("user_id", user_id).execute()
        data = res.data or []
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
    city: str
    location_id: UUID
    coach_name: str
    birth_date: str
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
        # Запрашиваем заявки с данными спортсмена (ФИО из профиля через athletes -> users -> profiles) и данными категории
        # Используем явное имя отношения (FK), так как Supabase нашел несколько связей
        query = supabase.table("applications").select("*, athletes(users!athletes_user_id_fkey(profiles(full_name))), competition_categories(*)")
        if competition_id:
            query = query.eq("competition_id", competition_id)
        response = query.execute()
        
        # Преобразуем данные, чтобы вынести ФИО и описание категории
        apps = []
        for app in response.data:
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
                    gender = "М" if cat["gender"] == "male" else "Ж"
                    
                    if cat.get('weight_max') == 999:
                        weight_str = f"{int(cat.get('weight_min'))}+ кг"
                    else:
                        weight_str = f"до {cat['weight_max']} кг" if cat.get('weight_max') else f"свыше {cat.get('weight_min')} кг"
                        
                    category_desc = f"{gender}, {cat['age_min']}-{cat['age_max']} лет, {weight_str}"
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
        client = supabase
        if authorization and admin_supabase:
            try:
                requester_id = await _get_user_id_from_bearer(authorization)
                codes = await _get_role_codes(requester_id)
                if _is_staff(codes):
                    client = admin_supabase
            except Exception:
                client = supabase

        # Получаем детальную информацию по заявке, включая паспорт
        query = client.table("applications").select(
            "*, "
            "athletes(coach_name, user_id, users!athletes_user_id_fkey(email, profiles(full_name, phone, city, location_id))), "
            "competition_categories(*)"
        ).eq("id", app_id).single().execute()
        
        if not query.data:
            raise HTTPException(status_code=404, detail="Application not found")
            
        app_data = query.data
        
        # Получаем паспорт отдельно, так как он привязан к athlete_id
        athlete_id = app_data.get("athlete_id")
        passport_data = None
        if athlete_id:
            pass_query = client.table("passports").select("*").eq("athlete_id", athlete_id).maybe_single().execute()
            passport_data = pass_query.data
            
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
                gender = "М" if cat.get("gender") == "male" else "Ж"
                
                if cat.get('weight_max') == 999:
                    weight_str = f"{int(cat.get('weight_min'))}+ кг"
                else:
                    weight_str = f"до {cat.get('weight_max')} кг" if cat.get("weight_max") else f"свыше {cat.get('weight_min')} кг"
                    
                category_desc = f"{gender}, {cat.get('age_min')}-{cat.get('age_max')} лет, {weight_str}"
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
        res = supabase.table("passports").update({"is_verified": payload.is_verified}).eq("id", passport_id).execute()
        if not res.data:
            raise HTTPException(status_code=404, detail="Passport not found")
        return res.data[0]
    except Exception as e:
        import traceback
        print(f"[Applications] Error verifying passport: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", response_model=Application)
async def create_application(app_in: ApplicationCreate):
    # В реальном проекте здесь будет проверка на то, что это атлет или тренер за атлета
    
    # Проверка, нет ли уже заявки от этого атлета на это соревнование
    check = supabase.table("applications") \
        .select("id") \
        .eq("athlete_id", app_in.athlete_id) \
        .eq("competition_id", app_in.competition_id) \
        .execute()
        
    if check.data:
        raise HTTPException(status_code=400, detail="Application already exists")
        
    res = supabase.table("applications").insert(app_in.model_dump()).execute()
    if not res.data:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return res.data[0]

@router.post("/me")
async def create_my_application(
    category_id: str,
    authorization: str | None = Header(default=None),
    user_id: str | None = None,
):
    if authorization:
        user_id = await _get_user_id_from_bearer(authorization)
    if not user_id:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    if not _applications_open_now():
        raise HTTPException(status_code=403, detail="Application deadline has passed")

    cat_res = admin_supabase.table("competition_categories").select("competition_id").eq("id", category_id).maybe_single().execute()
    if not cat_res.data or not cat_res.data.get("competition_id"):
        raise HTTPException(status_code=404, detail="Category not found")
    competition_id = str(cat_res.data["competition_id"])

    athlete_res = admin_supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single().execute()
    if not athlete_res.data:
        raise HTTPException(status_code=404, detail="Athlete not found")
        
    athlete_id = athlete_res.data["id"]
    
    # Check if already applied
    existing = admin_supabase.table("applications").select("id").eq("athlete_id", athlete_id).eq("category_id", category_id).execute()
    if existing.data:
        raise HTTPException(status_code=400, detail="Already applied to this category")
        
    res = admin_supabase.table("applications").insert({
        "competition_id": competition_id,
        "athlete_id": athlete_id,
        "category_id": category_id,
        "status": "pending"
    }).execute()
    
    return res.data[0]


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
    cat_res = admin_supabase.table("competition_categories").select("competition_id").eq("id", cat_id).maybe_single().execute()
    if not cat_res.data or not cat_res.data.get("competition_id"):
        raise HTTPException(status_code=404, detail="Category not found")
    competition_id = str(cat_res.data["competition_id"])

    user_id = str(uuid4())
    admin_supabase.table("users").insert({"id": user_id}).execute()
    admin_supabase.table("profiles").insert(
        {"user_id": user_id, "full_name": body.full_name, "city": body.city, "location_id": str(body.location_id)}
    ).execute()
    athlete_res = admin_supabase.table("athletes").insert({"user_id": user_id, "coach_name": body.coach_name}).execute()
    if not athlete_res.data:
        raise HTTPException(status_code=400, detail="Failed to create athlete")
    athlete_id = str(athlete_res.data[0]["id"])

    admin_supabase.table("passports").upsert(
        {"athlete_id": athlete_id, "birth_date": body.birth_date, "rank": body.rank, "photo_url": body.photo_url},
        on_conflict="athlete_id",
    ).execute()

    status = "weighed" if body.actual_weight is not None else "approved"
    app_payload = {
        "competition_id": competition_id,
        "athlete_id": athlete_id,
        "category_id": cat_id,
        "status": status,
        "declared_weight": body.declared_weight,
        "actual_weight": body.actual_weight,
    }
    app_res = admin_supabase.table("applications").insert(app_payload).execute()
    if not app_res.data:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return {"ok": True, "user_id": user_id, "athlete_id": athlete_id, "application": app_res.data[0]}


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
    cat_res = admin_supabase.table("competition_categories").select(
        "id,competition_id,gender,age_min,age_max"
    ).eq("id", cat_id).maybe_single().execute()
    if not cat_res.data:
        raise HTTPException(status_code=404, detail="Category not found")
    category = cat_res.data
    competition_id = str(category["competition_id"])

    comp_res = admin_supabase.table("competitions").select("start_date").eq("id", competition_id).maybe_single().execute()
    start_date_str = comp_res.data.get("start_date") if comp_res.data else None

    pass_res = admin_supabase.table("passports").select("birth_date,gender,photo_url").eq("athlete_id", str(body.athlete_id)).maybe_single().execute()
    passport = pass_res.data or {}
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

    existing_app = (
        admin_supabase.table("applications")
        .select("id,status,category_id")
        .eq("athlete_id", str(body.athlete_id))
        .eq("competition_id", competition_id)
        .maybe_single()
        .execute()
    )
    if existing_app.data:
        existing_id = str(existing_app.data.get("id"))
        existing_status = str(existing_app.data.get("status") or "")
        if existing_status != "rejected":
            raise HTTPException(
                status_code=400,
                detail=f"Athlete already has an application for this competition (status: {existing_status})",
            )
        del_res = admin_supabase.table("applications").delete().eq("id", existing_id).execute()
        if del_res.data is None:
            raise HTTPException(status_code=400, detail="Failed to delete rejected application")

    try:
        app_res = admin_supabase.table("applications").insert(
            {
                "competition_id": competition_id,
                "athlete_id": str(body.athlete_id),
                "category_id": cat_id,
                "status": "approved",
            }
        ).execute()
    except Exception as e:
        code = getattr(e, "code", None)
        msg = str(e)
        if code == "23505" or "23505" in msg or "duplicate key value" in msg:
            raise HTTPException(status_code=400, detail="Athlete already has an application for this competition")
        raise
    if not app_res.data:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return {
        "ok": True,
        "application": app_res.data[0],
        "replaced": bool(existing_app.data),
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

    app_res = admin_supabase.table("applications").select("athlete_id").eq("id", str(app_id)).maybe_single().execute()
    if not app_res.data or not app_res.data.get("athlete_id"):
        raise HTTPException(status_code=404, detail="Application not found")
    athlete_id = str(app_res.data["athlete_id"])

    ath_res = admin_supabase.table("athletes").select("user_id").eq("id", athlete_id).maybe_single().execute()
    if not ath_res.data or not ath_res.data.get("user_id"):
        raise HTTPException(status_code=404, detail="Athlete not found")
    user_id = str(ath_res.data["user_id"])

    admin_supabase.table("profiles").upsert(
        {
            "user_id": user_id,
            "full_name": body.full_name,
            "city": body.city,
            "location_id": str(body.location_id),
        },
        on_conflict="user_id",
    ).execute()
    admin_supabase.table("athletes").update({"coach_name": body.coach_name}).eq("id", athlete_id).execute()
    admin_supabase.table("passports").upsert(
        {"athlete_id": athlete_id, "birth_date": body.birth_date, "rank": body.rank, "photo_url": body.photo_url},
        on_conflict="athlete_id",
    ).execute()

    return {"ok": True}

@router.patch("/{app_id}/", response_model=Application)
@router.patch("/{app_id}", response_model=Application)
async def update_application_status(app_id: UUID, app_update: ApplicationUpdate):
    # В реальном проекте здесь будет проверка на роль секретаря/админа
    
    # Сначала получим текущую заявку, чтобы знать telegram_id и название соревнования
    # Используем явное имя отношения для athletes -> users
    try:
        query = supabase.table("applications") \
            .select("*, competition:competitions(name), athletes(users!athletes_user_id_fkey(telegram_id))") \
            .eq("id", app_id) \
            .single() \
            .execute()
    except Exception as e:
        print(f"[Applications] Error fetching app for update: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        
    if not query.data:
        raise HTTPException(status_code=404, detail="Application not found")
        
    old_app = query.data
    
    # Обновляем статус
    update_data = app_update.model_dump(exclude_unset=True)
    if not update_data:
        return old_app
        
    # Сериализуем UUID в строки перед отправкой в Supabase
    for key, value in update_data.items():
        if isinstance(value, UUID):
            update_data[key] = str(value)
            
    res = supabase.table("applications") \
        .update(update_data) \
        .eq("id", app_id) \
        .execute()
        
    if not res.data:
        raise HTTPException(status_code=404, detail="Application not found")
        
    updated_app = res.data[0]
    
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
                        cat_res = supabase.table("competition_categories").select("*").eq("id", cat_id).single().execute()
                        if cat_res.data:
                            cat = cat_res.data
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
