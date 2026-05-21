from fastapi import APIRouter, HTTPException, Response, Header, UploadFile, File
from pydantic import BaseModel
from typing import List, Optional
import random
import hashlib
import time
import os
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4
from urllib.parse import quote
import anyio
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from app.core.rest import rest_get, rest_post, rest_upsert, rest_patch, rest_delete
from app.core.local_auth import get_user_id_from_bearer as _local_get_user_id_from_bearer
from app.schemas.competition import Application, ApplicationCreate, ApplicationUpdate

from app.core.telegram import send_telegram_notification, get_telegram_file_url
from app.core.minio import MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY, put_object as _minio_put_object

router = APIRouter(prefix="/applications", tags=["applications"])

_me_cache: dict[str, tuple[float, str]] = {}
_MSK_TZ = timezone(timedelta(hours=3))
_APPLICATION_DEADLINE = datetime(2026, 4, 24, 19, 0, tzinfo=_MSK_TZ)

_minio_s3 = None


def _minio_client():
    global _minio_s3
    if _minio_s3 is None:
        if not (MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY and MINIO_BUCKET):
            raise RuntimeError("MinIO env is not configured")
        _minio_s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=os.getenv("MINIO_REGION") or "us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
    return _minio_s3


def _supabase_headers() -> dict[str, str] | None:
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or "").strip()
    if not key:
        return None
    return {"apikey": key, "authorization": f"Bearer {key}"}


def _is_telegram_file_id(v: str) -> bool:
    vv = (v or "").strip()
    if not vv:
        return False
    if vv.lower().startswith("http"):
        return False
    if vv.startswith("documents/") or vv.startswith("/"):
        return False
    return True


def _is_minio_key(v: str) -> bool:
    vv = (v or "").strip()
    if not vv:
        return False
    return vv.startswith("documents/") or vv.startswith("/")

def _applications_open_now() -> bool:
    now = datetime.now(_MSK_TZ)
    return now < _APPLICATION_DEADLINE

def _normalize_gender(g: str | None) -> str:
    s = (g or "").strip().lower()
    if s in {"male", "m", "м"}:
        return "male"
    if s in {"female", "f", "ж"}:
        return "female"
    return s

def _category_group(gender: str | None, age_min: int | None, age_max: int | None) -> str:
    g = _normalize_gender(gender)
    is_male = g == "male"
    is_female = g == "female"
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
        from app.core.roles import get_role_codes

        return await get_role_codes(user_id)
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
    gender: Optional[str] = None
    series: Optional[str] = None
    number: Optional[str] = None
    issued_by: Optional[str] = None
    issue_date: Optional[str] = None
    rank: str
    photo_url: str
    passport_scan_url: Optional[str] = None
    declared_weight: Optional[float] = None
    actual_weight: Optional[float] = None


class AdminUpdateAthleteProfile(BaseModel):
    full_name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    city: str
    location_id: UUID
    coach_name: str
    birth_date: Optional[str] = None
    gender: Optional[str] = None
    series: Optional[str] = None
    number: Optional[str] = None
    issued_by: Optional[str] = None
    issue_date: Optional[str] = None
    rank: Optional[str] = None
    photo_url: Optional[str] = None
    passport_scan_url: Optional[str] = None


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
    user_id = await _local_get_user_id_from_bearer(authorization)
    _me_cache[cache_key] = (time.time() + 30.0, user_id)
    return user_id

@router.get("/", response_model=List[Application])
async def get_applications(competition_id: Optional[UUID] = None):
    try:
        from sqlalchemy import select as _select
        from app.core.db import SessionLocal, tables

        apps_t = tables["applications"]
        athletes_t = tables.get("athletes")
        users_t = tables.get("users")
        profiles_t = tables.get("profiles")
        locs_t = tables.get("locations")
        cats_t = tables.get("competition_categories")
        comps_t = tables.get("competitions")

        async with SessionLocal() as session:
            stmt = _select(apps_t).order_by(apps_t.c.created_at.desc()).limit(10000)
            if competition_id:
                stmt = stmt.where(apps_t.c.competition_id == str(competition_id))
            res = await session.execute(stmt)
            rows = [dict(r) for r in res.mappings().all()]

            athlete_ids = {str(r.get("athlete_id")) for r in rows if r.get("athlete_id")}
            cat_ids = {str(r.get("category_id")) for r in rows if r.get("category_id")}
            comp_ids = {str(r.get("competition_id")) for r in rows if r.get("competition_id")}

            comp_start: dict[str, str | None] = {}
            if comps_t is not None and comp_ids:
                c_res = await session.execute(
                    _select(comps_t.c.id, comps_t.c.start_date).where(comps_t.c.id.in_(list(comp_ids)))
                )
                for r in c_res.mappings().all():
                    comp_start[str(r.get("id"))] = r.get("start_date")

            cats_map: dict[str, dict] = {}
            if cats_t is not None and cat_ids:
                cat_res = await session.execute(_select(cats_t).where(cats_t.c.id.in_(list(cat_ids))))
                for r in cat_res.mappings().all():
                    cats_map[str(r.get("id"))] = dict(r)

            athlete_map: dict[str, dict] = {}
            if athletes_t is not None and users_t is not None and profiles_t is not None and athlete_ids:
                j = athletes_t.join(users_t, athletes_t.c.user_id == users_t.c.id).outerjoin(
                    profiles_t, profiles_t.c.user_id == users_t.c.id
                )
                if locs_t is not None:
                    j = j.outerjoin(locs_t, profiles_t.c.location_id == locs_t.c.id)
                cols = [
                    athletes_t.c.id.label("athlete_id"),
                    profiles_t.c.full_name,
                    profiles_t.c.location_id,
                ]
                if locs_t is not None:
                    cols.append(locs_t.c.name.label("region_name"))
                a_res = await session.execute(_select(*cols).select_from(j).where(athletes_t.c.id.in_(list(athlete_ids))))
                for r in a_res.mappings().all():
                    athlete_map[str(r.get("athlete_id"))] = dict(r)

        apps_out = []
        for app in rows:
            athlete_id = str(app.get("athlete_id")) if app.get("athlete_id") else ""
            cat_id = str(app.get("category_id")) if app.get("category_id") else ""
            comp_id = str(app.get("competition_id")) if app.get("competition_id") else ""

            a = athlete_map.get(athlete_id) or {}
            full_name = a.get("full_name") or "Unknown"
            athlete_location_id = a.get("location_id")
            athlete_region = a.get("region_name")

            cat = cats_map.get(cat_id) or None
            at_date = comp_start.get(comp_id)
            category_desc = _format_category_label(cat or {}, at_date) if cat else "Unknown"

            app["athlete_name"] = full_name
            app["athlete_location_id"] = athlete_location_id
            app["athlete_region"] = athlete_region
            app["category_description"] = category_desc
            apps_out.append(app)

        return apps_out
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


@router.get("/photo-key/{key:path}")
async def get_photo_key_proxy(key: str):
    k = str(key or "").lstrip("/")
    if not k.startswith("documents/"):
        raise HTTPException(status_code=404, detail="Not found")

    def _get_from_minio():
        try:
            r = _minio_client().get_object(Bucket=MINIO_BUCKET, Key=k)
            return r.get("Body").read(), r.get("ContentType")
        except ClientError as e:
            code = str((e.response or {}).get("Error", {}).get("Code") or "")
            if code in {"NoSuchKey", "NoSuchObject", "404"}:
                return None
            raise

    minio_res = await anyio.to_thread.run_sync(_get_from_minio)
    if minio_res is not None:
        content, ct = minio_res
        return Response(
            content=content,
            media_type=ct or "application/octet-stream",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    supabase_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    headers = _supabase_headers()
    if not supabase_url or not headers:
        raise HTTPException(status_code=404, detail="Not found")

    encoded = quote(k, safe="/")
    url = f"{supabase_url}/storage/v1/object/avatars/{encoded}"
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, http2=False) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=404, detail="Not found")
        content = resp.content
        ct = (resp.headers.get("content-type") or "").split(";", 1)[0].strip() or None

    try:
        await _minio_put_object(k, content, content_type=ct)
    except Exception:
        pass

    return Response(
        content=content,
        media_type=ct or "application/octet-stream",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@router.post("/{app_id}/passport/photo")
async def upload_passport_photo(
    app_id: UUID,
    photo: UploadFile = File(...),
    authorization: str | None = Header(default=None),
):
    requester_id = await _get_user_id_from_bearer(authorization)
    codes = await _get_role_codes(requester_id)
    if not _is_staff(codes):
        raise HTTPException(status_code=403, detail="Forbidden")

    if not photo.content_type or not str(photo.content_type).startswith("image/"):
        raise HTTPException(status_code=400, detail="Only image files are supported")
    content = await photo.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 10MB)")

    ext = os.path.splitext(photo.filename or "")[1].lower()
    if not ext:
        ct = str(photo.content_type or "").lower()
        if ct == "image/png":
            ext = ".png"
        elif ct == "image/webp":
            ext = ".webp"
        else:
            ext = ".jpg"

    app_resp = await rest_get(
        "applications",
        {"select": "athlete_id", "id": f"eq.{str(app_id)}", "limit": "1"},
        write=True,
    )
    app_rows = app_resp.json()
    if not isinstance(app_rows, list) or not app_rows or not app_rows[0].get("athlete_id"):
        raise HTTPException(status_code=404, detail="Application not found")
    athlete_id = str(app_rows[0]["athlete_id"])

    object_path = f"documents/{athlete_id}/{uuid4().hex}{ext}"
    from app.core.minio import put_object

    photo_url = await put_object(
        object_path,
        content,
        content_type=photo.content_type or "application/octet-stream",
    )

    return {"ok": True, "photo_url": photo_url}

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
        base_resp = await rest_get(
            "applications",
            {"select": "*", "id": f"eq.{str(app_id)}", "limit": "1"},
            write=write,
        )
        base_rows = base_resp.json()
        if not isinstance(base_rows, list) or not base_rows:
            raise HTTPException(status_code=404, detail="Application not found")
        app_data = base_rows[0]

        athlete_id_val = app_data.get("athlete_id")
        if athlete_id_val:
            ath_resp = await rest_get(
                "athletes",
                {"select": "id,coach_name,user_id", "id": f"eq.{str(athlete_id_val)}", "limit": "1"},
                write=write,
            )
            ath_rows = ath_resp.json()
            athlete_row = ath_rows[0] if isinstance(ath_rows, list) and ath_rows else None
        else:
            athlete_row = None

        users_row = None
        profiles_row = None
        if isinstance(athlete_row, dict) and athlete_row.get("user_id"):
            u_resp = await rest_get(
                "users",
                {"select": "id,email", "id": f"eq.{str(athlete_row.get('user_id'))}", "limit": "1"},
                write=write,
            )
            u_rows = u_resp.json()
            users_row = u_rows[0] if isinstance(u_rows, list) and u_rows else None

            p_resp = await rest_get(
                "profiles",
                {"select": "user_id,full_name,phone,city,location_id", "user_id": f"eq.{str(athlete_row.get('user_id'))}", "limit": "1"},
                write=write,
            )
            p_rows = p_resp.json()
            profiles_row = p_rows[0] if isinstance(p_rows, list) and p_rows else None

        if isinstance(users_row, dict) and isinstance(profiles_row, dict):
            users_row["profiles"] = profiles_row
        if isinstance(athlete_row, dict) and isinstance(users_row, dict):
            athlete_row["users"] = users_row
        if athlete_row is not None:
            app_data["athletes"] = athlete_row

        if app_data.get("category_id"):
            cat_resp = await rest_get(
                "competition_categories",
                {"select": "*", "id": f"eq.{str(app_data.get('category_id'))}", "limit": "1"},
                write=write,
            )
            cat_rows = cat_resp.json()
            if isinstance(cat_rows, list) and cat_rows:
                app_data["competition_categories"] = cat_rows[0]

        if app_data.get("competition_id"):
            comp_resp = await rest_get(
                "competitions",
                {"select": "id,name,start_date", "id": f"eq.{str(app_data.get('competition_id'))}", "limit": "1"},
                write=write,
            )
            comp_rows = comp_resp.json()
            if isinstance(comp_rows, list) and comp_rows:
                app_data["competition"] = comp_rows[0]
        
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
                raw = passport_data["photo_url"]
                if isinstance(raw, str):
                    if _is_telegram_file_id(raw):
                        passport_data["photo_url"] = f"/applications/photo/{raw}"
                    elif _is_minio_key(raw):
                        passport_data["photo_url"] = f"/applications/photo-key/{raw.lstrip('/')}"
            
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

    if not prof.get("full_name") or not prof.get("city") or not prof.get("location_id"):
        raise HTTPException(status_code=400, detail="Fill full_name, city and region")
    if not coach.get("coach_name"):
        raise HTTPException(status_code=400, detail="Fill coach name")

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

    pass_payload: dict[str, object] = {
        "athlete_id": athlete_id,
        "birth_date": body.birth_date,
        "rank": body.rank,
        "photo_url": body.photo_url,
    }
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
    if body.passport_scan_url is not None:
        pass_payload["passport_scan_url"] = body.passport_scan_url
    await rest_upsert("passports", pass_payload, on_conflict="athlete_id")

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

    athlete_gender = _normalize_gender(str(passport.get("gender")))
    category_gender = _normalize_gender(str(category.get("gender")))
    if category_gender != athlete_gender:
        raise HTTPException(status_code=400, detail="Athlete gender does not match category")

    def _age_at(birth_date: str, at_date: str | None) -> int:
        b = datetime.fromisoformat(str(birth_date)).date()
        if at_date:
            s = str(at_date).replace("Z", "+00:00")
            at = datetime.fromisoformat(s).date()
        else:
            at = datetime.now(_MSK_TZ).date()
        return int(at.year) - int(b.year)

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
                "status": "pending",
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
    pass_payload: dict[str, object] = {"athlete_id": athlete_id}
    passport_has_changes = False
    if body.birth_date is not None:
        pass_payload["birth_date"] = body.birth_date
        passport_has_changes = True
    if body.rank is not None:
        pass_payload["rank"] = body.rank
        passport_has_changes = True
    if body.photo_url is not None:
        pass_payload["photo_url"] = body.photo_url
        passport_has_changes = True
    if body.gender is not None:
        pass_payload["gender"] = body.gender
        passport_has_changes = True
    if body.series is not None:
        pass_payload["series"] = body.series
        passport_has_changes = True
    if body.number is not None:
        pass_payload["number"] = body.number
        passport_has_changes = True
    if body.issued_by is not None:
        pass_payload["issued_by"] = body.issued_by
        passport_has_changes = True
    if body.issue_date is not None:
        pass_payload["issue_date"] = body.issue_date
        passport_has_changes = True
    if body.passport_scan_url is not None:
        pass_payload["passport_scan_url"] = body.passport_scan_url
        passport_has_changes = True
    if passport_has_changes:
        await rest_upsert("passports", pass_payload, on_conflict="athlete_id")

    return {"ok": True}

@router.patch("/{app_id}/", response_model=Application)
@router.patch("/{app_id}", response_model=Application)
async def update_application_status(
    app_id: UUID,
    app_update: ApplicationUpdate,
    authorization: str | None = Header(default=None),
):
    requester_id = await _get_user_id_from_bearer(authorization)
    codes = await _get_role_codes(requester_id)
    if not _is_staff(codes):
        raise HTTPException(status_code=403, detail="Forbidden")
    
    # Сначала получим текущую заявку, чтобы знать telegram_id и название соревнования
    # Используем явное имя отношения для athletes -> users
    base_resp = await rest_get(
        "applications",
        {"select": "*", "id": f"eq.{str(app_id)}", "limit": "1"},
        write=True,
    )
    base_rows = base_resp.json()
    if not isinstance(base_rows, list) or not base_rows:
        raise HTTPException(status_code=404, detail="Application not found")

    old_app = base_rows[0]
    if old_app.get("competition_id"):
        comp_resp = await rest_get(
            "competitions",
            {"select": "id,name,start_date", "id": f"eq.{str(old_app.get('competition_id'))}", "limit": "1"},
            write=True,
        )
        comp_rows = comp_resp.json()
        old_app["competition"] = comp_rows[0] if isinstance(comp_rows, list) and comp_rows else {}

    if old_app.get("category_id"):
        cat_resp = await rest_get(
            "competition_categories",
            {"select": "*", "id": f"eq.{str(old_app.get('category_id'))}", "limit": "1"},
            write=True,
        )
        cat_rows = cat_resp.json()
        old_app["competition_categories"] = cat_rows[0] if isinstance(cat_rows, list) and cat_rows else {}

    telegram_id = None
    if old_app.get("athlete_id"):
        ath_resp = await rest_get(
            "athletes",
            {"select": "id,user_id", "id": f"eq.{str(old_app.get('athlete_id'))}", "limit": "1"},
            write=True,
        )
        ath_rows = ath_resp.json()
        if isinstance(ath_rows, list) and ath_rows and isinstance(ath_rows[0], dict):
            uid = ath_rows[0].get("user_id")
            if uid:
                u_resp = await rest_get(
                    "users",
                    {"select": "id,telegram_id", "id": f"eq.{str(uid)}", "limit": "1"},
                    write=True,
                )
                u_rows = u_resp.json()
                if isinstance(u_rows, list) and u_rows and isinstance(u_rows[0], dict):
                    telegram_id = u_rows[0].get("telegram_id")
    if telegram_id is not None:
        old_app["athletes"] = {"users": {"telegram_id": telegram_id}}
    
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
