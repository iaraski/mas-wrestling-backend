from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from typing import List
from uuid import UUID
from datetime import datetime
from uuid import uuid4
import os
import httpx
import anyio
from app.core.supabase import supabase, admin_supabase, SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_ROLE_KEY
from app.core.rest import rest_get
from app.schemas.competition import Competition, CompetitionCreate, CompetitionUpdate
from app.core.cache import cache

router = APIRouter(prefix="/competitions", tags=["competitions"])

def _norm_iso(dt: str | datetime | None) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.isoformat().replace("+00:00", "Z")
    try:
        parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        return parsed.isoformat().replace("+00:00", "Z")
    except Exception:
        return str(dt)

def _cat_key(cat: dict) -> tuple:
    gender = (cat.get("gender") or "").lower()
    age_min = int(cat.get("age_min")) if cat.get("age_min") is not None else None
    age_max = int(cat.get("age_max")) if cat.get("age_max") is not None else None
    wmin = float(cat.get("weight_min")) if cat.get("weight_min") is not None else None
    wmax = cat.get("weight_max")
    wmax = float(wmax) if wmax is not None else None
    if wmax is not None and abs(wmax - 999.0) < 1e-6:
        wmax = 999.0
    day = _norm_iso(cat.get("competition_day"))
    mandate = _norm_iso(cat.get("mandate_day"))
    return (
        gender,
        age_min,
        age_max,
        None if wmin is None else round(wmin, 6),
        None if wmax is None else round(wmax, 6),
        day,
        mandate,
    )

async def _execute(query, *, retries: int = 2):
    for attempt in range(retries + 1):
        try:
            res = await anyio.to_thread.run_sync(query.execute)
            return res
        except Exception as e:
            if attempt >= retries:
                raise e
            await anyio.sleep(0.2 * (attempt + 1))

@router.get("/active")
async def get_active_competitions():
    try:
        cache_key = "competitions:active"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        params = {
            "select": "*,categories:competition_categories(*)",
            "end_date": f"gte.{datetime.now().isoformat()}",
            "order": "start_date.asc",
        }
        resp = await rest_get("competitions", params, write=False)
        data = resp.json()
        cache.set(cache_key, data, ttl_seconds=15.0)
        return data
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Supabase unavailable: {repr(e)}")

@router.get("/", response_model=List[Competition])
async def get_competitions():
    # Получаем соревнования с их категориями и названием локации
    try:
        cache_key = "competitions:list"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        params = {"select": "*,categories:competition_categories(*),locations(name)"}
        resp = await rest_get("competitions", params, write=False)
        rows = resp.json()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Supabase unavailable: {repr(e)}")
    
    # Добавляем location_name в результат
    data = []
    for comp in rows:
        if comp.get("locations"):
            comp["location_name"] = comp["locations"]["name"]
        data.append(comp)
        
    cache.set(cache_key, data, ttl_seconds=15.0)
    return data

@router.post("/", response_model=Competition)
async def create_competition(comp: CompetitionCreate):
    try:
        # 1. Создаем соревнование
        comp_data = comp.model_dump(exclude={"categories", "secretaries"})
        if not comp_data.get("preview_url"):
            comp_data.pop("preview_url", None)
        
        # Убеждаемся, что даты в правильном формате (строки ISO)
        for field in ["mandate_start_date", "mandate_end_date", "start_date", "end_date"]:
            if field in comp_data and isinstance(comp_data[field], datetime):
                comp_data[field] = comp_data[field].isoformat()

        # Convert UUIDs to strings
        if "location_id" in comp_data and comp_data["location_id"]:
            comp_data["location_id"] = str(comp_data["location_id"])

        print(f"[Backend] Creating competition: {comp_data['name']}")
        
        res = supabase.table("competitions").insert(comp_data).execute()
        
        print(f"[Backend] Competition insert response: {res.data}")
        
        if not res.data:
            raise HTTPException(status_code=400, detail="Supabase insert failed: no data returned")
        
        new_comp = res.data[0]
        print(f"[Backend] Competition created with ID: {new_comp['id']}")
        
        # 2. Создаем категории
        final_categories = []
        if comp.categories:
            categories_data = []
            for cat in comp.categories:
                cat_dict = cat.model_dump()
                cat_dict["competition_id"] = new_comp["id"]
                if isinstance(cat_dict.get("competition_day"), datetime):
                    cat_dict["competition_day"] = cat_dict["competition_day"].isoformat()
                if isinstance(cat_dict.get("mandate_day"), datetime):
                    cat_dict["mandate_day"] = cat_dict["mandate_day"].isoformat()
                categories_data.append(cat_dict)

            print(f"[Backend] Inserting {len(categories_data)} categories")
            cat_res = supabase.table("competition_categories").insert(categories_data).execute()
            final_categories = cat_res.data
            print(f"[Backend] Categories created: {len(final_categories)}")
            
        # 3. Добавляем секретарей
        if comp.secretaries:
            secretaries_data = [
                {"competition_id": new_comp["id"], "user_id": str(sec_id)}
                for sec_id in comp.secretaries
            ]
            supabase.table("competition_secretaries").insert(secretaries_data).execute()
            
        cache.invalidate_prefix("competitions:")
        return {**new_comp, "categories": final_categories}
    except Exception as e:
        print(f"[Backend] ERROR in create_competition: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{comp_id}", response_model=Competition)
async def get_competition(comp_id: UUID):
    try:
        cache_key = f"competitions:detail:{str(comp_id)}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        q = supabase.table("competitions").select("*, categories:competition_categories(*)").eq("id", str(comp_id)).single()
        response = await _execute(q)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Supabase unavailable: {repr(e)}")
    if not response.data:
        raise HTTPException(status_code=404, detail="Competition not found")
    data = response.data
    cache.set(cache_key, data, ttl_seconds=15.0)
    return data

@router.patch("/{comp_id}/", response_model=Competition)
@router.patch("/{comp_id}", response_model=Competition)
async def update_competition(comp_id: UUID, comp_update: CompetitionUpdate):
    try:
        comp_id_str = str(comp_id)
        update_data = comp_update.model_dump(exclude_unset=True)
        if "preview_url" in update_data and not update_data.get("preview_url"):
            update_data.pop("preview_url", None)
        
        # Разделяем данные: основная таблица vs связанные
        categories_data = update_data.pop("categories", None)
        secretaries_data = update_data.pop("secretaries", None)
        
        # Сериализация UUID для основной таблицы
        for key, value in update_data.items():
            if isinstance(value, UUID):
                update_data[key] = str(value)
            elif isinstance(value, datetime):
                update_data[key] = value.isoformat()

        if update_data:
            res = supabase.table("competitions").update(update_data).eq("id", comp_id_str).execute()
            if not res.data:
                raise HTTPException(status_code=404, detail="Competition not found")
        
        # Обновление категорий
        if categories_data is not None:
            # Получаем старые категории и используемые в заявках
            existing_res = (
                supabase.table("competition_categories")
                .select("id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day")
                .eq("competition_id", comp_id_str)
                .execute()
            )
            existing_cats = existing_res.data or []
            old_cat_ids = {str(cat["id"]) for cat in existing_cats}

            apps_res = (
                supabase.table("applications")
                .select("category_id")
                .eq("competition_id", comp_id_str)
                .execute()
            )
            used_cat_ids = {str(a["category_id"]) for a in (apps_res.data or []) if a.get("category_id")}

            existing_by_key: dict[tuple, list[dict]] = {}
            for c in existing_cats:
                existing_by_key.setdefault(_cat_key(c), []).append(c)
            
            # Вставляем новые и обновляем существующие
            if categories_data:
                for cat in categories_data:
                    cat_dict = cat if isinstance(cat, dict) else cat.model_dump()
                    cat_dict["competition_id"] = comp_id_str
                    if isinstance(cat_dict.get("competition_day"), datetime):
                        cat_dict["competition_day"] = cat_dict["competition_day"].isoformat()
                    if isinstance(cat_dict.get("mandate_day"), datetime):
                        cat_dict["mandate_day"] = cat_dict["mandate_day"].isoformat()
                    cat_dict["competition_day"] = _norm_iso(cat_dict.get("competition_day"))
                    cat_dict["mandate_day"] = _norm_iso(cat_dict.get("mandate_day"))
                    
                    if "id" in cat_dict and cat_dict["id"]:
                        # Обновляем существующую
                        cat_id = str(cat_dict.pop("id"))
                        if cat_id in old_cat_ids:
                            old_cat_ids.remove(cat_id)
                        supabase.table("competition_categories").update(cat_dict).eq("id", cat_id).execute()
                    else:
                        # Пытаемся сопоставить по полям, чтобы не плодить дубликаты
                        if "id" in cat_dict:
                            del cat_dict["id"]
                        key = _cat_key(cat_dict)
                        candidates = existing_by_key.get(key) or []
                        chosen = None
                        for c in candidates:
                            if str(c.get("id")) in used_cat_ids:
                                chosen = c
                                break
                        chosen = chosen or (candidates[0] if candidates else None)
                        if chosen:
                            chosen_id = str(chosen["id"])
                            if chosen_id in old_cat_ids:
                                old_cat_ids.remove(chosen_id)
                            supabase.table("competition_categories").update(cat_dict).eq("id", chosen_id).execute()
                        else:
                            supabase.table("competition_categories").insert(cat_dict).execute()
                        
            # Удаляем те, которых больше нет (если на них нет ссылок в заявках)
            for cat_id in old_cat_ids:
                if cat_id in used_cat_ids:
                    continue
                try:
                    supabase.table("competition_categories").delete().eq("id", cat_id).execute()
                except Exception as e:
                    print(f"Cannot delete category {cat_id}, likely has applications: {e}")

            # Чистим точные дубликаты (удаляем только неиспользуемые)
            refreshed = (
                supabase.table("competition_categories")
                .select("id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day")
                .eq("competition_id", comp_id_str)
                .execute()
            )
            groups: dict[tuple, list[dict]] = {}
            for c in (refreshed.data or []):
                groups.setdefault(_cat_key(c), []).append(c)
            for _, group in groups.items():
                if len(group) <= 1:
                    continue
                keep = None
                for c in group:
                    if str(c.get("id")) in used_cat_ids:
                        keep = c
                        break
                keep = keep or group[0]
                keep_id = str(keep.get("id"))
                for c in group:
                    cid = str(c.get("id"))
                    if cid == keep_id or cid in used_cat_ids:
                        continue
                    try:
                        supabase.table("competition_categories").delete().eq("id", cid).execute()
                    except Exception:
                        pass
                
        # Обновление секретарей (полная замена)
        if secretaries_data is not None:
            supabase.table("competition_secretaries").delete().eq("competition_id", comp_id_str).execute()
            if secretaries_data:
                secs_to_insert = [
                    {"competition_id": comp_id_str, "user_id": str(sec_id)}
                    for sec_id in secretaries_data
                ]
                supabase.table("competition_secretaries").insert(secs_to_insert).execute()
                
        # Возвращаем обновленное соревнование
        final_res = supabase.table("competitions").select("*, categories:competition_categories(*)").eq("id", comp_id_str).single().execute()
        cache.invalidate_prefix("competitions:")
        return final_res.data
        
    except Exception as e:
        print(f"[Backend] ERROR in update_competition: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{comp_id}/preview")
async def upload_competition_preview(comp_id: UUID, file: UploadFile = File(...)):
    try:
        if not admin_supabase:
            raise HTTPException(status_code=500, detail="Service role not configured for uploads")

        if not file.content_type or not file.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="Only image files are supported")

        bucket = os.getenv("SUPABASE_COMPETITION_PREVIEW_BUCKET", "competition-previews")
        filename = file.filename or "preview"
        ext = os.path.splitext(filename)[1].lower()
        if not ext:
            if file.content_type == "image/png":
                ext = ".png"
            elif file.content_type == "image/webp":
                ext = ".webp"
            else:
                ext = ".jpg"

        object_path = f"{comp_id}/preview{ext}"
        content = await file.read()

        if not content:
            raise HTTPException(status_code=400, detail="Empty file")

        if len(content) > 10 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="File too large (max 10MB)")

        try:
            admin_supabase.storage.from_(bucket).remove(
                [
                    f"{comp_id}/preview.jpg",
                    f"{comp_id}/preview.jpeg",
                    f"{comp_id}/preview.png",
                    f"{comp_id}/preview.webp",
                    f"{comp_id}/preview",
                ]
            )
        except Exception:
            pass

        admin_supabase.storage.from_(bucket).upload(
            object_path,
            content,
            file_options={"content-type": file.content_type or "application/octet-stream"},
        )

        preview_url = f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{object_path}"
        if not SUPABASE_SERVICE_ROLE_KEY or not SUPABASE_KEY:
            raise HTTPException(status_code=500, detail="Supabase keys not configured")

        async with httpx.AsyncClient(timeout=20.0, http2=False) as client:
            resp = await client.patch(
                f"{SUPABASE_URL}/rest/v1/competitions",
                params={"id": f"eq.{str(comp_id)}"},
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "apikey": SUPABASE_KEY,
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json={"preview_url": preview_url},
            )
        if resp.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=f"Failed to update preview_url: {resp.status_code} {resp.text}")
        cache.invalidate_prefix("competitions:")
        return {"preview_url": preview_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
