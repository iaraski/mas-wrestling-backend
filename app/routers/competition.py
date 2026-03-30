from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from typing import List
from uuid import UUID
from datetime import datetime
from uuid import uuid4
import os
from app.core.supabase import supabase, admin_supabase, SUPABASE_URL
from app.schemas.competition import Competition, CompetitionCreate, CompetitionUpdate

router = APIRouter(prefix="/competitions", tags=["competitions"])

@router.get("/active")
async def get_active_competitions():
    res = supabase.table("competitions").select("*, categories:competition_categories(*)").gte("end_date", datetime.now().isoformat()).order("start_date", desc=False).execute()
    return res.data

@router.get("/", response_model=List[Competition])
async def get_competitions():
    # Получаем соревнования с их категориями и названием локации
    response = supabase.table("competitions").select("*, categories:competition_categories(*), locations(name)").execute()
    
    # Добавляем location_name в результат
    data = []
    for comp in response.data:
        if comp.get("locations"):
            comp["location_name"] = comp["locations"]["name"]
        data.append(comp)
        
    return data

@router.post("/", response_model=Competition)
async def create_competition(comp: CompetitionCreate):
    try:
        # 1. Создаем соревнование
        comp_data = comp.model_dump(exclude={"categories", "secretaries"})
        
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
            
        return {**new_comp, "categories": final_categories}
    except Exception as e:
        print(f"[Backend] ERROR in create_competition: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{comp_id}", response_model=Competition)
async def get_competition(comp_id: UUID):
    response = supabase.table("competitions").select("*, categories:competition_categories(*)").eq("id", comp_id).single().execute()
    if not response.data:
        raise HTTPException(status_code=404, detail="Competition not found")
    return response.data

@router.patch("/{comp_id}/", response_model=Competition)
@router.patch("/{comp_id}", response_model=Competition)
async def update_competition(comp_id: UUID, comp_update: CompetitionUpdate):
    try:
        update_data = comp_update.model_dump(exclude_unset=True)
        
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
            res = supabase.table("competitions").update(update_data).eq("id", comp_id).execute()
            if not res.data:
                raise HTTPException(status_code=404, detail="Competition not found")
        
        # Обновление категорий
        if categories_data is not None:
            # Получаем старые категории
            old_cats_res = supabase.table("competition_categories").select("id").eq("competition_id", comp_id).execute()
            old_cat_ids = {str(cat["id"]) for cat in old_cats_res.data}
            
            # Вставляем новые и обновляем существующие
            if categories_data:
                for cat in categories_data:
                    cat_dict = cat if isinstance(cat, dict) else cat.model_dump()
                    cat_dict["competition_id"] = str(comp_id)
                    if isinstance(cat_dict.get("competition_day"), datetime):
                        cat_dict["competition_day"] = cat_dict["competition_day"].isoformat()
                    if isinstance(cat_dict.get("mandate_day"), datetime):
                        cat_dict["mandate_day"] = cat_dict["mandate_day"].isoformat()
                    
                    if "id" in cat_dict and cat_dict["id"]:
                        # Обновляем существующую
                        cat_id = str(cat_dict.pop("id"))
                        if cat_id in old_cat_ids:
                            old_cat_ids.remove(cat_id)
                        supabase.table("competition_categories").update(cat_dict).eq("id", cat_id).execute()
                    else:
                        # Вставляем новую (id не передаем, пусть генерируется базой)
                        if "id" in cat_dict:
                            del cat_dict["id"]
                        supabase.table("competition_categories").insert(cat_dict).execute()
                        
            # Удаляем те, которых больше нет (если на них нет ссылок в заявках)
            for cat_id in old_cat_ids:
                try:
                    supabase.table("competition_categories").delete().eq("id", cat_id).execute()
                except Exception as e:
                    print(f"Cannot delete category {cat_id}, likely has applications: {e}")
                
        # Обновление секретарей (полная замена)
        if secretaries_data is not None:
            supabase.table("competition_secretaries").delete().eq("competition_id", comp_id).execute()
            if secretaries_data:
                secs_to_insert = [
                    {"competition_id": str(comp_id), "user_id": str(sec_id)}
                    for sec_id in secretaries_data
                ]
                supabase.table("competition_secretaries").insert(secs_to_insert).execute()
                
        # Возвращаем обновленное соревнование
        final_res = supabase.table("competitions").select("*, categories:competition_categories(*)").eq("id", comp_id).single().execute()
        return final_res.data
        
    except Exception as e:
        print(f"[Backend] ERROR in update_competition: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{comp_id}/preview")
async def upload_competition_preview(comp_id: UUID, file: UploadFile = File(...)):
    try:
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

        object_path = f"{comp_id}/{uuid4().hex}{ext}"
        content = await file.read()

        client = admin_supabase or supabase
        client.storage.from_(bucket).upload(
            object_path,
            content,
            file_options={"content-type": file.content_type or "application/octet-stream"},
        )

        preview_url = f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{object_path}"
        supabase.table("competitions").update({"preview_url": preview_url}).eq("id", str(comp_id)).execute()
        return {"preview_url": preview_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
