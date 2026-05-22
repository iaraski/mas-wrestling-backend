from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from typing import List
from uuid import UUID
from datetime import datetime
from uuid import uuid4
import os
import io
import httpx
import anyio
from PIL import Image, ImageDraw, ImageFont
from app.core.rest import rest_get, rest_delete, rest_post, rest_patch
from app.schemas.competition import Competition, CompetitionCreate, CompetitionUpdate
from app.core.cache import cache
from app.core.db import get_db, tables
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

router = APIRouter(prefix="/competitions", tags=["competitions"])

def _norm_datetime(dt: str | datetime | None) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        # ensure it's timezone-aware if needed, but SQLAlchemy TIMESTAMP WITH TIME ZONE handles naive objects assuming UTC if configured,
        # but let's just parse strings
        return dt
    try:
        # Parse from string, adding timezone info if missing, or just using fromisoformat
        parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        return parsed
    except Exception:
        return None

def _cat_key(cat: dict) -> tuple:
    gender = (cat.get("gender") or "").lower()
    age_min = int(cat.get("age_min")) if cat.get("age_min") is not None else None
    age_max = int(cat.get("age_max")) if cat.get("age_max") is not None else None
    wmin = float(cat.get("weight_min")) if cat.get("weight_min") is not None else None
    wmax = cat.get("weight_max")
    wmax = float(wmax) if wmax is not None else None
    if wmax is not None and abs(wmax - 999.0) < 1e-6:
        wmax = 999.0
    day = _norm_datetime(cat.get("competition_day"))
    mandate = _norm_datetime(cat.get("mandate_day"))
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
            if hasattr(query, "execute_async"):
                return await query.execute_async()
            return await anyio.to_thread.run_sync(query.execute)
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
            "select": "id,name,scale,type,location_id,mandate_start_date,mandate_end_date,start_date,end_date,preview_url,description,mats_count,categories:competition_categories(id,competition_id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day)",
            "end_date": f"gte.{datetime.now().isoformat()}",
            "order": "start_date.asc",
            "limit": "50",
        }
        resp = await rest_get("competitions", params, write=False)
        data = resp.json()
        cache.set(cache_key, data, ttl_seconds=60.0)
        return data
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {repr(e)}")

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
        raise HTTPException(status_code=503, detail=f"Database unavailable: {repr(e)}")
    
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
        
        # Format dates
        if "start_date" in comp_data:
            comp_data["start_date"] = _norm_datetime(comp_data["start_date"])
        if "end_date" in comp_data:
            comp_data["end_date"] = _norm_datetime(comp_data["end_date"])
        if "mandate_start_date" in comp_data:
            comp_data["mandate_start_date"] = _norm_datetime(comp_data["mandate_start_date"])
        if "mandate_end_date" in comp_data:
            comp_data["mandate_end_date"] = _norm_datetime(comp_data["mandate_end_date"])

        # Convert UUIDs to strings
        if "location_id" in comp_data and comp_data["location_id"] is not None:
            comp_data["location_id"] = str(comp_data["location_id"])
        if "certificate_template_id" in comp_data and comp_data["certificate_template_id"] is not None:
            comp_data["certificate_template_id"] = str(comp_data["certificate_template_id"])
        
        # FIX: force dates back to parsed isoformat if model_dump converted them to strings
        # because asyncpg strict mode requires real python datetime objects, not strings.
        for dfield in ["start_date", "end_date", "mandate_start_date", "mandate_end_date"]:
            if comp_data.get(dfield):
                if isinstance(comp_data[dfield], str):
                    comp_data[dfield] = _norm_datetime(comp_data[dfield])

        print(f"[Backend] Creating competition: {comp_data['name']}")

        res = await rest_post("competitions", {}, comp_data, prefer="return=representation")
        rows = res.json()
        print(f"[Backend] Competition insert response: {rows}")

        if not isinstance(rows, list) or not rows:
            raise HTTPException(status_code=400, detail="Supabase insert failed: no data returned")

        new_comp = rows[0]
        print(f"[Backend] Competition created with ID: {new_comp['id']}")
        
        # 2. Создаем категории
        final_categories = []
        if comp.categories:
            categories_data = []
            for cat in comp.categories:
                cat_dict = cat.model_dump(exclude_none=True)
                cat_dict["competition_id"] = new_comp["id"]
                
                # Make sure string dates are parsed to datetime if they come as string
                for d_field in ["competition_day", "mandate_day"]:
                    val = cat_dict.get(d_field)
                    if isinstance(val, str):
                        try:
                            cat_dict[d_field] = datetime.fromisoformat(val.replace("Z", "+00:00"))
                        except ValueError:
                            pass
                            
                categories_data.append(cat_dict)

            print(f"[Backend] Inserting {len(categories_data)} categories")
            cat_res = await rest_post("competition_categories", {}, categories_data, prefer="return=representation")
            cat_rows = cat_res.json()
            final_categories = cat_rows if isinstance(cat_rows, list) else []
            print(f"[Backend] Categories created: {len(final_categories)}")
            
        # 3. Добавляем секретарей
        if comp.secretaries:
            secretaries_data = [
                {"competition_id": new_comp["id"], "user_id": str(sec_id)}
                for sec_id in comp.secretaries
            ]
            await rest_post("competition_secretaries", {}, secretaries_data, prefer="return=minimal")
            
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

        resp = await rest_get(
            "competitions",
            {"select": "*,categories:competition_categories(*)", "id": f"eq.{str(comp_id)}", "limit": "1"},
            write=False,
        )
        rows = resp.json()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {repr(e)}")
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=404, detail="Competition not found")
    data = rows[0]
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
        
        # Сериализация UUID и дат для основной таблицы
        for key, value in list(update_data.items()):
            if isinstance(value, UUID):
                update_data[key] = str(value)
            elif key in ["mandate_start_date", "mandate_end_date", "start_date", "end_date"]:
                update_data[key] = _norm_datetime(value)

        # Обновление основной таблицы соревнований
        if update_data:
            resp = await rest_patch(
                "competitions",
                {"id": f"eq.{comp_id_str}"},
                update_data,
                prefer="return=minimal",
            )
            if resp.status_code not in (200, 204):
                raise HTTPException(status_code=400, detail=f"Failed to update competition: {resp.text}")

        # Обновление секретарей (полная замена)
        if secretaries_data is not None:
            del_resp = await rest_delete(
                "competition_secretaries",
                {"competition_id": f"eq.{comp_id_str}"},
            )
            if del_resp.status_code not in (200, 204):
                raise HTTPException(status_code=400, detail=f"Failed to clear secretaries: {del_resp.text}")
            if secretaries_data:
                secs_to_insert = [
                    {"competition_id": comp_id_str, "user_id": str(sec_id)}
                    for sec_id in secretaries_data
                ]
                ins = await rest_post("competition_secretaries", {}, secs_to_insert, prefer="return=minimal")
                if ins.status_code not in (200, 201, 204):
                    raise HTTPException(status_code=400, detail=f"Failed to insert secretaries: {ins.text}")

        if categories_data is None:
            cache.invalidate_prefix("competitions:")
            detail = await get_competition(comp_id)
            return detail
        
        # Обновление категорий
        if categories_data is not None:
            # Получаем старые категории и используемые в заявках
            existing_resp = await rest_get(
                "competition_categories",
                {
                    "select": "id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day",
                    "competition_id": f"eq.{comp_id_str}",
                    "limit": "10000",
                },
                write=True,
            )
            existing_cats = existing_resp.json()
            if not isinstance(existing_cats, list):
                existing_cats = []
            old_cat_ids = {str(cat["id"]) for cat in existing_cats}

            apps_resp = await rest_get(
                "applications",
                {"select": "category_id", "competition_id": f"eq.{comp_id_str}", "limit": "10000"},
                write=True,
            )
            apps_rows = apps_resp.json()
            used_cat_ids = {str(a["category_id"]) for a in (apps_rows or []) if isinstance(a, dict) and a.get("category_id")} if isinstance(apps_rows, list) else set()

            existing_by_key: dict[tuple, list[dict]] = {}
            for c in existing_cats:
                existing_by_key.setdefault(_cat_key(c), []).append(c)
            
            # Вставляем новые и обновляем существующие
            if categories_data:
                for cat in categories_data:
                    cat_dict = cat if isinstance(cat, dict) else cat.model_dump(exclude_none=True)
                    cat_dict["competition_id"] = comp_id_str
                    
                    for d_field in ["competition_day", "mandate_day"]:
                        val = cat_dict.get(d_field)
                        if isinstance(val, str):
                            try:
                                cat_dict[d_field] = datetime.fromisoformat(val.replace("Z", "+00:00"))
                            except ValueError:
                                cat_dict[d_field] = None
                        elif val is None:
                            cat_dict[d_field] = None
                    
                    if "id" in cat_dict and cat_dict["id"]:
                        # Обновляем существующую
                        cat_id = str(cat_dict.pop("id"))
                        if cat_id in old_cat_ids:
                            old_cat_ids.remove(cat_id)
                        await rest_patch(
                            "competition_categories",
                            {"id": f"eq.{cat_id}"},
                            cat_dict,
                            prefer="return=minimal",
                        )
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
                            await rest_patch(
                                "competition_categories",
                                {"id": f"eq.{chosen_id}"},
                                cat_dict,
                                prefer="return=minimal",
                            )
                        else:
                            await rest_post("competition_categories", {}, cat_dict, prefer="return=minimal")
                        
            # Удаляем те, которых больше нет (если на них нет ссылок в заявках)
            for cat_id in old_cat_ids:
                if cat_id in used_cat_ids:
                    continue
                try:
                    await rest_delete("competition_categories", {"id": f"eq.{cat_id}"})
                except Exception as e:
                    print(f"Cannot delete category {cat_id}, likely has applications: {e}")

            # Чистим точные дубликаты (удаляем только неиспользуемые)
            refreshed = await rest_get(
                "competition_categories",
                {
                    "select": "id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day",
                    "competition_id": f"eq.{comp_id_str}",
                    "limit": "10000",
                },
                write=True,
            )
            groups: dict[tuple, list[dict]] = {}
            refreshed_rows = refreshed.json()
            if not isinstance(refreshed_rows, list):
                refreshed_rows = []
            for c in refreshed_rows:
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
                        await rest_delete("competition_categories", {"id": f"eq.{cid}"})
                    except Exception:
                        pass
                
        # Возвращаем обновленное соревнование
        final_resp = await rest_get(
            "competitions",
            {"select": "*,categories:competition_categories(*)", "id": f"eq.{comp_id_str}", "limit": "1"},
            write=False,
        )
        final_rows = final_resp.json()
        if not isinstance(final_rows, list) or not final_rows:
            raise HTTPException(status_code=404, detail="Competition not found")
        cache.invalidate_prefix("competitions:")
        return final_rows[0]
        
    except Exception as e:
        print(f"[Backend] ERROR in update_competition: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{comp_id}")
async def delete_competition(comp_id: UUID):
    cid = str(comp_id)
    try:
        from app.core.supabase import admin_supabase
        if not admin_supabase:
            raise HTTPException(status_code=500, detail="admin_supabase is not initialized")
        
        # Fast SQL delete
        await _execute(admin_supabase.table("competition_bouts").delete().eq("competition_id", cid))
        await _execute(admin_supabase.table("competition_mats").delete().eq("competition_id", cid))
        await _execute(admin_supabase.table("competition_category_assignments").delete().eq("competition_id", cid))
        await _execute(admin_supabase.table("applications").delete().eq("competition_id", cid))
        await _execute(admin_supabase.table("competition_categories").delete().eq("competition_id", cid))
        await _execute(admin_supabase.table("competition_secretaries").delete().eq("competition_id", cid))
        
        await _execute(admin_supabase.table("competitions").delete().eq("id", cid))
        cache.invalidate_prefix("competitions:")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True}

@router.post("/{comp_id}/preview")
async def upload_competition_preview(comp_id: UUID, file: UploadFile = File(...)):
    try:
        if not file.content_type or not file.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="Only image files are supported")

        filename = file.filename or "preview"
        ext = os.path.splitext(filename)[1].lower()
        if not ext:
            if file.content_type == "image/png":
                ext = ".png"
            elif file.content_type == "image/webp":
                ext = ".webp"
            else:
                ext = ".jpg"

        object_path = f"competition-previews/{comp_id}/preview{ext}"
        content = await file.read()

        if not content:
            raise HTTPException(status_code=400, detail="Empty file")

        if len(content) > 10 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="File too large (max 10MB)")

        from app.core.minio import delete_objects, put_object

        await delete_objects(f"competition-previews/{comp_id}/preview")
        preview_url = await put_object(
            object_path,
            content,
            content_type=file.content_type or "application/octet-stream",
        )
        resp = await rest_patch(
            "competitions",
            {"id": f"eq.{str(comp_id)}"},
            {"preview_url": preview_url},
            prefer="return=minimal",
        )
        if resp.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=f"Failed to update preview_url: {resp.status_code} {resp.text}")
        cache.invalidate_prefix("competitions:")
        return {"preview_url": preview_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{comp_id}/diplomas")
async def get_diplomas(comp_id: UUID, db: AsyncSession = Depends(get_db)):
    # 1. Fetch competition and template ID
    comp_id_str = str(comp_id)
    resp = await rest_get("competitions", {"select": "certificate_template_id", "id": f"eq.{comp_id_str}"})
    rows = resp.json()
    if not rows or not rows[0].get("certificate_template_id"):
        raise HTTPException(400, "Шаблон грамоты не выбран для этого соревнования")
        
    template_id = rows[0]["certificate_template_id"]
    
    # 2. Fetch template
    t = tables.get("certificate_templates")
    if t is None:
        raise HTTPException(500, "Table not ready")
    
    res = await db.execute(select(t).where(t.c.id == template_id))
    template = res.mappings().first()
    if not template:
        raise HTTPException(404, "Шаблон не найден")
        
    bg_url = template.get("background_image_url")
    bg_bytes = None
    if bg_url:
        try:
            async with httpx.AsyncClient() as client:
                bg_resp = await client.get(bg_url)
                bg_resp.raise_for_status()
                bg_bytes = bg_resp.content
        except Exception as e:
            raise HTTPException(500, f"Не удалось загрузить фон: {e}")
        
    from app.routers.live import get_competition_results
    results = await get_competition_results(comp_id)
    
    # 3. Generate pages
    pdf_pages = []
    
    # Load fonts
    font_regular_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Roboto-Regular.ttf")
    font_bold_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Roboto-Bold.ttf")
    
    if not os.path.exists(font_regular_path):
        raise HTTPException(500, "Font not found on server")
        
    comp_name = results.get("competition", {}).get("name", "")
    date_str = datetime.now().strftime("%d.%m.%Y")
    
    # We'll map standard places
    place_map = {0: "1", 1: "2", 2: "3", 3: "3"}
    
    # Фронтенд использует виртуальные координаты (A4_WIDTH=794, A4_HEIGHT=1123)
    is_portrait = template.get("format") == 'A4_PORTRAIT'
    virtual_width = 794 if is_portrait else 1123
    virtual_height = 1123 if is_portrait else 794
    
    for cat in results.get("categories", []):
        cat_name = cat.get("label", "")
        
        # Разделяем на возрастную и весовую категорию
        # Обычно label выглядит как: "Юноши 14-15 лет, До 70 кг" или "Юниорки, Свыше 80 кг"
        age_cat = cat_name
        weight_cat = ""
        if ", " in cat_name:
            parts = cat_name.rsplit(", ", 1)
            age_cat = parts[0]
            weight_cat = parts[1].replace(" кг", "")
            
            # Обработка веса 999.0
            if "До 999" in weight_cat:
                # Если сырые данные попали сюда (хотя _category_label должен это обрабатывать)
                weight_cat = "Свыше"

        winners = cat.get("winners", [])
        
        # In Double Elimination we usually have 4 winners (1, 2, 3, 3)
        # In Round Robin maybe 3 (1, 2, 3)
        for idx, winner in enumerate(winners):
            if idx > 3:
                break # Only top 4 maximum
            
            place = place_map.get(idx, str(idx+1))
            
            if bg_bytes:
                img = Image.open(io.BytesIO(bg_bytes)).convert("RGB")
            else:
                img = Image.new("RGB", (virtual_width, virtual_height), "white")
            draw = ImageDraw.Draw(img)
            
            img_width, img_height = img.size
            scale_x = img_width / virtual_width
            scale_y = img_height / virtual_height
            
            full_name = winner.get("name", "").strip()
            short_name = " ".join(full_name.split()[:2]) if full_name else ""

            # Map variables
            variables = {
                "{{athlete_name}}": short_name,
                "{{place}}": place,
                "{{category}}": cat_name,
                "{{weight_category}}": weight_cat,
                "{{age_category}}": age_cat,
                "{{competition_name}}": comp_name,
                "{{date}}": date_str,
                "{{team_name}}": winner.get("team", "")
            }
            
            # Fetch team if not present but needed
            # For simplicity, we just use what we have or empty string
            
            for el in template.get("elements", []):
                text = el.get("text", "")
                for k, v in variables.items():
                    text = text.replace(k, str(v))
                    
                # Масштабируем координаты и размер шрифта
                x = el.get("x", 0) * scale_x
                y = el.get("y", 0) * scale_y
                font_size = int(el.get("fontSize", 24) * scale_x * 1.33)
                color = el.get("color", "#000000")
                align = el.get("align", "left")
                font_weight = el.get("fontWeight", "normal")
                
                font_path = font_bold_path if font_weight == "bold" else font_regular_path
                try:
                    font = ImageFont.truetype(font_path, font_size)
                except:
                    font = ImageFont.load_default()
                    
                try:
                    bbox = draw.multiline_textbbox((0, 0), text, font=font, align=align)
                    w = bbox[2] - bbox[0]
                    h = bbox[3] - bbox[1]
                    
                    draw_y = y - h / 2
                    
                    draw_x = x
                    if align == "center":
                        draw_x = x - w / 2
                    elif align == "right":
                        draw_x = x - w
                        
                    draw.multiline_text((draw_x, draw_y), text, fill=color, font=font, align=align)
                except:
                    draw.text((x, y), text, fill=color, font=font)
                    
            pdf_pages.append(img)
            
    if not pdf_pages:
        raise HTTPException(400, "Нет призеров для генерации дипломов")
        
    pdf_bytes = io.BytesIO()
    pdf_pages[0].save(
        pdf_bytes, "PDF", resolution=100.0, save_all=True, append_images=pdf_pages[1:]
    )
    pdf_bytes.seek(0)
    
    return StreamingResponse(
        pdf_bytes, 
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=diplomas_{comp_id_str}.pdf"}
    )
