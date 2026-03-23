from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel
from typing import List, Optional
import random
from uuid import UUID
from app.core.supabase import supabase
from app.schemas.competition import Application, ApplicationCreate, ApplicationUpdate

from app.core.telegram import send_telegram_notification, get_telegram_file_url

router = APIRouter(prefix="/applications", tags=["applications"])

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
async def get_application_details(app_id: UUID):
    try:
        # Получаем детальную информацию по заявке, включая паспорт
        query = supabase.table("applications").select(
            "*, "
            "athletes(coach_name, users!athletes_user_id_fkey(email, profiles(full_name, phone))), "
            "competition_categories(*)"
        ).eq("id", app_id).single().execute()
        
        if not query.data:
            raise HTTPException(status_code=404, detail="Application not found")
            
        app_data = query.data
        
        # Получаем паспорт отдельно, так как он привязан к athlete_id
        athlete_id = app_data.get("athlete_id")
        passport_data = None
        if athlete_id:
            pass_query = supabase.table("passports").select("*").eq("athlete_id", athlete_id).maybe_single().execute()
            passport_data = pass_query.data
            
            # Если есть фото, возвращаем URL на наш собственный прокси-эндпоинт
            if passport_data and passport_data.get("photo_url"):
                file_id = passport_data["photo_url"]
                if not file_id.startswith("http"):
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
            
    # Логика автоматической жеребьевки:
    # Если статус меняется на 'weighed' и раньше он не был 'weighed'
    if update_data.get("status") == "weighed" and old_app.get("status") != "weighed":
        comp_id = old_app.get("competition_id")
        cat_id = update_data.get("category_id") or old_app.get("category_id")
        
        # Получаем все занятые номера жеребьевки для этой категории в этом соревновании
        try:
            used_draws_res = supabase.table("applications") \
                .select("draw_number") \
                .eq("competition_id", comp_id) \
                .eq("category_id", cat_id) \
                .not_.is_("draw_number", "null") \
                .execute()
                
            used_draws = {app["draw_number"] for app in used_draws_res.data}
            
            # Генерируем случайное число от 1 до 10000, которое еще не занято
            next_draw = random.randint(1, 10000)
            attempts = 0
            while next_draw in used_draws and attempts < 10000:
                next_draw = random.randint(1, 10000)
                attempts += 1
                
            update_data["draw_number"] = next_draw
        except Exception as e:
            print(f"Error generating draw_number: {e}")

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
