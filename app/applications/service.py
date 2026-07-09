from typing import Optional
from uuid import UUID

from fastapi import HTTPException

from app.applications import repository
from app.applications.helpers import age_at_date, format_category_label, normalize_gender, normalize_passport_photo_url, parse_date
from app.core.telegram import send_telegram_notification
from app.schemas.competition import (
    AdminApplyAthleteToCategory,
    AdminCreateAthleteApplication,
    AdminUpdateAthleteProfile,
    ApplicationCreate,
    ApplicationUpdate,
)


async def list_applications(competition_id: Optional[UUID]) -> list[dict]:
    rows = await repository.list_application_rows(competition_id)
    athlete_ids = {str(r.get("athlete_id")) for r in rows if r.get("athlete_id")}
    cat_ids = {str(r.get("category_id")) for r in rows if r.get("category_id")}
    comp_ids = {str(r.get("competition_id")) for r in rows if r.get("competition_id")}

    competition_start_map = await repository.get_competition_start_map(comp_ids)
    category_map = await repository.get_category_map(cat_ids)
    athlete_map = await repository.get_athlete_summary_map(athlete_ids)

    out: list[dict] = []
    for app in rows:
        athlete_id = str(app.get("athlete_id")) if app.get("athlete_id") else ""
        category_id = str(app.get("category_id")) if app.get("category_id") else ""
        competition_id_str = str(app.get("competition_id")) if app.get("competition_id") else ""

        athlete = athlete_map.get(athlete_id) or {}
        category = category_map.get(category_id) or None
        at_date = competition_start_map.get(competition_id_str)

        app["athlete_name"] = athlete.get("full_name") or "Unknown"
        app["athlete_location_id"] = athlete.get("location_id")
        app["athlete_region"] = athlete.get("region_name")
        app["category_description"] = format_category_label(category or {}, at_date) if category else "Unknown"
        out.append(app)

    return out


async def get_application_details(app_id: UUID, *, write: bool) -> dict:
    app_data = await repository.get_application(app_id, write=write)
    if app_data is None:
        raise HTTPException(status_code=404, detail="Application not found")

    athlete_id = app_data.get("athlete_id")
    athlete_row = await repository.get_athlete(str(athlete_id), write=write) if athlete_id else None

    users_row = None
    profiles_row = None
    if isinstance(athlete_row, dict) and athlete_row.get("user_id"):
        user_id = str(athlete_row["user_id"])
        users_row = await repository.get_user(user_id, write=write)
        profiles_row = await repository.get_profile(user_id, write=write)

    if isinstance(users_row, dict) and isinstance(profiles_row, dict):
        users_row["profiles"] = profiles_row
    if isinstance(athlete_row, dict) and isinstance(users_row, dict):
        athlete_row["users"] = users_row
    if athlete_row is not None:
        app_data["athletes"] = athlete_row

    if app_data.get("category_id"):
        category = await repository.get_category(str(app_data["category_id"]), write=write)
        if category is not None:
            app_data["competition_categories"] = category

    if app_data.get("competition_id"):
        competition = await repository.get_competition(str(app_data["competition_id"]), write=write)
        if competition is not None:
            app_data["competition"] = competition

    passport_data = None
    if athlete_id:
        passport_data = await repository.get_passport_by_athlete(str(athlete_id), write=write)
        if isinstance(passport_data, dict) and passport_data.get("photo_url"):
            passport_data["photo_url"] = normalize_passport_photo_url(passport_data.get("photo_url"))
    app_data["passport"] = passport_data

    full_name = "Unknown"
    phone = "Не указан"
    email = "Не указан"
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
                email = users_data.get("email", "Не указан")

    app_data["athlete_name"] = full_name
    app_data["athlete_phone"] = phone
    app_data["athlete_email"] = email

    category_desc = "Не указана"
    category = app_data.get("competition_categories")
    if category:
        if isinstance(category, list):
            category = category[0] if category else {}
        competition = app_data.get("competition") or app_data.get("competitions") or {}
        if isinstance(competition, list):
            competition = competition[0] if competition else {}
        at_date = competition.get("start_date") if isinstance(competition, dict) else None
        category_desc = format_category_label(category, at_date)
    app_data["category_description"] = category_desc

    return app_data


async def create_application(app_in: ApplicationCreate) -> dict:
    existing = await repository.find_application_for_competition(
        str(app_in.athlete_id),
        str(app_in.competition_id),
    )
    if existing is not None:
        raise HTTPException(status_code=400, detail="Application already exists")

    created = await repository.create_application(app_in.model_dump())
    if created is None:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return created


async def create_my_application(category_id: str, user_id: str) -> dict:
    competition_id = await repository.find_competition_id_by_category(category_id)
    if competition_id is None:
        raise HTTPException(status_code=404, detail="Category not found")

    athlete = await repository.find_athlete_by_user(user_id)
    if athlete is None or not athlete.get("id"):
        raise HTTPException(status_code=404, detail="Athlete not found")
    athlete_id = str(athlete["id"])

    profile = await repository.get_profile(user_id, write=True) or {}
    if not profile.get("full_name") or not profile.get("city") or not profile.get("location_id"):
        raise HTTPException(status_code=400, detail="Fill full_name, city and region")
    if not athlete.get("coach_name"):
        raise HTTPException(status_code=400, detail="Fill coach name")

    passport = await repository.get_passport_by_athlete(
        athlete_id,
        write=True,
        select="birth_date,rank,photo_url,gender",
    ) or {}
    if not passport.get("birth_date") or not passport.get("rank") or not passport.get("photo_url") or not passport.get("gender"):
        raise HTTPException(status_code=400, detail="Fill birth_date, gender, rank and upload photo")

    existing = await repository.find_application_for_competition(athlete_id, competition_id)
    if existing is not None:
        raise HTTPException(status_code=400, detail="Already applied to this competition")

    created = await repository.create_application(
        {
            "competition_id": competition_id,
            "athlete_id": athlete_id,
            "category_id": category_id,
            "status": "pending",
        }
    )
    if created is None:
        raise HTTPException(status_code=400, detail="Failed to create application")

    try:
        await repository.mark_registration_complete(user_id)
    except Exception:
        pass

    return created


def _build_passport_payload(body) -> dict[str, object]:
    payload: dict[str, object] = {}
    if getattr(body, "birth_date", None) is not None:
        payload["birth_date"] = parse_date(body.birth_date)
    if getattr(body, "rank", None) is not None:
        payload["rank"] = body.rank
    if getattr(body, "photo_url", None) is not None:
        payload["photo_url"] = body.photo_url
    if getattr(body, "gender", None) is not None:
        payload["gender"] = body.gender
    if getattr(body, "series", None) is not None:
        payload["series"] = body.series
    if getattr(body, "number", None) is not None:
        payload["number"] = body.number
    if getattr(body, "issued_by", None) is not None:
        payload["issued_by"] = body.issued_by
    if getattr(body, "issue_date", None) is not None:
        payload["issue_date"] = parse_date(body.issue_date)
    if getattr(body, "passport_scan_url", None) is not None:
        payload["passport_scan_url"] = body.passport_scan_url
    return payload


async def admin_create_athlete_and_application(body: AdminCreateAthleteApplication) -> dict:
    category = await repository.get_category(str(body.category_id), write=True)
    if category is None or not category.get("competition_id"):
        raise HTTPException(status_code=404, detail="Category not found")
    competition_id = str(category["competition_id"])

    from uuid import uuid4

    user_id = str(uuid4())
    await repository.create_user_with_id(user_id)
    await repository.create_profile(
        {
            "user_id": user_id,
            "full_name": body.full_name,
            "city": body.city,
            "location_id": str(body.location_id),
        }
    )
    athlete = await repository.create_athlete({"user_id": user_id, "coach_name": body.coach_name})
    if athlete is None or not athlete.get("id"):
        raise HTTPException(status_code=400, detail="Failed to create athlete")
    athlete_id = str(athlete["id"])

    passport_payload = {"athlete_id": athlete_id, **_build_passport_payload(body)}
    await repository.upsert_passport(passport_payload)

    status = "weighed" if body.actual_weight is not None else "approved"
    created = await repository.create_application(
        {
            "competition_id": competition_id,
            "athlete_id": athlete_id,
            "category_id": str(body.category_id),
            "status": status,
            "declared_weight": body.declared_weight,
            "actual_weight": body.actual_weight,
        }
    )
    if created is None:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return {"ok": True, "user_id": user_id, "athlete_id": athlete_id, "application": created}


async def admin_apply_athlete_to_category(body: AdminApplyAthleteToCategory) -> dict:
    category = await repository.get_category_competition_row(str(body.category_id))
    if category is None:
        raise HTTPException(status_code=404, detail="Category not found")
    competition_id = str(category["competition_id"])

    start_date = await repository.get_competition_start_date(competition_id)
    passport = await repository.get_passport_by_athlete(
        str(body.athlete_id),
        write=True,
        select="birth_date,gender,photo_url",
    ) or {}
    if not passport.get("birth_date") or not passport.get("gender"):
        raise HTTPException(status_code=400, detail="Athlete birth_date and gender are required")

    athlete_gender = normalize_gender(str(passport.get("gender")))
    category_gender = normalize_gender(str(category.get("gender")))
    if category_gender != athlete_gender:
        raise HTTPException(status_code=400, detail="Athlete gender does not match category")

    age = age_at_date(str(passport.get("birth_date")), start_date)
    if age is None:
        raise HTTPException(status_code=400, detail="Athlete birth_date is invalid")
    if age < int(category.get("age_min") or 0) or age > int(category.get("age_max") or 200):
        raise HTTPException(status_code=400, detail="Athlete age does not match category")

    existing = await repository.find_application_for_competition_with_status(str(body.athlete_id), competition_id)
    if existing:
        existing_id = str(existing.get("id"))
        existing_status = str(existing.get("status") or "")
        if existing_status != "rejected":
            raise HTTPException(
                status_code=400,
                detail=f"Athlete already has an application for this competition (status: {existing_status})",
            )
        delete_status = await repository.delete_application(existing_id)
        if delete_status not in (200, 204):
            raise HTTPException(status_code=400, detail="Failed to delete rejected application")

    try:
        created = await repository.create_application(
            {
                "competition_id": competition_id,
                "athlete_id": str(body.athlete_id),
                "category_id": str(body.category_id),
                "status": "pending",
            }
        )
    except Exception as e:
        code = getattr(e, "code", None)
        message = str(e)
        if code == "23505" or "23505" in message or "duplicate key value" in message:
            raise HTTPException(status_code=400, detail="Athlete already has an application for this competition")
        raise
    if created is None:
        raise HTTPException(status_code=400, detail="Failed to create application")
    return {"ok": True, "application": created, "replaced": bool(existing)}


async def admin_update_athlete_profile(app_id: UUID, body: AdminUpdateAthleteProfile) -> dict:
    athlete_id = await repository.get_application_athlete_id(str(app_id))
    if athlete_id is None:
        raise HTTPException(status_code=404, detail="Application not found")

    athlete = await repository.get_athlete(athlete_id, write=True)
    if athlete is None or not athlete.get("user_id"):
        raise HTTPException(status_code=404, detail="Athlete not found")
    user_id = str(athlete["user_id"])

    email = str(body.email or "").strip().lower() if body.email is not None else None
    if email:
        current_user = await repository.get_user(user_id, write=True)
        current_email = str(current_user.get("email") or "").strip().lower() if current_user else ""
        if current_email != email:
            await repository.update_user_email(user_id, email)

    profile_payload: dict[str, object] = {
        "user_id": user_id,
        "full_name": body.full_name,
        "city": body.city,
        "location_id": str(body.location_id),
    }
    if body.phone is not None:
        profile_payload["phone"] = str(body.phone).strip() or None
    await repository.upsert_profile(profile_payload)
    await repository.update_athlete(athlete_id, {"coach_name": body.coach_name})

    passport_payload = _build_passport_payload(body)
    if passport_payload:
        await repository.upsert_passport({"athlete_id": athlete_id, **passport_payload})

    return {"ok": True}


async def verify_passport(passport_id: UUID, *, is_verified: bool) -> dict:
    updated = await repository.update_passport_verification(passport_id, is_verified=is_verified)
    if updated is None:
        raise HTTPException(status_code=404, detail="Passport not found")
    return updated


def _build_application_notification_message(app_data: dict) -> str:
    competition = app_data.get("competition") or {}
    if isinstance(competition, list):
        competition = competition[0] if competition else {}
    comp_name = competition.get("name", "соревнование")
    new_status = app_data.get("status")

    if new_status == "approved":
        message = f"✅ Ваша заявка на <b>{comp_name}</b> одобрена!\n\nОжидаем вас на мандатной комиссии."
    elif new_status == "weighed":
        category = app_data.get("competition_categories") or {}
        if isinstance(category, list):
            category = category[0] if category else {}
        at_date = competition.get("start_date") if isinstance(competition, dict) else None
        category_str = format_category_label(category, at_date) if category else "неизвестно"
        comp_day = category.get("competition_day") if isinstance(category, dict) else None
        comp_day_str = f"\n📅 <b>День выступления:</b> {comp_day}" if comp_day else ""
        message = (
            f"⚖️ Вы успешно прошли мандатную комиссию!\n\n🏆 <b>Соревнование:</b> {comp_name}\n"
            f"👥 <b>Допущены в категории:</b> {category_str}{comp_day_str}\n\nЖелаем удачи на соревнованиях!"
        )
    else:
        message = f"❌ Ваша заявка на <b>{comp_name}</b> отклонена."

    if app_data.get("comment"):
        message += f"\n\n💬 Комментарий: {app_data['comment']}"
    return message


async def update_application_status(app_id: UUID, app_update: ApplicationUpdate) -> dict:
    old_app = await get_application_details(app_id, write=True)
    update_data = app_update.model_dump(exclude_unset=True)
    if not update_data:
        return old_app

    for key, value in list(update_data.items()):
        if isinstance(value, UUID):
            update_data[key] = str(value)

    updated = await repository.update_application(app_id, update_data)
    if updated is None:
        raise HTTPException(status_code=404, detail="Application not found")

    updated_app = await get_application_details(app_id, write=True)
    if "status" in update_data and updated_app.get("status") != old_app.get("status"):
        athlete = updated_app.get("athletes") or {}
        if isinstance(athlete, list):
            athlete = athlete[0] if athlete else {}
        user = athlete.get("users") or {}
        if isinstance(user, list):
            user = user[0] if user else {}
        telegram_id = user.get("telegram_id")
        if telegram_id:
            await send_telegram_notification(telegram_id, _build_application_notification_message(updated_app))

    return updated_app
