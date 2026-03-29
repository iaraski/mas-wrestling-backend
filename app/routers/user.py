from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
from uuid import UUID
from app.core.supabase import supabase, admin_supabase
from app.schemas.user import Role, UserProfile, RoleAssign, AdminCreate, ProfileResponse, ProfileCreate, PassportResponse, PassportBase, AthleteResponse

router = APIRouter(prefix="/users", tags=["users"])

@router.get("/me/profile")
async def get_my_profile(user_id: str):
    res = supabase.table("profiles").select("*, location:locations(id, name, parent:locations(id, name, parent:locations(id, name)))").eq("user_id", user_id).maybe_single().execute()
    if not res.data:
        # Return empty profile instead of 404
        return {"user_id": user_id, "full_name": "", "phone": "", "city": "", "location_id": None}
    return res.data

@router.put("/me/profile", response_model=ProfileResponse)
async def update_my_profile(user_id: str, profile: ProfileCreate):
    res = supabase.table("profiles").upsert(
        {
            "user_id": user_id,
            "full_name": profile.full_name,
            "phone": profile.phone,
            "location_id": profile.location_id,
            "city": profile.city,
        },
        on_conflict="user_id"
    ).execute()
    return res.data[0]

@router.get("/me/athlete")
async def get_my_athlete(user_id: str):
    res = supabase.table("athletes").select("*, passports(*)").eq("user_id", user_id).maybe_single().execute()
    if not res.data:
        # Return a dummy response instead of 404 so admins don't crash when visiting dashboard
        return {"id": "00000000-0000-0000-0000-000000000000", "user_id": user_id, "coach_name": "", "passports": []}
    return res.data

@router.put("/me/athlete")
async def update_my_athlete(user_id: str, coach_name: Optional[str] = None):
    res = supabase.table("athletes").upsert(
        {
            "user_id": user_id,
            "coach_name": coach_name,
        },
        on_conflict="user_id"
    ).execute()
    return res.data[0]

@router.put("/me/passport", response_model=PassportResponse)
async def update_my_passport(user_id: str, passport: PassportBase):
    # Get athlete ID first
    athlete_res = supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single().execute()
    if not athlete_res.data:
        raise HTTPException(status_code=404, detail="Athlete profile must be created first")
    
    athlete_id = athlete_res.data["id"]
    
    # Check if verified
    existing_passport = supabase.table("passports").select("is_verified").eq("athlete_id", athlete_id).maybe_single().execute()
    if existing_passport.data and existing_passport.data.get("is_verified"):
        raise HTTPException(status_code=403, detail="Passport is verified and cannot be edited")

    res = supabase.table("passports").upsert(
        {
            "athlete_id": athlete_id,
            "series": passport.series,
            "number": passport.number,
            "issued_by": passport.issued_by,
            "issue_date": str(passport.issue_date),
            "birth_date": str(passport.birth_date),
            "gender": passport.gender,
            "rank": passport.rank,
            "photo_url": passport.photo_url,
            "passport_scan_url": passport.passport_scan_url,
        },
        on_conflict="athlete_id"
    ).execute()
    return res.data[0]

@router.get("/me/applications")
async def get_my_applications(user_id: str):
    athlete_res = supabase.table("athletes").select("id").eq("user_id", user_id).maybe_single().execute()
    if not athlete_res.data:
        return []
    
    athlete_id = athlete_res.data["id"]
    res = supabase.table("applications").select(
        "id, status, draw_number, created_at, category_id, competitions(id, name, start_date), competition_categories(gender, age_min, age_max, weight_min, weight_max)"
    ).eq("athlete_id", athlete_id).order("created_at", desc=True).execute()
    
    return res.data

@router.post("/admin-create/", response_model=UserProfile)
@router.post("/admin-create", response_model=UserProfile)
async def create_admin_user(payload: AdminCreate):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="SUPABASE_SERVICE_ROLE_KEY is not set")

    is_admin = any("admin" in code for code in payload.role_codes)
    is_secretary = any("secretary" in code for code in payload.role_codes)
    if is_admin and is_secretary:
        raise HTTPException(status_code=400, detail="Администратор не может быть секретарем")

    if (is_admin or is_secretary) and not payload.location_id:
        raise HTTPException(status_code=400, detail="Для админов/секретарей нужна привязка к локации")

    try:
        auth_res = admin_supabase.auth.admin.create_user(
            {
                "email": payload.email,
                "password": payload.password,
                "email_confirm": True,
            }
        )
    except Exception as e:
        print(f"Error creating user in Supabase Auth: {e}")
        raise HTTPException(status_code=400, detail=f"Ошибка создания пользователя в Auth: {str(e)}")

    auth_user = None
    if isinstance(auth_res, dict):
        auth_user = auth_res.get("user")
    else:
        auth_user = getattr(auth_res, "user", None)

    if not auth_user or not getattr(auth_user, "id", None):
        raise HTTPException(status_code=400, detail="Failed to create auth user")

    user_id = getattr(auth_user, "id")

    try:
        supabase.table("users").upsert({"id": user_id, "email": payload.email}, on_conflict="id").execute()
        supabase.table("profiles").upsert(
            {
                "user_id": user_id,
                "full_name": payload.full_name,
                "phone": payload.phone,
                "location_id": str(payload.location_id) if payload.location_id else None,
            },
            on_conflict="user_id",
        ).execute()

        roles_res = supabase.table("roles").select("id, code").in_("code", payload.role_codes).execute()
        if not roles_res.data:
            raise HTTPException(status_code=400, detail="Invalid role codes")

        supabase.table("user_roles").delete().eq("user_id", user_id).execute()
        to_insert_roles = [{"user_id": str(user_id), "role_id": r["id"]} for r in roles_res.data]
        supabase.table("user_roles").insert(to_insert_roles).execute()

        supabase.table("staff_locations").delete().eq("user_id", user_id).execute()
        if payload.location_id and (is_admin or is_secretary):
            to_insert_staff = [
                {"user_id": str(user_id), "location_id": str(payload.location_id), "role_id": r["id"]}
                for r in roles_res.data
                if ("admin" in r["code"] or "secretary" in r["code"])
            ]
            if to_insert_staff:
                supabase.table("staff_locations").insert(to_insert_staff).execute()
    except Exception as e:
        print(f"Error inserting user details to public tables: {e}")
        # Если произошла ошибка при добавлении в публичные таблицы, стоит удалить пользователя из Auth
        try:
            admin_supabase.auth.admin.delete_user(str(user_id))
        except:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка сохранения данных: {str(e)}")

    res = supabase.table("users").select(
        "id, email, profiles(full_name, phone), user_roles(roles(code)), staff_locations(location_id, locations(name))"
    ).eq("id", user_id).single().execute()

    u = res.data
    profile = u.get("profiles")
    if isinstance(profile, list):
        profile = profile[0] if profile else None
    roles = [ur["roles"]["code"] for ur in u.get("user_roles", []) if ur.get("roles")]
    staff = u.get("staff_locations")
    if isinstance(staff, list):
        staff = staff[0] if staff else None

    loc_id = staff.get("location_id") if staff else None
    loc_name = staff.get("locations", {}).get("name") if staff and staff.get("locations") else None

    return UserProfile(
        user_id=u["id"],
        full_name=profile.get("full_name") if profile else None,
        phone=profile.get("phone") if profile else None,
        email=u.get("email"),
        roles=roles,
        location_id=loc_id,
        location_name=loc_name,
    )

@router.get("/roles", response_model=List[Role])
async def get_roles():
    response = supabase.table("roles").select("*").execute()
    return response.data

@router.get("/search", response_model=List[UserProfile])
async def search_users(query: str):
    res_users = supabase.table("users").select("id, telegram_id, email").ilike("telegram_id::text", f"%{query}%").execute()
    user_ids_by_tg = [u["id"] for u in res_users.data]
    
    res_profiles = supabase.table("profiles").select("user_id, full_name, phone").ilike("full_name", f"%{query}%").execute()
    user_ids_by_name = [p["user_id"] for p in res_profiles.data]
    
    all_user_ids = list(set(user_ids_by_tg + user_ids_by_name))
    
    if not all_user_ids:
        return []
        
    final_res = supabase.table("users") \
        .select("id, email, profiles(full_name, phone), user_roles(roles(code)), staff_locations(location_id, locations(name))") \
        .in_("id", all_user_ids) \
        .execute()
        
    users = []
    for u in final_res.data:
        profile = u.get("profiles")
        if isinstance(profile, list):
            profile = profile[0] if profile else None

        roles = [ur["roles"]["code"] for ur in u.get("user_roles", []) if ur.get("roles")]

        staff = u.get("staff_locations")
        if isinstance(staff, list):
            staff = staff[0] if staff else None

        loc_id = staff.get("location_id") if staff else None
        loc_name = staff.get("locations", {}).get("name") if staff and staff.get("locations") else None
            
        users.append(UserProfile(
            user_id=u["id"],
            full_name=profile.get("full_name") if profile else None,
            phone=profile.get("phone") if profile else None,
            email=u.get("email"),
            roles=roles,
            location_id=loc_id,
            location_name=loc_name
        ))
        
    return users

@router.post("/{user_id}/roles", response_model=UserProfile)
async def assign_roles(user_id: UUID, role_in: RoleAssign):
    is_admin = any("admin" in code for code in role_in.role_codes)
    is_secretary = any("secretary" in code for code in role_in.role_codes)
    
    if is_admin and is_secretary:
        raise HTTPException(status_code=400, detail="Администратор не может быть секретарем")
        
    roles_res = supabase.table("roles").select("id, code").in_("code", role_in.role_codes).execute()
    if not roles_res.data:
        raise HTTPException(status_code=400, detail="Invalid role codes")
        
    supabase.table("user_roles").delete().eq("user_id", user_id).execute()
    to_insert_roles = [{"user_id": str(user_id), "role_id": r["id"]} for r in roles_res.data]
    supabase.table("user_roles").insert(to_insert_roles).execute()
    
    supabase.table("staff_locations").delete().eq("user_id", user_id).execute()
    if role_in.location_id and (is_admin or is_secretary):
        to_insert_staff = [
            {"user_id": str(user_id), "location_id": str(role_in.location_id), "role_id": r["id"]} 
            for r in roles_res.data
            if ("admin" in r["code"] or "secretary" in r["code"])
        ]
        if to_insert_staff:
            supabase.table("staff_locations").insert(to_insert_staff).execute()
    
    res = supabase.table("users") \
        .select("id, email, profiles(full_name, phone), user_roles(roles(code)), staff_locations(location_id, locations(name))") \
        .eq("id", user_id) \
        .single() \
        .execute()
        
    u = res.data
    profile = u.get("profiles")
    if isinstance(profile, list):
        profile = profile[0] if profile else None

    roles = [ur["roles"]["code"] for ur in u.get("user_roles", []) if ur.get("roles")]
    staff = u.get("staff_locations")
    if isinstance(staff, list):
        staff = staff[0] if staff else None

    loc_id = staff.get("location_id") if staff else None
    loc_name = staff.get("locations", {}).get("name") if staff and staff.get("locations") else None
    
    return UserProfile(
        user_id=u["id"],
        full_name=profile.get("full_name") if profile else None,
        phone=profile.get("phone") if profile else None,
        email=u.get("email"),
        roles=roles,
        location_id=loc_id,
        location_name=loc_name
    )

@router.get("/secretaries", response_model=List[UserProfile])
async def get_secretaries(location_id: Optional[UUID] = None):
    query = supabase.table("staff_locations") \
        .select("user_id, roles!inner(code), location_id, locations(name), users!inner(email, profiles(full_name, phone))") \
        .ilike("roles.code", "%secretary%")
        
    if location_id:
        query = query.eq("location_id", location_id)
        
    res = query.execute()
    
    users_dict = {}
    for r in res.data:
        uid = r["user_id"]
        user = r.get("users") or {}
        profile = user.get("profiles")
        if isinstance(profile, list):
            profile = profile[0] if profile else None
        if uid not in users_dict:
            users_dict[uid] = {
                "user_id": uid,
                "full_name": profile.get("full_name") if profile else None,
                "phone": profile.get("phone") if profile else None,
                "email": user.get("email"),
                "location_id": r.get("location_id"),
                "location_name": r["locations"]["name"] if r.get("locations") else None,
                "roles": []
            }
        users_dict[uid]["roles"].append(r["roles"]["code"])
        
    return [UserProfile(**u) for u in users_dict.values()]


@router.delete("/{user_id}/")
@router.delete("/{user_id}")
async def delete_user(user_id: UUID):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="SUPABASE_SERVICE_ROLE_KEY is not set")
    
    try:
        # Delete from Supabase Auth
        # This usually cascades to public.users if FK is set up correctly
        res = admin_supabase.auth.admin.delete_user(str(user_id))
        
        # Also explicitly try to delete from public.users just in case cascade is missing
        # or if we want to be sure.
        # However, if cascade is ON, this second delete might find nothing, which is fine.
        supabase.table("users").delete().eq("id", str(user_id)).execute()
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
        
    return {"message": "User deleted successfully"}

@router.get("/admins", response_model=List[UserProfile])
async def get_admins():
    admin_codes = ["founder", "world_admin", "world_secretary", "country_admin", "country_secretary", "region_admin", "region_secretary"]
    
    res = supabase.table("staff_locations") \
        .select("user_id, roles!inner(code), location_id, locations(name), users!inner(email, profiles(full_name, phone))") \
        .in_("roles.code", admin_codes) \
        .execute()
        
    users_dict = {}
    for r in res.data:
        uid = r["user_id"]
        user = r.get("users") or {}
        profile = user.get("profiles")
        if isinstance(profile, list):
            profile = profile[0] if profile else None
        if uid not in users_dict:
            users_dict[uid] = {
                "user_id": uid,
                "full_name": profile.get("full_name") if profile else None,
                "phone": profile.get("phone") if profile else None,
                "email": user.get("email"),
                "location_id": r.get("location_id"),
                "location_name": r["locations"]["name"] if r.get("locations") else None,
                "roles": []
            }
        users_dict[uid]["roles"].append(r["roles"]["code"])
        
    return [UserProfile(**u) for u in users_dict.values()]
