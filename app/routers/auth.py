from fastapi import APIRouter, HTTPException, Header
from app.core.supabase import SUPABASE_URL, SUPABASE_KEY, admin_supabase
import httpx

router = APIRouter(prefix="/auth", tags=["auth"])

@router.get("/me")
async def get_me(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    if not admin_supabase:
        raise HTTPException(status_code=500, detail="SUPABASE_SERVICE_ROLE_KEY is not set")

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={
                "Authorization": f"Bearer {token}",
                "apikey": SUPABASE_KEY,
            },
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_data = resp.json()
    user_id = user_data.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    res = admin_supabase.table("users").select(
        "id, email, user_roles(roles(code))"
    ).eq("id", user_id).maybe_single().execute()

    role_codes: list[str] = []
    if res.data and res.data.get("user_roles"):
        role_codes = [
            ur.get("roles", {}).get("code")
            for ur in res.data.get("user_roles", [])
            if ur and ur.get("roles") and ur.get("roles", {}).get("code")
        ]
        
    print(f"User {user_id} roles: {role_codes}")

    is_admin = any(c in ["admin", "founder", "country_admin", "region_admin", "country_secretary", "region_secretary"] for c in role_codes)
    is_secretary = any(c in ["secretary", "country_secretary", "region_secretary"] for c in role_codes)

    primary_role = "athlete"
    if is_admin:
        primary_role = "admin"
    elif is_secretary:
        primary_role = "secretary"

    return {
        "user_id": user_id,
        "email": res.data.get("email") if res.data else user_data.get("email"),
        "role_codes": role_codes,
        "role": primary_role,
    }

