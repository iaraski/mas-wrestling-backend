from fastapi import APIRouter, HTTPException, Header, Body, Query
from fastapi.responses import RedirectResponse
from app.core.supabase import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_ROLE_KEY, admin_supabase
from app.core.telegram import send_telegram_notification
import httpx
import os
import secrets
import hmac
import hashlib
import time

_me_cache: dict[str, tuple[float, dict]] = {}

router = APIRouter(prefix="/auth", tags=["auth"])

TG_CONFIRM_SECRET = os.getenv("TG_CONFIRM_SECRET", "")
PUBLIC_WEB_URL = os.getenv("PUBLIC_WEB_URL", "https://mas-wrestling.pro")
APP_DEBUG = os.getenv("APP_DEBUG") == "1"

def _require_service_role():
    if not admin_supabase or not SUPABASE_SERVICE_ROLE_KEY:
        raise HTTPException(status_code=500, detail="Service role not configured")

def _resolve_user_id(*, user_id: str | None, telegram_id: int | None) -> str:
    if user_id:
        if telegram_id is None:
            return user_id
        q = (
            admin_supabase.table("users")
            .select("id")
            .eq("telegram_id", telegram_id)
            .maybe_single()
            .execute()
        )
        if not q.data or q.data.get("id") != user_id:
            raise HTTPException(status_code=403, detail="user_id does not match telegram_id")
        return user_id

    if telegram_id is None:
        raise HTTPException(status_code=400, detail="telegram_id is required")

    q = (
        admin_supabase.table("users")
        .select("id")
        .eq("telegram_id", telegram_id)
        .maybe_single()
        .execute()
    )
    if not q.data:
        raise HTTPException(status_code=404, detail="User not found by telegram_id")
    return q.data.get("id")

async def _get_auth_admin_user(user_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SUPABASE_URL}/auth/v1/admin/users/{user_id}",
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "apikey": SUPABASE_KEY,
            },
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()

def _is_email_confirmed(auth_user: dict) -> bool:
    return bool(auth_user.get("email_confirmed_at") or auth_user.get("confirmed_at"))

def _sign_tg_confirm(telegram_id: int, email: str) -> str:
    if not TG_CONFIRM_SECRET:
        return ""
    msg = f"{telegram_id}|{email}".encode("utf-8")
    return hmac.new(TG_CONFIRM_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()

def _check_tg_confirm_sig(telegram_id: int, email: str, sig: str | None) -> bool:
    if not TG_CONFIRM_SECRET:
        return True
    if not sig:
        return False
    expected = _sign_tg_confirm(telegram_id, email)
    return hmac.compare_digest(expected, sig)

async def _get_auth_user_by_email(email: str) -> dict | None:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SUPABASE_URL}/auth/v1/admin/users",
            headers={"Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}", "apikey": SUPABASE_KEY},
            params={"email": email},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    users_list = resp.json().get("users", [])
    return users_list[0] if users_list else None

@router.get("/me")
async def get_me(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    if not admin_supabase:
        raise HTTPException(status_code=500, detail="SUPABASE_SERVICE_ROLE_KEY is not set")

    cache_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
    cached = _me_cache.get(cache_key)
    if cached and cached[0] > time.time():
        return cached[1]

    try:
        timeout = httpx.Timeout(25.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout, http2=False, transport=httpx.AsyncHTTPTransport(retries=2)) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/user",
                headers={
                    "Authorization": f"Bearer {token}",
                    "apikey": SUPABASE_KEY,
                },
            )
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Supabase Auth unavailable: {repr(e)}")

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
        
    if APP_DEBUG:
        print(f"User {user_id} roles: {role_codes}")

    is_admin = any(c in ["admin", "founder", "country_admin", "region_admin", "country_secretary", "region_secretary"] for c in role_codes)
    is_secretary = any(c in ["secretary", "country_secretary", "region_secretary"] for c in role_codes)

    primary_role = "athlete"
    if is_admin:
        primary_role = "admin"
    elif is_secretary:
        primary_role = "secretary"

    result = {
        "user_id": user_id,
        "email": res.data.get("email") if res.data else user_data.get("email"),
        "role_codes": role_codes,
        "role": primary_role,
    }
    _me_cache[cache_key] = (time.time() + 30.0, result)
    return result

@router.post("/debug/clear-cache")
async def debug_clear_cache():
    if os.getenv("APP_DEBUG") != "1":
        raise HTTPException(status_code=404, detail="Not Found")
    _me_cache.clear()
    return {"ok": True}

@router.post("/bot-signup")
async def bot_signup(
    email: str = Body(...),
    password: str = Body(...),
    user_id: str | None = Body(None),
    telegram_id: int | None = Body(None),
):
    _require_service_role()
    db_user_id = _resolve_user_id(user_id=user_id, telegram_id=telegram_id)

    async with httpx.AsyncClient() as client:
        create_resp = await client.post(
            f"{SUPABASE_URL}/auth/v1/admin/users",
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "apikey": SUPABASE_KEY,
                "Content-Type": "application/json",
            },
            json={"email": email, "password": password, "email_confirm": False},
        )
        if create_resp.status_code not in (200, 201):
            # 422 means user already exists
            if create_resp.status_code != 422:
                raise HTTPException(status_code=create_resp.status_code, detail=create_resp.text)

        resend_resp = await client.post(
            f"{SUPABASE_URL}/auth/v1/resend",
            headers={
                "apikey": SUPABASE_KEY,
                "Content-Type": "application/json",
            },
            json={"type": "signup", "email": email},
        )
        if resend_resp.status_code not in (200, 201, 204):
            # Not fatal for flow, but surface for visibility
            if APP_DEBUG:
                print("[auth/bot-signup] resend failed", resend_resp.status_code, resend_resp.text)

    # Store email on our side too
    admin_supabase.table("users").update({"email": email}).eq("id", db_user_id).execute()

    return {"ok": True}

@router.post("/bot-init-email")
async def bot_init_email(
    email: str = Body(...),
    user_id: str | None = Body(None),
    telegram_id: int | None = Body(None),
):
    _require_service_role()
    # ensure user exists in our DB (will be used to persist email)
    db_user_id = _resolve_user_id(user_id=user_id, telegram_id=telegram_id)
    temp_password = secrets.token_urlsafe(18)

    # Create (or ensure) auth user by EMAIL only (we cannot set a custom id in Supabase Auth)
    async with httpx.AsyncClient() as client:
        create_resp = await client.post(
            f"{SUPABASE_URL}/auth/v1/admin/users",
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "apikey": SUPABASE_KEY,
                "Content-Type": "application/json",
            },
            json={
                "email": email,
                "password": temp_password,
                "email_confirm": False,
            },
        )
        # 422 if email already exists in Auth — это нормально
        if create_resp.status_code not in (200, 201, 422):
            raise HTTPException(status_code=create_resp.status_code, detail=create_resp.text)

        redirect_to = None
        if telegram_id is not None:
            sig = _sign_tg_confirm(telegram_id, email)
            redirect_to = (
                f"{os.getenv('PUBLIC_API_URL', 'https://api.mas-wrestling.pro')}/api/v1/auth/tg-email-confirmed"
                f"?telegram_id={telegram_id}&email={email}&sig={sig}"
            )

        resend_resp = await client.post(
            f"{SUPABASE_URL}/auth/v1/resend",
            headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
            json={
                "type": "signup",
                "email": email,
                "redirectTo": redirect_to,
                "redirect_to": redirect_to,
            },
        )
        if resend_resp.status_code not in (200, 201, 204):
            if APP_DEBUG:
                print("[auth/bot-init-email] resend failed", resend_resp.status_code, resend_resp.text)

    # Сохраняем email у нас
    admin_supabase.table("users").update({"email": email}).eq("id", db_user_id).execute()
    return {"ok": True}

@router.post("/bot-set-password")
async def bot_set_password(
    password: str = Body(...),
    user_id: str | None = Body(None),
    telegram_id: int | None = Body(None),
):
    _require_service_role()
    db_user_id = _resolve_user_id(user_id=user_id, telegram_id=telegram_id)
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    # Получаем email из нашей таблицы
    row = admin_supabase.table("users").select("email").eq("id", db_user_id).maybe_single().execute()
    email = row.data.get("email") if row and row.data else None
    if not email:
        raise HTTPException(status_code=400, detail="Email not set in users table")

    # Получаем auth user по email
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SUPABASE_URL}/auth/v1/admin/users",
            headers={"Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}", "apikey": SUPABASE_KEY},
            params={"email": email},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    users_list = resp.json().get("users", [])
    if not users_list:
        raise HTTPException(status_code=404, detail="Auth user not found by email")
    auth_user = users_list[0]
    if not _is_email_confirmed(auth_user):
        raise HTTPException(status_code=400, detail="Email is not confirmed yet")

    auth_id = auth_user.get("id")
    async with httpx.AsyncClient() as client:
        r = await client.patch(
            f"{SUPABASE_URL}/auth/v1/admin/users/{auth_id}",
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                "apikey": SUPABASE_KEY,
                "Content-Type": "application/json",
            },
            json={"password": password},
        )
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return {"ok": True}

@router.get("/bot-confirmation-status")
async def bot_confirmation_status(
    user_id: str | None = None,
    telegram_id: int | None = None,
):
    _require_service_role()
    db_user_id = _resolve_user_id(user_id=user_id, telegram_id=telegram_id)
    row = admin_supabase.table("users").select("email").eq("id", db_user_id).maybe_single().execute()
    email = row.data.get("email") if row and row.data else None
    if not email:
        return {"confirmed": False, "email": None}

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SUPABASE_URL}/auth/v1/admin/users",
            headers={"Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}", "apikey": SUPABASE_KEY},
            params={"email": email},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    users_list = resp.json().get("users", [])
    if not users_list:
        return {"confirmed": False, "email": email}
    auth_user = users_list[0]
    return {"confirmed": _is_email_confirmed(auth_user), "email": email}

@router.post("/bot-update-email")
async def bot_update_email(
    new_email: str = Body(...),
    user_id: str | None = Body(None),
    telegram_id: int | None = Body(None),
):
    _require_service_role()
    db_user_id = _resolve_user_id(user_id=user_id, telegram_id=telegram_id)

    # Найдем auth user по текущему email (если есть)
    row = admin_supabase.table("users").select("email").eq("id", db_user_id).maybe_single().execute()
    current_email = row.data.get("email") if row and row.data else None

    auth_id = None
    if current_email:
      async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SUPABASE_URL}/auth/v1/admin/users",
            headers={"Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}", "apikey": SUPABASE_KEY},
            params={"email": current_email},
        )
      if resp.status_code == 200:
          users_list = resp.json().get("users", [])
          if users_list:
              auth_id = users_list[0].get("id")

    # Если нашли auth_id — обновим email в Auth; если нет — просто отправим письмо на новый email (create or resend)
    if auth_id:
        async with httpx.AsyncClient() as client:
            r = await client.patch(
                f"{SUPABASE_URL}/auth/v1/admin/users/{auth_id}",
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "apikey": SUPABASE_KEY,
                    "Content-Type": "application/json",
                },
                json={"email": new_email, "email_confirm": False},
            )
            if r.status_code not in (200, 201):
                raise HTTPException(status_code=r.status_code, detail=r.text)
    else:
        # Создадим (если нет) и отправим письмо
        temp_password = secrets.token_urlsafe(18)
        async with httpx.AsyncClient() as client:
            create_resp = await client.post(
                f"{SUPABASE_URL}/auth/v1/admin/users",
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "apikey": SUPABASE_KEY,
                    "Content-Type": "application/json",
                },
                json={"email": new_email, "password": temp_password, "email_confirm": False},
            )
            if create_resp.status_code not in (200, 201, 422):
                raise HTTPException(status_code=create_resp.status_code, detail=create_resp.text)

    async with httpx.AsyncClient() as client:
        resend_resp = await client.post(
            f"{SUPABASE_URL}/auth/v1/resend",
            headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
            json={"type": "signup", "email": new_email},
        )
    if resend_resp.status_code not in (200, 201, 204):
        if APP_DEBUG:
            print("[auth/bot-update-email] resend failed", resend_resp.status_code, resend_resp.text)

    admin_supabase.table("users").update({"email": new_email}).eq("id", db_user_id).execute()
    return {"ok": True}

@router.post("/bot-resend-confirmation")
async def bot_resend_confirmation(
    payload: dict = Body(...),
):
    email = payload.get("email")
    if not email:
        raise HTTPException(status_code=422, detail="email is required")
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/auth/v1/resend",
            headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
            json={"type": "signup", "email": email},
        )
    if r.status_code not in (200, 201, 204):
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return {"ok": True}

@router.get("/tg-email-confirmed")
async def tg_email_confirmed(
    telegram_id: int = Query(...),
    email: str = Query(...),
    sig: str | None = Query(None),
):
    _require_service_role()
    if not _check_tg_confirm_sig(telegram_id, email, sig):
        raise HTTPException(status_code=403, detail="Invalid signature")

    auth_user = await _get_auth_user_by_email(email)
    if not auth_user or not _is_email_confirmed(auth_user):
        return RedirectResponse(f"{PUBLIC_WEB_URL}/auth/verified")

    await send_telegram_notification(
        telegram_id,
        "✅ Email подтвержден. Нажмите кнопку ниже, чтобы продолжить регистрацию.",
        reply_markup={
            "inline_keyboard": [[{"text": "Продолжить регистрацию", "callback_data": "email_continue"}]],
        },
    )

    return RedirectResponse(f"{PUBLIC_WEB_URL}/auth/verified")
