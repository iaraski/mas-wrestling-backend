from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import os
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr
from app.core.otp_store import generate_code
from app.core.otp_db import can_send as otp_can_send, store as otp_store_db, consume as otp_consume_db
from app.core.supabase import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_ROLE_KEY
from app.core.rest import rest_get, rest_upsert
import httpx

router = APIRouter(prefix="/auth-custom", tags=["auth-custom"])

class SendOtpBody(BaseModel):
    email: str

class VerifyOtpBody(BaseModel):
    email: str
    code: str
    password: str

def _normalize_email(email: str) -> str:
    e = str(email or "").strip().lower()
    if not e or "@" not in e or "." not in e.split("@")[-1]:
        raise HTTPException(status_code=400, detail="Некорректный email")
    if len(e) > 320:
        raise HTTPException(status_code=400, detail="Некорректный email")
    return e

def _smtp_send(to_email: str, subject: str, html_body: str, text_body: str) -> None:
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT") or "587")
    user = os.getenv("SMTP_USERNAME")
    pwd = os.getenv("SMTP_PASSWORD")
    from_email = os.getenv("SMTP_FROM_EMAIL") or "noreply@example.com"
    from_name = (os.getenv("SMTP_FROM_NAME") or "").strip()
    if not host or not user or not pwd:
        raise HTTPException(status_code=500, detail="SMTP not configured")
    use_ssl_env = (os.getenv("SMTP_USE_SSL") or "").strip().lower()
    use_starttls_env = (os.getenv("SMTP_USE_STARTTLS") or "").strip().lower()
    use_ssl = use_ssl_env in ("1", "true", "yes", "on") or port == 465
    use_starttls = (
        (use_starttls_env in ("", "1", "true", "yes", "on"))
        if not use_ssl
        else False
    )
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((from_name, from_email)) if from_name else from_email
    msg["To"] = to_email
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")
    try:
        if use_ssl:
            smtp = smtplib.SMTP_SSL(host, port, timeout=25)
        else:
            smtp = smtplib.SMTP(host, port, timeout=25)
        with smtp:
            smtp.ehlo_or_helo_if_needed()
            if use_starttls:
                ctx = ssl.create_default_context()
                smtp.starttls(context=ctx)
                smtp.ehlo_or_helo_if_needed()
            smtp.login(user, pwd)
            smtp.send_message(msg)
    except smtplib.SMTPException as e:
        raise HTTPException(status_code=500, detail=f"SMTP error: {type(e).__name__}") from e
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"SMTP connection error: {type(e).__name__}") from e

@router.post("/otp/send")
async def send_otp(body: SendOtpBody, background_tasks: BackgroundTasks):
    email = _normalize_email(body.email)
    if not await otp_can_send(email, min_interval_seconds=60):
        raise HTTPException(status_code=429, detail="Повторная отправка доступна через 60 секунд")
    code = generate_code()
    html = f"""
    <h2>Код подтверждения email</h2>
    <p>Ваш код для регистрации в MAS-WRESTLING ONLINE:</p>
    <p style="font-size:24px;font-weight:700;letter-spacing:2px;margin:16px 0;">{code}</p>
    <p>Введите этот код на сайте. Код действует ограниченное время.</p>
    """
    text = f"Код подтверждения: {code}\nВведите этот код на сайте. Код действует ограниченное время."
    await otp_store_db(email, code, ttl_seconds=600)
    background_tasks.add_task(_smtp_send, email, "Код подтверждения", html, text)
    return {"ok": True}

@router.post("/otp/verify")
async def verify_otp(body: VerifyOtpBody):
    email = _normalize_email(body.email)
    await otp_consume_db(email, body.code, max_attempts=5)
    if not body.password or len(str(body.password)) < 8:
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 8 символов")
    # Create or update user via Supabase Auth Admin
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_SERVICE_ROLE_KEY:
        raise HTTPException(status_code=500, detail="Supabase keys missing")
    async with httpx.AsyncClient(timeout=20.0) as client:
        user_id = None
        try:
            resp = await rest_get(
                "users",
                {"select": "id", "email": f"eq.{email}", "limit": "1"},
                write=True,
            )
            rows = resp.json()
            if isinstance(rows, list) and rows:
                user_id = rows[0].get("id")
        except Exception:
            user_id = None

        if user_id:
            upd = await client.put(
                f"{SUPABASE_URL}/auth/v1/admin/users/{user_id}",
                json={"password": body.password, "email_confirm": True},
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "apikey": SUPABASE_SERVICE_ROLE_KEY,
                    "Content-Type": "application/json",
                },
            )
            if upd.status_code not in (200, 201):
                raise HTTPException(status_code=400, detail=f"Failed to update auth user: {upd.text}")
        else:
            created = await client.post(
                f"{SUPABASE_URL}/auth/v1/admin/users",
                json={"email": email, "password": body.password, "email_confirm": True},
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "apikey": SUPABASE_SERVICE_ROLE_KEY,
                    "Content-Type": "application/json",
                },
            )
            if created.status_code not in (200, 201):
                raise HTTPException(status_code=400, detail=f"Failed to create auth user: {created.text}")
            j = created.json() if created.headers.get("content-type", "").startswith("application/json") else {}
            user_id = j.get("id") if isinstance(j, dict) else None

        if user_id:
            try:
                await rest_upsert("users", {"id": user_id, "email": email}, on_conflict="id")
            except Exception:
                pass
    return {"ok": True}
