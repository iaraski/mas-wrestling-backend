from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import os
import smtplib
import ssl
import threading
import time
from email.message import EmailMessage
from email.utils import formataddr
import json
import httpx
from app.core.otp_store import generate_code
from app.core.otp_db import consume as otp_consume_db, delete_sync as otp_delete_db_sync, store_sync as otp_store_db_sync
from app.core.local_auth import ensure_user_row_for_email, issue_access_token, set_user_password
from app.core.rest import rest_upsert
from app.core.supabase import SUPABASE_KEY, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL
from urllib.parse import quote_plus
import secrets

router = APIRouter(prefix="/auth-custom", tags=["auth-custom"])

_smtp_lock = threading.Lock()
_send_rl_lock = threading.Lock()
_send_next_allowed: dict[str, float] = {}
_auth_admin_client: httpx.AsyncClient | None = None


async def _get_auth_admin_client() -> httpx.AsyncClient:
    global _auth_admin_client
    if _auth_admin_client is None:
        timeout = httpx.Timeout(15.0, connect=6.0, read=15.0, write=15.0, pool=6.0)
        _auth_admin_client = httpx.AsyncClient(
            http2=False,
            timeout=timeout,
            limits=httpx.Limits(max_connections=30, max_keepalive_connections=12, keepalive_expiry=45.0),
            transport=httpx.AsyncHTTPTransport(retries=1),
        )
    return _auth_admin_client


async def _ensure_supabase_auth_user(email: str, password: str) -> str:
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_SERVICE_ROLE_KEY:
        raise HTTPException(status_code=500, detail="Supabase keys missing")

    client = await _get_auth_admin_client()

    user_id: str | None = None
    created = await client.post(
        f"{SUPABASE_URL}/auth/v1/admin/users",
        json={"email": email, "password": password, "email_confirm": True},
        headers={
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Content-Type": "application/json",
        },
    )

    if created.status_code in (200, 201):
        j = created.json() if created.headers.get("content-type", "").startswith("application/json") else {}
        user_id = j.get("id") if isinstance(j, dict) else None
    else:
        txt = (created.text or "").lower()
        if created.status_code in (400, 409, 422) and ("already" in txt or "exists" in txt or "registered" in txt):
            try:
                get_user = await client.get(
                    f"{SUPABASE_URL}/auth/v1/admin/users",
                    params={"email": email},
                    headers={
                        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                        "apikey": SUPABASE_SERVICE_ROLE_KEY,
                    },
                )
                if get_user.status_code == 200:
                    j = get_user.json()
                    if isinstance(j, dict):
                        user_id = j.get("id") or j.get("user", {}).get("id")
                    elif isinstance(j, list) and j:
                        user_id = (j[0] or {}).get("id")
            except Exception:
                user_id = None
            if user_id:
                upd = await client.put(
                    f"{SUPABASE_URL}/auth/v1/admin/users/{user_id}",
                    json={"password": password, "email_confirm": True},
                    headers={
                        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                        "apikey": SUPABASE_SERVICE_ROLE_KEY,
                        "Content-Type": "application/json",
                    },
                )
                if upd.status_code not in (200, 201):
                    raise HTTPException(status_code=400, detail="Не удалось обновить пароль. Попробуйте позже.")
            else:
                raise HTTPException(status_code=400, detail="Почта уже зарегистрирована.")
        else:
            raise HTTPException(status_code=400, detail="Не удалось создать пользователя. Попробуйте позже.")

    if not user_id:
        raise HTTPException(status_code=400, detail="Не удалось создать пользователя. Попробуйте позже.")
    return str(user_id)

def _truthy_env(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")

def _unisender_go_api_bases() -> list[str]:
    raw = (os.getenv("UNISENDER_GO_API_BASES") or "").strip()
    if raw:
        parts = [p.strip().rstrip("/") for p in raw.split(",") if p.strip()]
        return list(dict.fromkeys(parts))
    smtp_host = (os.getenv("SMTP_HOST") or "").strip().lower()
    if "go1." in smtp_host:
        return [
            "https://go1.unisender.ru/ru/transactional/api/v1",
            "https://goapi.unisender.ru/ru/transactional/api/v1",
            "https://go2.unisender.ru/ru/transactional/api/v1",
        ]
    if "go2." in smtp_host:
        return [
            "https://go2.unisender.ru/ru/transactional/api/v1",
            "https://goapi.unisender.ru/ru/transactional/api/v1",
            "https://go1.unisender.ru/ru/transactional/api/v1",
        ]
    return [
        "https://goapi.unisender.ru/ru/transactional/api/v1",
        "https://go2.unisender.ru/ru/transactional/api/v1",
        "https://go1.unisender.ru/ru/transactional/api/v1",
    ]

def _send_unisender_go_via_api(
    *,
    to_email: str,
    subject: str,
    html_body: str,
    text_body: str,
    from_email: str,
    from_name: str,
    api_key: str,
    retries: int,
    timeout: float,
) -> None:
    ctx = ssl.create_default_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    payload = {
        "message": {
            "recipients": [{"email": to_email}],
            "subject": subject,
            "from_email": from_email,
            "from_name": from_name or None,
            "body": {"html": html_body, "plaintext": text_body},
            "track_links": 0,
            "track_read": 0,
        }
    }
    headers = {"Accept": "application/json", "Content-Type": "application/json", "X-API-KEY": api_key}

    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        for base in _unisender_go_api_bases():
            url = base.rstrip("/") + "/email/send.json"
            try:
                print(
                    f"[OTP] Unisender Go API send start to={to_email} url={url} attempt={attempt+1}/{retries+1}"
                )
                with httpx.Client(
                    timeout=httpx.Timeout(timeout, connect=min(10.0, timeout)),
                    http2=False,
                    verify=ctx,
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5, keepalive_expiry=5.0),
                    transport=httpx.HTTPTransport(retries=2),
                ) as client:
                    resp = client.post(url, headers=headers, content=json.dumps(payload))
                if resp.status_code == 200:
                    print(f"[OTP] Unisender Go API send ok to={to_email}")
                    return
                msg = (resp.text or "").strip()
                raise RuntimeError(f"API {resp.status_code}: {msg[:500]}")
            except Exception as e:
                last_exc = e
                print(f"[OTP] Unisender Go API send failed url={url}: {type(e).__name__}: {str(e) or ''}".strip())
                continue
        if attempt < retries:
            time.sleep(0.8 * (attempt + 1))
            continue
        break
    raise RuntimeError(f"Email API send failed: {repr(last_exc)}")

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
    retries = int(os.getenv("SMTP_RETRIES") or "2")
    timeout = float(os.getenv("SMTP_TIMEOUT") or "25")

    force_go_api = _truthy_env("UNISENDER_GO_FORCE_API")
    is_unisender_host = "unisender" in str(host).lower()
    if force_go_api or is_unisender_host:
        api_key = (os.getenv("UNISENDER_GO_API_KEY") or "").strip() or pwd
        try:
            _send_unisender_go_via_api(
                to_email=to_email,
                subject=subject,
                html_body=html_body,
                text_body=text_body,
                from_email=from_email,
                from_name=from_name,
                api_key=api_key,
                retries=retries,
                timeout=timeout,
            )
            return
        except Exception as e:
            print(f"[OTP] Unisender Go API unavailable, fallback to SMTP: {type(e).__name__}: {str(e) or ''}".strip())

    tls_ctx = ssl.create_default_context()
    tls_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    tls_ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    ports = [int(port)]
    if int(port) != 25:
        ports.append(25)

    for attempt in range(retries + 1):
        try:
            last_exc: Exception | None = None
            for p in ports:
                try:
                    print(
                        f"[OTP] SMTP send start to={to_email} host={host} port={p} ssl=False starttls={use_starttls} attempt={attempt+1}/{retries+1}"
                    )
                    with _smtp_lock:
                        smtp = smtplib.SMTP(host, p, timeout=timeout)
                        with smtp:
                            smtp.ehlo_or_helo_if_needed()
                            if use_starttls:
                                smtp.starttls(context=tls_ctx)
                                smtp.ehlo_or_helo_if_needed()
                            smtp.login(user, pwd)
                            smtp.send_message(msg)
                    print(f"[OTP] SMTP send ok to={to_email}")
                    return
                except (smtplib.SMTPServerDisconnected, OSError) as e:
                    last_exc = e
                    print(f"[OTP] SMTP port {p} failed due to {type(e).__name__}: {str(e) or ''}".strip())
                    continue
            if last_exc is not None:
                raise last_exc
        except (smtplib.SMTPAuthenticationError, smtplib.SMTPRecipientsRefused) as e:
            raise HTTPException(status_code=500, detail=f"SMTP error: {type(e).__name__}: {str(e) or ''}".strip()) from e
        except (smtplib.SMTPServerDisconnected, OSError) as e:
            if attempt < retries:
                print(f"[OTP] SMTP retry due to {type(e).__name__}: {str(e) or ''}".strip())
                time.sleep(0.7 * (attempt + 1))
                continue
            raise HTTPException(status_code=500, detail=f"SMTP connection error: {type(e).__name__}: {str(e) or ''}".strip()) from e
        except smtplib.SMTPException as e:
            raise HTTPException(status_code=500, detail=f"SMTP error: {type(e).__name__}: {str(e) or ''}".strip()) from e


def _smtp_send_background(to_email: str, subject: str, html_body: str, text_body: str) -> None:
    try:
        _smtp_send(to_email, subject, html_body, text_body)
    except HTTPException as e:
        otp_delete_db_sync(to_email)
        with _send_rl_lock:
            _send_next_allowed.pop(str(to_email), None)
        print(f"[OTP] SMTP send failed to={to_email}: {e.detail}")
    except Exception as e:
        otp_delete_db_sync(to_email)
        with _send_rl_lock:
            _send_next_allowed.pop(str(to_email), None)
        print(f"[OTP] SMTP send failed to={to_email}: {repr(e)}")

@router.post("/otp/send")
async def send_otp(body: SendOtpBody):
    email = _normalize_email(body.email)
    now = time.time()
    with _send_rl_lock:
        nxt = float(_send_next_allowed.get(email) or 0.0)
        if now < nxt:
            raise HTTPException(status_code=429, detail="Повторная отправка доступна через 60 секунд")
        _send_next_allowed[email] = now + 60.0
    code = generate_code()
    html = f"""
    <h2>Код подтверждения email</h2>
    <p>Ваш код для регистрации в MAS-WRESTLING ONLINE:</p>
    <p style="font-size:24px;font-weight:700;letter-spacing:2px;margin:16px 0;">{code}</p>
    <p>Введите этот код на сайте. Код действует ограниченное время.</p>
    """
    text = f"Код подтверждения: {code}\nВведите этот код на сайте. Код действует ограниченное время."
    send_async_env = (os.getenv("SMTP_SEND_ASYNC") or "").strip().lower()
    send_async = send_async_env in ("1", "true", "yes", "on")
    print(f"[OTP] send_otp mode={'async' if send_async else 'sync'} SMTP_SEND_ASYNC={send_async_env!r}")
    if send_async:
        def _job():
            ok = otp_store_db_sync(email, code, ttl_seconds=600)
            if not ok:
                with _send_rl_lock:
                    _send_next_allowed.pop(email, None)
                print(f"[OTP] OTP store failed email={email}")
                return
            _smtp_send_background(email, "Код подтверждения", html, text)
        threading.Thread(
            target=_job,
            daemon=True,
        ).start()
        return {"ok": True, "queued": True}
    try:
        ok = otp_store_db_sync(email, code, ttl_seconds=600)
        if not ok:
            with _send_rl_lock:
                _send_next_allowed.pop(email, None)
            raise HTTPException(status_code=503, detail="OTP storage unavailable")
        print(f"[OTP] OTP stored email={email}")
        _smtp_send(email, "Код подтверждения", html, text)
        return {"ok": True, "queued": False}
    except HTTPException as e:
        otp_delete_db_sync(email)
        with _send_rl_lock:
            _send_next_allowed.pop(email, None)
        raise e

@router.post("/otp/verify")
async def verify_otp(body: VerifyOtpBody):
    email = _normalize_email(body.email)
    await otp_consume_db(email, body.code, max_attempts=5)
    if not body.password or len(str(body.password)) < 8:
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 8 символов")
    disable_supabase_signup = (os.getenv("AUTH_DISABLE_SUPABASE_AUTH_SIGNUP") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if disable_supabase_signup:
        user_id = await ensure_user_row_for_email(email)
        ok = await set_user_password(user_id, str(body.password))
        if not ok:
            raise HTTPException(
                status_code=500,
                detail="Local auth is not configured (apply backend/sql/local_auth.sql)",
            )
        access_token = issue_access_token(user_id=user_id, email=email)
        return {"ok": True, "access_token": access_token, "token_type": "bearer", "user_id": user_id, "email": email}

    user_id = await _ensure_supabase_auth_user(email, str(body.password))
    try:
        await rest_upsert("users", {"id": user_id, "email": email}, on_conflict="id")
    except Exception:
        pass
    ok = await set_user_password(user_id, str(body.password))
    if not ok:
        print("[OTP] Local auth table missing or unavailable; continuing with Supabase auth only")
    access_token = issue_access_token(user_id=user_id, email=email)
    return {"ok": True, "access_token": access_token, "token_type": "bearer", "user_id": user_id, "email": email}


class ResetSendBody(BaseModel):
    email: str


class ResetConfirmBody(BaseModel):
    email: str
    token: str
    password: str


@router.post("/reset/send")
async def reset_send(body: ResetSendBody):
    email = _normalize_email(body.email)
    now = time.time()
    key = f"reset:{email}"
    with _send_rl_lock:
        nxt = float(_send_next_allowed.get(key) or 0.0)
        if now < nxt:
            raise HTTPException(status_code=429, detail="Повторная отправка доступна через 60 секунд")
        _send_next_allowed[key] = now + 60.0
    token = secrets.token_urlsafe(32)
    public_web_url = (os.getenv("PUBLIC_WEB_URL") or "").strip().rstrip("/")
    if not public_web_url:
        public_web_url = "http://localhost:5173" if (os.getenv("APP_DEBUG") == "1") else "https://mas-wrestling.pro"
    reset_link = f"{public_web_url}/auth/reset?email={quote_plus(email)}&token={quote_plus(token)}"
    html = (
        "<h2>Восстановление пароля</h2>"
        "<p>Чтобы изменить пароль, перейдите по ссылке:</p>"
        f"<p><a href=\"{reset_link}\">{reset_link}</a></p>"
        "<p>Если вы не запрашивали восстановление, просто проигнорируйте это письмо.</p>"
    )
    text = (
        "Восстановление пароля\n"
        f"Ссылка: {reset_link}\n"
        "Если вы не запрашивали восстановление, просто проигнорируйте это письмо."
    )
    try:
        ok = otp_store_db_sync(email, token, ttl_seconds=900)
        if not ok:
            with _send_rl_lock:
                _send_next_allowed.pop(key, None)
            raise HTTPException(status_code=503, detail="OTP storage unavailable")
        _smtp_send(email, "Восстановление пароля", html, text)
        return {"ok": True, "queued": False}
    except HTTPException:
        otp_delete_db_sync(email)
        with _send_rl_lock:
            _send_next_allowed.pop(key, None)
        raise
    except Exception:
        otp_delete_db_sync(email)
        with _send_rl_lock:
            _send_next_allowed.pop(key, None)
        raise HTTPException(status_code=500, detail="Не удалось отправить письмо")


async def _get_user_id_by_email_or_sync_from_supabase(email: str) -> str:
    try:
        from app.core.rest import rest_get
        resp = await rest_get("users", {"select": "id", "email": f"eq.{email}", "limit": "1"}, write=True)
        rows = resp.json()
        if isinstance(rows, list) and rows and isinstance(rows[0], dict) and rows[0].get("id"):
            return str(rows[0]["id"])
    except Exception:
        pass

    if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
        try:
            client = await _get_auth_admin_client()
            get_user = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users",
                params={"email": email},
                headers={
                    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                    "apikey": SUPABASE_SERVICE_ROLE_KEY,
                },
            )
            if get_user.status_code == 200:
                j = get_user.json()
                supa_id = None
                if isinstance(j, dict):
                    supa_id = j.get("id") or j.get("user", {}).get("id")
                elif isinstance(j, list) and j:
                    supa_id = (j[0] or {}).get("id")
                if supa_id:
                    try:
                        await rest_upsert("users", {"id": supa_id, "email": email}, on_conflict="id")
                    except Exception:
                        pass
                    return str(supa_id)
        except Exception:
            pass

    return await ensure_user_row_for_email(email)


@router.post("/reset/confirm")
async def reset_confirm(body: ResetConfirmBody):
    email = _normalize_email(body.email)
    token = str(body.token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Неверная ссылка восстановления")
    await otp_consume_db(email, token, max_attempts=5)
    if not body.password or len(str(body.password)) < 8:
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 8 символов")
    user_id = await _get_user_id_by_email_or_sync_from_supabase(email)
    ok = await set_user_password(user_id, str(body.password))
    if not ok:
        raise HTTPException(status_code=500, detail="Local auth is not configured (apply backend/sql/local_auth.sql)")
    access_token = issue_access_token(user_id=user_id, email=email)
    return {"ok": True, "access_token": access_token, "token_type": "bearer", "user_id": user_id, "email": email}
