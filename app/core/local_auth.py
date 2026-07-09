import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import secrets
import time
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.rest import rest_get, rest_upsert
from app.core.db import SessionLocal, tables


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("utf-8")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))


def _jwt_secret() -> str:
    v = (os.getenv("AUTH_JWT_SECRET") or "").strip()
    if v:
        return v
    pepper = (os.getenv("OTP_PEPPER") or "").strip()
    if pepper:
        return pepper
    raise HTTPException(status_code=500, detail="AUTH_JWT_SECRET is not set")


def _jwt_issuer() -> str:
    return (os.getenv("AUTH_JWT_ISSUER") or "compease").strip()


def _jwt_ttl_seconds() -> int:
    try:
        return int(os.getenv("AUTH_JWT_TTL_SECONDS") or "86400")
    except Exception:
        return 86400


def _refresh_ttl_seconds() -> int:
    try:
        return int(os.getenv("AUTH_REFRESH_TTL_SECONDS") or "2592000")
    except Exception:
        return 2592000


def _jwt_exp_leeway_seconds() -> int:
    try:
        return int(os.getenv("AUTH_JWT_EXP_LEEWAY_SECONDS") or "30")
    except Exception:
        return 30


def issue_access_token(*, user_id: str, email: str | None = None) -> str:
    now = int(time.time())
    payload = {
        "iss": _jwt_issuer(),
        "sub": str(user_id),
        "iat": now,
        "exp": now + _jwt_ttl_seconds(),
    }
    if email:
        payload["email"] = str(email)
    header = {"alg": "HS256", "typ": "JWT"}
    head_b64 = _b64url_encode(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    msg = f"{head_b64}.{payload_b64}".encode("utf-8")
    sig = hmac.new(_jwt_secret().encode("utf-8"), msg, hashlib.sha256).digest()
    return f"{head_b64}.{payload_b64}.{_b64url_encode(sig)}"


def issue_refresh_token(*, user_id: str, email: str | None = None) -> str:
    now = int(time.time())
    payload = {
        "iss": _jwt_issuer(),
        "sub": str(user_id),
        "iat": now,
        "exp": now + _refresh_ttl_seconds(),
        "typ": "refresh",
        "jti": secrets.token_urlsafe(16),
    }
    if email:
        payload["email"] = str(email)
    header = {"alg": "HS256", "typ": "JWT"}
    head_b64 = _b64url_encode(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    msg = f"{head_b64}.{payload_b64}".encode("utf-8")
    sig = hmac.new(_jwt_secret().encode("utf-8"), msg, hashlib.sha256).digest()
    return f"{head_b64}.{payload_b64}.{_b64url_encode(sig)}"


def verify_access_token(token: str) -> dict:
    parts = str(token or "").split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid token")
    head_b64, payload_b64, sig_b64 = parts
    try:
        header = json.loads(_b64url_decode(head_b64))
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    if str(header.get("alg") or "") != "HS256":
        raise HTTPException(status_code=401, detail="Invalid token")

    msg = f"{head_b64}.{payload_b64}".encode("utf-8")
    expected = hmac.new(_jwt_secret().encode("utf-8"), msg, hashlib.sha256).digest()
    try:
        got = _b64url_decode(sig_b64)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    if not hmac.compare_digest(expected, got):
        raise HTTPException(status_code=401, detail="Invalid token")

    if str(payload.get("iss") or "") != _jwt_issuer():
        raise HTTPException(status_code=401, detail="Invalid token")

    now = int(time.time())
    exp = payload.get("exp")
    try:
        exp_i = int(exp)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    if now >= exp_i + max(0, _jwt_exp_leeway_seconds()):
        raise HTTPException(status_code=401, detail="Token expired")

    sub = payload.get("sub")
    if not sub:
        raise HTTPException(status_code=401, detail="Invalid token")
    return payload


def verify_refresh_token(token: str) -> dict:
    payload = verify_access_token(token)
    if str(payload.get("typ") or "") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    return payload


def auth_access_cookie_name() -> str:
    return (os.getenv("AUTH_ACCESS_COOKIE_NAME") or "ce_access").strip()


def auth_refresh_cookie_name() -> str:
    return (os.getenv("AUTH_REFRESH_COOKIE_NAME") or "ce_refresh").strip()


def auth_csrf_cookie_name() -> str:
    return (os.getenv("AUTH_CSRF_COOKIE_NAME") or "ce_csrf").strip()


def auth_csrf_header_name() -> str:
    return (os.getenv("AUTH_CSRF_HEADER_NAME") or "x-csrf-token").strip()


def auth_cookie_secure() -> bool:
    return (os.getenv("AUTH_COOKIE_SECURE") or "0").strip().lower() in {"1", "true", "yes", "on"}


def auth_cookie_samesite() -> str:
    value = (os.getenv("AUTH_COOKIE_SAMESITE") or "lax").strip().lower()
    if value not in {"lax", "strict", "none"}:
        return "lax"
    return value


def auth_cookie_domain() -> str | None:
    value = (os.getenv("AUTH_COOKIE_DOMAIN") or "").strip()
    return value or None


def auth_cookie_path() -> str:
    value = (os.getenv("AUTH_COOKIE_PATH") or "/").strip()
    return value or "/"


def access_cookie_max_age() -> int:
    return _jwt_ttl_seconds()


def refresh_cookie_max_age() -> int:
    return _refresh_ttl_seconds()


def new_csrf_token() -> str:
    return secrets.token_urlsafe(32)


def hash_refresh_token(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


async def store_refresh_token(*, user_id: str, refresh_token: str) -> None:
    table = tables.get("auth_refresh_tokens")
    if table is None:
        raise HTTPException(status_code=500, detail="auth_refresh_tokens table is not initialized")

    payload = verify_refresh_token(refresh_token)
    exp = int(payload.get("exp") or 0)
    expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
    token_hash = hash_refresh_token(refresh_token)

    from sqlalchemy.dialects.postgresql import insert as pg_insert

    stmt = pg_insert(table).values(
        {
            "token_hash": token_hash,
            "user_id": user_id,
            "expires_at": expires_at,
            "revoked_at": None,
            "replaced_by_hash": None,
        }
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[table.c.token_hash],
        set_={
            "user_id": user_id,
            "expires_at": expires_at,
            "revoked_at": None,
            "replaced_by_hash": None,
        },
    )
    async with SessionLocal() as session:
        await session.execute(stmt)
        await session.commit()


async def ensure_refresh_token_active(refresh_token: str) -> dict:
    payload = verify_refresh_token(refresh_token)
    table = tables.get("auth_refresh_tokens")
    if table is None:
        raise HTTPException(status_code=500, detail="auth_refresh_tokens table is not initialized")

    token_hash = hash_refresh_token(refresh_token)
    from sqlalchemy import select as _select

    async with SessionLocal() as session:
        res = await session.execute(
            _select(
                table.c.token_hash,
                table.c.user_id,
                table.c.expires_at,
                table.c.revoked_at,
                table.c.replaced_by_hash,
            ).where(table.c.token_hash == token_hash)
        )
        row = res.mappings().first()

    if not row:
        raise HTTPException(status_code=401, detail="Refresh token is not active")
    if row.get("revoked_at"):
        await revoke_all_refresh_tokens_for_user(str(row.get("user_id") or ""))
        raise HTTPException(status_code=401, detail="Refresh token reuse detected")
    expires_at = row.get("expires_at")
    if expires_at and isinstance(expires_at, datetime):
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at:
            raise HTTPException(status_code=401, detail="Refresh token expired")

    user_id = str(payload.get("sub") or "")
    if str(row.get("user_id") or "") != user_id:
        raise HTTPException(status_code=401, detail="Refresh token user mismatch")
    return payload


async def revoke_refresh_token(refresh_token: str, *, replaced_by_token: str | None = None) -> None:
    table = tables.get("auth_refresh_tokens")
    if table is None:
        return
    token_hash = hash_refresh_token(refresh_token)
    update_payload = {"revoked_at": datetime.now(timezone.utc)}
    if replaced_by_token:
        update_payload["replaced_by_hash"] = hash_refresh_token(replaced_by_token)

    from sqlalchemy import update as _update

    async with SessionLocal() as session:
        await session.execute(_update(table).where(table.c.token_hash == token_hash).values(update_payload))
        await session.commit()


async def revoke_all_refresh_tokens_for_user(user_id: str) -> None:
    if not user_id:
        return
    table = tables.get("auth_refresh_tokens")
    if table is None:
        return

    from sqlalchemy import update as _update

    async with SessionLocal() as session:
        await session.execute(
            _update(table)
            .where(table.c.user_id == user_id)
            .where(table.c.revoked_at.is_(None))
            .values({"revoked_at": datetime.now(timezone.utc)})
        )
        await session.commit()


def extract_token_from_authorization(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing authentication")
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing authentication")
    return token


def verify_session_access_token(authorization: str | None) -> dict:
    token = extract_token_from_authorization(authorization)
    return verify_access_token(token)


async def get_user_id_from_auth(authorization: str | None) -> str:
    payload = verify_session_access_token(authorization)
    return str(payload.get("sub"))


def _pbkdf2(password: str, salt: bytes, iterations: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iterations), dklen=32)


def _pwd_iters() -> int:
    try:
        return int(os.getenv("AUTH_PBKDF2_ITERS") or "200000")
    except Exception:
        return 200000


async def set_user_password(user_id: str, password: str) -> bool:
    salt = secrets.token_bytes(16)
    iters = _pwd_iters()
    h = _pbkdf2(password, salt, iters)
    payload = {
        "user_id": str(user_id),
        "password_salt": _b64url_encode(salt),
        "password_hash": _b64url_encode(h),
        "iterations": int(iters),
    }
    try:
        from app.core.supabase import admin_supabase
        await admin_supabase.table("auth_passwords").upsert(payload, on_conflict="user_id").execute_async()
        return True
    except Exception:
        return False


async def verify_user_password(
    user_id: str,
    password: str,
    *,
    session: AsyncSession | None = None,
) -> bool:
    try:
        auth_passwords_t = tables.get("auth_passwords")
        if auth_passwords_t is None:
            return False

        from sqlalchemy import select as _select

        stmt = (
            _select(
                auth_passwords_t.c.user_id,
                auth_passwords_t.c.password_salt,
                auth_passwords_t.c.password_hash,
                auth_passwords_t.c.iterations,
            )
            .where(auth_passwords_t.c.user_id == str(user_id))
            .limit(1)
        )

        if session is not None:
            res = await session.execute(stmt)
            row = res.mappings().first()
        else:
            async with SessionLocal() as own_session:
                res = await own_session.execute(stmt)
                row = res.mappings().first()

        if row is None:
            return False
        salt = _b64url_decode(str(row.get("password_salt") or ""))
        stored = _b64url_decode(str(row.get("password_hash") or ""))
        iters = int(row.get("iterations") or _pwd_iters())
        got = _pbkdf2(password, salt, iters)
        return hmac.compare_digest(stored, got)
    except Exception:
        return False


async def ensure_user_row_for_email(email: str) -> str:
    email_norm = str(email or "").strip().lower()
    resp = await rest_get(
        "users",
        {"select": "id", "email": f"eq.{email_norm}", "limit": "1"},
        write=True,
    )
    if resp.status_code == 200:
        rows = resp.json()
        row = rows[0] if isinstance(rows, list) and rows else None
        if isinstance(row, dict) and row.get("id"):
            return str(row["id"])

    user_id = str(uuid4())
    try:
        await rest_upsert("users", {"id": user_id, "email": email_norm}, on_conflict="id")
    except Exception:
        pass
    return user_id


async def get_user_id_from_bearer(authorization: str | None) -> str:
    return await get_user_id_from_auth(authorization)
