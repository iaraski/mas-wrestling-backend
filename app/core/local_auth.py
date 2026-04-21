import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from uuid import uuid4

import anyio
from fastapi import HTTPException

from app.core.rest import rest_get, rest_upsert
from app.core.supabase import SUPABASE_KEY, SUPABASE_URL, admin_supabase


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
        await rest_upsert("auth_passwords", payload, on_conflict="user_id")
        return True
    except Exception:
        return False


async def verify_user_password(user_id: str, password: str) -> bool:
    try:
        resp = await rest_get(
            "auth_passwords",
            {"select": "user_id,password_salt,password_hash,iterations", "user_id": f"eq.{user_id}", "limit": "1"},
            write=True,
        )
        if resp.status_code != 200:
            return False
        rows = resp.json()
        row = rows[0] if isinstance(rows, list) and rows else None
        if not isinstance(row, dict):
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
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    try:
        payload = verify_access_token(token)
        return str(payload.get("sub"))
    except HTTPException:
        pass

    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")

    def _sync_get_user():
        return admin_supabase.auth.get_user(token)

    try:
        auth_res = await anyio.to_thread.run_sync(_sync_get_user)
    except Exception as e:
        msg = repr(e)
        lowered = msg.lower()
        if "jwt" in lowered or "token" in lowered or "401" in lowered or "403" in lowered:
            raise HTTPException(status_code=401, detail="Invalid token")
        raise HTTPException(status_code=503, detail=f"Supabase Auth unavailable: {msg}")

    user_obj = None
    if isinstance(auth_res, dict):
        user_obj = auth_res.get("user") or auth_res.get("data") or auth_res.get("user_data")
    else:
        user_obj = getattr(auth_res, "user", None) or getattr(auth_res, "data", None)

    user_id = None
    if hasattr(user_obj, "id"):
        user_id = getattr(user_obj, "id")
    elif isinstance(user_obj, dict):
        user_id = user_obj.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    return str(user_id)


async def supabase_password_grant(email: str, password: str) -> dict | None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    import httpx

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=5.0), http2=False) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/auth/v1/token",
                params={"grant_type": "password"},
                headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
                json={"email": email, "password": password},
            )
    except Exception:
        return None
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except Exception:
        return None
