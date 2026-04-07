import hashlib
import hmac
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from fastapi import HTTPException

from app.core.rest import rest_get, rest_upsert, rest_delete, rest_patch
from app.core.supabase import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, admin_supabase
import httpx

def _pepper() -> str:
    p = os.getenv("OTP_PEPPER", "").strip()
    if not p:
        raise HTTPException(status_code=500, detail="OTP_PEPPER is not configured")
    return p

def hash_code(code: str) -> str:
    c = str(code).strip()
    return hashlib.sha256((_pepper() + ":" + c).encode("utf-8")).hexdigest()

def _split_hashes(value: str) -> list[str]:
    s = str(value or "").strip()
    if not s:
        return []
    return [p for p in s.split("|") if p]

def _now() -> datetime:
    return datetime.now(timezone.utc)

async def get_row(email: str) -> Optional[Dict[str, Any]]:
    # Fast path via admin_supabase
    if admin_supabase:
        try:
            res = (
                admin_supabase.table("otp_codes")
                .select("email,code_hash,expires_at,attempts,last_sent_at")
                .eq("email", str(email))
                .maybe_single()
                .execute()
            )
            data = getattr(res, "data", None)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    try:
        resp = await rest_get(
            "otp_codes",
            {
                "select": "email,code_hash,expires_at,attempts,last_sent_at",
                "email": f"eq.{email}",
                "limit": "1",
            },
            write=True,
        )
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0]
        return None
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"OTP storage unavailable: {type(e).__name__}") from e

async def can_send(email: str, min_interval_seconds: int = 60) -> bool:
    row = await get_row(email)
    if not row:
        return True
    last = row.get("last_sent_at")
    if not last:
        return True
    try:
        dt = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
    except Exception:
        return True
    return (_now() - dt).total_seconds() >= float(min_interval_seconds)

async def delete(email: str) -> None:
    try:
        await rest_delete("otp_codes", {"email": f"eq.{email}"})
    except Exception:
        return

def delete_sync(email: str) -> None:
    if admin_supabase:
        try:
            admin_supabase.table("otp_codes").delete().eq("email", str(email)).execute()
            return
        except Exception:
            pass
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return
    try:
        url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/otp_codes"
        params = {"email": f"eq.{email}"}
        headers = {
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
            client.delete(url, params=params, headers=headers)
    except Exception:
        return

async def store(email: str, code: str, ttl_seconds: int = 600) -> None:
    expires = _now() + timedelta(seconds=int(ttl_seconds))
    prev_hash: str | None = None
    try:
        row = await get_row(email)
        if row and row.get("code_hash") and row.get("expires_at"):
            try:
                exp = datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00"))
            except Exception:
                exp = _now() - timedelta(seconds=1)
            if _now() <= exp:
                prev_hash = str(row["code_hash"])
    except Exception:
        prev_hash = None

    new_hash = hash_code(code)
    code_hash = new_hash
    prev_parts = _split_hashes(prev_hash or "")
    if prev_parts:
        if len(prev_parts) >= 2:
            code_hash = f"{new_hash}|{prev_parts[0]}|{prev_parts[1]}"
        else:
            code_hash = f"{new_hash}|{prev_parts[0]}"
    try:
        await rest_upsert(
            "otp_codes",
            {
                "email": email,
                "code_hash": code_hash,
                "expires_at": expires.isoformat(),
                "attempts": 0,
                "last_sent_at": _now().isoformat(),
            },
            on_conflict="email",
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"OTP storage unavailable: {type(e).__name__}") from e

def store_sync(email: str, code: str, ttl_seconds: int = 600) -> bool:
    expires = _now() + timedelta(seconds=int(ttl_seconds))
    prev_first: str | None = None
    if admin_supabase:
        try:
            cur = (
                admin_supabase.table("otp_codes")
                .select("code_hash,expires_at")
                .eq("email", str(email))
                .maybe_single()
                .execute()
            )
            row = getattr(cur, "data", None) if cur else None
            if isinstance(row, dict) and row.get("code_hash") and row.get("expires_at"):
                try:
                    exp = datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00"))
                except Exception:
                    exp = _now() - timedelta(seconds=1)
                if _now() <= exp:
                    prev_parts = _split_hashes(str(row.get("code_hash") or ""))
                    if prev_parts:
                        prev_first = prev_parts[0]
        except Exception:
            prev_first = None

    new_hash = hash_code(code)
    code_hash = f"{new_hash}|{prev_first}" if prev_first else new_hash
    # If we also have a second previous, include it
    if admin_supabase and prev_first:
        try:
            cur2 = (
                admin_supabase.table("otp_codes")
                .select("code_hash")
                .eq("email", str(email))
                .maybe_single()
                .execute()
            )
            row2 = getattr(cur2, "data", None)
            if isinstance(row2, dict) and row2.get("code_hash"):
                p = _split_hashes(str(row2["code_hash"]))
                if len(p) >= 2:
                    code_hash = f"{new_hash}|{p[0]}|{p[1]}"
        except Exception:
            pass
    payload = {
        "email": str(email),
        "code_hash": code_hash,
        "expires_at": expires.isoformat(),
        "attempts": 0,
        "last_sent_at": _now().isoformat(),
    }
    if admin_supabase:
        try:
            admin_supabase.table("otp_codes").upsert(payload, on_conflict="email").execute()
            check = admin_supabase.table("otp_codes").select("email").eq("email", str(email)).maybe_single().execute()
            return bool(getattr(check, "data", None) and isinstance(check.data, dict) and check.data.get("email"))
        except Exception:
            pass
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return False
    try:
        url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/otp_codes"
        params = {"on_conflict": "email"}
        headers = {
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        with httpx.Client(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
            resp = client.post(url, params=params, headers=headers, json=payload)
            return resp.status_code in (200, 201, 204)
    except Exception:
        return False

async def consume(email: str, code: str, max_attempts: int = 5) -> None:
    row = await get_row(email)
    if not row:
        raise HTTPException(status_code=400, detail="Неверный или истёкший код")
    expires_at = row.get("expires_at")
    try:
        exp = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
    except Exception:
        exp = _now() - timedelta(seconds=1)
    if _now() > exp:
        try:
            if admin_supabase:
                admin_supabase.table("otp_codes").delete().eq("email", str(email)).execute()
            else:
                await rest_delete("otp_codes", {"email": f"eq.{email}"})
        except Exception:
            pass
        raise HTTPException(status_code=400, detail="Неверный или истёкший код")

    attempts = int(row.get("attempts") or 0)
    if attempts >= int(max_attempts):
        raise HTTPException(status_code=429, detail="Слишком много попыток, попробуйте позже")

    saved_hashes = _split_hashes(str(row.get("code_hash") or ""))
    candidate = hash_code(code)
    ok = any(hmac.compare_digest(h, candidate) for h in saved_hashes)
    if not ok:
        new_attempts = attempts + 1
        try:
            if admin_supabase:
                admin_supabase.table("otp_codes").update({"attempts": new_attempts}).eq("email", str(email)).execute()
            else:
                await rest_patch(
                    "otp_codes",
                    {"email": f"eq.{email}"},
                    {"attempts": new_attempts},
                    prefer="return=minimal",
                )
        except Exception:
            pass
        raise HTTPException(status_code=400, detail="Неверный или истёкший код")

    try:
        if admin_supabase:
            admin_supabase.table("otp_codes").delete().eq("email", str(email)).execute()
        else:
            await rest_delete("otp_codes", {"email": f"eq.{email}"})
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"OTP storage unavailable: {type(e).__name__}") from e
