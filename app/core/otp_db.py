import hashlib
import hmac
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from fastapi import HTTPException

from app.core.rest import rest_get, rest_upsert, rest_delete, rest_patch

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
        await rest_delete("otp_codes", {"email": f"eq.{email}"})
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"OTP storage unavailable: {type(e).__name__}") from e
