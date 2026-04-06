import time
from typing import Dict, Tuple, Optional
import secrets

_store: Dict[str, Tuple[str, float]] = {}

def generate_code() -> str:
    return f"{secrets.randbelow(1000000):06d}"

def put(email: str, code: str, ttl_seconds: int = 600) -> None:
    expires = time.time() + ttl_seconds
    _store[email.lower()] = (code, expires)

def verify(email: str, code: str) -> bool:
    key = email.lower()
    if key not in _store:
        return False
    saved, exp = _store[key]
    if time.time() > exp:
        _store.pop(key, None)
        return False
    if str(saved) != str(code).strip():
        return False
    _store.pop(key, None)
    return True

def peek(email: str) -> Optional[str]:
    val = _store.get(email.lower())
    if not val:
        return None
    code, exp = val
    if time.time() > exp:
        _store.pop(email.lower(), None)
        return None
    return code
