import anyio
import httpx
from typing import Dict, Any, Optional
from app.core.supabase import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_ROLE_KEY

_BASE = f"{SUPABASE_URL}/rest/v1"

_client: Optional[httpx.AsyncClient] = None

def _headers(write: bool = False) -> Dict[str, str]:
    token = SUPABASE_SERVICE_ROLE_KEY if write else SUPABASE_KEY
    return {
        "Authorization": f"Bearer {token}",
        "apikey": token,
        "Content-Type": "application/json",
    }

async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            http2=False,
            timeout=httpx.Timeout(20.0, connect=8.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20, keepalive_expiry=10.0),
            transport=httpx.AsyncHTTPTransport(retries=3),
        )
    return _client

async def rest_get(path: str, params: Dict[str, Any], *, write: bool = False) -> httpx.Response:
    url = f"{_BASE}/{path.lstrip('/')}"
    client = await _get_client()
    last_exc = None
    for attempt in range(3):
        try:
            resp = await client.get(url, params=params, headers=_headers(write))
            if resp.status_code >= 500:
                raise RuntimeError(f"Server error {resp.status_code}")
            return resp
        except Exception as e:
            last_exc = e
            if attempt >= 2:
                break
            await anyio.sleep(0.2 * (attempt + 1))
    raise RuntimeError(f"REST GET failed: {repr(last_exc)}")

async def rest_post(path: str, params: Dict[str, Any], json: Any, *, prefer: Optional[str] = None) -> httpx.Response:
    url = f"{_BASE}/{path.lstrip('/')}"
    client = await _get_client()
    headers = _headers(write=True)
    if prefer:
        headers["Prefer"] = prefer
    last_exc = None
    for attempt in range(3):
        try:
            resp = await client.post(url, params=params, headers=headers, json=json)
            if resp.status_code >= 500:
                raise RuntimeError(f"Server error {resp.status_code}")
            return resp
        except Exception as e:
            last_exc = e
            if attempt >= 2:
                break
            await anyio.sleep(0.25 * (attempt + 1))
    raise RuntimeError(f"REST POST failed: {repr(last_exc)}")

async def rest_delete(path: str, params: Dict[str, Any]) -> httpx.Response:
    url = f"{_BASE}/{path.lstrip('/')}"
    client = await _get_client()
    headers = _headers(write=True)
    last_exc = None
    for attempt in range(3):
        try:
            resp = await client.delete(url, params=params, headers=headers)
            if resp.status_code >= 500:
                raise RuntimeError(f"Server error {resp.status_code}")
            return resp
        except Exception as e:
            last_exc = e
            if attempt >= 2:
                break
            await anyio.sleep(0.25 * (attempt + 1))
    raise RuntimeError(f"REST DELETE failed: {repr(last_exc)}")

async def rest_patch(path: str, params: Dict[str, Any], json: Any, *, prefer: Optional[str] = None) -> httpx.Response:
    url = f"{_BASE}/{path.lstrip('/')}"
    client = await _get_client()
    headers = _headers(write=True)
    if prefer:
        headers["Prefer"] = prefer
    last_exc = None
    for attempt in range(3):
        try:
            resp = await client.patch(url, params=params, headers=headers, json=json)
            if resp.status_code >= 500:
                raise RuntimeError(f"Server error {resp.status_code}")
            return resp
        except Exception as e:
            last_exc = e
            if attempt >= 2:
                break
            await anyio.sleep(0.25 * (attempt + 1))
    raise RuntimeError(f"REST PATCH failed: {repr(last_exc)}")

async def rest_upsert(table: str, payload: Dict[str, Any], *, on_conflict: Optional[str] = None) -> None:
    params: Dict[str, Any] = {}
    if on_conflict:
        params["on_conflict"] = on_conflict
    resp = await rest_post(
        table,
        params=params,
        json=payload,
        prefer="resolution=merge-duplicates,return=minimal",
    )
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"Upsert {table} failed: {resp.status_code} {resp.text}")
