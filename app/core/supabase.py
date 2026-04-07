import os
from pathlib import Path
from supabase import create_client, Client, ClientOptions
import httpx
from dotenv import load_dotenv
from typing import Any, Dict, Optional, List

env_path = Path(__file__).parent.parent.parent / ".env"
root_env_path = env_path.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv(dotenv_path=root_env_path, override=False)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
APP_DEBUG = os.getenv("APP_DEBUG") == "1"

if APP_DEBUG:
    if SUPABASE_URL:
        print(f"[Supabase] URL loaded: {SUPABASE_URL[:20]}...")
    if SUPABASE_KEY:
        print(f"[Supabase] Key loaded, length: {len(SUPABASE_KEY)}")
    if SUPABASE_SERVICE_ROLE_KEY:
        print(f"[Supabase] Service key loaded, length: {len(SUPABASE_SERVICE_ROLE_KEY)}")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError(f"SUPABASE_URL and SUPABASE_KEY must be set in .env. Checked path: {env_path} (and {root_env_path})")

SUPABASE_URL = SUPABASE_URL.strip().rstrip("/")
if SUPABASE_URL.endswith("/rest/v1"):
    SUPABASE_URL = SUPABASE_URL[: -len("/rest/v1")]
if SUPABASE_URL.endswith("/auth/v1"):
    SUPABASE_URL = SUPABASE_URL[: -len("/auth/v1")]

if "/api/v1" in SUPABASE_URL:
    raise ValueError("SUPABASE_URL must be the Supabase project URL (https://<ref>.supabase.co), not the backend API URL")

try:
    # Настраиваем кастомный HTTP-клиент для обхода ошибки SSL: UNEXPECTED_EOF_WHILE_READING
    # Отключаем http2 и жестко ограничиваем время жизни keepalive-соединений
    custom_httpx_client = httpx.Client(
        http2=False,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=10.0),
        transport=httpx.HTTPTransport(retries=3),
        timeout=30.0
    )
    
    opts = ClientOptions(
        httpx_client=custom_httpx_client,
        postgrest_client_timeout=30
    )
    _sdk_supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=opts)
except Exception as e:
    print(f"[Supabase] Error creating client: {e}")
    raise e

_sdk_admin_supabase: Client | None = None
if SUPABASE_SERVICE_ROLE_KEY:
    try:
        _sdk_admin_supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, options=opts)
    except Exception as e:
        print(f"[Supabase] Error creating admin client: {e}")
        _sdk_admin_supabase = None


class _PGResponse:
    def __init__(self, data: Any):
        self.data = data


def _pg_in(values: List[Any]) -> str:
    parts: List[str] = []
    for v in values:
        if v is None:
            continue
        parts.append(str(v))
    return f"in.({','.join(parts)})"


class _PGClient:
    def __init__(self, base_url: str, token: str, http: httpx.Client):
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._http = http

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "apikey": self._token,
            "Content-Type": "application/json",
        }

    def request(self, method: str, table: str, *, params: Dict[str, str], json: Any, prefer: List[str]) -> _PGResponse:
        url = f"{self._base_url}/{table.lstrip('/')}"
        headers = self._headers()
        if prefer:
            headers["Prefer"] = ",".join(prefer)
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                resp = self._http.request(method, url, params=params, headers=headers, json=json)
                if resp.status_code >= 500:
                    raise RuntimeError(f"Server error {resp.status_code}")
                if resp.status_code == 204 or not resp.content:
                    return _PGResponse(None)
                return _PGResponse(resp.json())
            except Exception as e:
                last_exc = e
                if attempt >= 2:
                    break
                import time as _t

                _t.sleep(0.2 * (attempt + 1))
        raise RuntimeError(f"PostgREST request failed: {repr(last_exc)}")


class _PGQuery:
    def __init__(self, client: _PGClient, table: str):
        self._client = client
        self._table = table
        self._method: str = "GET"
        self._select: Optional[str] = None
        self._params: Dict[str, str] = {}
        self._json: Any = None
        self._prefer: List[str] = []
        self._single: bool = False
        self._maybe_single: bool = False
        self._order: List[str] = []

    def select(self, cols: str):
        self._method = "GET"
        self._select = cols
        return self

    def eq(self, col: str, val: Any):
        self._params[col] = f"eq.{val}"
        return self

    def neq(self, col: str, val: Any):
        self._params[col] = f"neq.{val}"
        return self

    def gte(self, col: str, val: Any):
        self._params[col] = f"gte.{val}"
        return self

    def lte(self, col: str, val: Any):
        self._params[col] = f"lte.{val}"
        return self

    def ilike(self, col: str, pattern: str):
        self._params[col] = f"ilike.{pattern}"
        return self

    def in_(self, col: str, values: List[Any]):
        self._params[col] = _pg_in(values)
        return self

    def or_(self, expr: str):
        self._params["or"] = expr
        return self

    def order(self, col: str, desc: bool = False):
        self._order.append(f"{col}.{'desc' if desc else 'asc'}")
        return self

    def limit(self, n: int):
        self._params["limit"] = str(int(n))
        return self

    def single(self):
        self._single = True
        self._maybe_single = False
        return self

    def maybe_single(self):
        self._maybe_single = True
        self._single = False
        return self

    def insert(self, payload: Any):
        self._method = "POST"
        self._json = payload
        self._prefer = ["return=representation"]
        return self

    def update(self, payload: Any):
        self._method = "PATCH"
        self._json = payload
        self._prefer = ["return=representation"]
        return self

    def delete(self):
        self._method = "DELETE"
        self._prefer = ["return=representation"]
        return self

    def upsert(self, payload: Any, on_conflict: str | None = None):
        self._method = "POST"
        self._json = payload
        self._prefer = ["resolution=merge-duplicates", "return=representation"]
        if on_conflict:
            self._params["on_conflict"] = on_conflict
        return self

    def execute(self) -> _PGResponse:
        params = dict(self._params)
        if self._select is not None:
            params["select"] = self._select
        if self._order:
            params["order"] = ",".join(self._order)
        resp = self._client.request(self._method, self._table, params=params, json=self._json, prefer=self._prefer)
        data = resp.data
        if self._single or self._maybe_single:
            if data is None:
                return _PGResponse(None)
            if isinstance(data, list):
                if not data:
                    return _PGResponse(None)
                return _PGResponse(data[0])
            if isinstance(data, dict):
                return _PGResponse(data)
            return _PGResponse(None)
        if data is None:
            return _PGResponse([])
        return _PGResponse(data)


class _SupabaseCompat:
    def __init__(self, sdk: Client, token: str, base_url: str, http: httpx.Client):
        self._sdk = sdk
        self._pg = _PGClient(f"{base_url}/rest/v1", token, http)

    def table(self, name: str) -> _PGQuery:
        return _PGQuery(self._pg, name)

    def from_(self, name: str) -> _PGQuery:
        return self.table(name)

    def __getattr__(self, item: str):
        return getattr(self._sdk, item)


supabase: Client = _SupabaseCompat(_sdk_supabase, SUPABASE_KEY, SUPABASE_URL, custom_httpx_client)  # type: ignore
admin_supabase: Client | None = None
if _sdk_admin_supabase and SUPABASE_SERVICE_ROLE_KEY:
    admin_supabase = _SupabaseCompat(_sdk_admin_supabase, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL, custom_httpx_client)  # type: ignore
