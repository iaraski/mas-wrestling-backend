import os
from pathlib import Path
from supabase import create_client, Client, ClientOptions
import httpx
from dotenv import load_dotenv

# Явно указываем путь к .env файлу в папке backend
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Отладочный вывод (без самого ключа для безопасности)
if SUPABASE_URL:
    print(f"[Supabase] URL loaded: {SUPABASE_URL[:20]}...")
if SUPABASE_KEY:
    print(f"[Supabase] Key loaded, length: {len(SUPABASE_KEY)}")
if SUPABASE_SERVICE_ROLE_KEY:
    print(f"[Supabase] Service key loaded, length: {len(SUPABASE_SERVICE_ROLE_KEY)}")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError(f"SUPABASE_URL and SUPABASE_KEY must be set in .env. Checked path: {env_path}")

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
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=opts)
except Exception as e:
    print(f"[Supabase] Error creating client: {e}")
    raise e

admin_supabase: Client | None = None
if SUPABASE_SERVICE_ROLE_KEY:
    try:
        admin_supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, options=opts)
    except Exception as e:
        print(f"[Supabase] Error creating admin client: {e}")
        admin_supabase = None
