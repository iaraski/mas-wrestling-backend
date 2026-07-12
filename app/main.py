from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import httpx
import time
import os
import asyncio
from datetime import datetime
from app.core.cache import cache
from app.core.db import init_db
from app.core.local_auth import (
    auth_access_cookie_name,
    auth_csrf_cookie_name,
    auth_csrf_header_name,
    auth_refresh_cookie_name,
)
from app.core.rest import rest_get

APP_DEBUG = os.getenv("APP_DEBUG") == "1"
LEGACY_EXECUTION_ENABLED = os.getenv("LEGACY_EXECUTION_ENABLED") == "1"

# Import routers
from app.routers import competition, application, application_media, application_admin, application_review, brackets, user, user_staff, user_admin, user_profile, user_debug, locations, bouts, auth, live

async def _warm_cache():
    try:
        resp = await rest_get(
            "locations",
            {"select": "*", "type": "eq.country", "order": "name.asc", "limit": "10000"},
            write=False,
        )
        rows = resp.json()
        if isinstance(rows, list):
            cache.set("locations:country:", rows, ttl_seconds=60.0)
    except Exception:
        pass

    try:
        resp = await rest_get(
            "competitions",
            {"select": "*, categories:competition_categories(*), locations(name)", "limit": "10000"},
            write=False,
        )
        rows = resp.json()
        if isinstance(rows, list):
            data = []
            for comp in rows:
                if comp.get("locations"):
                    comp["location_name"] = comp["locations"]["name"]
                data.append(comp)
            cache.set("competitions:list", data, ttl_seconds=15.0)
    except Exception:
        pass

    try:
        resp = await rest_get(
            "competitions",
            {
                "select": "*, categories:competition_categories(*)",
                "end_date": f"gte.{datetime.now().isoformat()}",
                "order": "start_date.asc",
                "limit": "10000",
            },
            write=False,
        )
        rows = resp.json()
        if isinstance(rows, list):
            cache.set("competitions:active", rows, ttl_seconds=15.0)
    except Exception:
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    if APP_DEBUG:
        print("[FastAPI] Server is starting up...")
    await init_db()
    warm_task = asyncio.create_task(_warm_cache())
    yield
    warm_task.cancel()
    if APP_DEBUG:
        print("[FastAPI] Server is shutting down...")

app = FastAPI(title="MAS-WRESTLING ONLINE API", lifespan=lifespan,default_response_class=JSONResponse)

# Centralized error response builder (keeps CORS headers via outer middleware)
def _error_json(status_code: int, request: Request, detail: object, *, exc: Exception | None = None):
    if os.getenv("APP_DEBUG") == "1":
        payload = {
            "detail": detail,
            "path": str(request.url.path),
        }
        if exc is not None:
            payload["error"] = repr(exc)
        return JSONResponse(status_code=status_code, content=payload)
    return JSONResponse(status_code=status_code, content={"detail": detail})

# Setup CORS (must be outermost to keep CORS headers on error responses)
origins = [
    "http://localhost:5173",
    "http://localhost",
    "http://127.0.0.1:5173",
    "http://127.0.0.1",
    "https://mas-wrestling.pro",
    "https://api.mas-wrestling.pro"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request Timing Middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    if APP_DEBUG:
        print(f"INCOMING: {request.method} {request.url.path}")

    access_cookie = request.cookies.get(auth_access_cookie_name())
    refresh_cookie = request.cookies.get(auth_refresh_cookie_name())
    csrf_cookie = request.cookies.get(auth_csrf_cookie_name())
    authorization_header = request.headers.get("authorization")
    has_bearer_authorization = bool(
        authorization_header and authorization_header.lower().startswith("bearer ")
    )

    if request.method.upper() not in {"GET", "HEAD", "OPTIONS"} and request.url.path != "/api/v1/auth/login":
        # CSRF is required for cookie-authenticated requests, but mobile clients
        # use explicit Bearer tokens and should not be blocked by cookie checks.
        if (access_cookie or refresh_cookie) and not has_bearer_authorization:
            csrf_header = request.headers.get(auth_csrf_header_name())
            if not csrf_cookie or not csrf_header or csrf_header != csrf_cookie:
                process_time = time.time() - start_time
                resp = _error_json(403, request, "CSRF validation failed")
                resp.headers["X-Process-Time"] = str(process_time)
                return resp

    if "authorization" not in request.headers:
        if access_cookie:
            headers = list(request.scope.get("headers") or [])
            headers.append((b"authorization", f"Bearer {access_cookie}".encode("latin-1")))
            request.scope["headers"] = headers
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        if APP_DEBUG:
            print(f"STATUS: {response.status_code} ({process_time:.4f}s)")
        return response
    except HTTPException as e:
        process_time = time.time() - start_time
        if APP_DEBUG:
            print(f"HTTPException {e.status_code} on {request.method} {request.url.path}: {e.detail}")
        resp = _error_json(e.status_code, request, e.detail)
        resp.headers["X-Process-Time"] = str(process_time)
        return resp
    except Exception as e:
        process_time = time.time() - start_time
        if APP_DEBUG:
            print(f"ERROR: {repr(e)} ({process_time:.4f}s)")
        resp = _error_json(500, request, "Internal Server Error", exc=e)
        resp.headers["X-Process-Time"] = str(process_time)
        return resp

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    return _error_json(500, request, "Internal Server Error", exc=exc)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if APP_DEBUG:
        print(f"HTTPException {exc.status_code} on {request.method} {request.url.path}: {exc.detail}")
    return _error_json(exc.status_code, request, exc.detail)

# Include routers
try:
    from app.routers import certificates
    app.include_router(certificates.router, prefix="/api/v1")
    app.include_router(competition.router, prefix="/api/v1")
    app.include_router(application.router, prefix="/api/v1")
    app.include_router(application_media.router, prefix="/api/v1")
    app.include_router(application_admin.router, prefix="/api/v1")
    app.include_router(application_review.router, prefix="/api/v1")
    app.include_router(user.router, prefix="/api/v1")
    app.include_router(user_staff.router, prefix="/api/v1")
    app.include_router(user_admin.router, prefix="/api/v1")
    app.include_router(user_profile.router, prefix="/api/v1")
    app.include_router(user_debug.router, prefix="/api/v1")
    app.include_router(locations.router, prefix="/api/v1")
    app.include_router(auth.router, prefix="/api/v1")
    from app.routers import auth_custom
    app.include_router(auth_custom.router, prefix="/api/v1")
    app.include_router(live.router, prefix="/api/v1")
    if LEGACY_EXECUTION_ENABLED:
        app.include_router(brackets.router, prefix="/api/v1")
        app.include_router(bouts.router, prefix="/api/v1")
    if APP_DEBUG:
        print("ROUTERS CONNECTED SUCCESSFULLY")
except Exception as e:
    print(f"Error connecting routers: {e}")

from app.core.telegram import BOT_TOKEN

@app.get("/api/v1/tg-file/{file_id:path}")
async def get_telegram_file(file_id: str):
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not configured")
        
    # Remove leading slash if it was accidentally captured by :path
    if file_id.startswith('/'):
        file_id = file_id[1:]
        
    print(f"Proxying file_id: {file_id}")
        
    async with httpx.AsyncClient() as client:
        # 1. Get file path from Telegram API
        # Need to URL encode the file_id properly or pass it as params to httpx
        resp = await client.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile", params={"file_id": file_id})
        data = resp.json()
        
        print(f"Telegram API response: {data}")
        
        if not data.get("ok"):
            raise HTTPException(status_code=404, detail="File not found in Telegram")
            
        file_path = data["result"]["file_path"]
        
        # 2. Proxy the file content
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        file_resp = await client.get(file_url)
        
        # Determine content type based on extension
        content_type = "application/octet-stream"
        if file_path.lower().endswith((".jpg", ".jpeg")):
            content_type = "image/jpeg"
        elif file_path.lower().endswith(".png"):
            content_type = "image/png"
        elif file_path.lower().endswith(".pdf"):
            content_type = "application/pdf"
            
        # Add explicit headers to force inline display instead of download
        headers = {
            "Content-Type": content_type,
            "Content-Disposition": "inline"
        }
            
        return Response(content=file_resp.content, media_type=content_type, headers=headers)

@app.get("/")
async def root():
    return {"status": "ok", "message": "CompEaseBot API is running"}

@app.get("/api/v1/debug/env")
async def debug_env():
    if os.getenv("APP_DEBUG") != "1":
        raise HTTPException(status_code=404, detail="Not Found")
    return {
        "has_database_url": bool(os.getenv("DATABASE_URL")),
        "database_url_prefix": (os.getenv("DATABASE_URL") or "")[:32],
        "has_minio_endpoint": bool(os.getenv("MINIO_ENDPOINT")),
        "has_minio_bucket": bool(os.getenv("MINIO_BUCKET")),
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
