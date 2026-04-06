from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import httpx
import time
import os
import asyncio
from datetime import datetime
from app.core.supabase import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_ROLE_KEY, admin_supabase
from app.core.supabase import supabase
from app.core.cache import cache

APP_DEBUG = os.getenv("APP_DEBUG") == "1"

# Import routers
from app.routers import competition, application, brackets, user, locations, bouts, auth, live

async def _warm_cache():
    try:
        q = supabase.table("locations").select("*").eq("type", "country").order("name")
        res = await asyncio.to_thread(q.execute)
        if res and hasattr(res, "data"):
            cache.set("locations:country:", res.data, ttl_seconds=60.0)
    except Exception:
        pass

    try:
        q = supabase.table("competitions").select("*, categories:competition_categories(*), locations(name)")
        res = await asyncio.to_thread(q.execute)
        if res and hasattr(res, "data"):
            data = []
            for comp in res.data:
                if comp.get("locations"):
                    comp["location_name"] = comp["locations"]["name"]
                data.append(comp)
            cache.set("competitions:list", data, ttl_seconds=15.0)
    except Exception:
        pass

    try:
        q = supabase.table("competitions").select("*, categories:competition_categories(*)").gte("end_date", datetime.now().isoformat()).order("start_date", desc=False)
        res = await asyncio.to_thread(q.execute)
        if res and hasattr(res, "data"):
            cache.set("competitions:active", res.data, ttl_seconds=15.0)
    except Exception:
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    if APP_DEBUG:
        print("[FastAPI] Server is starting up...")
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
    app.include_router(competition.router, prefix="/api/v1")
    app.include_router(application.router, prefix="/api/v1")
    app.include_router(brackets.router, prefix="/api/v1")
    app.include_router(user.router, prefix="/api/v1")
    app.include_router(locations.router, prefix="/api/v1")
    app.include_router(bouts.router, prefix="/api/v1")
    app.include_router(auth.router, prefix="/api/v1")
    from app.routers import auth_custom
    app.include_router(auth_custom.router, prefix="/api/v1")
    app.include_router(live.router, prefix="/api/v1")
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
        "supabase_url": SUPABASE_URL,
        "has_supabase_key": bool(SUPABASE_KEY),
        "supabase_key_len": len(SUPABASE_KEY) if SUPABASE_KEY else 0,
        "has_service_role_key": bool(SUPABASE_SERVICE_ROLE_KEY),
        "service_role_key_len": len(SUPABASE_SERVICE_ROLE_KEY) if SUPABASE_SERVICE_ROLE_KEY else 0,
        "admin_client_ready": bool(admin_supabase),
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
