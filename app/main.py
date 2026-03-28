from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import httpx
import time

# Import routers
from app.routers import competition, application, brackets, user, locations, bouts

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("[FastAPI] Server is starting up...")
    yield
    # Shutdown
    print("[FastAPI] Server is shutting down...")

app = FastAPI(title="CompEaseBot API", lifespan=lifespan,default_response_class=JSONResponse)

# Setup CORS
origins = [
    "http://localhost:5173",  # Local React dev server
    "http://localhost",
    "http://localhost:80",
    "http://127.0.0.1:5173",
    "http://127.0.0.1",
    "https://mas-wrestling.pro",
    "http://mas-wrestling.pro"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request Timing Middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    print(f"INCOMING: {request.method} {request.url.path}")
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        print(f"STATUS: {response.status_code} ({process_time:.4f}s)")
        return response
    except Exception as e:
        print(f"ERROR: {e}")
        raise e

# Include routers
try:
    app.include_router(competition.router)
    app.include_router(application.router)
    app.include_router(brackets.router)
    app.include_router(user.router)
    app.include_router(locations.router)
    app.include_router(bouts.router)
    print("ROUTERS CONNECTED SUCCESSFULLY")
except Exception as e:
    print(f"Error connecting routers: {e}")

from app.core.telegram import BOT_TOKEN

@app.get("/api/v1/tg-file/{file_id:path}")
async def get_telegram_file(file_id: str):
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not configured")
        
    async with httpx.AsyncClient() as client:
        # 1. Get file path from Telegram API
        resp = await client.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id}")
        data = resp.json()
        
        if not data.get("ok"):
            raise HTTPException(status_code=404, detail="File not found in Telegram")
            
        file_path = data["result"]["file_path"]
        
        # 2. Proxy the file content
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        file_resp = await client.get(file_url)
        
        return Response(content=file_resp.content, media_type=file_resp.headers.get("content-type"))

@app.get("/")
async def root():
    return {"status": "ok", "message": "CompEaseBot API is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
