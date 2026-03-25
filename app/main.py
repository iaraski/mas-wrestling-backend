from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import time
from fastapi.responses import JSONResponse
import httpx

# Import routers
from app.routers import competition, application, brackets, user, locations, bouts

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 [FastAPI] Server is starting up...")
    yield
    # Shutdown
    print("🛑 [FastAPI] Server is shutting down...")

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
    print(f"🚀 ИНКОМИНГ: {request.method} {request.url.path}")
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        print(f"✅ СТАТУС: {response.status_code} ({process_time:.4f}s)")
        return response
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        raise e

# Include routers
try:
    app.include_router(competition.router)
    app.include_router(application.router)
    app.include_router(brackets.router)
    app.include_router(user.router)
    app.include_router(locations.router)
    app.include_router(bouts.router)
    print("📦 РОУТЕРЫ ПОДКЛЮЧЕНЫ УСПЕШНО")
except Exception as e:
    print(f"❌ Ошибка подключения роутеров: {e}")

@app.get("/")
async def root():
    return {"status": "ok", "message": "CompEaseBot API is running"}
