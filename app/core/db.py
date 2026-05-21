import os
import asyncio
import sqlalchemy.pool
from pathlib import Path
from typing import AsyncIterator

from dotenv import load_dotenv
from sqlalchemy import MetaData, Table
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

env_path = Path(__file__).parent.parent.parent / ".env"
root_env_path = env_path.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv(dotenv_path=root_env_path, override=False)

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
DATABASE_SSL = (os.getenv("DATABASE_SSL") or "").strip().lower()
APP_DEBUG = os.getenv("APP_DEBUG") == "1"

if not DATABASE_URL:
    raise ValueError("DATABASE_URL must be set in .env")

connect_args: dict = {}
if DATABASE_SSL in {"disable", "disabled", "false", "0", "no"}:
    connect_args["ssl"] = False

from sqlalchemy.pool import NullPool

engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    poolclass=NullPool,
    connect_args=connect_args or None,
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

metadata = MetaData(schema="public")
tables: dict[str, Table] = {}


async def init_db() -> None:
    retries = int(os.getenv("DB_INIT_RETRIES") or "5")
    last_exc: Exception | None = None
    for attempt in range(max(0, retries) + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(lambda sync_conn: metadata.reflect(sync_conn, schema="public"))
            tables.clear()
            for _, t in metadata.tables.items():
                tables[t.name] = t
            return
        except Exception as e:
            last_exc = e
            if attempt >= retries:
                raise
            if APP_DEBUG:
                print(f"[DB] init_db failed (attempt={attempt + 1}/{retries + 1}): {type(e).__name__}: {e}")
            await asyncio.sleep(0.6 * (attempt + 1))
    if last_exc is not None:
        raise last_exc


async def get_db() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session
