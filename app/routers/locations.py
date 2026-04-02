from fastapi import APIRouter, HTTPException
from typing import List, Optional
from uuid import UUID
from app.core.supabase import supabase
from pydantic import BaseModel
import anyio
from app.core.cache import cache

router = APIRouter(prefix="/locations", tags=["locations"])

class Location(BaseModel):
    id: UUID
    name: str
    type: str
    parent_id: Optional[UUID] = None

@router.get("/", response_model=List[Location])
async def get_locations(type: Optional[str] = None, parent_id: Optional[str] = None):
    cache_key = f"locations:{type or ''}:{parent_id or ''}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    query = supabase.table("locations").select("*")
    if type:
        query = query.eq("type", type)
    if parent_id:
        query = query.eq("parent_id", parent_id)
    
    # Сортировка по имени
    response = await anyio.to_thread.run_sync(query.order("name").execute)
    data = response.data
    cache.set(cache_key, data, ttl_seconds=60.0)
    return data
