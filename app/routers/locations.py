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

class LocationPath(BaseModel):
    country_id: Optional[UUID] = None
    district_id: Optional[UUID] = None
    region_id: Optional[UUID] = None

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


@router.get("/path", response_model=LocationPath)
async def get_location_path(location_id: str):
    cache_key = f"locations:path:{location_id}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    def _fetch(loc_id: str):
        q = supabase.table("locations").select("id,type,parent_id").eq("id", loc_id).maybe_single()
        return q.execute()

    try:
        res = await anyio.to_thread.run_sync(_fetch, location_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to load location")

    loc = res.data or {}
    if not loc:
        raise HTTPException(status_code=404, detail="Location not found")

    region_id: Optional[str] = None
    district_id: Optional[str] = None
    country_id: Optional[str] = None

    loc_type = str(loc.get("type") or "")
    if loc_type == "region":
        region_id = str(loc.get("id"))
        parent_id = loc.get("parent_id")
        if parent_id:
            district_id = str(parent_id)
            try:
                dres = await anyio.to_thread.run_sync(_fetch, district_id)
                dloc = dres.data or {}
                parent2 = dloc.get("parent_id")
                if parent2:
                    country_id = str(parent2)
            except Exception:
                pass
    elif loc_type == "district":
        district_id = str(loc.get("id"))
        parent_id = loc.get("parent_id")
        if parent_id:
            country_id = str(parent_id)
    elif loc_type == "country":
        country_id = str(loc.get("id"))

    result = {
        "country_id": country_id,
        "district_id": district_id,
        "region_id": region_id,
    }
    cache.set(cache_key, result, ttl_seconds=300.0)
    return result
