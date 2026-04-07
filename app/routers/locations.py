from fastapi import APIRouter, HTTPException
from typing import List, Optional
from uuid import UUID
from app.core.rest import rest_get
from pydantic import BaseModel
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

    params = {"select": "id,name,type,parent_id", "order": "name.asc"}
    if type:
        params["type"] = f"eq.{type}"
    if parent_id:
        params["parent_id"] = f"eq.{parent_id}"
    try:
        resp = await rest_get("locations", params, write=False)
        data = resp.json()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Supabase unavailable: {repr(e)}")
    cache.set(cache_key, data, ttl_seconds=60.0)
    return data


@router.get("/path", response_model=LocationPath)
async def get_location_path(location_id: str):
    cache_key = f"locations:path:v2:{location_id}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    async def _fetch_loc(loc_id: str) -> dict:
        resp = await rest_get(
            "locations",
            {"select": "id,type,parent_id", "id": f"eq.{loc_id}", "limit": "1"},
            write=False,
        )
        j = resp.json()
        return j[0] if isinstance(j, list) and j else {}

    try:
        loc = await _fetch_loc(location_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to load location")
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
                dloc = await _fetch_loc(district_id)
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
