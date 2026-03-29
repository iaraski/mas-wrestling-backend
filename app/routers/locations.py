from fastapi import APIRouter, HTTPException
from typing import List, Optional
from uuid import UUID
from app.core.supabase import supabase
from pydantic import BaseModel

router = APIRouter(prefix="/locations", tags=["locations"])

class Location(BaseModel):
    id: UUID
    name: str
    type: str
    parent_id: Optional[UUID] = None

@router.get("/", response_model=List[Location])
async def get_locations(type: Optional[str] = None, parent_id: Optional[str] = None):
    query = supabase.table("locations").select("*")
    if type:
        query = query.eq("type", type)
    if parent_id:
        query = query.eq("parent_id", parent_id)
    
    # Сортировка по имени
    response = query.order("name").execute()
    return response.data
