from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from uuid import UUID

# 1. Схемы для Категорий (возрастные и весовые)
class CategoryBase(BaseModel):
    gender: str
    age_min: int
    age_max: int
    weight_min: float
    weight_max: Optional[float] = None
    competition_day: Optional[datetime] = None
    mandate_day: Optional[datetime] = None

class CategoryCreate(CategoryBase):
    pass

class Category(CategoryBase):
    id: UUID
    competition_id: UUID

# 4. Схемы для Сеток (Brackets)
class Match(BaseModel):
    id: Optional[UUID] = None
    round_number: int
    match_number: int
    athlete1_id: Optional[UUID] = None
    athlete2_id: Optional[UUID] = None
    winner_id: Optional[UUID] = None
    athlete1_name: Optional[str] = None
    athlete2_name: Optional[str] = None
    
class Bracket(BaseModel):
    category_id: UUID
    type: str # round_robin, single_elimination, double_elimination
    matches: List[Match]

    class Config:
        from_attributes = True

# 2. Схемы для Соревнований
class CompetitionBase(BaseModel):
    name: str
    description: Optional[str] = None
    scale: str # world, country, region
    type: str # open, restricted
    location_id: Optional[UUID] = None # Это ID региона или страны
    preview_url: Optional[str] = None
    city: Optional[str] = None
    street: Optional[str] = None
    house: Optional[str] = None
    mandate_start_date: datetime
    mandate_end_date: datetime
    start_date: datetime
    end_date: datetime
    mats_count: int = 1 # Количество помостов

class CompetitionCreate(CompetitionBase):
    categories: List[CategoryBase]
    secretaries: Optional[List[UUID]] = []

class CompetitionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    scale: Optional[str] = None
    type: Optional[str] = None
    location_id: Optional[UUID] = None
    preview_url: Optional[str] = None
    city: Optional[str] = None
    street: Optional[str] = None
    house: Optional[str] = None
    mandate_start_date: Optional[datetime] = None
    mandate_end_date: Optional[datetime] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    mats_count: Optional[int] = None
    # For simplicity in this iteration, categories and secretaries updates might require full replacement or separate endpoints.
    # We will include them for full replacement.
    categories: Optional[List[CategoryBase]] = None
    secretaries: Optional[List[UUID]] = None

class Competition(CompetitionBase):
    id: UUID
    created_by: Optional[UUID] = None
    created_at: datetime
    categories: List[Category] = []
    location_name: Optional[str] = None

    class Config:
        from_attributes = True

# 3. Схемы для Заявок (Applications)
class ApplicationBase(BaseModel):
    competition_id: UUID
    athlete_id: UUID
    category_id: Optional[UUID] = None
    declared_weight: Optional[float] = None
    draw_number: Optional[int] = None # Номер жеребьевки

class ApplicationCreate(ApplicationBase):
    pass

class ApplicationUpdate(BaseModel):
    status: Optional[str] = None # pending, approved, rejected
    comment: Optional[str] = None
    category_id: Optional[UUID] = None
    actual_weight: Optional[float] = None

class Application(ApplicationBase):
    id: UUID
    status: str
    comment: Optional[str] = None
    actual_weight: Optional[float] = None
    created_at: datetime
    updated_at: datetime
    athlete_name: Optional[str] = None
    category_description: Optional[str] = None

    class Config:
        from_attributes = True
