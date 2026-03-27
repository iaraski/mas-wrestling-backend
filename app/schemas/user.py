from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID
from datetime import date, datetime

class Role(BaseModel):
    id: UUID
    code: str

    class Config:
        from_attributes = True

class UserProfile(BaseModel):
    user_id: UUID
    full_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    roles: List[str] = [] # Role codes
    location_id: Optional[UUID] = None
    location_name: Optional[str] = None
    city: Optional[str] = None

    class Config:
        from_attributes = True

class RoleAssign(BaseModel):
    role_codes: List[str]
    location_id: Optional[UUID] = None

class AdminCreate(BaseModel):
    email: str
    password: str
    full_name: str
    phone: Optional[str] = None
    role_codes: List[str]
    location_id: Optional[UUID] = None

class PassportBase(BaseModel):
    series: str
    number: str
    issued_by: str
    issue_date: date
    birth_date: date
    gender: str
    rank: Optional[str] = None
    photo_url: Optional[str] = None
    passport_scan_url: Optional[str] = None
