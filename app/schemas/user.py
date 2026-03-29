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

class ProfileBase(BaseModel):
    full_name: str
    phone: Optional[str] = None
    location_id: Optional[UUID] = None
    city: Optional[str] = None

class ProfileCreate(ProfileBase):
    pass

class ProfileResponse(ProfileBase):
    id: Optional[UUID] = None
    user_id: UUID
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

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

class PassportResponse(PassportBase):
    id: Optional[UUID] = None
    athlete_id: UUID
    is_verified: Optional[bool] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class AthleteResponse(BaseModel):
    id: UUID
    user_id: UUID
    coach_name: Optional[str] = None
    club: Optional[str] = None
    passports: Optional[List[PassportResponse]] = None

    class Config:
        from_attributes = True
