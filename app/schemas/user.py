from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID

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
