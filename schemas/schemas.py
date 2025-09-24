# /schemas/schemas.py
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime


class UserBase(BaseModel):
    username: str
    email: EmailStr


class UserCreate(UserBase):
    password: str
    is_superadmin: bool = False


class UserUpdate(BaseModel):
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    is_active: Optional[bool] = None
    is_superadmin: Optional[bool] = None


class PasswordChange(BaseModel):
    new_password: str


class UserResponse(UserBase):
    id: int
    is_active: bool
    is_superadmin: bool
    created_at: datetime
    last_login: Optional[datetime]

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str


class LoginRequest(BaseModel):
    username: str
    password: str