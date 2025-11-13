from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, HttpUrl, EmailStr
from datetime import datetime

class NotificationType(str, Enum):
    email = "email"
    push = "push"


class RichMedia(BaseModel):
    icon: Optional[HttpUrl] = None  
    image: Optional[HttpUrl] = None  
    action_link: Optional[HttpUrl] = None  


class NotificationUserData(BaseModel):
    name: str
    link: HttpUrl
    meta: Optional[Dict[str, Any]] = None


class NotificationPayload(BaseModel):
    notification_type: NotificationType
    user_id: str
    template_code: str  
    variables: NotificationUserData
    request_id: str
    priority: int
    metadata: Optional[Dict[str, Any]] = None

class NotificationEvent(BaseModel):
    pattern: str
    data: NotificationPayload

class UserPreference(BaseModel):
    email: bool
    push: bool
    updated_at: datetime


class UserData(BaseModel):
    id: str
    email: EmailStr
    name: str
    push_token: str
    created_at: datetime
    updated_at: datetime
    preference: UserPreference


class APIResponse(BaseModel):
    success: bool
    data: UserData
    error: Optional[str]
    message: str
    meta: Optional[dict]
