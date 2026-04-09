"""
PriceShield v2 — models.py
Pydantic request/response models for all API endpoints.
No database dependency — works standalone on Render free tier.
"""
from __future__ import annotations
from typing import Optional, List
from pydantic import BaseModel, validator


# ── Request Models ──────────────────────────────────────────

class OTPSendRequest(BaseModel):
    phone: str

    @validator("phone")
    def validate_phone(cls, v):
        v = v.strip().replace(" ", "").replace("-", "")
        if v.startswith("+91"):
            v = v[3:]
        if not v.isdigit() or len(v) != 10:
            raise ValueError("Must be a valid 10-digit Indian mobile number")
        return v


class OTPVerifyRequest(BaseModel):
    phone: str
    otp: str

    @validator("otp")
    def validate_otp(cls, v):
        if not v.isdigit() or len(v) != 6:
            raise ValueError("OTP must be exactly 6 digits")
        return v


class SearchRequest(BaseModel):
    query: str
    mode: str = "new"
    cat_id: Optional[int] = None
    limit: int = 20

    @validator("query")
    def validate_query(cls, v):
        if len(v.strip()) < 2:
            raise ValueError("Query must be at least 2 characters")
        return v.strip()


class AffiliateLinkRequest(BaseModel):
    url: str
    store: str


class SocialScanRequest(BaseModel):
    url: str

    @validator("url")
    def validate_social_url(cls, v):
        social = ["instagram.com", "youtube.com", "youtu.be", "tiktok.com"]
        if not any(d in v.lower() for d in social):
            raise ValueError("Must be an Instagram, YouTube, or TikTok URL")
        return v


class IngredientsCheckRequest(BaseModel):
    ingredients: List[str]
    product_name: Optional[str] = None


class UserOnboardingRequest(BaseModel):
    user_id: str
    selected_cat_ids: List[int]

    @validator("selected_cat_ids")
    def validate_cats(cls, v):
        if len(v) < 1:
            raise ValueError("Must select at least one category")
        return v


# ── Response Models ─────────────────────────────────────────

class AuthResponse(BaseModel):
    success: bool
    token: Optional[str] = None
    user_id: Optional[str] = None
    is_new_user: bool = False
    message: str = ""


class AffiliateLinkResponse(BaseModel):
    original_url: str
    affiliate_url: str
    store: str
    tag: str
    param: str
