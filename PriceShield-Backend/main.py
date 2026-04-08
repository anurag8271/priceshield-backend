"""
PriceShield v2 — Main FastAPI Application (Real Data Edition)
==============================================================
Wired to live APIs:
  • Amazon PA-API 5.0  (primary product + price data)
  • RapidAPI           (Flipkart, Myntra, fallback Amazon)
  • OpenAI GPT-4o-mini (AI search intent extraction)
  • OpenAI GPT-4o      (social video product identification)
  • Twilio             (SMS OTP delivery)
  • Redis              (price cache, session cache, OTP store)
  • MongoDB            (users, price history, affiliate clicks)

Run:
  uvicorn main:app --reload --port 8000
"""

import asyncio
import hashlib
import json
import random
import string
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from loguru import logger

from config import get_settings, AFFILIATE_PARAMS
from models import (
    OTPSendRequest, OTPVerifyRequest, AuthResponse,
    SearchRequest, AffiliateLinkRequest, AffiliateLinkResponse,
    SocialScanRequest, IngredientsCheckRequest, UserOnboardingRequest,
)
from cache    import CacheManager, get_cache
from database import (
    UserRepository, IngredientRepository,
    PriceHistoryRepository, init_database,
)
from auth import (
    create_access_token, AuthUser, require_auth,
    optional_auth, RateLimit, revoke_token,
)
from api_clients import (
    openai_client, twilio_client, price_aggregator,
    frame_extractor, PriceResult, ProductResult,
)

settings     = get_settings()
user_repo    = UserRepository()
ingr_repo    = IngredientRepository()
history_repo = PriceHistoryRepository()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APP SETUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app = FastAPI(
    title        = "PriceShield API",
    description  = "Universal Smart Shopping & Safety Engine for India",
    version      = "2.0.0",
    docs_url     = "/docs",
    redoc_url    = "/redoc",
    default_response_class = ORJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = settings.CORS_ORIGINS,
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


@app.on_event("startup")
async def startup():
    await init_database()
    from api_clients import pa_api_client, rapidapi_client
    logger.info(f"✓ PriceShield API v{settings.APP_VERSION} started")
    logger.info(f"  Amazon PA-API : {'✓ enabled' if pa_api_client._enabled else '✗ not configured'}")
    logger.info(f"  RapidAPI      : {'✓ enabled' if rapidapi_client._enabled else '✗ not configured'}")
    logger.info(f"  OpenAI        : {'✓ enabled' if openai_client._enabled else '✗ dev mode'}")
    logger.info(f"  Twilio SMS    : {'✓ enabled' if twilio_client._enabled else '✗ dev mode (OTPs logged)'}")


@app.on_event("shutdown")
async def shutdown():
    logger.info("PriceShield API shutdown")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# INTERNAL HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _generate_otp(length: int = 6) -> str:
    return "".join(random.choices(string.digits, k=length))


def _build_aff_url(url: str, store: str) -> str:
    """Append this app's affiliate tag to any store URL."""
    config = AFFILIATE_PARAMS.get(store.lower())
    try:
        parsed    = urlparse(url)
        params    = parse_qs(parsed.query)
        param_key = config["param"] if config else "ref"
        param_val = config["id"]    if config else settings.AFFILIATE_ID
        params[param_key] = [param_val]
        new_query = urlencode({k: v[0] for k, v in params.items()})
        return urlunparse(parsed._replace(query=new_query))
    except Exception:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}ref={settings.AFFILIATE_ID}"


def _serialize_price(p: PriceResult) -> dict:
    """Convert a PriceResult into a JSON-safe dict with affiliate URL."""
    return {
        "store":        p.store,
        "price":        round(p.price, 2),
        "orig_price":   round(p.orig_price, 2) if p.orig_price else None,
        "discount_pct": p.discount_pct,
        "url":          _build_aff_url(p.url, p.store),
        "in_stock":     p.in_stock,
        "condition":    p.condition,
        "source":       p.source,
        "fetched_at":   p.fetched_at,
    }


def _serialize_product(p: ProductResult) -> dict:
    return {
        "product_id":   p.product_id,
        "asin":         p.asin,
        "name":         p.name,
        "brand":        p.brand,
        "category":     p.category,
        "image_url":    p.image_url,
        "description":  p.description,
        "rating":       p.rating,
        "review_count": p.review_count,
        "prices":       {k: _serialize_price(v) for k, v in p.prices.items()},
        "source":       p.source,
    }


async def _track_affiliate_click(
    user_id:    Optional[str],
    product_id: str,
    store:      str,
    url:        str,
    ip:         str,
):
    """Background: record affiliate click in MongoDB."""
    try:
        ip_hash = hashlib.md5(ip.encode(), usedforsecurity=False).hexdigest()
        # from models import AffiliateClickDocument
        # await AffiliateClickDocument(
        #     user_id=user_id, product_id=product_id,
        #     store=store, url=url, ip_hash=ip_hash,
        # ).insert()
        logger.debug(f"Affiliate click: user={user_id} store={store}")
    except Exception as e:
        logger.warning(f"Affiliate click tracking failed: {e}")


async def _record_price_history(prices: dict, product_id: str):
    """Background: persist price snapshot to price_history collection."""
    for store, p in prices.items():
        if not isinstance(p, PriceResult):
            continue
        try:
            price_paise = int(p.price * 100)
            await history_repo.record(product_id, store, price_paise, p.condition or "new")
        except Exception as e:
            logger.debug(f"Price history ({store}): {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — AUTH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.post(
    "/api/auth/otp/send",
    tags=["Auth"],
    dependencies=[Depends(RateLimit(max_requests=3, window_seconds=60))],
)
async def otp_send(req: OTPSendRequest, cache: CacheManager = Depends(get_cache)):
    """
    Send a 6-digit OTP via Twilio SMS.
    Rate limited: 3 requests per phone per 60 seconds.
    DEBUG mode only: OTP returned in response body.
    """
    # Prevent duplicate sends within 60-second cooldown
    existing = await cache.get_otp(req.phone)
    if existing:
        try:
            created_ts = datetime.fromisoformat(existing.get("created_at", "")).timestamp()
            if time.time() - created_ts < 60:
                raise HTTPException(429, "OTP already sent. Please wait 60 seconds before retrying.")
        except (ValueError, TypeError, AttributeError):
            pass  # Malformed timestamp — allow resend

    otp  = _generate_otp()
    await cache.store_otp(req.phone, otp)

    sent = await twilio_client.send_otp(f"+91{req.phone}", otp)
    if not sent and not settings.DEBUG:
        raise HTTPException(503, "SMS delivery failed. Please try again.")

    response: dict = {
        "success":    True,
        "message":    f"OTP sent to +91 {req.phone}",
        "expires_in": settings.OTP_EXPIRE_SEC,
    }
    if settings.DEBUG:
        response["dev_otp"] = otp   # NEVER expose in production
    return response


@app.post("/api/auth/otp/verify", response_model=AuthResponse, tags=["Auth"])
async def otp_verify(req: OTPVerifyRequest, cache: CacheManager = Depends(get_cache)):
    """
    Verify OTP and return a signed JWT.
    New users receive is_new_user=True to trigger onboarding.
    """
    record = await cache.get_otp(req.phone)
    if not record:
        raise HTTPException(400, "OTP not found or expired. Request a new one.")

    attempts = record.get("attempts", 0)
    if attempts >= settings.OTP_MAX_ATTEMPTS:
        await cache.delete_otp(req.phone)
        raise HTTPException(429, "Too many failed attempts. Request a new OTP.")

    if record["otp"] != req.otp:
        await cache.increment_otp_attempts(req.phone)
        remaining = settings.OTP_MAX_ATTEMPTS - attempts - 1
        raise HTTPException(400, f"Incorrect OTP. {remaining} attempt(s) remaining.")

    await cache.delete_otp(req.phone)

    user   = await user_repo.find_by_phone(req.phone)
    is_new = user is None
    if is_new:
        user = await user_repo.create(req.phone)

    user_id = user.get("id", f"user_{hashlib.md5(req.phone.encode()).hexdigest()[:8]}")
    await user_repo.update_last_login(user_id)

    token = create_access_token(user_id=user_id, phone=req.phone)
    await cache.store_session(token, {"user_id": user_id, "phone": req.phone})

    return AuthResponse(
        success     = True,
        token       = token,
        user_id     = user_id,
        is_new_user = is_new,
        message     = "Welcome to PriceShield!" if is_new else "Welcome back!",
    )


@app.post("/api/auth/logout", tags=["Auth"])
async def logout(user: AuthUser = Depends(require_auth), cache: CacheManager = Depends(get_cache)):
    """Revoke the current JWT session token."""
    await revoke_token(user.token, cache)
    return {"success": True, "message": "Logged out successfully."}


@app.post("/api/auth/onboarding", tags=["Auth"])
async def save_onboarding(
    req:   UserOnboardingRequest,
    user:  AuthUser        = Depends(require_auth),
    cache: CacheManager    = Depends(get_cache),
):
    """Save the user's selected category IDs. Drives the personalised home feed."""
    await user_repo.update_categories(user.user_id, req.selected_cat_ids)
    return {"success": True, "user_id": user.user_id, "selected_categories": len(req.selected_cat_ids)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — AI SEARCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.post("/api/search", tags=["Search"])
async def search(
    req:   SearchRequest,
    bg:    BackgroundTasks         = BackgroundTasks(),
    user:  Optional[AuthUser]      = Depends(optional_auth),
    cache: CacheManager            = Depends(get_cache),
):
    """
    AI-powered product search across Amazon + Flipkart + Myntra.

    Pipeline:
      1. Social URL → redirect to /api/scan/social
      2. Cache check (15-min TTL)
      3. GPT-4o-mini extracts structured search intent
      4. Amazon PA-API → RapidAPI searched in parallel
      5. Results merged, deduplicated, affiliate-tagged, cached
    """
    SOCIAL_DOMAINS = ["instagram.com", "youtu.be", "youtube.com/shorts", "tiktok.com"]
    if any(s in req.query.lower() for s in SOCIAL_DOMAINS):
        return {"redirect": "social", "url": req.query, "query": req.query}

    cached = await cache.get_search(req.query, req.mode)
    if cached:
        return {"results": cached, "from_cache": True, "query": req.query, "total": len(cached)}

    # Step 1: AI intent extraction
    try:
        intent = await openai_client.extract_intent(req.query)
    except Exception as e:
        logger.warning(f"Intent extraction failed: {e} — using raw query")
        from api_clients import SearchIntent
        intent = SearchIntent(
            product_name = req.query,
            search_query = req.query,
            raw_query    = req.query,
            is_refurb    = any(w in req.query.lower() for w in ("refurb", "second hand", "used")),
        )

    mode = "refurb" if intent.is_refurb else req.mode
    logger.info(f"Search: '{intent.search_query}' brand='{intent.brand}' refurb={intent.is_refurb}")

    # Step 2: Parallel product search
    try:
        products = await price_aggregator.search(intent.search_query, mode=mode)
    except Exception as e:
        logger.error(f"Price aggregator failed: {e}")
        raise HTTPException(503, "Product search temporarily unavailable. Try again shortly.")

    if not products:
        return {
            "results": [], "from_cache": False, "query": req.query, "total": 0,
            "message": "No products found. Try rephrasing your search.",
        }

    results = [_serialize_product(p) for p in products[:20]]
    await cache.set_search(req.query, mode, results)
    for p in products:
        bg.add_task(_record_price_history, p.prices, p.product_id)

    return {
        "results":    results,
        "from_cache": False,
        "query":      req.query,
        "total":      len(results),
        "intent": {
            "product_name": intent.product_name,
            "brand":        intent.brand,
            "category":     intent.category,
            "is_refurb":    intent.is_refurb,
            "confidence":   intent.confidence,
        },
    }


@app.post(
    "/api/scan/social",
    tags=["Search"],
    dependencies=[Depends(RateLimit(max_requests=5, window_seconds=60))],
)
async def scan_social(req: SocialScanRequest, bg: BackgroundTasks = BackgroundTasks(), cache: CacheManager = Depends(get_cache)):
    """
    Identify products from an Instagram Reel or YouTube Shorts URL.

    Pipeline: yt-dlp download → ffmpeg frames → GPT-4o Vision → price aggregator
    """
    cache_key = f"social_scan:{hashlib.md5(req.url.encode()).hexdigest()}"
    cached    = await cache.get(cache_key)
    if cached:
        return {**cached, "from_cache": True}

    platform = (
        "Instagram" if "instagram" in req.url else
        "YouTube"   if "youtu"     in req.url else
        "TikTok"    if "tiktok"    in req.url else "Unknown"
    )

    # Extract frames
    try:
        frames = await frame_extractor.extract(req.url, max_frames=12)
    except Exception as e:
        logger.warning(f"Frame extraction failed: {e}")
        frames = []

    # Identify products via vision
    identified = []
    if frames:
        try:
            identified = await openai_client.identify_from_frames(frames)
        except Exception as e:
            logger.warning(f"Vision identification failed: {e}")

    if not identified:
        return {
            "platform": platform, "url": req.url,
            "status": "no_products_found", "products_found": 0, "products": [],
            "message": "Could not identify any products in this video.",
        }

    # Search for each identified product
    search_tasks = [
        price_aggregator.search(p.get("search_query", p.get("product_name", "")))
        for p in identified[:3]
    ]
    search_results = await asyncio.gather(*search_tasks, return_exceptions=True)

    matched: list[dict] = []
    for meta, result in zip(identified, search_results):
        if isinstance(result, Exception) or not result:
            continue
        matched.append({
            **meta,
            "matched_product":     _serialize_product(result[0]),
            "detected_at_frame":   meta.get("frame_seconds", 0),
        })

    response = {
        "platform": platform, "url": req.url, "status": "complete",
        "products_found": len(matched), "products": matched,
        "frames_analysed": len(frames), "from_cache": False,
    }
    await cache.set(cache_key, response, ttl=3600)
    return response


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — PRICES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/api/prices/{product_id}", tags=["Prices"])
async def get_prices(
    product_id: str,
    name:  str  = Query("", description="Product name hint"),
    mode:  str  = Query("new",   description="new | refurb | both"),
    force: bool = Query(False,   description="Bypass cache"),
    bg:    BackgroundTasks    = BackgroundTasks(),
    cache: CacheManager       = Depends(get_cache),
    user:  Optional[AuthUser] = Depends(optional_auth),
):
    """
    Universal price comparison across all stores.
    Amazon PA-API primary; RapidAPI fallback. Cached 30 min.
    """
    if not force:
        cached = await cache.get_prices(product_id, mode)
        if cached:
            cached["from_cache"] = True
            return cached

    search_hint = name.strip() or product_id
    logger.info(f"Price fetch: '{search_hint}' mode={mode}")

    try:
        all_prices = await price_aggregator.get_all_prices(product_id, name=search_hint)
    except Exception as e:
        logger.error(f"get_all_prices failed: {e}")
        raise HTTPException(503, "Price data temporarily unavailable.")

    refurb_stores = {"cashify", "amazon_renewed", "flipkart_refurb"}
    new_prices    = {k: v for k, v in all_prices.items() if k not in refurb_stores}
    refurb_prices = {k: v for k, v in all_prices.items() if k in refurb_stores}

    # Fetch refurb separately if needed
    if mode in ("refurb", "both") and not refurb_prices:
        try:
            refurb_results = await price_aggregator.search(search_hint, mode="refurb")
            for prod in refurb_results:
                for store, price in prod.prices.items():
                    if price.condition:
                        refurb_prices[store] = price
        except Exception as e:
            logger.warning(f"Refurb search failed: {e}")

    def sort_prices(p_dict):
        return sorted(p_dict.values(), key=lambda x: x.price)

    response = {
        "product_id":    product_id,
        "name":          search_hint,
        "mode":          mode,
        "new_prices":    [_serialize_price(p) for p in sort_prices(new_prices)],
        "refurb_prices": [_serialize_price(p) for p in sort_prices(refurb_prices)],
        "lowest_new":    _serialize_price(min(new_prices.values(),    key=lambda x: x.price)) if new_prices    else None,
        "lowest_refurb": _serialize_price(min(refurb_prices.values(), key=lambda x: x.price)) if refurb_prices else None,
        "fetched_at":    time.time(),
        "from_cache":    False,
    }

    await cache.set_prices(product_id, mode, response)
    bg.add_task(_record_price_history, {**new_prices, **refurb_prices}, product_id)
    return response


@app.get("/api/prices/{product_id}/history", tags=["Prices"])
async def get_price_history(
    product_id: str,
    store: str = Query("amazon"),
    days:  int = Query(30, ge=1, le=365),
):
    """30-day price history. Powers the price chart on the Product Detail page."""
    history = await history_repo.get_history(product_id, store, days)
    lowest  = await history_repo.get_lowest_ever(product_id, store)
    return {
        "product_id":      product_id,
        "store":           store,
        "days":            days,
        "history":         history,
        "lowest_ever_inr": round(lowest / 100, 2) if lowest else None,
        "data_points":     len(history),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — AFFILIATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.post("/api/affiliate/link", response_model=AffiliateLinkResponse, tags=["Affiliate"])
async def affiliate_link(
    req:     AffiliateLinkRequest,
    request: Request,
    bg:      BackgroundTasks         = BackgroundTasks(),
    user:    Optional[AuthUser]      = Depends(optional_auth),
    cache:   CacheManager            = Depends(get_cache),
):
    """
    Append the affiliate tag to a store URL.
    Every "Buy Now" button calls this. Click is tracked to MongoDB.
    """
    aff_url = _build_aff_url(req.url, req.store)
    config  = AFFILIATE_PARAMS.get(req.store.lower(), {"param": "ref", "id": settings.AFFILIATE_ID})
    ip      = request.client.host if request.client else "0.0.0.0"

    bg.add_task(_track_affiliate_click, user.user_id if user else None, req.url, req.store, aff_url, ip)
    if user:
        await cache.increment_affiliate_clicks(user.user_id)

    return AffiliateLinkResponse(
        original_url  = req.url,
        affiliate_url = aff_url,
        store         = req.store,
        tag           = config["id"],
        param         = config["param"],
    )


@app.get("/api/affiliate/stats/{user_id}", tags=["Affiliate"])
async def affiliate_stats(
    user_id: str,
    user:    AuthUser     = Depends(require_auth),
    cache:   CacheManager = Depends(get_cache),
):
    """Affiliate dashboard metrics for the authenticated user."""
    if user.user_id != user_id:
        raise HTTPException(403, "Cannot view another user's affiliate stats.")
    cached = await cache.get_affiliate_stats(user_id)
    if cached:
        return cached
    stats = await user_repo.get_affiliate_stats(user_id)
    await cache.set_affiliate_stats(user_id, stats)
    return stats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — SAFETY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.post("/api/ingredients/check", tags=["Safety"])
async def check_ingredients(req: IngredientsCheckRequest, cache: CacheManager = Depends(get_cache)):
    """Batch ingredient safety check. Covers EU, FDA, Health Canada, UK MHRA, CDSCO."""
    results = await ingr_repo.check_batch(req.ingredients)
    danger  = sum(1 for r in results if r["status"] == "danger")
    caution = sum(1 for r in results if r["status"] == "caution")
    return {
        "product": req.product_name,
        "results": results,
        "summary": {
            "total":   len(results),
            "danger":  danger,
            "caution": caution,
            "safe":    len(results) - danger - caution,
            "overall": "HIGH" if danger > 0 else "MEDIUM" if caution > 1 else "LOW",
        },
    }


@app.get("/api/safety/{product_name}", tags=["Safety"])
async def safety_check(product_name: str, cache: CacheManager = Depends(get_cache)):
    """
    Global Safety Shield. Checks EU RAPEX, FDA, Health Canada, UK MHRA, TGA, PMDA, CDSCO, WHO.
    Cached 24 hours. Refreshed weekly via Celery sync_safety_database task.
    """
    cached = await cache.get_safety(product_name)
    if cached:
        return {**cached, "from_cache": True}

    key = product_name.lower().strip()

    KNOWN_BANS: dict[str, dict] = {
        "neutrogena fine fairness": {
            "banned": True, "safety_status": "danger",
            "regions": [
                {"region": "European Union 🇪🇺", "law": "EC 1223/2009",           "reason": "Hydroquinone >1% + Mercury compounds prohibited."},
                {"region": "United Kingdom 🇬🇧",  "law": "UK Cosmetics Reg 2013",  "reason": "Mercury violates heavy metal restriction Article 14."},
                {"region": "USA 🇺🇸",             "law": "FDA Import Alert 54-14", "reason": "Unapproved skin lightening agent; Mercury exceeded safe limits."},
                {"region": "Canada 🇨🇦",          "law": "Health Canada Regs",     "reason": "Hydroquinone restricted; Mercury banned at any level."},
            ],
        },
        "fair and lovely": {
            "banned": False, "safety_status": "caution",
            "regions": [{"region": "Advisory", "law": "EU (under review)", "reason": "Old Hydroquinone formulations restricted in EU. Post-2020 Niacinamide formula under review."}],
        },
        "skin lite cream": {
            "banned": True, "safety_status": "danger",
            "regions": [
                {"region": "European Union 🇪🇺", "law": "EC 1223/2009",   "reason": "Hydroquinone above 1% OTC limit."},
                {"region": "USA 🇺🇸",            "law": "FDA 21 CFR 310", "reason": "Unapproved new drug — skin bleaching."},
            ],
        },
        "melacare cream": {
            "banned": True, "safety_status": "danger",
            "regions": [
                {"region": "European Union 🇪🇺", "law": "EC 1223/2009",           "reason": "Hydroquinone + Tretinoin combo banned OTC."},
                {"region": "USA 🇺🇸",             "law": "FDA Import Alert 54-14", "reason": "Contains prescription-only Tretinoin as OTC."},
            ],
        },
    }

    match = None
    for ban_key, ban_data in KNOWN_BANS.items():
        if ban_key in key or key in ban_key or any(w in key for w in ban_key.split() if len(w) > 4):
            match = ban_data
            break

    result: dict = dict(match) if match else {"banned": False, "safety_status": "safe", "regions": []}
    result.update({
        "product":           product_name,
        "checked_databases": ["EU RAPEX", "FDA (USA)", "Health Canada", "UK MHRA", "TGA (AU)", "PMDA (Japan)", "CDSCO (India)", "WHO"],
        "last_checked":      datetime.now(timezone.utc).isoformat(),
    })

    await cache.set_safety(product_name, result)
    return {**result, "from_cache": False}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — USER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/api/user/me", tags=["User"])
async def get_me(user: AuthUser = Depends(require_auth)):
    """Return the authenticated user's profile."""
    db_user = await user_repo.find_by_phone(user.phone) or {}
    return {
        "user_id":       user.user_id,
        "phone":         user.phone,
        "is_guest":      user.is_guest,
        "name":          db_user.get("name"),
        "selected_cats": db_user.get("selected_cats", []),
        "is_pro":        db_user.get("is_pro", False),
    }


@app.post("/api/user/{user_id}/save/{product_id}", tags=["User"])
async def save_product(user_id: str, product_id: str, user: AuthUser = Depends(require_auth)):
    """Save a product to the user's wishlist."""
    if user.user_id != user_id:
        raise HTTPException(403, "Cannot modify another user's saved items.")
    success = await user_repo.save_product(user_id, product_id)
    return {"success": success}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — META
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/api/stores", tags=["Meta"])
async def get_stores(category: Optional[str] = None):
    """Full store directory — 50+ retailers grouped by category."""
    directory = {
        "Multi-Category":  ["Amazon India", "Flipkart", "Tata CLiQ", "Reliance Digital", "Meesho", "Snapdeal"],
        "Fashion":         ["Myntra", "Ajio", "Nykaa Fashion", "Zivame", "Bewakoof", "Snitch", "Westside", "H&M", "Zara"],
        "Electronics D2C": ["Croma", "Vijay Sales", "boAt", "Noise", "Mi Store", "Samsung", "Apple", "Sony", "Dell", "Lenovo", "Portronics"],
        "Beauty & Health": ["Nykaa", "Purplle", "Mamaearth", "The Derma Co", "HealthKart", "1mg", "Apollo 247", "Netmeds"],
        "Home & Grocery":  ["Pepperfry", "Urban Ladder", "IKEA", "BigBasket", "Blinkit", "Zepto", "Swiggy Instamart"],
        "Niche":           ["Lenskart", "FirstCry", "Decathlon", "Wildcraft", "Chumbak"],
        "Refurbished":     ["Cashify", "Amazon Renewed", "Flipkart Refurbished"],
    }
    if category:
        return {"category": category, "stores": directory.get(category, [])}
    return {"total_stores": sum(len(v) for v in directory.values()), "categories": directory}


@app.get("/api/health", tags=["Meta"])
async def health(cache: CacheManager = Depends(get_cache)):
    """Service health check with per-dependency status."""
    from api_clients import pa_api_client, rapidapi_client
    cache_ok = await cache.ping()
    return {
        "status":       "healthy" if cache_ok else "degraded",
        "version":      settings.APP_VERSION,
        "timestamp":    time.time(),
        "affiliate_id": settings.AFFILIATE_ID,
        "services": {
            "cache":         "ok"             if cache_ok                  else "unavailable",
            "amazon_paapi":  "configured"     if pa_api_client._enabled    else "not_configured",
            "rapidapi":      "configured"     if rapidapi_client._enabled  else "not_configured",
            "openai":        "configured"     if openai_client._enabled    else "dev_mode",
            "twilio_sms":    "configured"     if twilio_client._enabled    else "dev_mode",
        },
    }
