"""
PriceShield v2 — Configuration & Settings
Loads from .env file using pydantic-settings
"""
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import List


class Settings(BaseSettings):
    # ── App ─────────────────────────────────────────────
    APP_NAME:       str  = "PriceShield"
    APP_VERSION:    str  = "2.0.0"
    DEBUG:          bool = False
    ENVIRONMENT:    str  = "production"  # development | staging | production

    # ── Database ─────────────────────────────────────────
    MONGODB_URL:    str  = "mongodb://localhost:27017"
    MONGODB_DB:     str  = "priceshield"

    # ── Redis ─────────────────────────────────────────────
    REDIS_URL:      str  = "redis://localhost:6379"
    CACHE_TTL_PRICES:   int = 1800   # 30 min for price data
    CACHE_TTL_SAFETY:   int = 86400  # 24 hr for safety/ban data
    CACHE_TTL_PRODUCTS: int = 3600   # 1 hr for product metadata

    # ── Auth ──────────────────────────────────────────────
    JWT_SECRET:     str  = "change-me-in-production"
    JWT_ALGORITHM:  str  = "HS256"
    JWT_EXPIRE_MIN: int  = 60 * 24 * 7  # 7 days
    OTP_EXPIRE_SEC: int  = 300           # 5 min
    OTP_MAX_ATTEMPTS: int = 3

    # ── SMS ───────────────────────────────────────────────
    TWILIO_SID:     str  = ""
    TWILIO_TOKEN:   str  = ""
    TWILIO_FROM:    str  = ""
    # OR use MSG91:
    MSG91_API_KEY:  str  = ""
    MSG91_SENDER:   str  = "PRCSHL"

    # ── AI ────────────────────────────────────────────────
    OPENAI_API_KEY: str  = ""
    OPENAI_MODEL:   str  = "gpt-4o-mini"
    OPENAI_VISION:  str  = "gpt-4o"

    # ── Affiliate ─────────────────────────────────────────
    AFFILIATE_ID:   str  = "safebuy-21"

    # ── Scraping ──────────────────────────────────────────
    SCRAPER_TIMEOUT_MS:    int  = 15000
    SCRAPER_MAX_RETRIES:   int  = 3
    SCRAPER_CONCURRENCY:   int  = 6    # parallel store scrapers
    SCRAPER_USER_AGENT:    str  = (
        "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36"
    )

    # ── CORS ──────────────────────────────────────────────
    CORS_ORIGINS:   List[str] = ["http://localhost:3000", "https://priceshield.in"]

    # ── Sentry ────────────────────────────────────────────
    SENTRY_DSN:     str  = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# ── Affiliate param map ─────────────────────────────────────
AFFILIATE_PARAMS = {
    "amazon":         {"param": "tag",           "id": "safebuy-21"},
    "flipkart":       {"param": "affid",         "id": "safebuy-21"},
    "meesho":         {"param": "ref",           "id": "safebuy-21"},
    "croma":          {"param": "utm_campaign",  "id": "safebuy-21"},
    "myntra":         {"param": "affid",         "id": "safebuy-21"},
    "ajio":           {"param": "affiliateId",   "id": "safebuy-21"},
    "nykaa":          {"param": "affCode",       "id": "safebuy-21"},
    "purplle":        {"param": "ref",           "id": "safebuy-21"},
    "tatacliq":       {"param": "utm_source",    "id": "safebuy-21"},
    "bigbasket":      {"param": "utm_source",    "id": "safebuy-21"},
    "healthkart":     {"param": "utm_campaign",  "id": "safebuy-21"},
    "1mg":            {"param": "ref",           "id": "safebuy-21"},
    "netmeds":        {"param": "ref",           "id": "safebuy-21"},
    "cashify":        {"param": "ref",           "id": "safebuy-21"},
    "reliancedigital":{"param": "utm_campaign",  "id": "safebuy-21"},
    "snapdeal":       {"param": "clkid",         "id": "safebuy-21"},
    "vijaysales":     {"param": "ref",           "id": "safebuy-21"},
    "decathlon":      {"param": "ref",           "id": "safebuy-21"},
    "firstcry":       {"param": "affid",         "id": "safebuy-21"},
    "lenskart":       {"param": "ref",           "id": "safebuy-21"},
}

# ── Store scraper selectors ──────────────────────────────────
STORE_SELECTORS = {
    "amazon": {
        "search_url":   "https://www.amazon.in/s?k={query}",
        "product_url":  "https://www.amazon.in/dp/{asin}",
        "price":        ".a-price-whole",
        "price_frac":   ".a-price-fraction",
        "rating":       "span[data-hook='rating-out-of-text']",
        "review_count": "span[data-hook='total-review-count']",
        "availability": "#availability span",
        "title":        "#productTitle",
        "wait_for":     ".a-price-whole",
    },
    "flipkart": {
        "search_url":   "https://www.flipkart.com/search?q={query}",
        "price":        "._30jeq3._16Jk6d",
        "rating":       "._3LWZlK",
        "review_count": "._2_R_DZ span",
        "title":        ".B_NuCI",
        "wait_for":     "._30jeq3",
    },
    "croma": {
        "search_url":   "https://www.croma.com/searchB?q={query}",
        "price":        ".pdp-offer-price",
        "title":        ".pdp-product-title",
        "wait_for":     ".pdp-offer-price",
    },
    "nykaa": {
        "search_url":   "https://www.nykaa.com/search/result/?q={query}",
        "price":        ".css-1n1gvwt",
        "title":        ".css-1ilbhcl",
        "wait_for":     ".css-1n1gvwt",
    },
    "myntra": {
        "search_url":   "https://www.myntra.com/{query}",
        "price":        ".pdp-price strong",
        "title":        "h1.pdp-title",
        "wait_for":     ".pdp-price",
    },
    "cashify": {
        "search_url":   "https://www.cashify.in/buy-refurbished-{query}",
        "price":        ".price",
        "condition":    ".product-condition",
        "title":        ".product-name",
        "wait_for":     ".price",
    },
}

# ── Amazon PA-API 5.0 (added in v2 real-data edition) ──────
AMAZON_ACCESS_KEY:   str = ""
AMAZON_SECRET_KEY:   str = ""
AMAZON_PARTNER_TAG:  str = ""   # e.g. "safebuy-21"

# ── RapidAPI (fallback when PA-API not available) ───────────
RAPIDAPI_KEY:        str = ""   # from rapidapi.com dashboard
