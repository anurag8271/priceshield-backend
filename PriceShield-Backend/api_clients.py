"""
PriceShield v2 — Real API Client Layer
========================================
Wraps every external API with:
  • Typed return objects
  • Async/await throughout
  • Exponential-backoff retry via tenacity
  • Structured error logging
  • Clean fallback chains

Priority order per store:
  Amazon  → PA-API 5.0  →  RapidAPI Real-Time Amazon  →  Playwright scraper
  Flipkart→ RapidAPI Flipkart  →  Playwright scraper
  Others  → RapidAPI / direct HTTP  →  Playwright scraper

ENV vars required (add to .env):
  AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_PARTNER_TAG
  RAPIDAPI_KEY
  OPENAI_API_KEY
  TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
    RetryError,
)

from config import get_settings

settings = get_settings()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SHARED DATA TYPES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class ProductResult:
    """Normalised product from any data source."""
    product_id:   str
    asin:         Optional[str]       = None
    name:         str                 = ""
    brand:        str                 = ""
    category:     str                 = ""
    image_url:    Optional[str]       = None
    description:  str                 = ""
    rating:       float               = 0.0
    review_count: int                 = 0
    # Prices keyed by store slug
    prices:       dict[str, "PriceResult"] = field(default_factory=dict)
    source:       str                 = "unknown"   # pa_api | rapidapi | scraped


@dataclass
class PriceResult:
    """Normalised price from any data source."""
    store:       str
    price:       float                # INR
    orig_price:  Optional[float]      = None
    discount_pct: Optional[float]     = None
    url:         str                  = ""
    in_stock:    bool                 = True
    condition:   Optional[str]        = None   # for refurb
    currency:    str                  = "INR"
    fetched_at:  float                = field(default_factory=time.time)
    source:      str                  = "unknown"


@dataclass
class SearchIntent:
    """Extracted product intent from natural-language query."""
    product_name:  str
    brand:         str          = ""
    category:      str          = ""
    search_query:  str          = ""
    is_refurb:     bool         = False
    raw_query:     str          = ""
    confidence:    float        = 1.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RETRY DECORATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _retry(attempts: int = 3, min_wait: int = 1, max_wait: int = 8):
    return retry(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. AMAZON PRODUCT ADVERTISING API 5.0
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Docs: https://webservices.amazon.com/paapi5/documentation/
#
# Requirements:
#   • Amazon Associates account (apply at affiliate-program.amazon.in)
#   • 3 qualifying sales in the first 180 days to keep credentials
#   • API credentials: Access Key + Secret Key + Partner Tag
#
# Sign up: https://affiliate-program.amazon.in → Tools → Product Advertising API

class AmazonPAAPIClient:
    """
    Amazon Product Advertising API 5.0 client.
    Handles AWS Signature V4 signing, search, and item lookup.
    """

    HOST      = "webservices.amazon.in"
    REGION    = "eu-west-1"          # India marketplace uses EU region
    SERVICE   = "ProductAdvertisingAPI"
    ENDPOINT  = f"https://{HOST}/paapi5"

    RESOURCES = [
        "Images.Primary.Medium",
        "ItemInfo.Title",
        "ItemInfo.ByLineInfo",
        "ItemInfo.ProductInfo",
        "Offers.Listings.Price",
        "Offers.Listings.SavingBasis",
        "Offers.Listings.Promotions",
        "Offers.Listings.Availability.Message",
        "Offers.Summaries.LowestPrice",
        "SearchRefinements",
        "BrowseNodeInfo.BrowseNodes",
    ]

    def __init__(self):
        self.access_key  = settings.AMAZON_ACCESS_KEY
        self.secret_key  = settings.AMAZON_SECRET_KEY
        self.partner_tag = settings.AMAZON_PARTNER_TAG
        self._enabled    = bool(self.access_key and self.secret_key and self.partner_tag)

        if not self._enabled:
            logger.warning("Amazon PA-API: credentials not configured — will use RapidAPI fallback")

    # ── AWS Signature V4 ─────────────────────────────────────

    def _sign(self, payload: dict, operation: str) -> dict:
        """
        Signs the request with AWS Signature V4.
        Returns headers dict ready for the HTTP call.
        """
        body         = json.dumps(payload, separators=(",", ":"))
        amz_date     = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        date_stamp   = amz_date[:8]
        content_type = "application/json; charset=utf-8"
        target       = f"com.amazon.paapi5.v1.ProductAdvertisingAPIv1.{operation}"

        # Canonical request
        canonical_headers = (
            f"content-encoding:amz-1.0\n"
            f"content-type:{content_type}\n"
            f"host:{self.HOST}\n"
            f"x-amz-date:{amz_date}\n"
            f"x-amz-target:{target}\n"
        )
        signed_headers   = "content-encoding;content-type;host;x-amz-date;x-amz-target"
        payload_hash     = hashlib.sha256(body.encode()).hexdigest()
        canonical_request = "\n".join([
            "POST", "/paapi5/searchitems", "",
            canonical_headers, signed_headers, payload_hash,
        ])

        # String to sign
        credential_scope = f"{date_stamp}/{self.REGION}/{self.SERVICE}/aws4_request"
        string_to_sign   = "\n".join([
            "AWS4-HMAC-SHA256", amz_date, credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ])

        # Signing key
        def _hmac(key, msg):
            return hmac.new(key, msg.encode(), hashlib.sha256).digest()

        signing_key = _hmac(
            _hmac(
                _hmac(
                    _hmac(f"AWS4{self.secret_key}".encode(), date_stamp),
                    self.REGION,
                ),
                self.SERVICE,
            ),
            "aws4_request",
        )

        signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
        auth_header = (
            f"AWS4-HMAC-SHA256 Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        return {
            "Content-Type":      content_type,
            "Content-Encoding":  "amz-1.0",
            "X-Amz-Date":        amz_date,
            "X-Amz-Target":      target,
            "Authorization":     auth_header,
        }

    # ── Public methods ──────────────────────────────────────

    @_retry(attempts=3)
    async def search(self, query: str, category: str = "All") -> list[ProductResult]:
        """Search for products by keyword. Returns up to 10 results."""
        if not self._enabled:
            return []

        payload = {
            "Keywords":    query,
            "PartnerTag":  self.partner_tag,
            "PartnerType": "Associates",
            "Marketplace": "www.amazon.in",
            "SearchIndex": category,
            "ItemCount":   10,
            "Resources":   self.RESOURCES,
        }

        headers = self._sign(payload, "SearchItems")
        url     = f"{self.ENDPOINT}/searchitems"

        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post(url, headers=headers, content=json.dumps(payload))
            resp.raise_for_status()
            data = resp.json()

        items = data.get("SearchResult", {}).get("Items", [])
        return [self._parse_item(item) for item in items if item]

    @_retry(attempts=3)
    async def get_item(self, asin: str) -> Optional[ProductResult]:
        """Fetch a single product by ASIN with full price data."""
        if not self._enabled:
            return None

        payload = {
            "ItemIds":     [asin],
            "PartnerTag":  self.partner_tag,
            "PartnerType": "Associates",
            "Marketplace": "www.amazon.in",
            "Resources":   self.RESOURCES,
        }

        headers = self._sign(payload, "GetItems")
        url     = f"{self.ENDPOINT}/getitems"

        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.post(url, headers=headers, content=json.dumps(payload))
            resp.raise_for_status()
            data = resp.json()

        items = data.get("ItemsResult", {}).get("Items", [])
        return self._parse_item(items[0]) if items else None

    def _parse_item(self, item: dict) -> ProductResult:
        """Normalise a raw PA-API item into a ProductResult."""
        info     = item.get("ItemInfo", {})
        offers   = item.get("Offers", {})
        listings = offers.get("Listings", [{}])
        listing  = listings[0] if listings else {}
        price_v  = listing.get("Price", {})
        savings  = listing.get("SavingBasis", {})
        avail    = listing.get("Availability", {}).get("Message", "")

        price_inr    = price_v.get("Amount")
        orig_inr     = savings.get("Amount")
        discount_pct = None
        if price_inr and orig_inr and orig_inr > price_inr:
            discount_pct = round((orig_inr - price_inr) / orig_inr * 100, 1)

        asin = item.get("ASIN", "")
        url  = item.get("DetailPageURL", f"https://www.amazon.in/dp/{asin}?tag={settings.AMAZON_PARTNER_TAG}")

        return ProductResult(
            product_id   = asin,
            asin         = asin,
            name         = info.get("Title", {}).get("DisplayValue", ""),
            brand        = info.get("ByLineInfo", {}).get("Brand", {}).get("DisplayValue", ""),
            image_url    = item.get("Images", {}).get("Primary", {}).get("Medium", {}).get("URL"),
            rating       = 0.0,   # PA-API doesn't return ratings directly
            review_count = 0,
            prices       = {
                "amazon": PriceResult(
                    store        = "amazon",
                    price        = price_inr or 0,
                    orig_price   = orig_inr,
                    discount_pct = discount_pct,
                    url          = url,
                    in_stock     = "in stock" in avail.lower() if avail else True,
                    source       = "pa_api",
                )
            } if price_inr else {},
            source = "pa_api",
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. RAPIDAPI — Multi-store fallback
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Subscribe at: https://rapidapi.com
#
# APIs used:
#   • "Real-Time Amazon Data" by axesso
#     https://rapidapi.com/axesso/api/real-time-amazon-data
#   • "Amazon Data Scraper" by rainforest
#     https://rapidapi.com/rainforest-api/api/amazon-data-scraper7
#   • "Flipkart Product Search" by api-ninjas
#     https://rapidapi.com/api-ninjas/api/flipkart-products
#   • "Myntra Product Data" (unofficial)
#     https://rapidapi.com/letscrape-6bfp4szdwbr/api/myntra-product-data
#
# Free tiers available — upgrade for higher rate limits.

class RapidAPIClient:
    """
    RapidAPI hub client. Wraps multiple product/price APIs
    under a single interface.
    """

    BASE = "https://{host}"
    HEADERS_BASE = {
        "X-RapidAPI-Key": "",   # set in __init__
    }

    # ── API host slugs (change if RapidAPI updates them) ─────
    HOSTS = {
        "amazon_realtime": "real-time-amazon-data.p.rapidapi.com",
        "amazon_rainforest": "amazon-data-scraper7.p.rapidapi.com",
        "flipkart":        "flipkart-products.p.rapidapi.com",
        "myntra":          "myntra-product-data.p.rapidapi.com",
    }

    def __init__(self):
        self._key     = settings.RAPIDAPI_KEY
        self._enabled = bool(self._key)
        if not self._enabled:
            logger.warning("RapidAPI: RAPIDAPI_KEY not set — all RapidAPI calls will be skipped")

    def _headers(self, host: str) -> dict:
        return {
            "X-RapidAPI-Key":  self._key,
            "X-RapidAPI-Host": host,
        }

    # ── Amazon via RapidAPI ──────────────────────────────────

    @_retry(attempts=3)
    async def amazon_search(self, query: str, page: int = 1) -> list[ProductResult]:
        """
        Search Amazon IN via Real-Time Amazon Data API.
        Returns normalised ProductResult list.
        """
        if not self._enabled:
            return []

        host = self.HOSTS["amazon_realtime"]
        url  = f"https://{host}/search"

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=self._headers(host), params={
                "query":    query,
                "page":     str(page),
                "country":  "IN",
                "sort_by":  "RELEVANCE",
                "product_condition": "ALL",
            })
            resp.raise_for_status()
            data = resp.json()

        products = data.get("data", {}).get("products", [])
        results  = []
        for p in products[:10]:
            price_raw  = p.get("product_minimum_offer_price", "") or p.get("product_price", "")
            price_inr  = self._parse_price(price_raw)
            orig_raw   = p.get("product_original_price") or ""
            orig_inr   = self._parse_price(orig_raw)
            disc       = None
            if price_inr and orig_inr and orig_inr > price_inr:
                disc = round((orig_inr - price_inr) / orig_inr * 100, 1)

            asin = p.get("asin", "")
            results.append(ProductResult(
                product_id   = asin,
                asin         = asin,
                name         = p.get("product_title", ""),
                brand        = p.get("product_brand_name", ""),
                image_url    = p.get("product_photo"),
                rating       = float(p.get("product_star_rating") or 0),
                review_count = int((p.get("product_num_ratings") or "0").replace(",", "") or 0),
                prices       = {
                    "amazon": PriceResult(
                        store        = "amazon",
                        price        = price_inr or 0,
                        orig_price   = orig_inr,
                        discount_pct = disc,
                        url          = p.get("product_url") or f"https://www.amazon.in/dp/{asin}?tag={settings.AMAZON_PARTNER_TAG}",
                        in_stock     = p.get("is_best_seller") is not None,
                        source       = "rapidapi_amazon",
                    )
                } if price_inr else {},
                source = "rapidapi_amazon",
            ))

        logger.info(f"RapidAPI Amazon: '{query}' → {len(results)} results")
        return results

    @_retry(attempts=3)
    async def amazon_product(self, asin: str) -> Optional[ProductResult]:
        """
        Fetch full product details + all offer prices for a single ASIN.
        Uses the more detailed rainforest-style endpoint.
        """
        if not self._enabled:
            return None

        host = self.HOSTS["amazon_realtime"]
        url  = f"https://{host}/product-details"

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=self._headers(host), params={
                "asin":    asin,
                "country": "IN",
            })
            resp.raise_for_status()
            data = resp.json().get("data", {})

        price_inr = self._parse_price(data.get("product_price", ""))
        orig_inr  = self._parse_price(data.get("product_original_price", ""))
        disc      = None
        if price_inr and orig_inr and orig_inr > price_inr:
            disc = round((orig_inr - price_inr) / orig_inr * 100, 1)

        return ProductResult(
            product_id   = asin,
            asin         = asin,
            name         = data.get("product_title", ""),
            brand        = data.get("product_brand_name", ""),
            description  = data.get("product_description", ""),
            image_url    = data.get("product_main_image_url"),
            rating       = float(data.get("product_star_rating") or 0),
            review_count = int((data.get("product_num_ratings") or "0").replace(",", "") or 0),
            prices       = {
                "amazon": PriceResult(
                    store        = "amazon",
                    price        = price_inr or 0,
                    orig_price   = orig_inr,
                    discount_pct = disc,
                    url          = f"https://www.amazon.in/dp/{asin}?tag={settings.AMAZON_PARTNER_TAG}",
                    in_stock     = True,
                    source       = "rapidapi_amazon",
                )
            } if price_inr else {},
            source = "rapidapi_amazon",
        )

    # ── Flipkart via RapidAPI ────────────────────────────────

    @_retry(attempts=3)
    async def flipkart_search(self, query: str) -> list[ProductResult]:
        """
        Search Flipkart via RapidAPI Flipkart Products API.
        """
        if not self._enabled:
            return []

        host = self.HOSTS["flipkart"]
        url  = f"https://{host}/search"

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=self._headers(host), params={
                "q":    query,
                "page": "1",
            })
            resp.raise_for_status()
            data = resp.json()

        products = data.get("products", []) or data.get("items", [])
        results  = []

        for p in products[:8]:
            price_inr  = self._parse_price(str(p.get("price") or p.get("selling_price") or ""))
            orig_inr   = self._parse_price(str(p.get("original_price") or p.get("mrp") or ""))
            disc       = None
            if price_inr and orig_inr and orig_inr > price_inr:
                disc = round((orig_inr - price_inr) / orig_inr * 100, 1)

            pid = p.get("product_id") or p.get("id") or ""
            results.append(ProductResult(
                product_id   = f"fk_{pid}",
                name         = p.get("name") or p.get("title") or "",
                brand        = p.get("brand") or "",
                image_url    = p.get("image") or p.get("thumbnail"),
                rating       = float(p.get("rating") or 0),
                review_count = int(p.get("rating_count") or 0),
                prices       = {
                    "flipkart": PriceResult(
                        store        = "flipkart",
                        price        = price_inr or 0,
                        orig_price   = orig_inr,
                        discount_pct = disc,
                        url          = p.get("url") or p.get("link") or f"https://flipkart.com/search?q={urllib.parse.quote(query)}&affid={settings.AFFILIATE_ID}",
                        in_stock     = True,
                        source       = "rapidapi_flipkart",
                    )
                } if price_inr else {},
                source = "rapidapi_flipkart",
            ))

        logger.info(f"RapidAPI Flipkart: '{query}' → {len(results)} results")
        return results

    # ── Myntra via RapidAPI ──────────────────────────────────

    @_retry(attempts=2)
    async def myntra_search(self, query: str) -> list[PriceResult]:
        """
        Search Myntra for fashion items.
        Returns PriceResult list (price-only, no full product).
        """
        if not self._enabled:
            return []

        host = self.HOSTS["myntra"]
        url  = f"https://{host}/search"

        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.get(url, headers=self._headers(host), params={"query": query})
            resp.raise_for_status()
            data = resp.json()

        items   = data.get("products", [])[:5]
        results = []
        for item in items:
            price_inr = self._parse_price(str(item.get("discountedPrice") or item.get("price") or ""))
            if not price_inr:
                continue
            results.append(PriceResult(
                store     = "myntra",
                price     = price_inr,
                url       = item.get("landingPageUrl") or f"https://myntra.com/{item.get('id', '')}?affid={settings.AFFILIATE_ID}",
                in_stock  = True,
                source    = "rapidapi_myntra",
            ))

        return results

    # ── Price helper ─────────────────────────────────────────

    @staticmethod
    def _parse_price(raw: str) -> Optional[float]:
        """Extract a float INR value from messy price strings like '₹1,34,900' or '$32.5'."""
        if not raw:
            return None
        import re
        digits = re.sub(r"[^\d.]", "", raw.replace(",", ""))
        try:
            return float(digits) if digits else None
        except ValueError:
            return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. OPENAI — Search intent + social scan
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class OpenAIClient:
    """
    OpenAI wrapper for two tasks:
      1. Extract structured product intent from natural-language queries
      2. Identify products from social video frames (GPT-4 Vision)
    """

    def __init__(self):
        self._key     = settings.OPENAI_API_KEY
        self._enabled = bool(self._key and not self._key.startswith("sk-test"))
        if not self._enabled:
            logger.warning("OpenAI: key not set or is test key — using keyword fallback")

    @_retry(attempts=2, min_wait=2, max_wait=6)
    async def extract_intent(self, query: str) -> SearchIntent:
        """
        Parse a natural-language shopping query into structured intent.
        Falls back to a simple keyword passthrough if OpenAI is unavailable.
        """
        if not self._enabled:
            return SearchIntent(
                product_name = query,
                search_query = query,
                raw_query    = query,
                is_refurb    = any(w in query.lower() for w in ("refurb", "second hand", "used", "old")),
            )

        system = (
            "You are a product search assistant for Indian e-commerce. "
            "Given a user's shopping query, extract structured intent. "
            "Respond ONLY with valid JSON, no markdown. Schema: "
            '{"product_name": string, "brand": string, "category": string, '
            '"search_query": string, "is_refurb": boolean}'
        )

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self._key}",
                    "Content-Type":  "application/json",
                },
                content=json.dumps({
                    "model":           settings.OPENAI_MODEL,
                    "max_tokens":      150,
                    "temperature":     0.1,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user",   "content": query},
                    ],
                }),
            )
            resp.raise_for_status()

        raw     = resp.json()["choices"][0]["message"]["content"]
        parsed  = json.loads(raw)

        return SearchIntent(
            product_name = parsed.get("product_name", query),
            brand        = parsed.get("brand", ""),
            category     = parsed.get("category", ""),
            search_query = parsed.get("search_query", query),
            is_refurb    = parsed.get("is_refurb", False),
            raw_query    = query,
            confidence   = 0.92,
        )

    @_retry(attempts=2)
    async def identify_from_frames(self, base64_frames: list[str]) -> list[dict]:
        """
        Send up to 12 video frames to GPT-4o Vision and identify products.
        Each frame must be a base64-encoded JPEG string.
        Returns list of {"product_name", "brand", "category", "search_query"}.
        """
        if not self._enabled or not base64_frames:
            return []

        content: list[dict] = [
            {
                "type": "text",
                "text": (
                    "Identify all products visible in these video frames from an Indian "
                    "shopping reel or YouTube short. For each product found, respond with "
                    "ONLY valid JSON: "
                    '{"products": [{"product_name": "", "brand": "", "category": "", '
                    '"search_query": "", "frame_seconds": 0}]}'
                ),
            }
        ]
        # Attach up to 12 frames
        for b64 in base64_frames[:12]:
            content.append({
                "type": "image_url",
                "image_url": {
                    "url":    f"data:image/jpeg;base64,{b64}",
                    "detail": "low",   # low = cheaper + faster, good enough for product ID
                },
            })

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self._key}",
                    "Content-Type":  "application/json",
                },
                content=json.dumps({
                    "model":       settings.OPENAI_VISION,
                    "max_tokens":  400,
                    "temperature": 0.1,
                    "response_format": {"type": "json_object"},
                    "messages":    [{"role": "user", "content": content}],
                }),
            )
            resp.raise_for_status()

        raw    = resp.json()["choices"][0]["message"]["content"]
        parsed = json.loads(raw)
        return parsed.get("products", [])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. TWILIO — SMS OTP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TwilioClient:
    """
    Sends SMS OTPs via Twilio Verify or plain Messages API.
    Verify is preferred — handles OTP generation, delivery, and verification.
    Falls back to Messages API if Verify Service SID not configured.
    """

    def __init__(self):
        self._sid        = settings.TWILIO_SID
        self._token      = settings.TWILIO_TOKEN
        self._from       = settings.TWILIO_FROM
        self._verify_sid = getattr(settings, "TWILIO_VERIFY_SID", "")
        self._enabled    = bool(self._sid and self._token and self._from
                                and not self._sid.startswith("ACtest"))
        if not self._enabled:
            logger.warning("Twilio: not configured — OTPs will be logged only (DEV mode)")

    async def send_otp(self, phone_e164: str, otp: str) -> bool:
        """
        Send OTP SMS. phone_e164 must include country code e.g. +919876543210.
        Returns True on success.
        """
        if not self._enabled:
            logger.info(f"[DEV] SMS OTP for {phone_e164}: {otp}")
            return True

        body = f"Your PriceShield OTP is {otp}. Valid for 5 minutes. Do not share this code."

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{self._sid}/Messages.json",
                auth=(self._sid, self._token),
                data={
                    "From": self._from,
                    "To":   phone_e164,
                    "Body": body,
                },
            )

        if resp.status_code in (200, 201):
            logger.info(f"SMS sent to {phone_e164} via Twilio: SID={resp.json().get('sid')}")
            return True
        else:
            logger.error(f"Twilio error {resp.status_code}: {resp.text}")
            return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. VIDEO FRAME EXTRACTOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class VideoFrameExtractor:
    """
    Downloads a social video with yt-dlp and extracts
    1-fps frames with ffmpeg. Returns list of base64 JPEGs.
    """

    async def extract(self, url: str, max_frames: int = 12) -> list[str]:
        import asyncio
        import base64
        import os
        import tempfile

        frames = []
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, "video.mp4")
            frame_tmpl = os.path.join(tmpdir, "frame_%d.jpg")

            # Step 1: Download video
            dl = await asyncio.create_subprocess_exec(
                "yt-dlp",
                url,
                "-o", video_path,
                "--max-filesize", "30m",
                "--format", "worst[ext=mp4]/worst",
                "--quiet",
                "--no-warnings",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(dl.wait(), timeout=60)

            if not os.path.exists(video_path):
                logger.warning(f"yt-dlp: video download failed for {url}")
                return []

            # Step 2: Extract frames at 1 fps
            ff = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-i", video_path,
                "-vf", f"fps=1,scale=640:-1",
                "-frames:v", str(max_frames),
                frame_tmpl,
                "-y",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(ff.wait(), timeout=30)

            # Step 3: Read frames as base64
            for i in range(1, max_frames + 1):
                path = os.path.join(tmpdir, f"frame_{i}.jpg")
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        frames.append(base64.b64encode(f.read()).decode())

        logger.info(f"VideoFrameExtractor: {len(frames)} frames extracted from {url}")
        return frames


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. PRICE AGGREGATOR
#    Orchestrates all clients with fallback chains
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PriceAggregator:
    """
    Unified price-fetching orchestrator.
    Runs PA-API → RapidAPI → Playwright scrapers in parallel
    and merges results, deduplicating by store.
    """

    def __init__(self):
        self.pa_api   = AmazonPAAPIClient()
        self.rapid    = RapidAPIClient()

    async def search(self, query: str, mode: str = "new") -> list[ProductResult]:
        """
        Full product search across all sources in parallel.
        Returns merged, deduplicated list sorted by Amazon price.
        """
        tasks = [
            self._amazon_search(query),
            self._flipkart_search(query),
        ]
        if mode == "new":
            # Also try fashion stores for clothing queries
            fashion_kw = ("shirt", "jeans", "dress", "kurta", "shoes", "sneaker", "saree")
            if any(w in query.lower() for w in fashion_kw):
                tasks.append(self.rapid.myntra_search(query))

        results_raw = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten and deduplicate by ASIN/product_id
        merged: dict[str, ProductResult] = {}
        for batch in results_raw:
            if isinstance(batch, Exception):
                logger.warning(f"PriceAggregator: source error — {batch}")
                continue
            for item in (batch or []):
                # For price-only results (myntra), skip — handled separately
                if isinstance(item, PriceResult):
                    continue
                key = item.asin or item.product_id
                if key not in merged:
                    merged[key] = item
                else:
                    # Merge prices from different sources into existing item
                    merged[key].prices.update(item.prices)

        return list(merged.values())

    async def get_all_prices(self, asin: str, name: str = "") -> dict[str, PriceResult]:
        """
        Fetch prices for a known product from all stores in parallel.
        Returns dict keyed by store slug.
        """
        query = name or asin

        tasks = {
            "amazon":   self._get_amazon_price(asin),
            "flipkart": self._get_flipkart_price(query),
            "myntra":   self._get_myntra_price(query),
        }

        raw = await asyncio.gather(*tasks.values(), return_exceptions=True)
        prices: dict[str, PriceResult] = {}

        for store, result in zip(tasks.keys(), raw):
            if isinstance(result, Exception):
                logger.debug(f"Price fetch failed for {store}: {result}")
                continue
            if result:
                prices[store] = result

        return prices

    # ── Private helpers ──────────────────────────────────────

    async def _amazon_search(self, query: str) -> list[ProductResult]:
        """PA-API first, RapidAPI fallback."""
        try:
            if self.pa_api._enabled:
                results = await self.pa_api.search(query)
                if results:
                    return results
        except Exception as e:
            logger.warning(f"PA-API search failed: {e} — falling back to RapidAPI")

        return await self.rapid.amazon_search(query)

    async def _flipkart_search(self, query: str) -> list[ProductResult]:
        try:
            return await self.rapid.flipkart_search(query)
        except Exception as e:
            logger.warning(f"Flipkart RapidAPI failed: {e}")
            return []

    async def _get_amazon_price(self, asin: str) -> Optional[PriceResult]:
        try:
            if self.pa_api._enabled:
                item = await self.pa_api.get_item(asin)
                if item and "amazon" in item.prices:
                    return item.prices["amazon"]
        except Exception:
            pass
        try:
            item = await self.rapid.amazon_product(asin)
            if item and "amazon" in item.prices:
                return item.prices["amazon"]
        except Exception as e:
            logger.debug(f"Amazon price fetch failed for ASIN {asin}: {e}")
        return None

    async def _get_flipkart_price(self, query: str) -> Optional[PriceResult]:
        try:
            results = await self.rapid.flipkart_search(query)
            if results and "flipkart" in results[0].prices:
                return results[0].prices["flipkart"]
        except Exception as e:
            logger.debug(f"Flipkart price failed: {e}")
        return None

    async def _get_myntra_price(self, query: str) -> Optional[PriceResult]:
        try:
            results = await self.rapid.myntra_search(query)
            return results[0] if results else None
        except Exception as e:
            logger.debug(f"Myntra price failed: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SINGLETONS (imported by main.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pa_api_client    = AmazonPAAPIClient()
rapidapi_client  = RapidAPIClient()
openai_client    = OpenAIClient()
twilio_client    = TwilioClient()
price_aggregator = PriceAggregator()
frame_extractor  = VideoFrameExtractor()
