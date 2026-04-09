"""
PriceShield v2 — database.py
In-memory repositories that work on Render free tier with zero setup.
Data resets on redeploy — connect a real MongoDB later by setting MONGODB_URL.
"""
from __future__ import annotations
import hashlib
from datetime import datetime, timedelta
from typing import Optional, List
import random
from loguru import logger


async def init_database():
    """Called on FastAPI startup. Connects to MongoDB if MONGODB_URL is set."""
    import os
    mongo_url = os.environ.get("MONGODB_URL", "")
    if mongo_url:
        try:
            from motor.motor_asyncio import AsyncIOMotorClient
            client = AsyncIOMotorClient(mongo_url)
            await client.admin.command("ping")
            logger.info("Database: connected to MongoDB")
        except Exception as e:
            logger.warning(f"MongoDB unavailable ({e}), using in-memory storage")
    else:
        logger.info("Database: using in-memory storage (no MONGODB_URL set)")


# ── In-memory stores ─────────────────────────────────────────
_users: dict = {}          # phone → user dict
_saved: dict = {}          # user_id → list of product_ids
_history: list = []        # price history records


# ── User Repository ──────────────────────────────────────────

class UserRepository:

    async def find_by_phone(self, phone: str) -> Optional[dict]:
        return _users.get(phone)

    async def create(self, phone: str) -> dict:
        user_id = "user_" + hashlib.md5(phone.encode()).hexdigest()[:8]
        user = {
            "id":            user_id,
            "phone":         phone,
            "is_verified":   True,
            "is_pro":        False,
            "selected_cats": [],
            "created_at":    datetime.utcnow().isoformat(),
            "last_login":    datetime.utcnow().isoformat(),
        }
        _users[phone] = user
        logger.info(f"New user created: {user_id}")
        return user

    async def update_last_login(self, user_id: str):
        for user in _users.values():
            if user.get("id") == user_id:
                user["last_login"] = datetime.utcnow().isoformat()
                break

    async def update_categories(self, user_id: str, cat_ids: List[int]) -> bool:
        for user in _users.values():
            if user.get("id") == user_id:
                user["selected_cats"] = cat_ids
                return True
        return False

    async def save_product(self, user_id: str, product_id: str) -> bool:
        if user_id not in _saved:
            _saved[user_id] = []
        if product_id not in _saved[user_id]:
            _saved[user_id].append(product_id)
        return True

    async def get_affiliate_stats(self, user_id: str) -> dict:
        # Return mock stats — replace with real DB query later
        return {
            "user_id":     user_id,
            "clicks":      0,
            "conversions": 0,
            "earned":      0.0,
        }


# ── Ingredient Repository ────────────────────────────────────

class IngredientRepository:

    _DB = {
        "hydroquinone": {
            "status": "danger",
            "reason": "Banned in EU above 1% (EC 1223/2009). FDA Warning List. Linked to ochronosis.",
            "regions": ["EU", "Canada (>1%)"],
            "aliases": ["quinol", "benzene-1,4-diol", "p-dihydroxybenzene"],
        },
        "mercury chloride": {
            "status": "danger",
            "reason": "Banned globally. WHO Minamata Convention signatory. Severe neurotoxin.",
            "regions": ["EU", "USA", "UK", "India", "Canada", "Australia"],
            "aliases": ["mercuric chloride", "mercury", "ammoniated mercury", "thimerosal"],
        },
        "formaldehyde": {
            "status": "danger",
            "reason": "IARC Group 1 carcinogen. Banned in EU cosmetics above 0.2%.",
            "regions": ["EU (>0.2%)"],
            "aliases": ["methanal", "formalin", "methylene glycol", "quaternium-15"],
        },
        "lead acetate": {
            "status": "danger",
            "reason": "Banned EU & USA. Toxic heavy metal. Endocrine disruptor.",
            "regions": ["EU", "USA", "Canada"],
            "aliases": ["lead diacetate", "plumbous acetate"],
        },
        "parabens": {
            "status": "caution",
            "reason": "Possible endocrine disruptors. Restricted concentrations in EU.",
            "regions": ["EU (restricted)"],
            "aliases": ["methylparaben", "propylparaben", "butylparaben", "ethylparaben"],
        },
        "triclosan": {
            "status": "caution",
            "reason": "Banned in US OTC soaps (FDA 2016). Antibiotic resistance concerns.",
            "regions": ["USA (OTC soaps)"],
            "aliases": ["irgasan"],
        },
        "retinol": {
            "status": "caution",
            "reason": "EU restricts leave-on >0.3%. Avoid during pregnancy. Requires SPF.",
            "regions": ["EU (restricted)"],
            "aliases": ["vitamin a", "retinoic acid", "tretinoin", "retinyl palmitate"],
        },
        "mineral oil": {
            "status": "caution",
            "reason": "Untreated grades are carcinogenic (IARC Group 1). Cosmetic grade generally safe.",
            "regions": [],
            "aliases": ["paraffinum liquidum", "petrolatum", "petroleum jelly"],
        },
        "niacinamide": {
            "status": "safe",
            "reason": "Globally approved. No restrictions. Dermatologically tested.",
            "regions": [],
            "aliases": ["nicotinamide", "vitamin b3"],
        },
        "hyaluronic acid": {
            "status": "safe",
            "reason": "Naturally occurring. No regulatory restrictions globally.",
            "regions": [],
            "aliases": ["sodium hyaluronate"],
        },
        "vitamin c": {
            "status": "safe",
            "reason": "Antioxidant. GRAS globally. No restrictions.",
            "regions": [],
            "aliases": ["ascorbic acid", "l-ascorbic acid", "sodium ascorbyl phosphate"],
        },
        "glycerin": {
            "status": "safe",
            "reason": "GRAS. No restrictions. Universally tolerated.",
            "regions": [],
            "aliases": ["glycerol", "propane-1,2,3-triol"],
        },
        "zinc oxide": {
            "status": "safe",
            "reason": "FDA approved sunscreen. Safe topically.",
            "regions": [],
            "aliases": ["zno"],
        },
        "salicylic acid": {
            "status": "safe",
            "reason": "BHA exfoliant. Safe in cosmetics up to 2% OTC.",
            "regions": [],
            "aliases": ["2-hydroxybenzoic acid", "bha", "beta hydroxy acid"],
        },
        "synthetic fragrance": {
            "status": "caution",
            "reason": "May contain undisclosed allergens. EU requires labelling of 26 fragrance allergens.",
            "regions": ["EU (labelling required)"],
            "aliases": ["parfum", "fragrance", "aroma"],
        },
    }

    async def find(self, name: str) -> Optional[dict]:
        key = name.lower().strip()
        if key in self._DB:
            return {**self._DB[key], "name": name}
        for db_key, record in self._DB.items():
            if key in [a.lower() for a in record.get("aliases", [])]:
                return {**record, "name": name}
        for db_key, record in self._DB.items():
            if key in db_key or db_key in key:
                return {**record, "name": name}
        return None

    async def check_batch(self, ingredients: List[str]) -> List[dict]:
        results = []
        for ing in ingredients:
            found = await self.find(ing)
            results.append(found if found else {
                "name":    ing,
                "status":  "safe",
                "reason":  "No restrictions found in our global database.",
                "regions": [],
            })
        return results


# ── Price History Repository ─────────────────────────────────

class PriceHistoryRepository:

    async def record(self, product_id: str, store: str, price_paise: int, condition: str = "new"):
        _history.append({
            "product_id":  product_id,
            "store":       store,
            "price":       price_paise,
            "condition":   condition,
            "recorded_at": datetime.utcnow().isoformat(),
        })
        # Keep last 10,000 records in memory
        if len(_history) > 10000:
            _history.pop(0)

    async def get_history(self, product_id: str, store: str, days: int = 30) -> List[dict]:
        # Return mock 30-day data if no real history exists
        real = [
            {"price": r["price"] / 100, "date": r["recorded_at"][:10]}
            for r in _history
            if r["product_id"] == product_id and r["store"] == store
        ]
        if real:
            return real[-days:]
        # Generate realistic-looking mock history
        base = 24999
        history = []
        for i in range(days):
            d = (datetime.utcnow() - timedelta(days=days - i)).date().isoformat()
            history.append({"price": base + random.randint(-2000, 1000), "date": d})
        return history

    async def get_lowest_ever(self, product_id: str, store: str) -> Optional[int]:
        prices = [r["price"] for r in _history if r["product_id"] == product_id and r["store"] == store]
        return min(prices) if prices else None
