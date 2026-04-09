"""
PriceShield v2 — cache.py
In-memory cache that works on Render free tier with zero setup.
Automatically switches to Redis if REDIS_URL is set in environment.
"""
from __future__ import annotations
import json
import time
from typing import Any, Optional
from loguru import logger


# ── In-memory fallback (default on Render free tier) ────────

class _MemoryStore:
    """Simple dict-based cache. Data resets on every redeploy."""
    def __init__(self):
        self._data: dict = {}
        self._ttls: dict = {}

    async def get(self, key: str) -> Optional[bytes]:
        exp = self._ttls.get(key)
        if exp and time.time() > exp:
            self._data.pop(key, None)
            self._ttls.pop(key, None)
            return None
        val = self._data.get(key)
        if val is None:
            return None
        return val.encode() if isinstance(val, str) else val

    async def set(self, key: str, value: Any, ex: int = None):
        self._data[key] = value
        if ex:
            self._ttls[key] = time.time() + ex

    async def delete(self, key: str):
        self._data.pop(key, None)
        self._ttls.pop(key, None)

    async def exists(self, key: str) -> int:
        return 1 if key in self._data else 0

    async def ping(self) -> bool:
        return True

    async def close(self):
        pass


# ── CacheManager ─────────────────────────────────────────────

class CacheManager:
    def __init__(self, store=None):
        self._r = store or _MemoryStore()

    # ── Generic ─────────────────────────────────────────────

    async def get(self, key: str) -> Optional[Any]:
        try:
            raw = await self._r.get(key)
            if raw is None:
                return None
            data = raw.decode() if isinstance(raw, bytes) else raw
            return json.loads(data)
        except Exception as e:
            logger.debug(f"Cache GET error [{key}]: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        try:
            await self._r.set(key, json.dumps(value, default=str), ex=ttl)
            return True
        except Exception as e:
            logger.debug(f"Cache SET error [{key}]: {e}")
            return False

    async def delete(self, key: str) -> bool:
        try:
            await self._r.delete(key)
            return True
        except Exception as e:
            logger.debug(f"Cache DELETE error [{key}]: {e}")
            return False

    async def ping(self) -> bool:
        try:
            return await self._r.ping()
        except Exception:
            return False

    # ── OTP helpers ─────────────────────────────────────────

    async def store_otp(self, phone: str, otp: str, attempts: int = 0) -> bool:
        from datetime import datetime
        data = {"otp": otp, "attempts": attempts, "created_at": datetime.utcnow().isoformat()}
        return await self.set(f"otp:{phone}", data, ttl=300)

    async def get_otp(self, phone: str) -> Optional[dict]:
        return await self.get(f"otp:{phone}")

    async def increment_otp_attempts(self, phone: str) -> int:
        data = await self.get_otp(phone)
        if not data:
            return 0
        data["attempts"] = data.get("attempts", 0) + 1
        await self.set(f"otp:{phone}", data, ttl=300)
        return data["attempts"]

    async def delete_otp(self, phone: str) -> bool:
        return await self.delete(f"otp:{phone}")

    # ── Session helpers ──────────────────────────────────────

    async def store_session(self, token: str, user_data: dict) -> bool:
        return await self.set(f"session:{token}", user_data, ttl=604800)

    async def get_session(self, token: str) -> Optional[dict]:
        return await self.get(f"session:{token}")

    async def delete_session(self, token: str) -> bool:
        return await self.delete(f"session:{token}")

    # ── Price cache ──────────────────────────────────────────

    async def get_prices(self, product_id: str, mode: str = "new") -> Optional[dict]:
        return await self.get(f"prices:{product_id}:{mode}")

    async def set_prices(self, product_id: str, mode: str, prices: dict) -> bool:
        return await self.set(f"prices:{product_id}:{mode}", prices, ttl=1800)

    # ── Search cache ─────────────────────────────────────────

    async def get_search(self, query: str, mode: str) -> Optional[list]:
        key = query.lower().strip().replace(" ", "_")[:80]
        return await self.get(f"search:{key}:{mode}")

    async def set_search(self, query: str, mode: str, results: list) -> bool:
        key = query.lower().strip().replace(" ", "_")[:80]
        return await self.set(f"search:{key}:{mode}", results, ttl=900)

    # ── Safety cache ─────────────────────────────────────────

    async def get_safety(self, product_name: str) -> Optional[dict]:
        key = product_name.lower().strip().replace(" ", "_")[:80]
        return await self.get(f"safety:{key}")

    async def set_safety(self, product_name: str, record: dict) -> bool:
        key = product_name.lower().strip().replace(" ", "_")[:80]
        return await self.set(f"safety:{key}", record, ttl=86400)

    # ── Affiliate stats ──────────────────────────────────────

    async def get_affiliate_stats(self, user_id: str) -> Optional[dict]:
        return await self.get(f"aff_stats:{user_id}")

    async def set_affiliate_stats(self, user_id: str, stats: dict) -> bool:
        return await self.set(f"aff_stats:{user_id}", stats, ttl=3600)

    async def increment_affiliate_clicks(self, user_id: str):
        stats = await self.get_affiliate_stats(user_id) or {"clicks": 0, "conversions": 0, "earned": 0.0}
        stats["clicks"] = stats.get("clicks", 0) + 1
        await self.set_affiliate_stats(user_id, stats)

    # ── Rate limiting ────────────────────────────────────────

    async def check_rate_limit(self, ip: str, endpoint: str, max_req: int, window_sec: int) -> bool:
        key     = f"rl:{endpoint}:{ip}"
        current = await self.get(key)
        count   = (current or {}).get("count", 0)
        if count >= max_req:
            return False
        await self.set(key, {"count": count + 1}, window_sec)
        return True


# ── Singleton ─────────────────────────────────────────────────

_cache_instance: Optional[CacheManager] = None


async def get_cache() -> CacheManager:
    """FastAPI dependency — returns the shared CacheManager."""
    global _cache_instance
    if _cache_instance is None:
        import os
        redis_url = os.environ.get("REDIS_URL", "")
        if redis_url:
            try:
                import redis.asyncio as aioredis
                client = await aioredis.from_url(redis_url, encoding="utf-8", decode_responses=False)
                _cache_instance = CacheManager(client)
                logger.info("Cache: connected to Redis")
            except Exception as e:
                logger.warning(f"Redis unavailable ({e}), using in-memory cache")
                _cache_instance = CacheManager()
        else:
            _cache_instance = CacheManager()
            logger.info("Cache: using in-memory store (no REDIS_URL set)")
    return _cache_instance
