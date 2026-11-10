"""
Redis caching.

Four things every backend engineer must be able to explain about caching, all
demonstrated below.

1. CACHE-ASIDE (lazy loading) — the default pattern.
       look in cache -> hit? return it
                     -> miss? read the database, write the cache, return it
   The application owns the logic. Simple, resilient (a Redis outage degrades
   to "slow", not "broken"), and the right default. The alternatives —
   read-through, write-through, write-behind — push that logic into the cache
   layer and are worth knowing by name.

2. INVALIDATION — the genuinely hard part.
   "There are only two hard things in computer science: cache invalidation and
   naming things." Three strategies, in increasing difficulty:
       TTL             let it expire. Simple, always correct eventually,
                       serves stale data for up to the TTL.
       write-through   update the cache when you update the database. Fast
                       reads, but the two can diverge if a write fails midway.
       delete-on-write what this file does. On write, DELETE the key rather
                       than updating it. The next read repopulates from the
                       source of truth. Deleting is safer than updating because
                       a failed delete is a cache miss (harmless) while a
                       failed update is stale data (harmful).

3. WHAT NOT TO CACHE. Never cache something whose staleness causes an incorrect
   decision. Stock levels are shown from the cache here, but the RESERVATION
   never reads the cache — it reads the row under a lock. Showing "3 left" when
   there are 2 is a minor annoyance; SELLING the third is a refund and an angry
   customer. Cache for display, never for decisions.

4. THE THREE FAILURE MODES, with their fixes:
       STAMPEDE / dogpile  a hot key expires and 500 concurrent requests all
                           miss and all hit the database at once. Fix: a short
                           lock so one request repopulates while the others
                           wait or serve stale. Implemented below.
       PENETRATION         requests for a key that does not exist bypass the
                           cache every time — a trivial DoS. Fix: cache the
                           negative result too, briefly.
       AVALANCHE           many keys share an expiry time and all die together.
                           Fix: add random jitter to each TTL.
"""

from __future__ import annotations

import json
import random
from typing import Any

import redis.asyncio as aioredis
from eventflow_common import get_logger

from .config import settings

log = get_logger(__name__)

_redis: aioredis.Redis | None = None

NEGATIVE = "__MISSING__"
NEGATIVE_TTL = 30


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            f"{settings.redis_url}/0",
            encoding="utf-8", decode_responses=True,
            socket_connect_timeout=2, socket_timeout=2,
            # TIMEOUTS ON THE CACHE ARE NOT OPTIONAL. Without them, a hung
            # Redis makes every request hang — the cache you added to make
            # things faster becomes the reason the whole service is down.
            health_check_interval=30,
            max_connections=50,
        )
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


def _jittered(ttl: int) -> int:
    """±20% so a batch of keys written together does not expire together."""
    return max(1, int(ttl * random.uniform(0.8, 1.2)))


async def cache_get(key: str) -> Any | None:
    """Returns None on miss, the sentinel NEGATIVE for a cached 'does not exist'."""
    try:
        redis = await get_redis()
        raw = await redis.get(key)
    except Exception as exc:
        # FAIL OPEN. A cache error must degrade to a database read, never to an
        # error response. The cache is an optimisation; treating its failure as
        # fatal converts a performance feature into a liability.
        log.warning("cache read failed, falling through", extra={"key": key,
                                                                 "error": str(exc)})
        return None
    if raw is None:
        return None
    return NEGATIVE if raw == NEGATIVE else json.loads(raw)


async def cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    try:
        redis = await get_redis()
        await redis.set(key, json.dumps(value, default=str),
                        ex=_jittered(ttl or settings.cache_ttl_seconds))
    except Exception as exc:
        log.warning("cache write failed", extra={"key": key, "error": str(exc)})


async def cache_negative(key: str) -> None:
    """Cache 'this does not exist' briefly — stops cache-penetration attacks."""
    try:
        redis = await get_redis()
        await redis.set(key, NEGATIVE, ex=NEGATIVE_TTL)
    except Exception:
        pass


async def cache_delete(*keys: str) -> None:
    """Delete-on-write invalidation. Call this from EVERY write path."""
    if not keys:
        return
    try:
        redis = await get_redis()
        await redis.delete(*keys)
    except Exception as exc:
        log.warning("cache invalidation failed", extra={"keys": keys,
                                                        "error": str(exc)})
        # Worth noting honestly: a failed invalidation serves stale data until
        # the TTL expires. That is why a TTL is kept even on delete-on-write —
        # it is the backstop for exactly this case.


async def acquire_rebuild_lock(key: str, ttl: int = 10) -> bool:
    """
    Stampede protection. Exactly one caller gets True and rebuilds the entry;
    the others see False and fall back (serve stale, or wait briefly).

    Use it only for genuinely expensive rebuilds. For a cheap primary-key
    lookup, the lock costs more than the extra database hits it prevents —
    measure before adding machinery.
    """
    try:
        redis = await get_redis()
        return bool(await redis.set(f"rebuild:{key}", "1", nx=True, ex=ttl))
    except Exception:
        return True   # cache down => everyone rebuilds; correct, just slower


def product_key(sku: str) -> str:
    """
    A NAMESPACED, VERSIONED key.

    The `v1:` prefix is a deploy-safety feature: change the cached shape and
    bump to `v2:`, and old entries are simply never read again. Without it, a
    deploy that changes the serialised shape reads v1 data with v2 code and you
    get a KeyError on every cache hit until the TTL drains.
    """
    return f"inv:v1:product:{sku}"
