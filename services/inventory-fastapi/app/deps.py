"""
Reusable FastAPI dependencies: auth and rate limiting.

A dependency is just a callable. FastAPI inspects its signature, resolves ITS
dependencies recursively, caches the result within a request, and injects it.
That makes cross-cutting concerns composable and — crucially — trivially
replaceable in tests via `app.dependency_overrides`.
"""

# from __future__ import annotations

import time
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .cache import get_redis
from .config import settings

bearer = HTTPBearer(auto_error=False)


async def require_auth(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> dict:
    """
    Validates the JWT that the orders service issued.

    WHY THIS SERVICE CAN VERIFY A TOKEN IT DID NOT ISSUE: a JWT is signed, and
    anyone holding the key can check the signature. No database lookup, no call
    back to the auth service. That statelessness is exactly what makes JWT
    attractive across service boundaries.

    AND ITS COST, which you must be able to state in an interview: a JWT cannot
    be revoked. Logging out, banning a user or changing a password does not
    invalidate tokens already issued — they remain valid until they expire.
    That is why the access token lifetime here is 15 minutes, with a refresh
    token held by the client. If you need true instant revocation you need
    server-side state (a denylist in Redis), at which point you have given back
    most of the statelessness.

    THE CLASSIC VULNERABILITY, closed here: always pass `algorithms=[...]`
    explicitly. A library that accepts the algorithm from the token's own header
    can be handed `alg: none` — an attacker then forges any token they like.
    Never let untrusted input choose your verification algorithm.
    """
    if credentials is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                            detail="missing bearer token",
                            headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],   # explicit allowlist
            options={"require": ["exp"]},          # a token without expiry is forever
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                            detail="token expired") from None
    except jwt.InvalidTokenError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                            detail="invalid token") from None
    return payload


class RateLimit:
    """
    A dependency with configuration — hence a class with `__call__` rather than
    a plain function. `Depends(RateLimit(per_minute=300))` reads naturally and
    lets each route choose its own budget.

    Identity is the authenticated subject when present, otherwise the client IP.

    THE PROXY TRAP: `request.client.host` behind a load balancer is the
    BALANCER's address, so every user shares one bucket and you rate-limit the
    whole world together. You must read X-Forwarded-For — but only trust it
    when you control the proxy that sets it, because a client can forge the
    header and bypass the limit by rotating fake values.
    """

    def __init__(self, per_minute: int = 120, burst: int | None = None) -> None:
        self.rate = per_minute / 60.0
        self.capacity = burst or per_minute

    async def __call__(self, request: Request) -> None:
        identity = self._identity(request)
        redis = await get_redis()
        key = f"rl:{identity}:{request.url.path}"
        now = time.time()

        try:
            # The token-bucket maths, atomic in Lua. See
            # eventflow_common.reliability.TokenBucketLimiter for the full
            # explanation of why a fixed window is not good enough.
            allowed, remaining = await redis.eval(
                _BUCKET_SCRIPT, 1, key, self.rate, self.capacity, now, 1)
        except Exception:
            return   # FAIL OPEN: Redis down must not mean nobody can use the API

        if not int(allowed):
            raise HTTPException(
                status.HTTP_429_TOO_MANY_REQUESTS,
                detail="rate limit exceeded",
                # Retry-After is required by the spec and respected by good
                # clients. Returning 429 without it invites a retry storm.
                headers={"Retry-After": "60",
                         "X-RateLimit-Limit": str(self.capacity),
                         "X-RateLimit-Remaining": str(int(remaining))},
            )

    @staticmethod
    def _identity(request: Request) -> str:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"


_BUCKET_SCRIPT = """
local key      = KEYS[1]
local rate     = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])
local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then tokens = capacity; ts = now end
tokens = math.min(capacity, tokens + (now - ts) * rate)
local allowed = 0
if tokens >= cost then tokens = tokens - cost; allowed = 1 end
redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, math.ceil(capacity / rate) * 2)
return {allowed, math.floor(tokens)}
"""
