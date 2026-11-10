"""
FastAPI application.

THE LIFESPAN CONTEXT MANAGER is the modern replacement for @app.on_event, and
it exists for a reason worth understanding: startup and shutdown for a given
resource belong in ONE place, so you cannot add a connection pool and forget to
close it. Everything before `yield` runs at boot; everything after runs on
shutdown. If startup raises, the process exits instead of serving traffic in a
half-initialised state.
"""

from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager

from eventflow_common import get_logger, setup_logging
from eventflow_common.observability import (
    HTTP_REQUESTS,
    HTTP_SECONDS,
    correlation_id_var,
)
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy import text

from .cache import close_redis, get_redis
from .config import settings
from .db import dispose_engine, engine
from .routes import router

setup_logging(settings.service_name)
log = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    log.info("starting up", extra={"environment": settings.environment})
    await get_redis()                    # fail fast if Redis is unreachable
    async with engine.begin() as conn:   # and if Postgres is
        await conn.execute(text("SELECT 1"))
    log.info("dependencies reachable, accepting traffic")

    yield          # <-- the application serves requests here

    log.info("shutting down")
    await close_redis()
    await dispose_engine()
    log.info("shutdown complete")


app = FastAPI(
    title="EventFlow Inventory Service",
    version="1.0.0",
    description="Stock, reservations, and the compensating release path.",
    lifespan=lifespan,
    # Interactive docs are free, derived from the Pydantic models, and always
    # accurate. Turn them OFF in production: they publish your entire internal
    # API surface to anyone who finds the URL.
    docs_url="/docs" if settings.environment == "local" else None,
    redoc_url=None,
    openapi_url="/openapi.json" if settings.environment == "local" else None,
)

app.include_router(router)


# ---------------------------------------------------------------- middleware
@app.middleware("http")
async def correlation_and_metrics(request: Request, call_next):
    """
    One middleware doing both jobs, because each `@app.middleware` wraps the
    whole stack and adds latency. Keep the chain short in an async service.
    """
    incoming = request.headers.get("x-correlation-id", "")
    incoming = "".join(c for c in incoming if c.isalnum() or c in "-_")[:64]
    correlation_id = incoming or str(uuid.uuid4())
    token = correlation_id_var.set(correlation_id)

    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Correlation-ID"] = correlation_id
        return response
    finally:
        elapsed = time.perf_counter() - started
        # The ROUTE TEMPLATE, never the raw path — bounded label cardinality.
        route = request.scope.get("route")
        path = getattr(route, "path", "unmatched")
        HTTP_REQUESTS.labels(settings.service_name, request.method, path,
                             str(status_code)).inc()
        HTTP_SECONDS.labels(settings.service_name, request.method, path).observe(elapsed)
        correlation_id_var.reset(token)


# ------------------------------------------------------------ error handling
@app.exception_handler(RequestValidationError)
async def validation_error_handler(_request: Request, exc: RequestValidationError):
    """
    Reshape FastAPI's default 422 into the same error envelope the orders
    service uses. CONSISTENCY ACROSS SERVICES IS A FEATURE: a client should not
    need per-service error parsing just because you chose different frameworks.
    """
    return JSONResponse(status_code=422, content={
        "error": "validation_error",
        "detail": "Request validation failed.",
        "fields": [{"field": ".".join(str(p) for p in e["loc"][1:]),
                    "message": e["msg"]} for e in exc.errors()],
        "correlation_id": correlation_id_var.get(),
    })


@app.exception_handler(Exception)
async def unhandled_error_handler(_request: Request, exc: Exception):
    log.exception("unhandled exception", extra={"error_type": type(exc).__name__})
    return JSONResponse(status_code=500, content={
        "error": "internal_error",
        "detail": "An unexpected error occurred.",
        "correlation_id": correlation_id_var.get(),
    })
    # Never return str(exc) to a client. Exception text routinely contains
    # table names, SQL fragments, file paths and occasionally credentials.


# ------------------------------------------------------------------- health
@app.get("/health/live", include_in_schema=False)
async def liveness():
    """No dependency checks. See the long explanation in the Django service's
    health.py — a liveness probe that checks the database will restart every
    replica during a database blip."""
    return {"status": "alive", "service": settings.service_name}


@app.get("/health/ready", include_in_schema=False)
async def readiness():
    checks: dict[str, str] = {}
    ok = True
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"
        ok = False
    try:
        redis = await get_redis()
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"   # degraded, not fatal: cache only
    return JSONResponse({"status": "ready" if ok else "not_ready", "checks": checks},
                        status_code=200 if ok else 503)


@app.get("/metrics", include_in_schema=False)
async def metrics():
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
