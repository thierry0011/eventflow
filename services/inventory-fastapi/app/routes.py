"""
HTTP routes.

FastAPI's dependency injection is the headline feature. `Depends(get_session)`
means the framework builds a session, injects it, and tears it down — including
on exceptions. The same mechanism provides auth, pagination, rate limiting and
anything else cross-cutting, and each dependency is independently testable and
independently overridable in tests (`app.dependency_overrides`).

Note the explicit `response_model=` and `status_code=` on every route. They are
not decoration: `response_model` FILTERS the response, so an internal field can
never leak even if your service returns an object that has one.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from . import service
from .cache import cache_delete, cache_get, cache_negative, cache_set, product_key
from .db import get_session
from .deps import RateLimit, require_auth
from .models import Product
from .schemas import (
    ErrorResponse,
    PageMeta,
    PriceRequest,
    PriceResponse,
    ProductCreate,
    ProductOut,
    ProductPage,
    RestockIn,
)

router = APIRouter(prefix="/api/v1", tags=["inventory"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.get("/products", response_model=ProductPage)
async def list_products(
    session: SessionDep,
    _: Annotated[None, Depends(RateLimit(per_minute=300))],
    cursor: str | None = Query(None, description="SKU to continue after"),
    limit: int = Query(25, ge=1, le=100),
    in_stock_only: bool = Query(False),
):
    """
    Keyset (cursor) pagination.

        WHERE sku > :cursor ORDER BY sku LIMIT :n

    Constant time at any depth because it uses the primary-key index, and it
    never skips or repeats a row when data changes between pages. Compare with
    OFFSET, which gets linearly slower and shifts under concurrent inserts —
    the same lesson as the Django service's pagination.py, in a different
    framework. The pattern is the knowledge; the framework is the syntax.
    """
    query = select(Product).order_by(Product.sku).limit(limit + 1)
    # limit + 1: fetch one extra row purely to answer "is there a next page?"
    # without running a second COUNT query over the whole table.
    if cursor:
        query = query.where(Product.sku > cursor)
    if in_stock_only:
        query = query.where(Product.quantity_on_hand > Product.quantity_reserved)

    rows = (await session.scalars(query)).all()
    has_more = len(rows) > limit
    rows = rows[:limit]

    return ProductPage(
        items=[ProductOut.model_validate(r) for r in rows],
        meta=PageMeta(next_cursor=rows[-1].sku if (rows and has_more) else None,
                      has_more=has_more),
    )


@router.get("/products/{sku}", response_model=ProductOut,
            responses={404: {"model": ErrorResponse}})
async def get_product(sku: str, session: SessionDep):
    """
    CACHE-ASIDE, end to end. Read this with cache.py open.

    Note what is cached: the DISPLAY view of a product. The reservation path in
    service.py never touches this cache — it reads the row under a lock. Cache
    for display, never for decisions.
    """
    key = product_key(sku)

    cached = await cache_get(key)
    if cached == "__MISSING__":
        raise HTTPException(404, detail="product not found")   # negative cache hit
    if cached is not None:
        return ProductOut(**cached)

    product = await session.get(Product, sku)
    if product is None:
        await cache_negative(key)   # stops repeated misses from hammering the DB
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="product not found")

    payload = ProductOut.model_validate(product)
    await cache_set(key, payload.model_dump(mode="json"))
    return payload


@router.post("/products", response_model=ProductOut,
             status_code=status.HTTP_201_CREATED,
             dependencies=[Depends(require_auth)])
async def create_product(body: ProductCreate, session: SessionDep):
    product = Product(**body.model_dump())
    session.add(product)
    await session.flush()
    await cache_delete(product_key(product.sku))   # invalidate the negative entry
    return ProductOut.model_validate(product)


@router.post("/products/{sku}/restock", response_model=ProductOut,
             dependencies=[Depends(require_auth)])
async def restock_product(sku: str, body: RestockIn, session: SessionDep):
    try:
        product = await service.restock(session, sku, body.quantity)
    except service.UnknownSku:
        raise HTTPException(404, detail="product not found") from None
    await session.flush()
    await cache_delete(product_key(sku))   # DELETE-ON-WRITE, on every write path
    return ProductOut.model_validate(product)


@router.post("/products/prices", response_model=PriceResponse)
async def get_prices(body: PriceRequest, session: SessionDep, request: Request):
    """
    Called synchronously by the orders service while creating an order.

    An internal, read-only, bounded endpoint — the kind of synchronous call
    that is legitimate in an event-driven system. See the long comment in the
    orders service's `fetch_prices` for why this one is acceptable and why a
    synchronous "reserve stock" would not be.
    """
    prices = await service.get_prices(session, body.skus)
    return PriceResponse(prices=prices)


@router.get("/reservations/{order_id}")
async def get_reservations(order_id: str, session: SessionDep):
    from .models import Reservation

    rows = (await session.scalars(
        select(Reservation).where(Reservation.order_id == order_id)
    )).all()
    return {"order_id": order_id, "reservations": [
        {"sku": r.sku, "quantity": r.quantity, "status": r.status,
         "expires_at": r.expires_at.isoformat()} for r in rows
    ]}
