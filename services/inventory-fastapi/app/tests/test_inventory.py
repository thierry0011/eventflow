"""
Tests for the inventory service.

Demonstrates async testing with pytest-asyncio, FastAPI dependency overrides,
and — most importantly — an actual concurrency test.
"""

from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal

import pytest
import pytest_asyncio
from eventflow_common import new_event
from eventflow_common.envelope import EventType
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app import service
from app.db import Base, get_session
from app.main import app
from app.models import OutboxMessage, Product, Reservation, ReservationStatus

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def session():
    """
    An in-memory SQLite database per test.

    THE TRADE-OFF, stated honestly because it matters: SQLite is fast and needs
    no container, but it is NOT Postgres. It ignores `SELECT FOR UPDATE`, has
    weaker type affinity, and does not support JSONB. So these tests verify
    LOGIC, and the concurrency and constraint behaviour must additionally be
    verified against real Postgres — which is what the `integration` marker and
    `make test` inside the compose stack are for.

    Testing exclusively against SQLite and shipping to Postgres is a well-known
    way to be surprised in production. Know what your test double does not
    model.
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        yield s
    await engine.dispose()


@pytest_asyncio.fixture
async def product(session):
    p = Product(sku="SKU-A", name="Widget", unit_price=Decimal("10.00"),
                quantity_on_hand=5)
    session.add(p)
    await session.commit()
    return p


@pytest_asyncio.fixture
async def client(session):
    """
    `dependency_overrides` replaces the real session dependency with the test
    one. This is FastAPI's killer testing feature: no monkeypatching, no import
    gymnastics, just swap the dependency.
    """
    async def override():
        yield session

    app.dependency_overrides[get_session] = override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


# ------------------------------------------------------------- reservation
async def test_reserving_available_stock_succeeds_and_emits_an_event(session, product):
    order_id = str(uuid.uuid4())
    await service.reserve_stock(session, order_id=order_id,
                                lines=[{"sku": "SKU-A", "quantity": 2}])
    await session.commit()

    await session.refresh(product)
    assert product.quantity_reserved == 2
    assert product.available == 3

    outbox = await session.scalar(select(OutboxMessage))
    assert outbox.event_type == EventType.INVENTORY_RESERVED.value


async def test_reserving_more_than_available_is_rejected(session, product):
    order_id = str(uuid.uuid4())
    with pytest.raises(service.InsufficientStock):
        await service.reserve_stock(session, order_id=order_id,
                                    lines=[{"sku": "SKU-A", "quantity": 99}])
    await session.commit()

    await session.refresh(product)
    assert product.quantity_reserved == 0        # nothing was taken

    # THE SUBTLE PART: the rejection EVENT must still have been written, even
    # though the operation "failed". Without it the order waits in PENDING
    # until the saga timeout — a bad customer experience caused by treating a
    # business outcome as an exception to be swallowed.
    outbox = await session.scalar(select(OutboxMessage))
    assert outbox.event_type == EventType.INVENTORY_REJECTED.value
    assert "SKU-A" in outbox.payload["payload"]["unavailable_skus"]


async def test_reserving_twice_for_one_order_is_idempotent(session, product):
    order_id = str(uuid.uuid4())
    lines = [{"sku": "SKU-A", "quantity": 2}]

    await service.reserve_stock(session, order_id=order_id, lines=lines)
    await session.commit()
    await service.reserve_stock(session, order_id=order_id, lines=lines)
    await session.commit()

    await session.refresh(product)
    assert product.quantity_reserved == 2        # NOT 4
    count = await session.scalar(
        select(Reservation).where(Reservation.order_id == uuid.UUID(order_id)))
    assert count is not None


async def test_releasing_returns_the_stock(session, product):
    order_id = str(uuid.uuid4())
    await service.reserve_stock(session, order_id=order_id,
                                lines=[{"sku": "SKU-A", "quantity": 3}])
    await session.commit()

    released = await service.release_reservation(session, order_id=order_id)
    await session.commit()

    assert released == 1
    await session.refresh(product)
    assert product.quantity_reserved == 0
    assert product.available == 5


async def test_releasing_twice_is_harmless(session, product):
    order_id = str(uuid.uuid4())
    await service.reserve_stock(session, order_id=order_id,
                                lines=[{"sku": "SKU-A", "quantity": 1}])
    await session.commit()

    assert await service.release_reservation(session, order_id=order_id) == 1
    await session.commit()
    assert await service.release_reservation(session, order_id=order_id) == 0
    await session.commit()

    await session.refresh(product)
    assert product.quantity_reserved == 0        # not -1


async def test_expired_reservations_are_swept(session, product):
    from datetime import datetime, timedelta, timezone

    order_id = uuid.uuid4()
    session.add(Reservation(
        id=uuid.uuid4(), order_id=order_id, sku="SKU-A", quantity=2,
        status=ReservationStatus.HELD,
        expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    ))
    product.quantity_reserved = 2
    await session.commit()

    assert await service.expire_stale_reservations(session) == 1
    await session.commit()

    await session.refresh(product)
    assert product.quantity_reserved == 0


# -------------------------------------------------------------------- API
async def test_get_product_returns_the_available_count(client, product):
    response = await client.get("/api/v1/products/SKU-A")
    assert response.status_code == 200
    assert response.json()["available"] == 5


async def test_unknown_product_is_404(client):
    assert (await client.get("/api/v1/products/NOPE")).status_code == 404


async def test_invalid_sku_format_is_rejected_at_the_boundary(client):
    response = await client.post("/api/v1/products", json={
        "sku": "lowercase bad", "name": "x", "unit_price": "1.00",
        "quantity_on_hand": 1,
    })
    assert response.status_code in (401, 422)
    # The regex in ProductCreate rejects it before a single line of business
    # logic runs. Validation at the boundary means the inside never has to ask.


async def test_pagination_returns_a_cursor(client, session):
    for i in range(30):
        session.add(Product(sku=f"SKU-{i:03d}", name=f"p{i}",
                            unit_price=Decimal("1.00"), quantity_on_hand=1))
    await session.commit()

    page = (await client.get("/api/v1/products?limit=10")).json()
    assert len(page["items"]) == 10
    assert page["meta"]["has_more"] is True

    second = (await client.get(
        f"/api/v1/products?limit=10&cursor={page['meta']['next_cursor']}")).json()
    assert second["items"][0]["sku"] > page["items"][-1]["sku"]   # no overlap


# ------------------------------------------------------------- concurrency
@pytest.mark.integration
async def test_concurrent_reservations_cannot_oversell():
    """
    THE TEST THAT MATTERS, and the one almost nobody writes.

    Marked `integration` because it requires REAL POSTGRES: SQLite does not
    implement SELECT ... FOR UPDATE, so under SQLite this test would pass for
    the wrong reason and give you false confidence in code that oversells.

    Run with:  pytest -m integration   (inside `make up`)

    It fires 10 concurrent reservations at a product with 5 units. Exactly 5
    must succeed. If the row lock were missing, you would see 6-10 succeed and
    `quantity_reserved` exceed `quantity_on_hand` — until the CHECK constraint
    caught it, which is precisely why the constraint is there.
    """
    from app.config import settings

    engine = create_async_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    sku = f"SKU-RACE-{uuid.uuid4().hex[:8]}"
    async with factory() as s:
        async with s.begin():
            s.add(Product(sku=sku, name="contended", unit_price=Decimal("1.00"),
                          quantity_on_hand=5))

    async def attempt() -> bool:
        async with factory() as s:
            try:
                async with s.begin():
                    await service.reserve_stock(
                        s, order_id=str(uuid.uuid4()),
                        lines=[{"sku": sku, "quantity": 1}])
                return True
            except service.InsufficientStock:
                return False

    results = await asyncio.gather(*[attempt() for _ in range(10)])
    assert sum(results) == 5, f"expected exactly 5 winners, got {sum(results)}"

    async with factory() as s:
        product = await s.get(Product, sku)
        assert product.quantity_reserved == 5
        assert product.available == 0

    await engine.dispose()
