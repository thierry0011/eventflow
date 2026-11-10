"""
Inventory business logic.

This file contains the single most important piece of code in the repo for
understanding correctness under concurrency: `reserve_stock`.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from eventflow_common import EventType, get_logger, new_event
from eventflow_common.observability import correlation_id_var
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .models import (
    OutboxMessage,
    ProcessedEvent,
    Product,
    Reservation,
    ReservationStatus,
)

log = get_logger(__name__)
PRODUCER = "inventory-service"


class InsufficientStock(Exception):
    def __init__(self, unavailable: list[str]) -> None:
        super().__init__(f"insufficient stock for: {', '.join(unavailable)}")
        self.unavailable = unavailable


class UnknownSku(Exception):
    def __init__(self, skus: list[str]) -> None:
        super().__init__(f"unknown SKUs: {', '.join(skus)}")
        self.skus = skus


# ===========================================================================
# THE RESERVATION — read this one slowly
# ===========================================================================
async def reserve_stock(session: AsyncSession, *, order_id: str,
                        lines: list[dict], causation_id: str | None = None) -> Reservation | None:
    """
    Reserve stock for an order, atomically, and emit the result event.

    THE RACE THIS PREVENTS. One unit left; two orders arrive in the same
    millisecond on two different replicas:

        replica A: SELECT available -> 1   "fine, I'll take it"
        replica B: SELECT available -> 1   "fine, I'll take it"
        replica A: UPDATE reserved = 1
        replica B: UPDATE reserved = 2     <- oversold. One customer gets a
                                              cancellation email next week.

    This is a read-modify-write race, and it is the oldest bug in commerce.

    THE FIX, in three layers, each catching what the one above might miss:

      1. SELECT ... FOR UPDATE  (pessimistic row lock)
         Replica B blocks at the SELECT until A commits, then re-reads and sees
         available = 0. The window is closed, not narrowed.

      2. ORDER BY sku on the locking query  (DEADLOCK PREVENTION)
         Order 1 wants (A, B). Order 2 wants (B, A). Each locks its first row
         and waits forever for the other — a deadlock. Postgres kills one after
         a second, but you have lost a request and gained a confusing log line.
         Acquiring locks in a CONSISTENT GLOBAL ORDER makes deadlock
         impossible. Any total order works as long as everyone uses the same
         one; alphabetical by primary key is easy to remember and audit.

      3. CHECK CONSTRAINTS in the schema  (last line of defence)
         `quantity_reserved <= quantity_on_hand` is enforced by Postgres. If a
         future refactor loses the lock, the database rejects the write instead
         of silently overselling. Application logic is where bugs live;
         constraints are where truth lives.

    And note what is NOT used here: the Redis distributed lock from the shared
    library. A Redis lock is fine for "do not run this cron twice"; it is NOT
    adequate for money or stock, because under a partition two holders can
    believe they own it. When correctness is non-negotiable, use the database's
    own locking — it is in the same transaction as the write it protects.
    """
    skus = [line["sku"] for line in lines]
    wanted = {line["sku"]: int(line["quantity"]) for line in lines}

    # Idempotency first: a redelivered order.created must not reserve twice.
    existing = await session.scalar(
        select(Reservation).where(Reservation.order_id == uuid.UUID(order_id),
                                  Reservation.status == ReservationStatus.HELD)
    )
    if existing is not None:
        log.info("reservation already exists, skipping", extra={"order_id": order_id})
        return existing

    products = (await session.scalars(
        select(Product)
        .where(Product.sku.in_(skus))
        .order_by(Product.sku)        # layer 2: deterministic lock order
        .with_for_update()            # layer 1: pessimistic lock
    )).all()

    found = {p.sku: p for p in products}
    missing = [s for s in skus if s not in found]
    if missing:
        await _reject(session, order_id, f"unknown SKUs: {', '.join(missing)}",
                      missing, causation_id)
        raise UnknownSku(missing)

    short = [sku for sku, qty in wanted.items() if found[sku].available < qty]
    if short:
        await _reject(session, order_id, "insufficient stock", short, causation_id)
        raise InsufficientStock(short)

    expires_at = datetime.now(timezone.utc) + timedelta(
        seconds=settings.reservation_ttl_seconds)
    reservation_id = uuid.uuid4()

    for sku, qty in wanted.items():
        found[sku].quantity_reserved += qty
        found[sku].version += 1
        session.add(Reservation(
            id=reservation_id if sku == skus[0] else uuid.uuid4(),
            order_id=uuid.UUID(order_id), sku=sku, quantity=qty,
            status=ReservationStatus.HELD, expires_at=expires_at,
        ))

    # Same transaction, so the stock change and the announcement of it commit
    # together. This is the outbox pattern again, from the other side.
    _enqueue(session, EventType.INVENTORY_RESERVED, order_id, {
        "order_id": order_id,
        "reservation_id": str(reservation_id),
        "expires_at": expires_at.isoformat(),
        "lines": [{"sku": s, "quantity": q} for s, q in wanted.items()],
    }, causation_id)

    log.info("stock reserved", extra={"order_id": order_id,
                                      "reservation_id": str(reservation_id),
                                      "skus": list(wanted)})
    return await session.scalar(
        select(Reservation).where(Reservation.id == reservation_id))


async def release_reservation(session: AsyncSession, *, order_id: str,
                              reason: str = "order cancelled",
                              causation_id: str | None = None) -> int:
    """
    The COMPENSATING ACTION. Called when an order is cancelled.

    Compensation is not rollback. The reservation genuinely happened and was
    genuinely committed; releasing it is a new forward transaction that undoes
    its effect. The audit trail keeps both facts, which is what an auditor and
    a support engineer both need.

    Idempotent by construction: only rows still in HELD are touched, so a
    redelivered order.cancelled finds nothing to do and succeeds quietly.
    """
    reservations = (await session.scalars(
        select(Reservation)
        .where(Reservation.order_id == uuid.UUID(order_id),
               Reservation.status == ReservationStatus.HELD)
        .order_by(Reservation.sku)
        .with_for_update()
    )).all()

    if not reservations:
        return 0

    for reservation in reservations:
        product = await session.get(Product, reservation.sku, with_for_update=True)
        if product is not None:
            # max(0, ...) is belt-and-braces against a negative value that the
            # CHECK constraint would reject anyway — fail closed, not loudly.
            product.quantity_reserved = max(0, product.quantity_reserved
                                            - reservation.quantity)
            product.version += 1
        reservation.status = ReservationStatus.RELEASED

    _enqueue(session, EventType.INVENTORY_RELEASED, order_id, {
        "order_id": order_id, "reason": reason,
        "released": [{"sku": r.sku, "quantity": r.quantity} for r in reservations],
    }, causation_id)

    log.info("reservation released", extra={"order_id": order_id,
                                            "count": len(reservations),
                                            "reason": reason})
    return len(reservations)


async def expire_stale_reservations(session: AsyncSession) -> int:
    """
    Sweeper for reservations whose TTL has passed without a confirmation.

    Without this, every abandoned order permanently removes stock from sale.
    Any resource you hold on behalf of an in-flight workflow needs an expiry
    AND a sweeper that enforces it — the expiry column alone does nothing.
    """
    now = datetime.now(timezone.utc)
    stale = (await session.scalars(
        select(Reservation)
        .where(Reservation.status == ReservationStatus.HELD,
               Reservation.expires_at < now)
        .order_by(Reservation.sku)
        .with_for_update(skip_locked=True)
        .limit(500)
    )).all()

    for reservation in stale:
        product = await session.get(Product, reservation.sku, with_for_update=True)
        if product is not None:
            product.quantity_reserved = max(0, product.quantity_reserved
                                            - reservation.quantity)
        reservation.status = ReservationStatus.RELEASED

    if stale:
        log.warning("expired stale reservations", extra={"count": len(stale)})
    return len(stale)


async def restock(session: AsyncSession, sku: str, quantity: int) -> Product:
    """
    Demonstrates an ATOMIC UPDATE instead of read-modify-write.

        UPDATE products SET quantity_on_hand = quantity_on_hand + :n WHERE ...

    The increment happens inside the database, so there is no window between
    reading and writing and no lock needed. When an operation can be expressed
    as a single relative UPDATE, prefer it — it is both faster and safer than
    anything you can do in Python.

    (You cannot use this trick for the reservation above, because that needs a
    CONDITIONAL change: "only if enough is available". That condition requires
    reading the current value and deciding, hence the lock.)
    """
    result = await session.execute(
        update(Product)
        .where(Product.sku == sku)
        .values(quantity_on_hand=Product.quantity_on_hand + quantity,
                version=Product.version + 1)
        .returning(Product)
    )
    product = result.scalar_one_or_none()
    if product is None:
        raise UnknownSku([sku])

    _enqueue(session, EventType.PRODUCT_RESTOCKED, sku,
             {"sku": sku, "quantity_added": quantity,
              "quantity_on_hand": product.quantity_on_hand})
    return product


async def get_prices(session: AsyncSession, skus: list[str]) -> dict[str, Decimal]:
    rows = await session.execute(
        select(Product.sku, Product.unit_price).where(Product.sku.in_(skus))
    )
    return {sku: price for sku, price in rows.all()}


# ===========================================================================
# helpers
# ===========================================================================
def _enqueue(session: AsyncSession, event_type: EventType, aggregate_id: str,
             payload: dict, causation_id: str | None = None) -> None:
    event = new_event(event_type, aggregate_id, payload, PRODUCER,
                      correlation_id=correlation_id_var.get(),
                      causation_id=causation_id)
    session.add(OutboxMessage(
        event_id=uuid.UUID(event.event_id), event_type=event.event_type,
        aggregate_id=event.aggregate_id, topic=settings.kafka_topic_inventory,
        payload=event.to_dict(), correlation_id=event.correlation_id,
    ))


async def _reject(session: AsyncSession, order_id: str, reason: str,
                  skus: list[str], causation_id: str | None) -> None:
    _enqueue(session, EventType.INVENTORY_REJECTED, order_id, {
        "order_id": order_id, "reason": reason, "unavailable_skus": skus,
    }, causation_id)
    log.info("reservation rejected", extra={"order_id": order_id, "reason": reason})


async def already_processed(session: AsyncSession, event) -> bool:
    """
    Idempotency guard, inside the caller's transaction.

    `flush()` sends the INSERT to the database NOW so the unique violation
    surfaces here, instead of at commit time where it would already have been
    mixed with the business writes. `begin_nested()` wraps it in a SAVEPOINT so
    the violation rolls back only this insert, not the whole transaction.
    """
    try:
        async with session.begin_nested():
            session.add(ProcessedEvent(event_id=uuid.UUID(event.event_id),
                                       event_type=event.event_type))
            await session.flush()
        return False
    except IntegrityError:
        log.info("duplicate event ignored", extra={"event_id": event.event_id})
        return True
