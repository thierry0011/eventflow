"""
SQLAlchemy 2.0 models.

Note the style: `Mapped[int]` with `mapped_column(...)`. This is the 2.0
declarative syntax, and it is a real improvement over `Column(Integer)` — mypy
and your editor now know that `product.available` is an `int`, so a typo is
caught before runtime. If you learned SQLAlchemy from an older tutorial, this
is the part to relearn.

The schema mirrors the Django service's: domain tables plus an outbox plus a
processed-events table. The patterns are framework-independent, which is one of
the lessons of this repo — Django's ORM and SQLAlchemy differ in syntax, not in
what a correct distributed design needs.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Integer,
    JSON,
    ForeignKey,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Product(Base):
    __tablename__ = "products"

    sku: Mapped[str] = mapped_column(String(64), primary_key=True)
    # Natural key. A SKU is already unique, stable and meaningful, so a
    # surrogate id would add a join for nothing. Use a natural key when one
    # genuinely exists and never changes; use a surrogate when it might.

    name: Mapped[str] = mapped_column(String(255))
    unit_price: Mapped[Decimal] = mapped_column(Numeric(12, 2))

    # THE TWO-COUNTER MODEL — the heart of correct inventory.
    #
    #   quantity_on_hand   physically in the warehouse
    #   quantity_reserved  promised to orders that are not yet shipped
    #   available          = on_hand - reserved   (what may still be sold)
    #
    # A single `stock` column cannot express "sold but not yet picked", so two
    # customers can both buy the last unit between the check and the shipment.
    # Reservation is what makes the promise real at the moment it is made.
    quantity_on_hand: Mapped[int] = mapped_column(default=0)
    quantity_reserved: Mapped[int] = mapped_column(default=0)

    version: Mapped[int] = mapped_column(default=0)
    # OPTIMISTIC LOCKING counter. Every write does
    #     UPDATE ... SET version = version + 1 WHERE sku = ? AND version = ?
    # If the row changed since you read it, zero rows match and you retry with
    # fresh data. No locks held, excellent under low contention. Compare with
    # the pessimistic SELECT FOR UPDATE used in service.py for the hot path —
    # knowing when to use which is a senior-level distinction.

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 server_default=func.now(),
                                                 onupdate=utcnow)

    reservations: Mapped[list[Reservation]] = relationship(back_populates="product")

    __table_args__ = (
        # Enforced by the database: stock can never go negative, and you can
        # never reserve more than you physically have. Even a buggy deploy, a
        # migration script or a manual psql session cannot violate this.
        CheckConstraint("quantity_on_hand >= 0", name="ck_on_hand_non_negative"),
        CheckConstraint("quantity_reserved >= 0", name="ck_reserved_non_negative"),
        CheckConstraint("quantity_reserved <= quantity_on_hand",
                        name="ck_reserved_not_exceeding_stock"),
        Index("idx_product_name", "name"),
    )

    @property
    def available(self) -> int:
        return self.quantity_on_hand - self.quantity_reserved


class ReservationStatus:
    HELD = "HELD"
    RELEASED = "RELEASED"
    COMMITTED = "COMMITTED"   # order shipped; stock physically left


class Reservation(Base):
    """
    A time-bounded claim on stock.

    `expires_at` is what stops abandoned carts from starving real customers. A
    reservation with no expiry is a stock leak with good intentions.
    """

    __tablename__ = "reservations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True,
                                          default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    sku: Mapped[str] = mapped_column(ForeignKey("products.sku"), index=True)
    quantity: Mapped[int] = mapped_column()
    status: Mapped[str] = mapped_column(String(16), default=ReservationStatus.HELD,
                                        index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 server_default=func.now())

    product: Mapped[Product] = relationship(back_populates="reservations")

    __table_args__ = (
        # One reservation per (order, sku). The database-level guarantee behind
        # idempotent reservation: a redelivered order.created physically cannot
        # create a second claim on the same stock.
        UniqueConstraint("order_id", "sku", name="uq_reservation_order_sku"),
        Index("idx_reservation_expiry", "status", "expires_at"),  # for the sweeper
    )


class OutboxMessage(Base):
    """Same pattern as the Django service — see its models.py for the full
    explanation of why a dual write is unsafe."""

    __tablename__ = "outbox_messages"

    # BIGINT on Postgres; INTEGER on SQLite, which only autoincrements INTEGER PKs.
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), unique=True,
                                                default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(100))
    aggregate_id: Mapped[str] = mapped_column(String(64), index=True)
    topic: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict] = mapped_column(JSON().with_variant(JSONB, "postgresql"))
    # with_variant: JSONB on Postgres (unchanged schema), plain JSON on the
    # SQLite used by the unit tests, which cannot compile JSONB.
    # JSONB, not JSON: binary storage, indexable, queryable with ->> and @>.
    # Plain JSON in Postgres stores the raw text and reparses on every read.
    correlation_id: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True),
                                                          nullable=True)
    attempts: Mapped[int] = mapped_column(default=0)
    last_error: Mapped[str] = mapped_column(String(2000), default="")

    __table_args__ = (
        Index("idx_inv_outbox_unpublished", "created_at",
              postgresql_where=(published_at.is_(None))),   # partial index
    )


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                   server_default=func.now(),
                                                   index=True)
