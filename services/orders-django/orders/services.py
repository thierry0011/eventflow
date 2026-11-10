"""
The service layer: all business logic lives here.

WHY A SERVICE LAYER AT ALL — the architectural habit that most separates mid
from senior Django code.

Django tutorials put logic in views. Slightly better tutorials put it in models
("fat models"). Both break down at exactly the same point: when the same
operation must be callable from an HTTP view, a Celery task, a management
command and an event consumer. Logic inside a view is reachable only through
HTTP; logic inside a model method cannot coordinate several models and an
outbox write without the model knowing about infrastructure.

So:
    views      parse input, check permissions, call a service, render output
    services   business rules, transactions, event emission  <-- the real code
    models     data shape, constraints, trivial derived properties
    tasks      scheduling and retry policy around a service call

Every entry point in this service calls the same functions below. That is why
`create_order` behaves identically from the API, the smoke test and the shell.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import httpx
from django.conf import settings
from django.db import IntegrityError, transaction
from django.utils import timezone
from eventflow_common import CircuitBreaker, CircuitOpenError, EventType, get_logger, new_event
from eventflow_common.observability import correlation_id_var

from .models import Order, OrderItem, OrderStatus, OutboxMessage, ProcessedEvent

log = get_logger(__name__)

PRODUCER = "orders-service"


class DomainError(Exception):
    """Business rule violated. Maps to 4xx — the caller can fix this."""

    def __init__(self, message: str, code: str = "domain_error") -> None:
        super().__init__(message)
        self.code = code


class PricingUnavailable(Exception):
    """A dependency is down. Maps to 503 — the caller should retry later."""


# ===========================================================================
# Synchronous call to another service — and when that is acceptable
# ===========================================================================
_pricing_breaker = CircuitBreaker("inventory-pricing", failure_threshold=5,
                                  recovery_timeout=20.0)


def fetch_prices(skus: list[str]) -> dict[str, Decimal]:
    """
    This repo is event-driven, so why is there a synchronous HTTP call here?

    Because event-driven does not mean "never call anybody". It means commands
    that change state are async; QUERIES for data you need RIGHT NOW to answer
    a user are often legitimately synchronous. The user is holding the phone
    waiting for a price.

    What makes a synchronous cross-service call acceptable:
      * it is a READ, so retrying it is free and safe
      * there is a TIMEOUT (a call without a timeout is an outage with a delay)
      * there is a CIRCUIT BREAKER, so a dead dependency fails in 0ms, not 30s
      * there is a DEGRADED PATH (here: a cache; elsewhere: a default, a queue)

    What would NOT be acceptable: synchronously calling inventory to RESERVE
    stock. That is a state change across a service boundary, it cannot be
    safely retried, and it makes order creation fail whenever inventory is
    down. That is exactly why reservation goes through the saga instead.
    """
    from django.core.cache import cache

    cached = cache.get_many([f"price:{s}" for s in skus])
    prices = {k.split(":", 1)[1]: Decimal(v) for k, v in cached.items()}
    missing = [s for s in skus if s not in prices]
    if not missing:
        return prices

    def _call() -> dict[str, Any]:
        with httpx.Client(timeout=httpx.Timeout(2.0, connect=1.0)) as client:
            response = client.post(
                "http://inventory-api:8000/api/v1/products/prices",
                json={"skus": missing},
                headers={"X-Correlation-ID": correlation_id_var.get()},
            )
            response.raise_for_status()
            return response.json()

    try:
        data = _pricing_breaker.call(_call)
    except CircuitOpenError:
        log.warning("pricing circuit open, serving from cache only")
        raise PricingUnavailable("pricing service unavailable") from None
    except httpx.HTTPError as exc:
        log.warning("pricing call failed", extra={"error": str(exc)})
        raise PricingUnavailable("pricing service unavailable") from exc

    for sku, price in data.get("prices", {}).items():
        prices[sku] = Decimal(str(price))
        cache.set(f"price:{sku}", str(price), timeout=60)

    unknown = [s for s in skus if s not in prices]
    if unknown:
        raise DomainError(f"unknown SKUs: {', '.join(unknown)}", code="unknown_sku")
    return prices


# ===========================================================================
# COMMAND: create an order
# ===========================================================================
@transaction.atomic
def create_order(*, customer, lines: list[dict[str, Any]],
                 idempotency_key: str | None = None) -> Order:
    """
    Creates an order and emits order.created ATOMICALLY via the outbox.

    Read the transaction boundary carefully — this is the whole lesson:

        @transaction.atomic covers
            * the Order row
            * the OrderItem rows
            * the OutboxMessage row
        and NOTHING ELSE.

        It does NOT cover the Kafka publish (that is the relay's job, after
        commit) and it does NOT cover the pricing HTTP call (that happens
        BEFORE the transaction opens).

    Why the HTTP call must be outside: a transaction holds a database
    connection and row locks for its entire life. Doing network I/O inside one
    means a slow dependency holds Postgres resources hostage, and under load you
    exhaust the connection pool and the whole service stops. The rule is
    absolute:

        NEVER DO NETWORK I/O INSIDE A DATABASE TRANSACTION.

    Keep transactions short, local, and purely about the database.
    """
    if not lines:
        raise DomainError("an order needs at least one line", code="empty_order")

    # Collapse duplicate SKUs the client may have sent as separate lines.
    merged: dict[str, int] = {}
    for line in lines:
        qty = int(line["quantity"])
        if qty <= 0:
            raise DomainError("quantity must be positive", code="invalid_quantity")
        merged[line["sku"]] = merged.get(line["sku"], 0) + qty

    prices = fetch_prices(list(merged))   # outside the transaction. See above.

    order = Order(
        customer=customer,
        status=OrderStatus.PENDING,
        currency="USD",
        idempotency_key=idempotency_key,
    )

    items = [
        OrderItem(order=order, sku=sku, quantity=qty, unit_price=prices[sku])
        for sku, qty in merged.items()
    ]
    order.total_amount = sum((i.line_total for i in items), Decimal("0.00"))

    try:
        order.save()
        OrderItem.objects.bulk_create(items)
        # bulk_create: ONE INSERT with many rows instead of N round-trips.
        # With 20 line items that is 1 query instead of 20 — the same habit
        # that kills N+1 problems on the read side.
    except IntegrityError as exc:
        # The unique constraint on idempotency_key fired: a concurrent duplicate
        # request won the race. Return THEIR order — both callers get the same
        # answer and only one order exists.
        if idempotency_key and "idempotency" in str(exc).lower():
            existing = Order.objects.filter(idempotency_key=idempotency_key).first()
            if existing:
                return existing
        raise

    _enqueue_event(
        event_type=EventType.ORDER_CREATED,
        aggregate_id=str(order.id),
        topic=settings.KAFKA_TOPIC_ORDERS,
        payload={
            "order_id": str(order.id),
            "customer_id": str(customer.id),
            "total_amount": str(order.total_amount),
            "currency": order.currency,
            "lines": [
                {"sku": i.sku, "quantity": i.quantity, "unit_price": str(i.unit_price)}
                for i in items
            ],
        },
    )

    log.info("order created", extra={"order_id": str(order.id),
                                     "customer_id": customer.id,
                                     "total": str(order.total_amount),
                                     "line_count": len(items)})
    return order


def _enqueue_event(*, event_type: EventType, aggregate_id: str, topic: str,
                   payload: dict[str, Any], causation_id: str | None = None) -> OutboxMessage:
    """
    Write to the outbox. MUST be called inside an open transaction — that is
    the entire guarantee. Django raises if you call this outside one in tests
    configured with `--strict`, and in production a stray call outside a
    transaction silently loses the atomicity you think you have.
    """
    event = new_event(event_type, aggregate_id, payload, PRODUCER,
                      correlation_id=correlation_id_var.get(),
                      causation_id=causation_id)
    return OutboxMessage.objects.create(
        event_id=event.event_id,
        event_type=event.event_type,
        aggregate_id=event.aggregate_id,
        topic=topic,
        payload=event.to_dict(),
        correlation_id=event.correlation_id,
    )


# ===========================================================================
# SAGA HANDLERS — the other half of the choreography
# ===========================================================================
def handle_inventory_reserved(event) -> None:
    """PENDING -> CONFIRMED. Called by the Kafka consumer."""
    with transaction.atomic():
        if _already_processed(event):
            return
        order = _lock_order(event.payload["order_id"])
        if order is None or order.is_terminal:
            # Already cancelled (likely by the timeout sweeper) while inventory
            # was reserving. The reservation must be given back, or that stock
            # is leaked until its TTL expires.
            if order:
                _enqueue_event(
                    event_type=EventType.ORDER_CANCELLED,
                    aggregate_id=str(order.id),
                    topic=settings.KAFKA_TOPIC_ORDERS,
                    payload={"order_id": str(order.id),
                             "reason": "order already terminal; releasing reservation"},
                    causation_id=event.event_id,
                )
            return

        order.status = OrderStatus.CONFIRMED
        order.reservation_id = uuid.UUID(event.payload["reservation_id"])
        order.save(update_fields=["status", "reservation_id", "updated_at"])
        # update_fields writes only the columns that changed. Without it Django
        # UPDATEs every column, which can clobber a concurrent writer's change
        # to an unrelated field — a lost update you will never reproduce.

        _enqueue_event(
            event_type=EventType.ORDER_CONFIRMED,
            aggregate_id=str(order.id),
            topic=settings.KAFKA_TOPIC_ORDERS,
            payload={"order_id": str(order.id),
                     "customer_id": str(order.customer_id),
                     "total_amount": str(order.total_amount)},
            causation_id=event.event_id,
        )
    log.info("order confirmed", extra={"order_id": event.payload["order_id"]})


def handle_inventory_rejected(event) -> None:
    """
    PENDING -> CANCELLED. The compensating transaction.

    A saga has no rollback. Each step already committed in its own database, so
    "undo" is a new forward action that negates the old one. Here there is
    nothing physical to undo — no money moved yet — so compensation is just the
    status change plus telling the customer. In a payment saga this is where
    you would issue a refund, and a refund is a NEW transaction, not a rollback.
    """
    with transaction.atomic():
        if _already_processed(event):
            return
        order = _lock_order(event.payload["order_id"])
        if order is None or order.is_terminal:
            return

        order.status = OrderStatus.CANCELLED
        order.failure_reason = event.payload.get("reason", "inventory unavailable")
        order.save(update_fields=["status", "failure_reason", "updated_at"])

        _enqueue_event(
            event_type=EventType.ORDER_CANCELLED,
            aggregate_id=str(order.id),
            topic=settings.KAFKA_TOPIC_ORDERS,
            payload={"order_id": str(order.id),
                     "customer_id": str(order.customer_id),
                     "reason": order.failure_reason},
            causation_id=event.event_id,
        )
    log.info("order cancelled", extra={"order_id": event.payload["order_id"],
                                       "reason": event.payload.get("reason")})


def cancel_order(order_id: str, reason: str = "cancelled by customer") -> Order:
    """
    User-initiated cancellation. Emits order.cancelled, which inventory consumes
    to RELEASE any reservation it holds — compensation flowing the other way.
    """
    with transaction.atomic():
        order = _lock_order(order_id)
        if order is None:
            raise DomainError("order not found", code="not_found")
        if order.is_terminal:
            raise DomainError(f"order is already {order.status}",
                              code="invalid_state_transition")

        order.status = OrderStatus.CANCELLED
        order.failure_reason = reason
        order.save(update_fields=["status", "failure_reason", "updated_at"])

        _enqueue_event(
            event_type=EventType.ORDER_CANCELLED,
            aggregate_id=str(order.id),
            topic=settings.KAFKA_TOPIC_ORDERS,
            payload={"order_id": str(order.id),
                     "customer_id": str(order.customer_id),
                     "reason": reason,
                     "reservation_id": str(order.reservation_id)
                     if order.reservation_id else None},
        )
    return order


# ===========================================================================
# helpers
# ===========================================================================
def _lock_order(order_id: str) -> Order | None:
    """
    SELECT ... FOR UPDATE — a pessimistic row lock.

    Two consumers can process `inventory.reserved` and the timeout sweeper's
    cancellation for the SAME order at the same instant. Both read status=
    PENDING, both decide they may act, both write. One update is silently lost
    and the order ends in a state neither intended.

    FOR UPDATE makes the second reader BLOCK until the first commits, so it
    re-reads the already-updated row and correctly sees that the order is
    terminal. This is the standard fix for read-modify-write races, and the
    reason every handler above re-checks `is_terminal` AFTER taking the lock.

    (Optimistic locking — a version column plus `UPDATE ... WHERE version = n` —
    is the alternative, better under low contention. Know both.)
    """
    return (Order.objects
            .select_for_update()
            .select_related("customer")   # one JOIN instead of a second query
            .filter(pk=order_id)
            .first())


def _already_processed(event) -> bool:
    """
    Idempotency guard. Note it runs INSIDE the caller's transaction, so the
    dedup row and the business change commit together or not at all.
    """
    try:
        ProcessedEvent.objects.create(
            event_id=event.event_id,
            event_type=event.event_type,
            consumer_group=settings.CONSUMER_GROUP,
        )
        return False
    except IntegrityError:
        log.info("duplicate event ignored", extra={"event_id": event.event_id,
                                                   "event_type": event.event_type})
        return True


def expire_pending_orders(older_than_seconds: int) -> int:
    """
    The saga timeout. Called every minute by Celery beat.

    Every saga needs one. Without it, a single lost message leaves an order
    PENDING for eternity and nobody notices until a customer complains. With
    it, the system self-heals: the order is cancelled, the customer is told,
    and any reservation is released.

    `iterator()` streams rows instead of loading them all into memory; on a
    sweeper that could match 100k rows, `.all()` is an OOM kill.
    """
    cutoff = timezone.now() - timezone.timedelta(seconds=older_than_seconds)
    stale = (Order.objects
             .filter(status=OrderStatus.PENDING, created_at__lt=cutoff)
             .values_list("id", flat=True)
             .iterator(chunk_size=500))

    count = 0
    for order_id in stale:
        try:
            cancel_order(str(order_id), reason="timed out waiting for inventory")
            count += 1
        except DomainError:
            continue   # someone else resolved it between the scan and the lock
    if count:
        log.warning("expired stale pending orders", extra={"count": count})
    return count
