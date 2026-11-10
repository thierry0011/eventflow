"""
Domain models for the orders service.

Four tables, and three of them are infrastructure. That ratio is normal and
worth internalising: in a distributed system, most of your schema exists to
make the network survivable.

    Order, OrderItem   the business
    OutboxMessage      makes "save + publish" atomic
    ProcessedEvent     makes consuming idempotent
"""

from __future__ import annotations

import uuid
from decimal import Decimal

from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.utils import timezone


class TimestampedModel(models.Model):
    """
    auto_now_add / auto_now give you created/updated for free.
    `created_at` is indexed because you will sort and filter by it constantly —
    "recent orders", "orders in the last hour", every dashboard ever.
    """

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True   # no table of its own; fields are copied into children


class OrderStatus(models.TextChoices):
    """
    The saga state machine, written down.

        PENDING ──inventory.reserved──> CONFIRMED ──> COMPLETED
           │
           └──inventory.rejected / timeout / user──> CANCELLED

    TextChoices rather than raw strings: you get validation, a readable admin
    dropdown, and `OrderStatus.PENDING` autocompletes instead of you guessing
    whether it was "pending" or "PENDING" at the call site.

    Storing a string rather than an int is deliberate: `status = 3` in a
    database dump tells a support engineer nothing at 3am.
    """

    PENDING = "PENDING", "Pending inventory"
    CONFIRMED = "CONFIRMED", "Confirmed"
    CANCELLED = "CANCELLED", "Cancelled"
    COMPLETED = "COMPLETED", "Completed"

    @classmethod
    def terminal(cls) -> set[str]:
        return {cls.CANCELLED, cls.COMPLETED}


class Order(TimestampedModel):
    # UUID primary key, not an auto-incrementing integer. Three reasons:
    #   1. Sequential ids leak business volume — /orders/4471 tells a competitor
    #      how many orders you have taken ("German tank problem").
    #   2. Ids can be generated client-side or by another service without a
    #      round trip, which matters when services are distributed.
    #   3. Merging data across shards/regions does not collide.
    # The cost: 16 bytes instead of 8, and random insert order fragments the
    # B-tree. If that cost ever matters, UUIDv7 is time-ordered and fixes it.
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    customer = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,   # PROTECT, not CASCADE: deleting a customer
                                    # must never silently delete financial
                                    # records. Make the deletion fail loudly.
        related_name="orders",
    )
    status = models.CharField(max_length=20, choices=OrderStatus.choices,
                              default=OrderStatus.PENDING, db_index=True)

    # MONEY IS NEVER A FLOAT. 0.1 + 0.2 != 0.3 in binary floating point, and
    # those fractions of a cent become a reconciliation nightmare. Decimal in
    # Python, NUMERIC in Postgres. (The alternative — integer minor units, i.e.
    # store cents — is also correct and common.)
    total_amount = models.DecimalField(max_digits=12, decimal_places=2,
                                       default=Decimal("0.00"))
    currency = models.CharField(max_length=3, default="USD")

    # Saga bookkeeping
    reservation_id = models.UUIDField(null=True, blank=True)
    failure_reason = models.TextField(blank=True, default="")

    # API idempotency: a UNIQUE constraint, not just a Redis key. Redis is the
    # fast path; the database constraint is the thing that is actually true.
    # Defence in depth — the cache can be flushed, the constraint cannot.
    idempotency_key = models.CharField(max_length=128, null=True, blank=True,
                                       unique=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            # COMPOSITE INDEXES ARE ORDER-SENSITIVE. This one serves
            # "this customer's orders, newest first" — the most common query in
            # the app. It can also serve "this customer's orders" (leftmost
            # prefix) but NOT "all orders sorted by date".
            models.Index(fields=["customer", "-created_at"], name="idx_order_cust_date"),
            # Serves the saga sweeper: "PENDING orders older than X".
            models.Index(fields=["status", "created_at"], name="idx_order_status_date"),
        ]
        constraints = [
            # Enforced by the DATABASE, so it holds even for a stray script, a
            # bad migration or a direct psql session. Application-level checks
            # are advisory; database constraints are law.
            models.CheckConstraint(check=models.Q(total_amount__gte=0),
                                   name="order_total_non_negative"),
        ]

    def __str__(self) -> str:
        return f"Order {self.id} [{self.status}]"

    @property
    def is_terminal(self) -> bool:
        return self.status in OrderStatus.terminal()

    def recalculate_total(self) -> Decimal:
        total = sum((i.line_total for i in self.items.all()), Decimal("0.00"))
        self.total_amount = total
        return total


class OrderItem(models.Model):
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name="items")
    # CASCADE is right HERE: a line item has no meaning without its order.
    # Compare with the PROTECT on customer above — the rule is "does the child
    # exist independently?", not a global preference.

    sku = models.CharField(max_length=64, db_index=True)
    quantity = models.PositiveIntegerField(validators=[MinValueValidator(1)])

    # Price is COPIED onto the line, not looked up through a foreign key.
    # This is intentional denormalisation: an invoice must show the price the
    # customer actually paid, not today's price. Historical records must be
    # immutable snapshots.
    unit_price = models.DecimalField(max_digits=12, decimal_places=2)

    class Meta:
        constraints = [
            # Same SKU twice on one order is a client bug; make it impossible.
            models.UniqueConstraint(fields=["order", "sku"], name="uniq_order_sku"),
        ]

    @property
    def line_total(self) -> Decimal:
        return self.unit_price * self.quantity

    def __str__(self) -> str:
        return f"{self.quantity} x {self.sku}"


# ===========================================================================
# THE TRANSACTIONAL OUTBOX
# ===========================================================================
class OutboxMessage(models.Model):
    """
    Solves the DUAL-WRITE PROBLEM, which is the first genuinely hard problem in
    event-driven systems.

    The naive code every junior writes:

        order.save()                       # (1) database
        kafka.publish(OrderCreated(...))   # (2) broker

    There is no transaction spanning a database and a broker. So:
      * crash between (1) and (2) -> order exists, nobody is ever told.
        Stock is never reserved. The customer waits forever.
      * publish succeeds, then the transaction rolls back -> you announced an
        order that does not exist. Downstream reserves stock for a ghost.

    Swapping the order does not help; it only changes which failure you get.

    THE OUTBOX: write the event into a table IN THE SAME TRANSACTION as the
    business change. One transaction, one database, fully atomic. A separate
    relay process then polls the table and publishes to Kafka, marking rows
    published afterwards.

        BEGIN
          INSERT INTO orders ...
          INSERT INTO outbox ...
        COMMIT
                      ↓ (separate process, later)
          SELECT ... FROM outbox WHERE published_at IS NULL FOR UPDATE SKIP LOCKED
          publish to kafka
          UPDATE outbox SET published_at = now()

    What you get: the event is published AT LEAST ONCE and NEVER WITHOUT the
    business change. The crash window moves from "lost event" (unrecoverable)
    to "duplicate event" (recoverable, because consumers are idempotent).

    This is the pattern that makes event-driven architecture trustworthy.
    Learn it properly — it is the most common senior interview question in this
    whole area. The production-grade alternative is Change Data Capture
    (Debezium tailing the WAL), which removes the polling but adds an
    operational component.
    """

    id = models.BigAutoField(primary_key=True)  # sequential: cheap, ordered reads
    event_id = models.UUIDField(unique=True, default=uuid.uuid4)
    event_type = models.CharField(max_length=100)
    aggregate_id = models.CharField(max_length=64, db_index=True)
    topic = models.CharField(max_length=100)
    payload = models.JSONField()
    correlation_id = models.CharField(max_length=64)

    created_at = models.DateTimeField(auto_now_add=True)
    published_at = models.DateTimeField(null=True, blank=True)
    attempts = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default="")

    class Meta:
        indexes = [
            # PARTIAL INDEX: only rows that are NOT yet published.
            # The outbox is mostly history — millions of published rows the
            # relay never looks at again. A partial index stays tiny (it only
            # covers the handful of pending rows) so the relay's hot query is
            # always fast regardless of table size. A full index on
            # `published_at` would grow forever for no benefit.
            models.Index(fields=["created_at"], name="idx_outbox_unpublished",
                         condition=models.Q(published_at__isnull=True)),
        ]

    def __str__(self) -> str:
        state = "published" if self.published_at else "pending"
        return f"{self.event_type} ({state})"


# ===========================================================================
# IDEMPOTENT CONSUMPTION
# ===========================================================================
class ProcessedEvent(models.Model):
    """
    The consumer side of at-least-once delivery.

    Kafka redelivers on rebalance, on crash, on an uncommitted offset. Your
    handler WILL see the same event twice. Insert its event_id here inside the
    same transaction as the side effect; the UNIQUE constraint makes the second
    attempt fail, you catch IntegrityError, and the work is skipped.

    Why the DATABASE and not just Redis: the de-duplication record and the
    business change must commit or roll back together. If the dedup key lives in
    Redis and the transaction rolls back, you have recorded "handled" for work
    that never happened — and the redelivery that would have fixed it is now
    suppressed. Redis is a fast pre-filter; Postgres is the truth.

    Housekeeping: prune rows older than your Kafka retention (nothing older can
    be redelivered). An unbounded dedup table is a slow-motion outage.
    """

    event_id = models.UUIDField(primary_key=True)
    event_type = models.CharField(max_length=100)
    consumer_group = models.CharField(max_length=100)
    processed_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        indexes = [models.Index(fields=["processed_at"], name="idx_processed_at")]

    def __str__(self) -> str:
        return f"{self.event_type} {self.event_id}"
