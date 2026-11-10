"""
The event envelope — the contract between services.

THE SINGLE MOST IMPORTANT FILE IN THE REPO.

In a microservice system, the events are the API. HTTP endpoints are an
implementation detail of one service; events are the public, permanent,
cross-team contract. Get this wrong and every service pays for it forever.

An envelope separates two things that juniors usually mash together:

    METADATA  — who emitted this, when, what is it, how do I trace it,
                how do I detect a duplicate.  (stable forever)
    PAYLOAD   — the business facts.           (evolves with the domain)

Rules this envelope enforces, and why:

1. EVERY event has a unique `event_id`.
   Consumers key their idempotency check on it. Without it, exactly-once
   processing is impossible — and at-least-once delivery means duplicates WILL
   happen, not might.

2. EVERY event names its `aggregate_id`.
   This becomes the Kafka partition key. All events for order X land on the
   same partition, so they are consumed in order. Kafka guarantees ordering
   *within a partition only* — picking the wrong key silently destroys ordering.

3. EVERY event carries a `correlation_id`.
   One user action, one id, stitched across every service and log line. The
   first thing you will want at 3am is "show me everything that happened for
   this request".

4. EVERY event is VERSIONED and NAMED IN THE PAST TENSE.
   `order.created`, not `create_order`. An event is a fact that already
   happened; the emitter is not asking anyone to do anything. If you name
   events as commands you have built RPC with extra latency.

5. Events are FAT ENOUGH TO ACT ON.
   `order.created` carries the line items, not just an id. The alternative
   ("event-carried state transfer" vs "notification only") is a real trade-off
   — see docs/04-NOTES-EVENT-DRIVEN.md — but a notification-only event forces
   every consumer to call back into the producer, which rebuilds the coupling
   you paid for the broker to remove.

SCHEMA EVOLUTION, the rule that saves your job:
    You may ADD optional fields. You may never REMOVE a field, RENAME a field,
    or CHANGE a field's type or meaning. Breaking change => new version
    (`order.created.v2`), published alongside v1 until every consumer migrates.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """
    The complete catalogue of events in the system.

    Keeping this in one enum is a small thing with a large payoff: you can read
    the entire choreography of the system in fifteen lines, and a typo in a
    topic name becomes an ImportError instead of a message that silently goes
    nowhere and is never consumed.
    """

    # emitted by orders-django
    ORDER_CREATED = "order.created"
    ORDER_CONFIRMED = "order.confirmed"
    ORDER_CANCELLED = "order.cancelled"          # also the compensating event

    # emitted by inventory-fastapi
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_REJECTED = "inventory.rejected"
    INVENTORY_RELEASED = "inventory.released"    # compensation completed
    PRODUCT_RESTOCKED = "product.restocked"


SCHEMA_VERSION = 1


@dataclass(frozen=True)
class EventEnvelope:
    """
    Immutable (`frozen=True`) on purpose. An event is a record of something that
    already happened; code that mutates one has misunderstood the model.
    """

    event_id: str
    event_type: str
    aggregate_id: str
    occurred_at: str
    producer: str
    correlation_id: str
    payload: dict[str, Any]
    schema_version: int = SCHEMA_VERSION
    causation_id: str | None = None   # which event caused this one

    # -- serialisation ----------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_bytes(self) -> bytes:
        # sort_keys gives byte-identical output for identical content, which
        # makes tests, hashing and log diffing sane.
        return json.dumps(self.to_dict(), sort_keys=True, default=str).encode()

    @classmethod
    def from_bytes(cls, raw: bytes | str) -> EventEnvelope:
        data = json.loads(raw)
        # Forward compatibility: a newer producer may send fields this consumer
        # has never heard of. Dropping unknown keys instead of exploding is what
        # lets you deploy producer and consumer independently — which is the
        # entire point of microservices.
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in known})

    # -- routing ----------------------------------------------------------
    @property
    def partition_key(self) -> bytes:
        """Kafka key. Same aggregate => same partition => ordered delivery."""
        return self.aggregate_id.encode()

    @property
    def routing_key(self) -> str:
        """RabbitMQ routing key, e.g. 'order.created'."""
        return self.event_type

    def __str__(self) -> str:  # what shows up in logs
        return f"<{self.event_type} agg={self.aggregate_id} id={self.event_id[:8]}>"


def new_event(
    event_type: EventType | str,
    aggregate_id: str,
    payload: dict[str, Any],
    producer: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> EventEnvelope:
    """
    The ONLY sanctioned way to create an event.

    Centralising construction means timestamps are always UTC and ISO-8601,
    ids are always real UUID4s, and nobody hand-rolls an envelope that is
    missing a correlation id at 2am under deadline pressure.
    """
    return EventEnvelope(
        event_id=str(uuid.uuid4()),
        event_type=event_type.value if isinstance(event_type, EventType) else event_type,
        aggregate_id=str(aggregate_id),
        # timezone-aware UTC. Naive datetimes are a production incident waiting
        # for a daylight-saving boundary.
        occurred_at=datetime.now(timezone.utc).isoformat(),
        producer=producer,
        correlation_id=correlation_id or str(uuid.uuid4()),
        causation_id=causation_id,
        payload=payload,
    )


# ---------------------------------------------------------------------------
# Payload shapes, documented as plain dataclasses.
#
# These are documentation with a type-checker attached, not runtime validation.
# Each service validates incoming payloads with its own Pydantic/DRF schema at
# its own boundary — a consumer must never trust that a producer sent what the
# docs promised.
#
# In a larger system you would put these in a schema registry (Avro / Protobuf /
# JSON Schema) with automated compatibility checks in CI. See
# docs/04-NOTES-EVENT-DRIVEN.md, "Schema registry".
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderLine:
    sku: str
    quantity: int
    unit_price: str  # money as a decimal STRING — never float. See the notes.


@dataclass(frozen=True)
class OrderCreatedPayload:
    order_id: str
    customer_id: str
    total_amount: str
    currency: str
    lines: list[OrderLine] = field(default_factory=list)


@dataclass(frozen=True)
class InventoryReservedPayload:
    order_id: str
    reservation_id: str
    expires_at: str


@dataclass(frozen=True)
class InventoryRejectedPayload:
    order_id: str
    reason: str
    unavailable_skus: list[str] = field(default_factory=list)
