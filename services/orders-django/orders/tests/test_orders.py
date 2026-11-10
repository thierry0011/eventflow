"""
Tests for the orders service.

WHAT TO TEST, and the trap to avoid.

The trap: testing that Django works. `test_order_can_be_saved` asserts that
`Model.save()` saves. Django's own test suite covers that. Your test adds
maintenance cost and zero information.

Test YOUR rules and YOUR invariants:
    * an order created => an outbox row created, in the SAME transaction
    * a duplicate event => no second side effect
    * a rejection => the order ends CANCELLED and a compensating event is emitted
    * an expired PENDING order => swept
    * an idempotency key replay => one order, not two

THE TEST PYRAMID, applied here:
    many   unit tests        services.py with everything external faked. Fast.
    some   integration tests real Postgres, real transactions. Slower, truer.
    few    end-to-end tests  the whole compose stack (scripts/smoke_test.sh).

An inverted pyramid — mostly end-to-end — gives you a 40-minute suite that
fails for unrelated reasons and that nobody trusts. Push tests down the pyramid
whenever you can.

NOTE ON MOCKING: Kafka is faked, Postgres is NOT. Mocking the database means
you never test your constraints, your transactions or your SQL — the exact
places bugs live. Mock what is slow or external; keep what encodes your logic.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest
from django.contrib.auth import get_user_model
from django.utils import timezone
from eventflow_common import new_event
from eventflow_common.envelope import EventType

from orders import services
from orders.models import Order, OrderItem, OrderStatus, OutboxMessage, ProcessedEvent

pytestmark = pytest.mark.django_db


@pytest.fixture
def user(db):
    return get_user_model().objects.create_user(username="tester", password="x" * 12)


@pytest.fixture(autouse=True)
def fake_prices():
    """
    Autouse: every test gets deterministic prices without asking.

    Patched at the point of USE (`orders.services.fetch_prices`), not at the
    point of definition. Patching `httpx.Client` instead would couple the test
    to the HTTP library — refactor to `requests` and every test breaks even
    though behaviour did not change.
    """
    with patch.object(services, "fetch_prices") as mocked:
        mocked.side_effect = lambda skus: {s: Decimal("10.00") for s in skus}
        yield mocked


# ---------------------------------------------------------------- creation
def test_create_order_writes_order_and_outbox_atomically(user):
    order = services.create_order(
        customer=user, lines=[{"sku": "SKU-A", "quantity": 2}],
    )

    assert order.status == OrderStatus.PENDING
    assert order.total_amount == Decimal("20.00")
    assert OrderItem.objects.filter(order=order).count() == 1

    # THE POINT OF THE TEST: the event exists, and it exists in the database,
    # not in a broker. Order row and outbox row are now inseparable.
    outbox = OutboxMessage.objects.get(aggregate_id=str(order.id))
    assert outbox.event_type == EventType.ORDER_CREATED.value
    assert outbox.published_at is None            # relay has not run yet
    assert outbox.payload["payload"]["total_amount"] == "20.00"


def test_duplicate_skus_are_merged_into_one_line(user):
    order = services.create_order(customer=user, lines=[
        {"sku": "SKU-A", "quantity": 1},
        {"sku": "SKU-A", "quantity": 3},
    ])
    assert order.items.count() == 1
    assert order.items.first().quantity == 4


def test_empty_order_is_rejected(user):
    with pytest.raises(services.DomainError) as exc:
        services.create_order(customer=user, lines=[])
    assert exc.value.code == "empty_order"


def test_order_creation_rolls_back_entirely_on_failure(user):
    """
    If anything inside the transaction fails, NOTHING is written — no order,
    no items, and critically no outbox row announcing an order that does not
    exist. This is the atomicity the outbox pattern depends on.
    """
    before = OutboxMessage.objects.count()
    with patch.object(OutboxMessage.objects, "create", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError):
            services.create_order(customer=user, lines=[{"sku": "SKU-A", "quantity": 1}])

    assert Order.objects.count() == 0
    assert OutboxMessage.objects.count() == before


# ------------------------------------------------------------------- saga
def _inventory_event(event_type, order_id, **payload):
    return new_event(event_type, str(order_id),
                     {"order_id": str(order_id), **payload}, producer="inventory")


def test_reservation_confirms_the_order_and_emits_confirmation(user):
    order = services.create_order(customer=user, lines=[{"sku": "SKU-A", "quantity": 1}])
    event = _inventory_event(EventType.INVENTORY_RESERVED, order.id,
                             reservation_id="3f2504e0-4f89-11d3-9a0c-0305e82c3301",
                             expires_at=timezone.now().isoformat())

    services.handle_inventory_reserved(event)

    order.refresh_from_db()
    assert order.status == OrderStatus.CONFIRMED
    assert OutboxMessage.objects.filter(
        event_type=EventType.ORDER_CONFIRMED.value, aggregate_id=str(order.id)
    ).exists()


def test_rejection_cancels_the_order_with_a_reason(user):
    order = services.create_order(customer=user, lines=[{"sku": "SKU-A", "quantity": 1}])
    event = _inventory_event(EventType.INVENTORY_REJECTED, order.id,
                             reason="insufficient stock", unavailable_skus=["SKU-A"])

    services.handle_inventory_rejected(event)

    order.refresh_from_db()
    assert order.status == OrderStatus.CANCELLED
    assert order.failure_reason == "insufficient stock"


def test_replaying_the_same_event_has_no_second_effect(user):
    """
    THE IDEMPOTENCY TEST. If you write only one test for an event-driven
    system, write this one. At-least-once delivery guarantees this scenario
    will happen in production.
    """
    order = services.create_order(customer=user, lines=[{"sku": "SKU-A", "quantity": 1}])
    event = _inventory_event(EventType.INVENTORY_RESERVED, order.id,
                             reservation_id="3f2504e0-4f89-11d3-9a0c-0305e82c3301",
                             expires_at=timezone.now().isoformat())

    services.handle_inventory_reserved(event)
    services.handle_inventory_reserved(event)   # exact same event_id
    services.handle_inventory_reserved(event)

    assert ProcessedEvent.objects.filter(event_id=event.event_id).count() == 1
    # And only ONE confirmation was emitted, not three.
    assert OutboxMessage.objects.filter(
        event_type=EventType.ORDER_CONFIRMED.value).count() == 1


def test_reservation_arriving_after_cancellation_releases_the_stock(user):
    """
    The race the saga timeout creates: the sweeper cancels an order at t=900s,
    and inventory's reservation lands at t=901s. Without this branch the stock
    stays reserved for an order nobody will ever pay for.
    """
    order = services.create_order(customer=user, lines=[{"sku": "SKU-A", "quantity": 1}])
    services.cancel_order(str(order.id), reason="timed out")

    event = _inventory_event(EventType.INVENTORY_RESERVED, order.id,
                             reservation_id="3f2504e0-4f89-11d3-9a0c-0305e82c3301",
                             expires_at=timezone.now().isoformat())
    services.handle_inventory_reserved(event)

    order.refresh_from_db()
    assert order.status == OrderStatus.CANCELLED      # stays cancelled
    cancellations = OutboxMessage.objects.filter(
        event_type=EventType.ORDER_CANCELLED.value, aggregate_id=str(order.id))
    assert cancellations.count() == 2   # the sweep, plus the release instruction


def test_cancelling_a_terminal_order_is_a_conflict(user):
    order = services.create_order(customer=user, lines=[{"sku": "SKU-A", "quantity": 1}])
    services.cancel_order(str(order.id))
    with pytest.raises(services.DomainError) as exc:
        services.cancel_order(str(order.id))
    assert exc.value.code == "invalid_state_transition"


# ---------------------------------------------------------------- sweeper
def test_sweeper_cancels_only_orders_older_than_the_timeout(user):
    fresh = services.create_order(customer=user, lines=[{"sku": "SKU-A", "quantity": 1}])
    stale = services.create_order(customer=user, lines=[{"sku": "SKU-B", "quantity": 1}])

    # Bypass auto_now_add with a direct UPDATE — the field is not assignable.
    Order.objects.filter(pk=stale.pk).update(
        created_at=timezone.now() - timezone.timedelta(hours=2))

    assert services.expire_pending_orders(older_than_seconds=900) == 1

    fresh.refresh_from_db(); stale.refresh_from_db()
    assert fresh.status == OrderStatus.PENDING
    assert stale.status == OrderStatus.CANCELLED


# -------------------------------------------------------------------- API
def test_idempotency_key_replay_returns_the_same_order(client, user):
    from rest_framework.test import APIClient

    api = APIClient()
    api.force_authenticate(user=user)
    body = {"items": [{"sku": "SKU-A", "quantity": 1}]}
    headers = {"HTTP_IDEMPOTENCY_KEY": "key-123"}

    first = api.post("/api/v1/orders", body, format="json", **headers)
    second = api.post("/api/v1/orders", body, format="json", **headers)

    assert first.status_code == 201
    assert second.status_code == 200          # not created again
    assert first.data["id"] == second.data["id"]
    assert Order.objects.count() == 1


def test_a_user_cannot_read_another_users_order(user):
    from rest_framework.test import APIClient

    other = get_user_model().objects.create_user(username="mallory", password="y" * 12)
    order = services.create_order(customer=other, lines=[{"sku": "SKU-A", "quantity": 1}])

    api = APIClient()
    api.force_authenticate(user=user)
    response = api.get(f"/api/v1/orders/{order.id}/")

    assert response.status_code == 404
    # 404, NOT 403. Returning 403 confirms the resource exists, which leaks
    # information to an attacker enumerating ids. "You may not know whether it
    # exists" is the stronger answer.


def test_unauthenticated_requests_are_rejected():
    from rest_framework.test import APIClient

    assert APIClient().get("/api/v1/orders/").status_code == 401
