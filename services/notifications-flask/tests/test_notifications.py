"""
Tests for the notification service.

Shows the payoff of the application factory: `create_app(TestConfig)` gives a
fully isolated app with an in-memory database, per test, with no environment
juggling and no import-time side effects.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from eventflow_common import new_event
from eventflow_common.envelope import EventType

from app import create_app
from app.config import TestConfig
from app.extensions import db
from app.models import Notification, NotificationStatus, ProcessedEvent


@pytest.fixture
def app():
    application = create_app(TestConfig)
    with application.app_context():
        db.create_all()
        yield application
        db.session.remove()
        db.drop_all()


@pytest.fixture
def client(app):
    return app.test_client()


def _order_event(event_type=EventType.ORDER_CONFIRMED, **payload):
    order_id = payload.pop("order_id", str(uuid.uuid4()))
    return new_event(event_type, order_id,
                     {"order_id": order_id, "customer_id": "42",
                      "total_amount": "99.00", **payload},
                     producer="orders")


# --------------------------------------------------------------- the bridge
def test_confirmed_order_produces_a_notification_and_a_work_item(app):
    from app.consumer import build_handler

    publisher = MagicMock()
    handler = build_handler(app, publisher)
    event = _order_event()

    handler(event)

    with app.app_context():
        notification = db.session.scalar(db.select(Notification))
        assert notification is not None
        assert notification.status == NotificationStatus.PENDING
        assert notification.order_id == event.payload["order_id"]

    # The work item went to RabbitMQ with the event type as its routing key.
    publisher.publish.assert_called_once()
    assert publisher.publish.call_args.kwargs["routing_key"] == \
        EventType.ORDER_CONFIRMED.value


def test_events_without_a_template_are_ignored(app):
    from app.consumer import build_handler

    publisher = MagicMock()
    handler = build_handler(app, publisher)

    handler(_order_event(EventType.INVENTORY_REJECTED, reason="no stock"))

    with app.app_context():
        assert db.session.scalar(db.select(db.func.count(Notification.id))) == 0
    publisher.publish.assert_not_called()
    # inventory.rejected is deliberately mapped to None — the customer hears
    # about it via order.cancelled instead. Asserting the SILENCE protects that
    # decision from being accidentally reverted.


def test_duplicate_events_produce_one_notification(app):
    from app.consumer import build_handler

    publisher = MagicMock()
    handler = build_handler(app, publisher)
    event = _order_event()

    handler(event)
    handler(event)
    handler(event)

    with app.app_context():
        assert db.session.scalar(db.select(db.func.count(Notification.id))) == 1
        assert db.session.scalar(db.select(db.func.count(ProcessedEvent.event_id))) == 1
    assert publisher.publish.call_count == 1


# --------------------------------------------------------------- the worker
def test_worker_marks_a_notification_sent(app):
    from app.worker import build_handler

    with app.app_context():
        notification = Notification(
            event_id=str(uuid.uuid4()), event_type="order.confirmed",
            order_id=str(uuid.uuid4()), recipient="a@example.com",
            subject="s", body="b", status=NotificationStatus.PENDING)
        db.session.add(notification)
        db.session.commit()
        notification_id = notification.id

    handler = build_handler(app)
    with patch("app.worker.send_email"):      # patched: no real flakiness
        handler({"notification_id": notification_id})

    with app.app_context():
        refreshed = db.session.get(Notification, notification_id)
        assert refreshed.status == NotificationStatus.SENT
        assert refreshed.sent_at is not None
        assert refreshed.attempts == 1


def test_worker_reraises_on_failure_so_rabbit_can_retry(app):
    from app.worker import build_handler

    with app.app_context():
        notification = Notification(
            event_id=str(uuid.uuid4()), event_type="order.confirmed",
            order_id=str(uuid.uuid4()), recipient="a@example.com",
            subject="s", body="b")
        db.session.add(notification)
        db.session.commit()
        notification_id = notification.id

    handler = build_handler(app)
    with patch("app.worker.send_email", side_effect=ConnectionError("smtp down")):
        with pytest.raises(ConnectionError):
            handler({"notification_id": notification_id})
        # THE RE-RAISE IS THE CONTRACT. If the handler swallowed the exception,
        # RabbitWorker would ack the message as successful and the notification
        # would never be retried or dead-lettered. Silently swallowing an
        # exception in a message handler is how messages disappear.

    with app.app_context():
        refreshed = db.session.get(Notification, notification_id)
        assert refreshed.status == NotificationStatus.FAILED
        assert refreshed.attempts == 1
        assert "smtp down" in refreshed.last_error


def test_worker_does_not_resend_an_already_sent_notification(app):
    from app.worker import build_handler

    with app.app_context():
        notification = Notification(
            event_id=str(uuid.uuid4()), event_type="order.confirmed",
            order_id=str(uuid.uuid4()), recipient="a@example.com",
            subject="s", body="b", status=NotificationStatus.SENT, attempts=1)
        db.session.add(notification)
        db.session.commit()
        notification_id = notification.id

    handler = build_handler(app)
    with patch("app.worker.send_email") as send:
        handler({"notification_id": notification_id})
    send.assert_not_called()      # no duplicate email to a real human


# ------------------------------------------------------------------- the API
def test_notifications_can_be_filtered_by_order(client, app):
    target = str(uuid.uuid4())
    with app.app_context():
        for order_id in (target, str(uuid.uuid4())):
            db.session.add(Notification(
                event_id=str(uuid.uuid4()), event_type="order.confirmed",
                order_id=order_id, recipient="a@example.com",
                subject="s", body="b"))
        db.session.commit()

    body = client.get(f"/api/v1/notifications?order_id={target}").get_json()
    assert len(body["items"]) == 1
    assert body["items"][0]["order_id"] == target


def test_invalid_query_parameters_are_rejected(client):
    assert client.get("/api/v1/notifications?limit=99999").status_code == 400
    assert client.get("/api/v1/notifications?status=BANANA").status_code == 400


def test_liveness_needs_no_dependencies(client):
    assert client.get("/health/live").status_code == 200
