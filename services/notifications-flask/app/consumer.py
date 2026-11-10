"""
The Kafka -> RabbitMQ bridge.

    python -m app.consumer

THIS FILE IS THE ANSWER TO "WHY BOTH BROKERS?", so read it with
libs/eventflow_common/bus.py open at the RabbitMQ section.

What it does:
    1. consume domain EVENTS from Kafka (facts: an order was confirmed)
    2. decide, per event, what HUMAN COMMUNICATION that implies
    3. record the intended notification in the database (the audit log)
    4. publish a WORK ITEM to RabbitMQ for the worker to actually send

Why not just send the email here? Because consuming and sending have completely
different failure characteristics:

    Kafka consumption must keep up with the stream. If this loop blocks for 30
    seconds on a slow SMTP server, consumer lag grows, the group rebalances,
    and you start reprocessing. The consumer's only job is to be fast.

    Sending is slow, flaky, and needs per-message retry with backoff and a
    place to put permanent failures. That is precisely what a RabbitMQ work
    queue with a TTL retry queue and a DLQ gives you, and precisely what Kafka
    does NOT give you — Kafka has no per-message retry; its unit of progress is
    the partition offset, so one stuck message blocks everything behind it.

So: Kafka for the stream, RabbitMQ for the work. Each tool doing the job its
data structure is actually good at. That sentence is the interview answer.
"""

from __future__ import annotations

from eventflow_common import EventType, get_logger, setup_logging
from eventflow_common.bus import KafkaEventConsumer, RabbitPublisher
from eventflow_common.reliability import GracefulShutdown

from . import create_app
from .extensions import db
from .models import Notification, NotificationStatus, ProcessedEvent

setup_logging("notifications-bridge")
log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Event -> message templates
#
# Kept as pure data. A new event type that needs an email is one dict entry,
# not a new branch in a growing if/elif ladder. When this grows past a dozen
# entries, it wants to move into the database so non-engineers can edit copy
# without a deploy.
# ---------------------------------------------------------------------------
TEMPLATES = {
    EventType.ORDER_CREATED.value: {
        "subject": "We have received your order",
        "body": ("Thanks! Order {order_id} is being processed. "
                 "We will confirm as soon as stock is secured."),
    },
    EventType.ORDER_CONFIRMED.value: {
        "subject": "Your order is confirmed",
        "body": "Good news — order {order_id} is confirmed. Total: {total_amount}.",
    },
    EventType.ORDER_CANCELLED.value: {
        "subject": "Your order was cancelled",
        "body": "Order {order_id} could not be completed. Reason: {reason}.",
    },
    EventType.INVENTORY_REJECTED.value: None,
    # Explicitly None, not simply absent. This records a DECISION: inventory
    # rejection is an internal detail, and the customer hears about it via
    # order.cancelled instead. Without the explicit entry, the next engineer
    # will assume it was an oversight and "fix" it into a duplicate email.
}


def build_handler(app, publisher: RabbitPublisher):
    def handle(event) -> None:
        template = TEMPLATES.get(event.event_type)
        if template is None:
            return

        # Flask's app context is NOT automatic outside a request. Any code that
        # touches db.session from a background process must open one
        # explicitly, or you get the classic "Working outside of application
        # context" error. This is Flask's biggest gotcha for background work.
        with app.app_context():
            if _already_processed(event):
                return

            payload = event.payload
            order_id = payload.get("order_id", event.aggregate_id)
            recipient = f"customer-{payload.get('customer_id', 'unknown')}@example.com"

            notification = Notification(
                event_id=event.event_id,
                event_type=event.event_type,
                order_id=order_id,
                recipient=recipient,
                channel="email",
                subject=template["subject"],
                body=template["body"].format(
                    order_id=order_id,
                    total_amount=payload.get("total_amount", "-"),
                    reason=payload.get("reason", "not specified"),
                ),
                status=NotificationStatus.PENDING,
            )
            db.session.add(notification)
            db.session.commit()
            # COMMIT FIRST, PUBLISH SECOND — and note this is a dual write,
            # the very thing the outbox pattern exists to prevent.
            #
            # It is acceptable HERE, and you should be able to say why: the
            # downside of the crash window is a notification row stuck in
            # PENDING. No money, no stock, no cross-service state. A reaper
            # that re-publishes PENDING rows older than a minute closes it
            # completely (left as an exercise).
            #
            # The judgement being modelled: know the pattern, know its cost,
            # and apply it where the consequence justifies it. Using a full
            # outbox for every side effect everywhere is over-engineering;
            # knowing WHICH writes need one is the senior skill.

            publisher.publish(
                routing_key=event.event_type,
                body={"notification_id": notification.id,
                      "recipient": notification.recipient,
                      "subject": notification.subject,
                      "body": notification.body,
                      "channel": notification.channel},
                correlation_id=event.correlation_id,
            )
            log.info("notification queued", extra={
                "notification_id": notification.id,
                "event_type": event.event_type, "order_id": order_id})

    return handle


def _already_processed(event) -> bool:
    from sqlalchemy.exc import IntegrityError

    try:
        db.session.add(ProcessedEvent(event_id=event.event_id,
                                      event_type=event.event_type))
        db.session.commit()
        return False
    except IntegrityError:
        db.session.rollback()
        # ROLLBACK IS MANDATORY. A failed statement poisons the SQLAlchemy
        # session: every subsequent query raises PendingRollbackError until you
        # roll back. Forgetting this turns one duplicate event into a worker
        # that fails on everything afterwards.
        log.info("duplicate event ignored", extra={"event_id": event.event_id})
        return True


def main() -> None:
    app = create_app()
    shutdown = GracefulShutdown()
    publisher = RabbitPublisher(app.config["RABBITMQ_URL"],
                                app.config["RABBITMQ_EXCHANGE"])
    consumer = KafkaEventConsumer(
        topics=[app.config["KAFKA_TOPIC_ORDERS"], app.config["KAFKA_TOPIC_INVENTORY"]],
        group_id=app.config["CONSUMER_GROUP"],
        bootstrap_servers=app.config["KAFKA_BOOTSTRAP_SERVERS"],
    )
    try:
        consumer.run(build_handler(app, publisher),
                     should_continue=lambda: not shutdown.requested)
    finally:
        publisher.close()
        log.info("bridge stopped cleanly")


if __name__ == "__main__":
    main()
