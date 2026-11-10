"""
The RabbitMQ worker — where notifications are actually "sent".

    python -m app.worker

Scale this by running more replicas. They are COMPETING CONSUMERS on one queue:
RabbitMQ hands each message to exactly one of them, so throughput scales
linearly with replica count and no coordination is needed. Contrast with Kafka,
where parallelism is capped by the PARTITION COUNT — 3 partitions means a
fourth consumer in the group sits idle. Knowing which broker scales which way
is the kind of detail that separates "I have used Kafka" from "I have operated
Kafka".

Retry behaviour, provided by the topology in eventflow_common.bus:

    attempt 1 fails -> republished to the retry queue (TTL 10s)
    10s later       -> dead-lettered BACK to the work queue automatically
    ... up to 5 attempts ...
    attempt 5 fails -> dead-lettered to notifications.dlq, where a human looks

The worker itself never sleeps between retries, which matters: a sleeping
consumer still occupies its prefetch slot and its memory. The BROKER does the
waiting. That is the right place for it.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone

from eventflow_common import get_logger, setup_logging
from eventflow_common.bus import RabbitWorker

from . import create_app
from .extensions import db
from .models import Notification, NotificationStatus

setup_logging("notifications-worker")
log = get_logger(__name__)

FAILURE_RATE = 0.15   # simulated flakiness so you can watch retries happen


def send_email(recipient: str, subject: str, body: str) -> None:
    """
    Stands in for a real provider (SES, SendGrid, Mailgun).

    Deliberately fails ~15% of the time so that running `make smoke` and
    watching `make logs s=notifications-worker` SHOWS you the retry path
    instead of describing it. Watch a message fail, land in the retry queue,
    and come back ten seconds later. Then open the RabbitMQ UI at
    localhost:15672 and look at the queue depths while it happens.

    In a real implementation this is also where you would put:
      * a timeout on the provider call (always)
      * a circuit breaker around the provider (see reliability.py)
      * provider-side idempotency keys, so a retry does not double-send
    """
    if random.random() < FAILURE_RATE:
        raise ConnectionError("simulated SMTP failure")
    log.info("email sent", extra={"recipient": recipient, "subject": subject})


def build_handler(app):
    def handle(message: dict) -> None:
        with app.app_context():
            notification = db.session.get(Notification, message["notification_id"])
            if notification is None:
                log.warning("notification row missing, discarding",
                            extra={"notification_id": message["notification_id"]})
                return   # return, not raise: retrying will never find it

            if notification.status == NotificationStatus.SENT:
                # IDEMPOTENCY AT THE LAST MILE. The message may be redelivered
                # after a successful send whose ack was lost. Checking state
                # before acting is what stops a customer getting the same email
                # five times — the most visible possible symptom of a system
                # that does not take at-least-once delivery seriously.
                log.info("already sent, skipping",
                         extra={"notification_id": notification.id})
                return

            notification.attempts += 1
            try:
                send_email(notification.recipient, notification.subject,
                           notification.body)
            except Exception as exc:
                notification.last_error = str(exc)[:1000]
                notification.status = (NotificationStatus.DEAD
                                       if notification.attempts >= 5
                                       else NotificationStatus.FAILED)
                db.session.commit()
                raise   # RE-RAISE so RabbitWorker routes it to retry/DLQ

            notification.status = NotificationStatus.SENT
            notification.sent_at = datetime.now(timezone.utc)
            notification.last_error = ""
            db.session.commit()

    return handle


def main() -> None:
    app = create_app()
    worker = RabbitWorker(
        handler=build_handler(app),
        url=app.config["RABBITMQ_URL"],
        exchange=app.config["RABBITMQ_EXCHANGE"],
        prefetch=10,
        # prefetch=10: at most 10 unacked messages per worker. Unlimited (the
        # default) lets one worker hoard the whole queue while its colleagues
        # idle. See the explanation on RabbitWorker.
    )
    worker.run()


if __name__ == "__main__":
    main()
