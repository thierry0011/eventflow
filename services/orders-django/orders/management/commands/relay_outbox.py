"""
The outbox relay.

    python manage.py relay_outbox

Reads unpublished outbox rows and publishes them to Kafka. Runs as its own
container, forever.

THE ONE SQL TRICK TO REMEMBER FROM THIS WHOLE REPO:

    SELECT ... FOR UPDATE SKIP LOCKED

Without SKIP LOCKED, running two relay replicas means replica B blocks waiting
for replica A's locks — zero parallelism, and your "scaled out" relay is one
relay with a spare. With SKIP LOCKED, replica B simply steps over the rows A has
locked and takes the next free ones. Instant, correct, horizontal scaling of a
queue built on a plain SQL table.

It is also how you build a decent job queue on Postgres without adding a broker
at all. Worth knowing both for the outbox and for the many times "we need a
queue" really means "we need a table and SKIP LOCKED".

DELIVERY SEMANTICS, stated honestly: this is AT-LEAST-ONCE. The relay can
publish to Kafka and then crash before marking the row published; on restart it
publishes again. That is not a defect to be engineered away — it is the correct
trade, and it is why every consumer in this system is idempotent. Chasing
exactly-once across a broker boundary is a well-known way to waste a quarter.
"""

from __future__ import annotations

import json
import time

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from eventflow_common import EventEnvelope, get_logger
from eventflow_common.bus import KafkaPublisher
from eventflow_common.observability import OUTBOX_PENDING, correlation_id_var
from eventflow_common.reliability import GracefulShutdown

from orders.models import OutboxMessage

log = get_logger(__name__)

MAX_ATTEMPTS = 10


class Command(BaseCommand):
    help = "Publish pending outbox messages to Kafka (runs forever)"

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true",
                            help="drain once and exit (used by tests)")
        parser.add_argument("--batch-size", type=int,
                            default=settings.OUTBOX_BATCH_SIZE)

    def handle(self, *args, **options):
        shutdown = GracefulShutdown()
        publisher = KafkaPublisher(settings.KAFKA_BOOTSTRAP_SERVERS)
        interval = settings.OUTBOX_POLL_INTERVAL_SECONDS
        batch_size = options["batch_size"]

        log.info("outbox relay started", extra={"batch_size": batch_size,
                                                "interval_seconds": interval})
        try:
            while not shutdown.requested:
                published = self._drain(publisher, batch_size)
                if options["once"]:
                    break
                # Only sleep when there was nothing to do. A full batch means
                # there is probably more waiting — go straight round again
                # instead of adding latency for no reason.
                if published < batch_size:
                    time.sleep(interval)
        finally:
            publisher.close()
            log.info("outbox relay stopped cleanly")

    def _drain(self, publisher: KafkaPublisher, batch_size: int) -> int:
        count = 0
        # One transaction per message, not one for the whole batch. A single
        # poisonous message then fails alone instead of rolling back the 99 that
        # published fine — and the lock is held for milliseconds, not seconds.
        while count < batch_size:
            with transaction.atomic():
                row = (OutboxMessage.objects
                       .select_for_update(skip_locked=True)
                       .filter(published_at__isnull=True,
                               attempts__lt=MAX_ATTEMPTS)
                       .order_by("id")       # id order == causal order
                       .first())
                if row is None:
                    break

                token = correlation_id_var.set(row.correlation_id)
                try:
                    event = EventEnvelope.from_bytes(json.dumps(row.payload).encode())
                    publisher.publish(row.topic, event, flush=True)
                    # flush=True: block for the broker ack. Only mark the row
                    # published once Kafka has DURABLY accepted it. Marking it
                    # first would lose the event on a broker failure.
                    row.published_at = timezone.now()
                    row.save(update_fields=["published_at"])
                    count += 1
                except Exception as exc:
                    row.attempts += 1
                    row.last_error = str(exc)[:2000]
                    row.save(update_fields=["attempts", "last_error"])
                    log.exception("outbox publish failed", extra={
                        "outbox_id": row.id, "event_type": row.event_type,
                        "attempts": row.attempts,
                    })
                    if row.attempts >= MAX_ATTEMPTS:
                        # Give up on this row so it stops blocking the queue.
                        # It stays in the table, unpublished, with the error
                        # recorded — a dead-letter row a human must look at.
                        # ALERT ON THIS. Silent give-up is data loss.
                        log.critical("outbox message dead-lettered", extra={
                            "outbox_id": row.id, "event_type": row.event_type})
                    break   # back off rather than spinning on a broken broker
                finally:
                    correlation_id_var.reset(token)

        pending = OutboxMessage.objects.filter(published_at__isnull=True).count()
        OUTBOX_PENDING.labels(settings.SERVICE_NAME).set(pending)
        if count:
            log.info("outbox drained", extra={"published": count, "pending": pending})
        return count
