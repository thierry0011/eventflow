"""
Celery tasks.

THE FIVE RULES OF A GOOD CELERY TASK. Break any of them and you will eventually
have a production incident with your name on it.

1. PASS IDS, NEVER OBJECTS.
       send_email.delay(user)        # wrong
       send_email.delay(user.id)     # right
   The argument is serialised to JSON and may run minutes later on another
   machine. A serialised object is a stale snapshot; by the time it runs, the
   real row may have changed, and you will act on data that is no longer true.

2. TASKS MUST BE IDEMPOTENT.
   `acks_late=True` (set in settings) means a task is re-queued if the worker
   dies mid-run. Combined with retries, every task WILL run twice eventually.
   Design for it.

3. ALWAYS SET A TIME LIMIT.
   A task without one can hang forever holding a worker slot. Enough of those
   and your queue is full of zombies and nothing runs.

4. RETRY ONLY TRANSIENT FAILURES, WITH BACKOFF AND JITTER.
   Retrying a ValidationError 5 times is 5 identical failures. `autoretry_for`
   should name connection and timeout errors, nothing else.

5. NEVER CHAIN TASKS TO MAKE A WORKFLOW ACROSS SERVICES.
   That is what events are for. Celery chains are fine inside one service;
   across services they are a distributed monolith with a Redis dependency.
"""

from __future__ import annotations

from celery import shared_task
from django.conf import settings
from eventflow_common import get_logger
from eventflow_common.observability import OUTBOX_PENDING

from . import services
from .models import OutboxMessage, ProcessedEvent

log = get_logger(__name__)


@shared_task(
    bind=True,
    autoretry_for=(ConnectionError, TimeoutError),
    retry_backoff=True,        # 1s, 2s, 4s, 8s ...
    retry_backoff_max=300,
    retry_jitter=True,         # spread the herd; see reliability.retry
    max_retries=5,
    time_limit=120,
    soft_time_limit=100,
)
def expire_stale_orders(self) -> int:
    """
    The saga timeout sweeper. Runs every minute (see config/celery.py).

    `shared_task`, not `@app.task`: shared_task does not bind to a specific
    Celery app at import time, so the module stays importable in tests and in
    any app that includes it. Use it by default.
    """
    return services.expire_pending_orders(settings.RESERVATION_TTL_SECONDS)


@shared_task
def report_outbox_depth() -> int:
    """
    Publishes outbox depth as a gauge.

    ALERT ON THE DERIVATIVE, NOT THE VALUE. A pending count of 500 is fine if
    it is 0 again in two seconds. A pending count of 50 that has not moved in
    five minutes means the relay is dead and events are silently not leaving
    the service. Alert on "pending > 0 AND unchanged for 5 minutes".
    """
    pending = OutboxMessage.objects.filter(published_at__isnull=True).count()
    OUTBOX_PENDING.labels(settings.SERVICE_NAME).set(pending)
    return pending


@shared_task
def prune_processed_events(older_than_days: int = 7) -> int:
    """
    Housekeeping for the idempotency table.

    Kafka retention here is 7 days, so nothing older than that can possibly be
    redelivered and its dedup row is dead weight. Unbounded growth of an
    infrastructure table is a classic slow-burn outage: fine for a year, then
    the index no longer fits in memory and every insert gets slow at once.

    Deleting in CHUNKS, not one giant DELETE: a multi-million-row delete takes a
    long lock, bloats the WAL, and can stall replication. Small batches with
    breathing room between them is how you do maintenance on a live database.
    """
    from django.utils import timezone

    cutoff = timezone.now() - timezone.timedelta(days=older_than_days)
    total = 0
    while True:
        ids = list(ProcessedEvent.objects
                   .filter(processed_at__lt=cutoff)
                   .values_list("event_id", flat=True)[:1000])
        if not ids:
            break
        deleted, _ = ProcessedEvent.objects.filter(event_id__in=ids).delete()
        total += deleted
    if total:
        log.info("pruned processed events", extra={"deleted": total})
    return total
