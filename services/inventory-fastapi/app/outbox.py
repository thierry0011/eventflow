"""
Async outbox relay for the inventory service.

    python -m app.outbox

Functionally identical to the Django relay (`relay_outbox.py`) — deliberately.
Read both side by side: the SQL is the same, the pattern is the same, only the
ORM syntax differs. That comparison is the fastest way to internalise that
these patterns are architecture, not framework features.

This one also runs the reservation-expiry sweeper on the same loop, since both
are periodic maintenance and neither is heavy. Whether to combine such jobs is
a judgement call: one process is simpler to operate, two scale and fail
independently. At this size, one.
"""

from __future__ import annotations

import asyncio
import json
import signal

from eventflow_common import EventEnvelope, get_logger, setup_logging
from eventflow_common.bus import AsyncKafkaPublisher
from eventflow_common.observability import OUTBOX_PENDING, correlation_id_var
from sqlalchemy import func, select, update

from . import service
from .config import settings
from .db import SessionFactory, dispose_engine
from .models import OutboxMessage

setup_logging(settings.service_name)
log = get_logger(__name__)

MAX_ATTEMPTS = 10
_stop = asyncio.Event()


async def drain_once(publisher: AsyncKafkaPublisher) -> int:
    published = 0
    async with SessionFactory() as session:
        async with session.begin():
            rows = (await session.scalars(
                select(OutboxMessage)
                .where(OutboxMessage.published_at.is_(None),
                       OutboxMessage.attempts < MAX_ATTEMPTS)
                .order_by(OutboxMessage.id)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
                # SKIP LOCKED again: multiple relay replicas divide the work
                # instead of queueing behind each other. See relay_outbox.py.
            )).all()

            for row in rows:
                token = correlation_id_var.set(row.correlation_id)
                try:
                    event = EventEnvelope.from_bytes(json.dumps(row.payload).encode())
                    await publisher.publish(row.topic, event)
                    row.published_at = func.now()
                    published += 1
                except Exception as exc:
                    row.attempts += 1
                    row.last_error = str(exc)[:2000]
                    log.exception("outbox publish failed",
                                  extra={"outbox_id": row.id,
                                         "attempts": row.attempts})
                    if row.attempts >= MAX_ATTEMPTS:
                        log.critical("outbox message dead-lettered",
                                     extra={"outbox_id": row.id,
                                            "event_type": row.event_type})
                    break   # stop the batch; the broker is probably unwell
                finally:
                    correlation_id_var.reset(token)

        pending = await session.scalar(
            select(func.count()).select_from(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
        )
        OUTBOX_PENDING.labels(settings.service_name).set(pending or 0)
    return published


async def sweep_expired_reservations() -> None:
    """Every 60s, release reservations whose TTL has passed."""
    while not _stop.is_set():
        try:
            async with SessionFactory() as session:
                async with session.begin():
                    await service.expire_stale_reservations(session)
        except Exception:
            log.exception("reservation sweep failed")
            # Swallow and continue. A periodic job that dies on its first error
            # stops running forever, and nobody notices until the thing it was
            # preventing has been accumulating for a week.
        await _sleep_or_stop(60)


async def _sleep_or_stop(seconds: float) -> None:
    """
    Interruptible sleep. A plain `asyncio.sleep(60)` would make SIGTERM wait up
    to a minute, and the orchestrator SIGKILLs after 30 seconds — so the
    process dies hard, mid-work, every single deploy.
    """
    try:
        await asyncio.wait_for(_stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def relay_loop() -> None:
    publisher = AsyncKafkaPublisher(settings.kafka_bootstrap_servers)
    await publisher.start()
    try:
        while not _stop.is_set():
            published = await drain_once(publisher)
            if published < settings.outbox_batch_size:
                await _sleep_or_stop(settings.outbox_poll_interval_seconds)
    finally:
        await publisher.stop()


async def main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _stop.set)

    log.info("outbox relay + reservation sweeper started")
    # gather runs both coroutines concurrently on ONE event loop — no threads,
    # no extra containers. This is what async buys you for IO-bound work.
    await asyncio.gather(relay_loop(), sweep_expired_reservations())
    await dispose_engine()
    log.info("relay stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())
