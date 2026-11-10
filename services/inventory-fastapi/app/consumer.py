"""
The inventory service's Kafka consumer.

    python -m app.consumer

Consumes orders.events and performs the reservation half of the saga.

WHY `asyncio.run` AND NOT THE WEB SERVER: the same separation-of-processes rule
as the Django consumer. A consumer and an API have different scaling signals,
different failure modes, and different deploy cadences. One concern, one
process.

AN ASYNC-SPECIFIC WARNING, and it is the one that bites people moving from
Django to FastAPI: ONE BLOCKING CALL STALLS EVERY COROUTINE ON THE LOOP. A
`time.sleep(1)`, a `requests.get(...)`, or a psycopg2 query inside this file
does not slow one task down — it freezes all of them, including the heartbeat
that tells Kafka you are alive. The symptom is a consumer that gets kicked out
of the group for no apparent reason. If you must call blocking code, wrap it in
`asyncio.to_thread(...)`.
"""

from __future__ import annotations

import asyncio
import signal

from eventflow_common import EventType, get_logger, setup_logging
from eventflow_common.bus import AsyncKafkaConsumer

from . import service
from .cache import close_redis, product_key
from .config import settings
from .db import SessionFactory, dispose_engine

setup_logging(settings.service_name)
log = get_logger(__name__)

_stop = asyncio.Event()


async def handle_order_created(event) -> None:
    """
    TRANSACTION BOUNDARY, stated explicitly because it is the whole point:

        one session, one transaction, covering
            the dedup row
            the stock update
            the reservation rows
            the outbox row announcing the result

    All five commit together or none do. If the process dies anywhere in here,
    Postgres rolls everything back, Kafka never gets the offset commit, and the
    event is redelivered to a clean slate. That is what makes at-least-once
    delivery safe rather than terrifying.
    """
    async with SessionFactory() as session:
        async with session.begin():
            if await service.already_processed(session, event):
                return

            payload = event.payload
            try:
                await service.reserve_stock(
                    session,
                    order_id=payload["order_id"],
                    lines=payload["lines"],
                    causation_id=event.event_id,
                )
            except (service.InsufficientStock, service.UnknownSku) as exc:
                # NOT AN ERROR — a business outcome. The rejection event was
                # already queued inside reserve_stock, so this transaction must
                # still COMMIT. Re-raising here would roll back the rejection
                # and the order would hang in PENDING forever.
                #
                # Distinguishing "the business said no" (commit, emit, move on)
                # from "the infrastructure failed" (roll back, retry) is one of
                # the sharpest junior/senior lines in event-driven code.
                log.info("reservation refused", extra={
                    "order_id": payload["order_id"], "reason": str(exc)})

        # Cache invalidation AFTER the commit, never inside the transaction.
        # Invalidating first means a concurrent read can repopulate the cache
        # with pre-commit data and you are stale until the TTL saves you.
        skus = [line["sku"] for line in event.payload.get("lines", [])]
        if skus:
            from .cache import cache_delete
            await cache_delete(*[product_key(s) for s in skus])


async def handle_order_cancelled(event) -> None:
    """The compensating path. Releases whatever is still held for this order."""
    async with SessionFactory() as session:
        async with session.begin():
            if await service.already_processed(session, event):
                return
            await service.release_reservation(
                session,
                order_id=event.payload["order_id"],
                reason=event.payload.get("reason", "order cancelled"),
                causation_id=event.event_id,
            )


HANDLERS = {
    EventType.ORDER_CREATED.value: handle_order_created,
    EventType.ORDER_CANCELLED.value: handle_order_cancelled,
    # order.confirmed is deliberately absent. Inventory does not care that the
    # order was confirmed — the stock is already held. Consume only what you
    # act on; a handler that exists only to log is noise.
}


async def dispatch(event) -> None:
    handler = HANDLERS.get(event.event_type)
    if handler is None:
        log.debug("ignoring event", extra={"event_type": event.event_type})
        return
    await handler(event)


async def main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _stop.set)
        # add_signal_handler, not signal.signal: this version is loop-aware and
        # wakes the loop immediately, so shutdown does not wait for the current
        # poll timeout to expire.

    consumer = AsyncKafkaConsumer(
        topics=[settings.kafka_topic_orders],
        group_id=settings.consumer_group,
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )
    await consumer.start()
    try:
        await consumer.run(dispatch, should_continue=lambda: not _stop.is_set())
    finally:
        await close_redis()
        await dispose_engine()
        log.info("consumer shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())
