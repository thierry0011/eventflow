"""
Broker clients: Kafka (sync + async) and RabbitMQ.

WHY THIS REPO USES BOTH — the question you will be asked in an interview.

Kafka and RabbitMQ are not competitors, they are different data structures.

    KAFKA is a DISTRIBUTED, REPLAYABLE LOG.
      Messages are appended to a partitioned log and kept for a retention period
      whether or not anyone read them. Consumers track their own offset. So:
        * a new consumer can read history from the beginning (replay)
        * many independent consumer groups read the same stream, each at its
          own pace, without the producer knowing they exist
        * ordering is guaranteed per partition
        * throughput is enormous (sequential disk writes, zero-copy sends)
      Use it as the EVENT BACKBONE — the system's source of truth about what
      happened. In this repo: order.created, inventory.reserved, etc.

    RABBITMQ is a SMART MESSAGE BROKER / WORK QUEUE.
      Messages are routed by an exchange to queues and DELETED once acked. So:
        * rich routing (direct, topic, fanout, headers)
        * per-message ack/nack/requeue, TTL, priorities
        * dead-letter exchanges for free
        * competing consumers share one queue and the broker load-balances
      Use it for WORK DISTRIBUTION — "someone send this email, retry it if it
      fails, give up after five tries and put it somewhere I can inspect".

The decision rule:
    Does anyone other than the immediate consumer care that this happened, now
    or in the future?            -> it is an EVENT      -> Kafka
    Is this one unit of work that must be done once and then forgotten?
                                 -> it is a TASK/COMMAND -> RabbitMQ (or Celery)

EventFlow uses Kafka between services (facts) and RabbitMQ inside the
notification service (work items) — so you implement, not just read about, both.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Iterable

from .envelope import EventEnvelope
from .observability import (
    EVENTS_CONSUMED,
    EVENTS_PUBLISHED,
    EVENT_HANDLER_SECONDS,
    correlation_id_var,
    get_logger,
)
from .reliability import retry

log = get_logger(__name__)

SERVICE = os.getenv("SERVICE_NAME", "unknown")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


# ===========================================================================
# KAFKA PRODUCER (synchronous — Django, scripts)
# ===========================================================================
class KafkaPublisher:
    """
    Producer settings are where durability is won or lost. The defaults are
    tuned for throughput; these are tuned for not losing your customers' orders.

      acks="all"           wait until every in-sync replica has the record.
                           acks=1 loses data when the leader dies before the
                           followers caught up. acks=0 is fire-and-forget.
      enable_idempotence   the broker de-duplicates producer retries, so a
                           retry after a timeout cannot write the record twice.
      retries + backoff    survive a leader election without the caller noticing.
      linger_ms=5          wait 5ms to batch records. Costs 5ms of latency, buys
                           a large throughput win. Pure latency: set it to 0.
      compression="gzip"   JSON compresses ~5-10x. Cheap CPU, big network win.
      max_in_flight=1      the Java client allows up to 5 with idempotence on,
                           but kafka-python refuses idempotence unless it is 1.
                           Either way: >1 WITHOUT idempotence lets a retry
                           reorder records.
    """

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        from kafka import KafkaProducer  # imported lazily: keeps test startup fast

        self._producer = KafkaProducer(
            bootstrap_servers=(bootstrap_servers or BOOTSTRAP).split(","),
            acks="all",
            enable_idempotence=True,
            retries=10,
            retry_backoff_ms=200,
            max_in_flight_requests_per_connection=1,  # kafka-python requires 1 for idempotence
            linger_ms=5,
            compression_type="gzip",
            request_timeout_ms=15_000,
            value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
        )

    @retry((ConnectionError, TimeoutError, OSError), max_attempts=3)
    def publish(self, topic: str, event: EventEnvelope, *, flush: bool = True) -> None:
        future = self._producer.send(
            topic,
            key=event.partition_key,     # SAME AGGREGATE -> SAME PARTITION -> ORDER
            value=event.to_bytes(),
            headers=[
                ("event_type", event.event_type.encode()),
                ("correlation_id", event.correlation_id.encode()),
                ("schema_version", str(event.schema_version).encode()),
            ],
        )
        if flush:
            # Block until the broker acknowledges. Slower, but the caller now
            # KNOWS the event is durable. The outbox relay needs this guarantee
            # before it may mark a row as published.
            meta = future.get(timeout=15)
            log.info("event published", extra={
                "event_type": event.event_type, "event_id": event.event_id,
                "topic": meta.topic, "partition": meta.partition,
                "offset": meta.offset,
            })
        EVENTS_PUBLISHED.labels(SERVICE, event.event_type, "kafka").inc()

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


# ===========================================================================
# KAFKA CONSUMER (synchronous)
# ===========================================================================
class KafkaEventConsumer:
    """
    Consumer settings, and the one decision that matters most:

      enable_auto_commit=False
        Auto-commit commits offsets on a timer, which means it can commit an
        offset for a message your handler has not finished (or has crashed on).
        That is AT-MOST-ONCE delivery: on restart the message is skipped and the
        event is lost forever. Manual commit AFTER successful handling gives
        AT-LEAST-ONCE: on crash the message is redelivered — which is fine,
        because every handler here is idempotent.

      auto_offset_reset="earliest"
        A brand new consumer group reads the topic from the beginning. "latest"
        would silently skip everything that happened before it booted.

      group_id
        Members of a group SHARE the partitions (work splitting, scale out).
        Different groups each get a FULL copy of the stream (fan-out). This
        single parameter is how one topic feeds both the inventory service and
        the notification service independently.

      max_poll_records / max_poll_interval_ms
        If your handler is slow and you fetched 500 records, you will miss the
        poll deadline, the broker assumes you died, and the group rebalances —
        your work is redone by someone else. Fetch small batches when handlers
        are slow.
    """

    def __init__(self, topics: Iterable[str], group_id: str,
                 bootstrap_servers: str | None = None) -> None:
        from kafka import KafkaConsumer

        self.topics = list(topics)
        self.group_id = group_id
        self._consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=(bootstrap_servers or BOOTSTRAP).split(","),
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=50,
            max_poll_interval_ms=300_000,
            session_timeout_ms=45_000,
            heartbeat_interval_ms=3_000,
            consumer_timeout_ms=1_000,   # poll returns so the shutdown flag is checked
        )
        log.info("consumer started", extra={"topics": self.topics, "group": group_id})

    def run(self, handler: Callable[[EventEnvelope], None],
            should_continue: Callable[[], bool] = lambda: True) -> None:
        """
        The consume loop. Read it once and you understand every consumer you
        will ever write.

        Failure policy here is deliberately simple and deliberately loud: if the
        handler raises, we do NOT commit, we log, and we stop. The message will
        be redelivered on restart. That is right for a poison message you want
        a human to see. Production alternatives — retry topic, or dead-letter
        topic after N attempts — are described in docs/04-NOTES-EVENT-DRIVEN.md
        and implemented for RabbitMQ below.
        """
        try:
            while should_continue():
                batch = self._consumer.poll(timeout_ms=1_000, max_records=50)
                for _tp, records in batch.items():
                    for record in records:
                        if not should_continue():
                            return
                        self._handle_one(record, handler)
                    # Commit once per partition-batch, not once per message:
                    # committing per message is a network round-trip per event
                    # and will dominate your throughput.
                    self._consumer.commit()
        finally:
            self.close()

    def _handle_one(self, record: Any, handler: Callable[[EventEnvelope], None]) -> None:
        try:
            event = EventEnvelope.from_bytes(record.value)
        except Exception:
            # Unparseable message. Retrying will never help — it is poison.
            # Log loudly with the raw bytes and move on; blocking the whole
            # partition on one malformed record is worse.
            log.exception("undecodable message, skipping", extra={
                "topic": record.topic, "partition": record.partition,
                "offset": record.offset, "raw": str(record.value[:500]),
            })
            EVENTS_CONSUMED.labels(SERVICE, "unknown", "failed").inc()
            return

        token = correlation_id_var.set(event.correlation_id)
        try:
            with EVENT_HANDLER_SECONDS.labels(SERVICE, event.event_type).time():
                handler(event)
            EVENTS_CONSUMED.labels(SERVICE, event.event_type, "ok").inc()
            log.info("event handled", extra={
                "event_type": event.event_type, "event_id": event.event_id,
                "aggregate_id": event.aggregate_id,
                "partition": record.partition, "offset": record.offset,
                "lag_seconds": round(time.time() - record.timestamp / 1000, 3),
            })
        except Exception:
            EVENTS_CONSUMED.labels(SERVICE, event.event_type, "failed").inc()
            log.exception("handler failed; offset NOT committed", extra={
                "event_type": event.event_type, "event_id": event.event_id,
            })
            raise
        finally:
            correlation_id_var.reset(token)

    def close(self) -> None:
        try:
            self._consumer.close()   # commits nothing; leaves the group cleanly
            log.info("consumer closed", extra={"group": self.group_id})
        except Exception:  # pragma: no cover
            pass


# ===========================================================================
# KAFKA, ASYNC (for the FastAPI service)
# ===========================================================================
class AsyncKafkaPublisher:
    """
    Same semantics as KafkaPublisher, on aiokafka.

    Why a separate class rather than calling the sync one from async code:
    a blocking network call inside an event loop stalls EVERY coroutine on that
    loop. One synchronous `producer.send().get()` in a FastAPI handler can turn
    a 2000 rps service into a 50 rps service. In async code, every I/O call must
    be awaited — no exceptions, no "it is only a few milliseconds".
    """

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self._bootstrap = bootstrap_servers or BOOTSTRAP
        self._producer: Any = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            compression_type="gzip",
        )
        await self._producer.start()

    async def publish(self, topic: str, event: EventEnvelope) -> None:
        assert self._producer is not None, "call start() first"
        await self._producer.send_and_wait(
            topic, key=event.partition_key, value=event.to_bytes(),
        )
        EVENTS_PUBLISHED.labels(SERVICE, event.event_type, "kafka").inc()
        log.info("event published", extra={"event_type": event.event_type,
                                           "event_id": event.event_id})

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()


class AsyncKafkaConsumer:
    def __init__(self, topics: Iterable[str], group_id: str,
                 bootstrap_servers: str | None = None) -> None:
        self.topics, self.group_id = list(topics), group_id
        self._bootstrap = bootstrap_servers or BOOTSTRAP
        self._consumer: Any = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self._bootstrap,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        log.info("async consumer started", extra={"topics": self.topics,
                                                  "group": self.group_id})

    async def run(self, handler: Callable[[EventEnvelope], Any],
                  should_continue: Callable[[], bool] = lambda: True) -> None:
        assert self._consumer is not None, "call start() first"
        try:
            while should_continue():
                batch = await self._consumer.getmany(timeout_ms=1_000, max_records=50)
                for _tp, records in batch.items():
                    for record in records:
                        event = EventEnvelope.from_bytes(record.value)
                        token = correlation_id_var.set(event.correlation_id)
                        try:
                            with EVENT_HANDLER_SECONDS.labels(SERVICE, event.event_type).time():
                                await handler(event)
                            EVENTS_CONSUMED.labels(SERVICE, event.event_type, "ok").inc()
                        except Exception:
                            EVENTS_CONSUMED.labels(SERVICE, event.event_type, "failed").inc()
                            log.exception("handler failed", extra={
                                "event_id": event.event_id,
                                "event_type": event.event_type})
                            raise
                        finally:
                            correlation_id_var.reset(token)
                if batch:
                    await self._consumer.commit()
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()


# ===========================================================================
# RABBITMQ
# ===========================================================================
class RabbitPublisher:
    """
    Topology created here, and every piece of it earns its place:

        exchange  eventflow.notifications   (topic)
        queue     notifications.work        main work queue
        queue     notifications.retry       holds failures, TTL 10s, then
                                            dead-letters BACK to the work queue
        queue     notifications.dlq         gave up — a human looks here

    THE RETRY TRICK, worth memorising: RabbitMQ has no native delayed retry.
    You get one by giving a queue a message TTL plus a dead-letter exchange
    pointing at where you want the message to go when the TTL expires. Nack a
    failed message to the retry queue; 10 seconds later the broker moves it back
    to the work queue automatically. Delay without a scheduler and without the
    consumer sleeping (a sleeping consumer blocks its prefetch slot).

    Durability checklist, all four needed or you WILL lose messages on restart:
        durable exchange + durable queue + persistent messages (delivery_mode=2)
        + publisher confirms
    """

    def __init__(self, url: str | None = None, exchange: str | None = None) -> None:
        import pika

        self.url = url or RABBIT_URL
        self.exchange = exchange or os.getenv("RABBITMQ_EXCHANGE", "eventflow.notifications")
        params = pika.URLParameters(self.url)
        params.heartbeat = 30          # detect dead TCP connections
        params.blocked_connection_timeout = 60
        self._conn = pika.BlockingConnection(params)
        self._ch = self._conn.channel()
        self._ch.confirm_delivery()    # publisher confirms: publish() now raises on failure
        declare_topology(self._ch, self.exchange)

    def publish(self, routing_key: str, body: dict[str, Any] | bytes,
                *, correlation_id: str | None = None) -> None:
        import pika

        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        self._ch.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=2,                       # persist to disk
                content_type="application/json",
                correlation_id=correlation_id or correlation_id_var.get(),
                timestamp=int(time.time()),
            ),
            mandatory=True,   # raise if no queue would receive it, instead of silence
        )
        EVENTS_PUBLISHED.labels(SERVICE, routing_key, "rabbitmq").inc()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # pragma: no cover
            pass


WORK_QUEUE = "notifications.work"
RETRY_QUEUE = "notifications.retry"
DLQ = "notifications.dlq"
RETRY_DELAY_MS = 10_000
MAX_DELIVERIES = 5


def declare_topology(channel: Any, exchange: str) -> None:
    """Idempotent: every process declares it at startup, so order does not matter."""
    channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=f"{exchange}.retry", exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=f"{exchange}.dlx", exchange_type="topic", durable=True)

    channel.queue_declare(queue=WORK_QUEUE, durable=True)
    channel.queue_bind(WORK_QUEUE, exchange, routing_key="#")  # '#' = everything

    # TTL + dead-letter back to the main exchange == a delay line
    channel.queue_declare(queue=RETRY_QUEUE, durable=True, arguments={
        "x-message-ttl": RETRY_DELAY_MS,
        "x-dead-letter-exchange": exchange,
    })
    channel.queue_bind(RETRY_QUEUE, f"{exchange}.retry", routing_key="#")

    channel.queue_declare(queue=DLQ, durable=True)
    channel.queue_bind(DLQ, f"{exchange}.dlx", routing_key="#")


class RabbitWorker:
    """
    Competing-consumer worker with bounded retries and a dead-letter queue.

    prefetch_count (QoS) is the knob juniors always miss. Default: the broker
    pushes as many messages as it can at one consumer, which then sits on a
    thousand unacked messages while its idle colleague gets none. Setting
    prefetch=10 means "never give me more than 10 unacked", so work spreads
    across replicas and a slow consumer cannot hoard the queue.
    """

    def __init__(self, handler: Callable[[dict[str, Any]], None],
                 url: str | None = None, exchange: str | None = None,
                 prefetch: int = 10) -> None:
        import pika

        self.handler = handler
        self.exchange = exchange or os.getenv("RABBITMQ_EXCHANGE", "eventflow.notifications")
        params = pika.URLParameters(url or RABBIT_URL)
        params.heartbeat = 30
        self._conn = pika.BlockingConnection(params)
        self._ch = self._conn.channel()
        declare_topology(self._ch, self.exchange)
        self._ch.basic_qos(prefetch_count=prefetch)

    def run(self) -> None:
        self._ch.basic_consume(queue=WORK_QUEUE, on_message_callback=self._on_message)
        log.info("rabbit worker consuming", extra={"queue": WORK_QUEUE})
        try:
            self._ch.start_consuming()
        except KeyboardInterrupt:
            self.stop()

    def _on_message(self, ch: Any, method: Any, props: Any, body: bytes) -> None:
        import pika

        token = correlation_id_var.set(props.correlation_id or "-")
        attempts = _death_count(props) + 1
        try:
            self.handler(json.loads(body))
            ch.basic_ack(method.delivery_tag)          # ACK ONLY AFTER SUCCESS
            EVENTS_CONSUMED.labels(SERVICE, method.routing_key, "ok").inc()
        except Exception as exc:
            EVENTS_CONSUMED.labels(SERVICE, method.routing_key, "failed").inc()
            target = f"{self.exchange}.retry" if attempts < MAX_DELIVERIES \
                else f"{self.exchange}.dlx"
            log.exception("work item failed", extra={
                "attempts": attempts, "routing_to": target, "error": str(exc)})
            ch.basic_publish(
                exchange=target, routing_key=method.routing_key, body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2, correlation_id=props.correlation_id,
                    headers=(props.headers or {}) | {"x-attempts": attempts,
                                                     "x-last-error": str(exc)[:500]},
                ),
            )
            # Ack the original: we have taken responsibility by republishing it.
            # basic_nack(requeue=True) instead would hot-loop the same failure.
            ch.basic_ack(method.delivery_tag)
        finally:
            correlation_id_var.reset(token)

    def stop(self) -> None:
        try:
            self._ch.stop_consuming()
            self._conn.close()
        except Exception:  # pragma: no cover
            pass


def _death_count(props: Any) -> int:
    headers = props.headers or {}
    if "x-attempts" in headers:
        return int(headers["x-attempts"])
    for death in headers.get("x-death", []) or []:
        if death.get("queue") == RETRY_QUEUE:
            return int(death.get("count", 0))
    return 0
