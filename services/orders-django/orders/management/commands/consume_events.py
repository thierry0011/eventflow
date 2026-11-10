"""
The orders service's Kafka consumer.

    python manage.py consume_events

Subscribes to inventory.events and advances the saga.

WHY THIS IS A SEPARATE PROCESS FROM THE WEB SERVER, which is not negotiable:

  * A consumer is a long-running loop. A WSGI worker is a request/response
    machine. Running the loop inside a web worker blocks that worker forever.
  * They scale on different signals. Web scales on request rate; the consumer
    scales on partition count and lag. Coupling them means over-provisioning one
    to serve the other.
  * They fail differently. A Kafka outage must not take your read API down.
  * Deployment: you may want to pause consumers during a migration while the
    API keeps serving. Impossible if they share a process.

The same reasoning applies to Celery workers, the outbox relay, and cron jobs.
One concern, one process type. Look at docker-compose.yml: four containers for
this one service, all from the same image, each with a different command. That
is the normal shape of a production Python service.
"""

from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand
from eventflow_common import EventType, get_logger
from eventflow_common.bus import KafkaEventConsumer
from eventflow_common.reliability import GracefulShutdown

from orders import services

log = get_logger(__name__)


class Command(BaseCommand):
    help = "Consume inventory events and advance the order saga"

    def handle(self, *args, **options):
        shutdown = GracefulShutdown()

        # A dispatch dict, not an if/elif ladder. Adding an event type becomes
        # one line, and an unknown type is handled explicitly rather than
        # falling off the end of the ladder into silence.
        handlers = {
            EventType.INVENTORY_RESERVED.value: services.handle_inventory_reserved,
            EventType.INVENTORY_REJECTED.value: services.handle_inventory_rejected,
        }

        def dispatch(event) -> None:
            handler = handlers.get(event.event_type)
            if handler is None:
                # NOT an error. A consumer subscribes to a topic and will see
                # event types it does not care about — including new ones added
                # by a producer that deployed this morning. Ignoring them
                # quietly (and committing the offset) is what lets producers and
                # consumers deploy independently. Crashing on an unknown type
                # recreates the lockstep deployment you built microservices to
                # escape.
                log.debug("ignoring event type", extra={"event_type": event.event_type})
                return
            handler(event)

        consumer = KafkaEventConsumer(
            topics=[settings.KAFKA_TOPIC_INVENTORY],
            group_id=settings.CONSUMER_GROUP,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        )
        consumer.run(dispatch, should_continue=lambda: not shutdown.requested)
        log.info("consumer exited cleanly")
