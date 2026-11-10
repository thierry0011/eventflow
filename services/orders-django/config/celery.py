"""
Celery application.

WHEN TO USE CELERY AND WHEN TO USE KAFKA — asked in every interview:

    Celery  = MY OWN deferred work. "Send this email", "regenerate this report",
              "expire stale orders every minute". Nobody outside this service
              knows or cares that the task exists. The task is an implementation
              detail and can change freely.

    Kafka   = A FACT other services need. "An order was created." The producer
              does not know who consumes it; the event is a public contract.

Using Kafka for "send an email" couples your mail logic to a durable public
contract. Using Celery to tell another service something means that service now
needs your broker, your task signature and your Python code — you have built a
distributed monolith.
"""

import os

from celery import Celery
from celery.schedules import crontab

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("orders")
# namespace="CELERY" => every CELERY_* setting in settings.py is picked up, so
# there is exactly one place config lives.
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

app.conf.beat_schedule = {
    # The saga timeout. If inventory never answers (crashed, poisoned, network
    # partition), an order would sit PENDING forever. A periodic sweeper is the
    # safety net every saga needs — without it, "eventual consistency" quietly
    # becomes "never consistent".
    "expire-stale-pending-orders": {
        "task": "orders.tasks.expire_stale_orders",
        "schedule": crontab(minute="*"),      # every minute
    },
    # Operational visibility: publishes outbox depth as a metric. If this climbs
    # and does not come down, the relay is stuck and events are not leaving.
    "report-outbox-depth": {
        "task": "orders.tasks.report_outbox_depth",
        "schedule": 30.0,
    },
}


@app.task(bind=True)
def debug_task(self):  # pragma: no cover
    print(f"request: {self.request!r}")
