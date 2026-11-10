"""
Importing the Celery app here is required, not stylistic.

Django imports this package at startup; this line guarantees the Celery app
exists and its `@shared_task` decorators are bound before any task is called.
Without it you get the classic "received unregistered task of type ..." error
that costs beginners an afternoon.
"""

from .celery import app as celery_app

__all__ = ("celery_app",)
