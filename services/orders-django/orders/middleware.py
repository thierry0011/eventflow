"""
Middleware: correlation ids and RED metrics.

Middleware is the right place for anything that must happen on EVERY request
and is not business logic. If you find yourself copying the same three lines
into every view, you wanted middleware.

Django middleware is a chain of callables wrapping each other. The code before
`self.get_response(request)` runs on the way in; the code after runs on the way
out. Exceptions from a view propagate out through that same chain, which is why
the timing below must be in a `finally`.
"""

from __future__ import annotations

import time
import uuid
from typing import Callable

from django.http import HttpRequest, HttpResponse
from eventflow_common.observability import (
    HTTP_REQUESTS,
    HTTP_SECONDS,
    correlation_id_var,
    get_logger,
)

log = get_logger(__name__)

HEADER = "HTTP_X_CORRELATION_ID"


class CorrelationIdMiddleware:
    """
    Accept a correlation id from the caller, or mint one; put it in a ContextVar
    so every log line and every event emitted during this request carries it;
    echo it back in the response header so the CLIENT can quote it in a bug
    report.

    This is the cheapest observability win available. Five minutes of work, and
    "the checkout failed at about 2pm" becomes a single filter in your log tool
    that shows every step across all three services.

    Security note: the incoming value is used in logs and propagated outward, so
    it is bounded and sanitised. Never trust a header's length or content.
    """

    def __init__(self, get_response: Callable) -> None:
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        incoming = request.META.get(HEADER, "")
        incoming = "".join(c for c in incoming if c.isalnum() or c in "-_")[:64]
        correlation_id = incoming or str(uuid.uuid4())

        token = correlation_id_var.set(correlation_id)
        request.correlation_id = correlation_id
        try:
            response = self.get_response(request)
            response["X-Correlation-ID"] = correlation_id
            return response
        finally:
            correlation_id_var.reset(token)


class MetricsMiddleware:
    """
    The RED method: Rate, Errors, Duration. Three numbers that tell you whether
    a service is healthy, and the basis of almost every useful alert.

    THE CARDINALITY TRAP, which breaks real Prometheus installations:
    a time series is created for every unique combination of label values. Using
    the raw path as a label means /orders/<uuid> creates one series PER ORDER.
    A million orders is a million time series and a dead monitoring stack.

    So we label with the ROUTE PATTERN (`/api/v1/orders/<pk>/`), resolved by
    Django's URL resolver, not the concrete path. Bounded cardinality, forever.
    Never put a user id, order id, email or free-text value in a metric label.
    """

    def __init__(self, get_response: Callable) -> None:
        self.get_response = get_response
        from django.conf import settings
        self.service = settings.SERVICE_NAME

    def __call__(self, request: HttpRequest) -> HttpResponse:
        started = time.perf_counter()
        status_code = 500
        try:
            response = self.get_response(request)
            status_code = response.status_code
            return response
        finally:
            elapsed = time.perf_counter() - started
            route = self._route(request)
            HTTP_REQUESTS.labels(self.service, request.method, route,
                                 str(status_code)).inc()
            HTTP_SECONDS.labels(self.service, request.method, route).observe(elapsed)

            if elapsed > 1.0:
                # Log slow requests explicitly. A p99 spike in a dashboard tells
                # you something is slow; this line tells you WHICH request was.
                log.warning("slow request", extra={
                    "method": request.method, "route": route,
                    "duration_ms": round(elapsed * 1000, 1), "status": status_code,
                })

    @staticmethod
    def _route(request: HttpRequest) -> str:
        match = getattr(request, "resolver_match", None)
        if match and match.route:
            return f"/{match.route}"
        return "unmatched"
