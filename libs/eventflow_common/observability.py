"""
Observability: structured logging, correlation ids, metrics.

The junior/senior difference in one sentence:

    A junior adds logging when something breaks.
    A senior designs the system so that when something breaks, the answer is
    already in the logs.

Three things make that true, and all three are here:

1. STRUCTURED LOGS (JSON). `logger.info("order created", order_id=..., total=...)`
   not `logger.info(f"order {id} created for {total}")`. A string must be parsed
   with a regex that someone will break next sprint; a JSON field can be
   filtered, aggregated and alerted on. Log lines are data, not prose.

2. CORRELATION IDS. One id per user action, injected at the HTTP edge, carried
   through every event envelope, attached to every log line automatically via a
   ContextVar. Without it, debugging a distributed system means grepping three
   services by timestamp and hoping.

3. METRICS, not just logs. Logs answer "what happened to THIS request". Metrics
   answer "is the system healthy" — and the four that matter for every service
   are the RED metrics: Rate, Errors, Duration. For consumers add a fourth:
   LAG. Consumer lag is the heartbeat of an event-driven system.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

# ---------------------------------------------------------------------------
# Correlation id
#
# ContextVar, not threading.local: a ContextVar is correct under threads AND
# under asyncio tasks. Using threading.local in an async service is a classic
# bug — two concurrent requests on the same thread will read each other's value.
# ---------------------------------------------------------------------------
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")


def new_correlation_id() -> str:
    return str(uuid.uuid4())


@contextmanager
def correlation_context(correlation_id: str | None = None):
    """Bind a correlation id for the duration of a block (used by consumers)."""
    token = correlation_id_var.set(correlation_id or new_correlation_id())
    try:
        yield correlation_id_var.get()
    finally:
        correlation_id_var.reset(token)


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------
_RESERVED = {
    "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
    "levelname", "levelno", "lineno", "module", "msecs", "message", "msg", "name",
    "pathname", "process", "processName", "relativeCreated", "stack_info",
    "thread", "threadName", "taskName",
}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
                  + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id_var.get(),
        }
        # anything passed via logger.info("...", extra={...}) becomes a field
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                doc[key] = value
        if record.exc_info:
            doc["exception"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


class ConsoleFormatter(logging.Formatter):
    """Human-readable. For local work only — machines get JSON."""

    COLORS = {"DEBUG": "\033[36m", "INFO": "\033[32m", "WARNING": "\033[33m",
              "ERROR": "\033[31m", "CRITICAL": "\033[1;31m"}

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        extras = " ".join(
            f"\033[2m{k}=\033[0m{v}"
            for k, v in record.__dict__.items()
            if k not in _RESERVED and not k.startswith("_")
        )
        cid = correlation_id_var.get()[:8]
        base = (f"{color}{record.levelname:<7}\033[0m "
                f"\033[2m[{cid}]\033[0m {record.getMessage()}")
        if extras:
            base = f"{base}  {extras}"
        if record.exc_info:
            base = f"{base}\n{self.formatException(record.exc_info)}"
        return base


def setup_logging(service: str | None = None) -> None:
    """
    Call ONCE at process start, before anything else logs.

    Note what is NOT here: no file handlers, no log rotation, no syslog. A
    containerised 12-factor app writes to stdout and nothing else. Collecting,
    rotating and shipping logs is the platform's job (Docker, k8s, Loki,
    CloudWatch). An app that manages its own log files cannot be scaled
    horizontally without losing lines.
    """
    service = service or os.getenv("SERVICE_NAME", "unknown-service")
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    fmt = os.getenv("LOG_FORMAT", "json").lower()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service) if fmt == "json" else ConsoleFormatter())

    root = logging.getLogger()
    root.handlers.clear()        # stop duplicate lines when a framework pre-configured one
    root.addHandler(handler)
    root.setLevel(level)

    # Third-party libraries are chatty. Turn them down, do not turn them off —
    # you want their warnings, not their per-poll debug spam.
    for noisy in ("kafka", "aiokafka", "pika", "urllib3", "asyncio", "botocore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


# ---------------------------------------------------------------------------
# Metrics
#
# prometheus_client is optional here so the shared lib never hard-fails in a
# context that does not have it (a test runner, a one-off script). Graceful
# degradation of an observability dependency is deliberate: monitoring must
# never be the reason the thing it monitors fell over.
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge, Histogram

    EVENTS_PUBLISHED = Counter(
        "eventflow_events_published_total",
        "Events successfully published to a broker",
        ["service", "event_type", "broker"],
    )
    EVENTS_CONSUMED = Counter(
        "eventflow_events_consumed_total",
        "Events consumed and handled",
        ["service", "event_type", "outcome"],   # outcome: ok | duplicate | failed
    )
    EVENT_HANDLER_SECONDS = Histogram(
        "eventflow_event_handler_seconds",
        "Time spent inside an event handler",
        ["service", "event_type"],
        # Explicit buckets. The defaults are tuned for HTTP; event handlers that
        # touch a database and a broker live in a different range.
        buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
    )
    OUTBOX_PENDING = Gauge(
        "eventflow_outbox_pending",
        "Outbox rows not yet published — if this climbs, the relay is stuck",
        ["service"],
    )
    HTTP_REQUESTS = Counter(
        "eventflow_http_requests_total", "HTTP requests",
        ["service", "method", "path", "status"],
    )
    HTTP_SECONDS = Histogram(
        "eventflow_http_request_seconds", "HTTP request duration",
        ["service", "method", "path"],
    )
    METRICS_ENABLED = True
except ImportError:  # pragma: no cover
    METRICS_ENABLED = False

    class _Noop:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def dec(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
        def time(self): return _NoopTimer()

    class _NoopTimer:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    EVENTS_PUBLISHED = EVENTS_CONSUMED = EVENT_HANDLER_SECONDS = _Noop()
    OUTBOX_PENDING = HTTP_REQUESTS = HTTP_SECONDS = _Noop()
