"""
Health endpoints and the metrics scrape target.

LIVENESS vs READINESS is a distinction people get wrong constantly, and getting
it wrong converts a small incident into a large one.

    LIVENESS  — "is this process broken beyond recovery?"
                Failure action: KILL AND RESTART THE CONTAINER.
                Therefore it must check NOTHING EXTERNAL. If liveness checked
                the database, a 60-second database blip would make Kubernetes
                kill every replica simultaneously, and when the database came
                back there would be nothing left running to serve traffic. You
                would have turned a blip into an outage with your own probe.

    READINESS — "can I serve a request RIGHT NOW?"
                Failure action: REMOVE FROM THE LOAD BALANCER, keep running.
                This one SHOULD check dependencies, because a pod that cannot
                reach Postgres should not receive traffic — but it should stay
                alive so it can rejoin the moment the dependency recovers.

    STARTUP (k8s startupProbe) — for slow boots. It suppresses the other probes
                until the app is up, so a 90-second migration does not look like
                a liveness failure.
"""

from __future__ import annotations

import os

from django.db import connection
from django.http import HttpResponse, JsonResponse
from eventflow_common import get_logger

log = get_logger(__name__)


def liveness(_request):
    """Deliberately trivial. If the process can run this, it is alive."""
    return JsonResponse({"status": "alive", "service": os.getenv("SERVICE_NAME", "orders")})


def readiness(_request):
    """
    Checks each dependency with a SHORT timeout and reports per-dependency
    detail. "not ready" without saying which dependency failed wastes the first
    ten minutes of every incident.
    """
    checks: dict[str, str] = {}
    ok = True

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"
        ok = False

    try:
        from django.core.cache import cache
        cache.set("health:ping", "1", timeout=5)
        checks["redis"] = "ok" if cache.get("health:ping") == "1" else "degraded"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"
        # Redis is a CACHE here, not a source of truth. The service can still
        # answer requests without it, just more slowly. Degraded != unready.
        # Decide per dependency whether its loss is fatal; do not reflexively
        # fail readiness on everything.

    try:
        from .models import OutboxMessage
        pending = OutboxMessage.objects.filter(published_at__isnull=True).count()
        checks["outbox_pending"] = str(pending)
        if pending > 10_000:
            checks["outbox"] = "backlog"   # visible, but not a readiness failure
    except Exception as exc:  # pragma: no cover
        checks["outbox_pending"] = f"error: {exc}"

    return JsonResponse({"status": "ready" if ok else "not_ready", "checks": checks},
                        status=200 if ok else 503)


def metrics(_request):
    """
    Prometheus scrape endpoint.

    In production this port is NOT exposed publicly — metrics leak internal
    structure and traffic volumes. Bind it to an internal network or protect it
    at the ingress.
    """
    try:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
    except ImportError:  # pragma: no cover
        return HttpResponse("prometheus_client not installed\n", status=501)
    return HttpResponse(generate_latest(), content_type=CONTENT_TYPE_LATEST)
