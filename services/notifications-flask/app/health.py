"""Health and metrics blueprint. Same liveness/readiness split as the other
two services — see orders-django/orders/health.py for the full reasoning."""

from __future__ import annotations

from flask import Blueprint, Response, current_app

from .extensions import db, get_redis

health_bp = Blueprint("health", __name__)


@health_bp.get("/health/live")
def liveness():
    return {"status": "alive", "service": current_app.config["SERVICE_NAME"]}


@health_bp.get("/health/ready")
def readiness():
    checks: dict[str, str] = {}
    ok = True

    try:
        db.session.execute(db.text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"
        ok = False

    try:
        get_redis().ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    try:
        import pika

        connection = pika.BlockingConnection(
            pika.URLParameters(current_app.config["RABBITMQ_URL"]))
        connection.close()
        checks["rabbitmq"] = "ok"
    except Exception as exc:
        checks["rabbitmq"] = f"error: {exc}"
        ok = False
        # RabbitMQ IS fatal for this service: without it the bridge cannot
        # queue work and the API's whole purpose is gone. Compare with Redis
        # above, which is only a cache here. Decide per dependency whether its
        # loss means "unready" or merely "degraded" — never reflexively fail
        # readiness on everything you happen to connect to.

    return ({"status": "ready" if ok else "not_ready", "checks": checks},
            200 if ok else 503)


@health_bp.get("/metrics")
def metrics():
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
