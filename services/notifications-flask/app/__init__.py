"""
THE APPLICATION FACTORY PATTERN — the single most important thing to learn
about Flask, and the thing tutorials get wrong.

The tutorial version:

    app = Flask(__name__)       # module-level global
    @app.route("/")
    def index(): ...

Three problems, all of which show up the moment the project is real:

  1. UNTESTABLE. The app is configured at import time from the ambient
     environment. You cannot create a second app with test config without
     reimporting the module, and you cannot run two configurations in one test
     session at all.
  2. CIRCULAR IMPORTS. Views need `app`, `app` is in the module that imports
     views. Everyone eventually "solves" this with an import at the bottom of
     the file, which is a smell, not a solution.
  3. EXTENSIONS BIND TOO EARLY. `SQLAlchemy(app)` at import time means the
     database is bound before configuration is decided.

The factory fixes all three: `create_app(config)` builds and returns a fresh,
fully configured app. Extensions are created unbound in extensions.py and
attached inside the factory with `init_app(app)`.

    production:  gunicorn "app:create_app()"
    tests:       app = create_app(TestConfig)

Django's equivalent is settings modules plus `DJANGO_SETTINGS_MODULE`; FastAPI's
is dependency injection. Every mature framework has an answer to "how do I
build a differently-configured instance". This is Flask's.
"""

from __future__ import annotations

import os

from eventflow_common import get_logger, setup_logging
from flask import Flask, g, request

from .config import get_config
from .extensions import db, init_extensions

log = get_logger(__name__)


def create_app(config_object: type | None = None) -> Flask:
    setup_logging(os.getenv("SERVICE_NAME", "notifications"))

    app = Flask(__name__)
    app.config.from_object(config_object or get_config())

    init_extensions(app)
    _register_blueprints(app)
    _register_hooks(app)
    _register_error_handlers(app)

    log.info("flask app created", extra={"env": app.config.get("ENVIRONMENT")})
    return app


def _register_blueprints(app: Flask) -> None:
    """
    BLUEPRINTS are Flask's modularity unit — a group of routes with a shared
    url_prefix, registered onto an app. They are what keeps a Flask project
    from becoming one 2000-line views.py, and they are the reason the factory
    pattern works at all (a blueprint does not need the app object to exist).
    """
    from .api import api_bp
    from .health import health_bp

    app.register_blueprint(health_bp)
    app.register_blueprint(api_bp, url_prefix="/api/v1")


def _register_hooks(app: Flask) -> None:
    import time
    import uuid

    from eventflow_common.observability import (
        HTTP_REQUESTS,
        HTTP_SECONDS,
        correlation_id_var,
    )

    @app.before_request
    def start_request() -> None:
        incoming = request.headers.get("X-Correlation-ID", "")
        incoming = "".join(c for c in incoming if c.isalnum() or c in "-_")[:64]
        correlation_id = incoming or str(uuid.uuid4())
        g.correlation_id = correlation_id
        g.correlation_token = correlation_id_var.set(correlation_id)
        g.started = time.perf_counter()
        # `g` is Flask's per-request scratch space. It is request-scoped and
        # thread-local — safe here, and a trap if you ever store something in
        # it expecting it to survive into a background thread, where it is
        # simply absent.

    @app.after_request
    def finish_request(response):
        response.headers["X-Correlation-ID"] = getattr(g, "correlation_id", "-")
        elapsed = time.perf_counter() - getattr(g, "started", time.perf_counter())
        # request.url_rule.rule is the ROUTE TEMPLATE, not the raw path —
        # bounded metric cardinality, same rule as the other two services.
        route = request.url_rule.rule if request.url_rule else "unmatched"
        service = app.config["SERVICE_NAME"]
        HTTP_REQUESTS.labels(service, request.method, route,
                             str(response.status_code)).inc()
        HTTP_SECONDS.labels(service, request.method, route).observe(elapsed)
        return response

    @app.teardown_request
    def teardown(exc: BaseException | None) -> None:
        # teardown ALWAYS runs, including when the view raised — which
        # after_request does not guarantee. Cleanup belongs here.
        token = getattr(g, "correlation_token", None)
        if token is not None:
            correlation_id_var.reset(token)
        if exc is not None:
            db.session.rollback()
        db.session.remove()
        # session.remove() returns the connection to the pool. Forget it and
        # you leak a connection per request until Postgres refuses new ones.


def _register_error_handlers(app: Flask) -> None:
    from marshmallow import ValidationError
    from werkzeug.exceptions import HTTPException

    @app.errorhandler(ValidationError)
    def handle_validation(exc: ValidationError):
        return {"error": "validation_error", "detail": "Request validation failed.",
                "fields": exc.messages,
                "correlation_id": getattr(g, "correlation_id", None)}, 400

    @app.errorhandler(HTTPException)
    def handle_http(exc: HTTPException):
        return {"error": (exc.name or "error").lower().replace(" ", "_"),
                "detail": exc.description,
                "correlation_id": getattr(g, "correlation_id", None)}, exc.code

    @app.errorhandler(Exception)
    def handle_unexpected(exc: Exception):
        log.exception("unhandled exception", extra={"error_type": type(exc).__name__})
        return {"error": "internal_error", "detail": "An unexpected error occurred.",
                "correlation_id": getattr(g, "correlation_id", None)}, 500
