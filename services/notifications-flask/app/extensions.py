"""
Extensions, created UNBOUND.

This file is the other half of the application-factory pattern. Each extension
is instantiated here with no app, and bound inside `create_app` via
`init_app(app)`.

Why that matters: any module may `from .extensions import db` without importing
the app, which is what breaks the circular-import cycle that plagues
single-file Flask projects. The same object can also be bound to a different
app in tests.
"""

from __future__ import annotations

import redis as redis_lib
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


db = SQLAlchemy(model_class=Base)

_redis_client: redis_lib.Redis | None = None


def init_extensions(app: Flask) -> None:
    db.init_app(app)

    global _redis_client
    _redis_client = redis_lib.from_url(
        f"{app.config['REDIS_URL']}/0",
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
        health_check_interval=30,
    )

    if app.config.get("TESTING"):
        with app.app_context():
            db.create_all()
            # create_all is acceptable ONLY in tests. Real schema changes go
            # through migrations — see the long note in the inventory
            # service's alembic/env.py.


def get_redis() -> redis_lib.Redis:
    if _redis_client is None:
        raise RuntimeError("extensions not initialised; call create_app first")
    return _redis_client
