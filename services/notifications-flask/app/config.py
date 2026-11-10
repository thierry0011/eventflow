"""
Configuration classes.

Flask's `app.config.from_object(SomeClass)` reads UPPERCASE attributes off a
class. Subclassing gives you environment variants without duplication, and
TestConfig is what makes the application factory worth having.
"""

from __future__ import annotations

import os


def _db_url() -> str:
    return (f"postgresql+psycopg://{os.getenv('POSTGRES_USER', 'eventflow')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'eventflow')}@"
            f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{os.getenv('POSTGRES_DB', 'notifications_db')}")


class BaseConfig:
    SERVICE_NAME = os.getenv("SERVICE_NAME", "notifications")
    ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
    SECRET_KEY = os.getenv("JWT_SECRET", "dev-only-change-me")

    SQLALCHEMY_DATABASE_URI = _db_url()
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    # TRACK_MODIFICATIONS fires a signal on every object change and costs
    # memory and time for a feature almost nobody uses. Off, always.
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_size": 5,
        "max_overflow": 5,
        "pool_pre_ping": True,
        "pool_recycle": 1800,
    }

    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "orders.events")
    KAFKA_TOPIC_INVENTORY = os.getenv("KAFKA_TOPIC_INVENTORY", "inventory.events")
    CONSUMER_GROUP = "notifications-service"
    # A DIFFERENT consumer group from the inventory service. Same topic, two
    # groups => each gets a full independent copy of the stream. This one line
    # is the entire reason adding a new consumer of order events requires zero
    # changes to the producer. Fan-out for free.

    RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "eventflow.notifications")

    JSON_SORT_KEYS = False


class DevConfig(BaseConfig):
    DEBUG = True


class ProdConfig(BaseConfig):
    DEBUG = False
    # DEBUG=True in production exposes the Werkzeug interactive debugger, which
    # is a remote code execution console on your server. People have lost jobs
    # over this exact setting.


class TestConfig(BaseConfig):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
    SQLALCHEMY_ENGINE_OPTIONS = {}   # SQLite rejects pool_size


def get_config() -> type[BaseConfig]:
    return {"local": DevConfig, "dev": DevConfig,
            "production": ProdConfig}.get(os.getenv("ENVIRONMENT", "local"), ProdConfig)
    # Note the DEFAULT: an unrecognised ENVIRONMENT falls back to ProdConfig,
    # the SAFE option. Defaulting to DevConfig would mean a typo in a
    # deployment variable silently turns the debugger on in production.
