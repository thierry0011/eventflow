"""
Django settings for the orders service.

Read this file as a checklist of every production concern Django hands you.
Juniors treat settings.py as boilerplate. Every line here is a decision, and
the comments say what the decision is.

12-FACTOR: every value that differs between environments comes from the
environment. There is no `if ENVIRONMENT == "prod"` branching in this file,
because that pattern means your test environment runs different code paths
than production and the difference only shows up when it hurts.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from eventflow_common import setup_logging

BASE_DIR = Path(__file__).resolve().parent.parent

setup_logging(os.getenv("SERVICE_NAME", "orders"))


def env(key: str, default: str | None = None, *, required: bool = False) -> str:
    """
    Fail at STARTUP on missing config, never at 3am on the first request that
    happens to need it. "Crash early and loudly" is a feature.
    """
    value = os.getenv(key, default)
    if required and not value:
        raise RuntimeError(f"required environment variable {key} is not set")
    return value or ""


def env_bool(key: str, default: bool = False) -> bool:
    return os.getenv(key, str(default)).lower() in {"1", "true", "yes", "on"}


def env_int(key: str, default: int) -> int:
    return int(os.getenv(key, default))


# --- core -----------------------------------------------------------------
SECRET_KEY = env("DJANGO_SECRET_KEY", required=True)
DEBUG = env_bool("DJANGO_DEBUG", False)
# DEBUG=True in production leaks your settings, your SQL and your stack traces
# on any 500 page. It is the single most common Django security failure.

ALLOWED_HOSTS = env("DJANGO_ALLOWED_HOSTS", "*").split(",")
CSRF_TRUSTED_ORIGINS = [o for o in env("CSRF_TRUSTED_ORIGINS", "").split(",") if o]

SERVICE_NAME = env("SERVICE_NAME", "orders")
ENVIRONMENT = env("ENVIRONMENT", "local")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "django_filters",
    "django_celery_beat",
    "orders",
]

MIDDLEWARE = [
    # Order matters and runs top-down on the way in, bottom-up on the way out.
    "orders.middleware.CorrelationIdMiddleware",   # first: everything below logs with it
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "orders.middleware.MetricsMiddleware",         # last: measures everything above
]

ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"

TEMPLATES = [{
    "BACKEND": "django.template.backends.django.DjangoTemplates",
    "DIRS": [],
    "APP_DIRS": True,
    "OPTIONS": {"context_processors": [
        "django.template.context_processors.request",
        "django.contrib.auth.context_processors.auth",
        "django.contrib.messages.context_processors.messages",
    ]},
}]

# --- database -------------------------------------------------------------
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": env("POSTGRES_DB", "orders_db"),
        "USER": env("POSTGRES_USER", "eventflow"),
        "PASSWORD": env("POSTGRES_PASSWORD", "eventflow"),
        "HOST": env("POSTGRES_HOST", "localhost"),
        "PORT": env("POSTGRES_PORT", "5432"),

        # CONN_MAX_AGE: reuse a connection across requests instead of opening a
        # new TCP+TLS+auth handshake every time. 60s is a sane default.
        # CAUTION: with many gunicorn workers this multiplies — workers x
        # replicas must stay under Postgres `max_connections`. Past a few dozen
        # replicas you need PgBouncer, and then you set CONN_MAX_AGE=0 and let
        # the pooler own pooling.
        "CONN_MAX_AGE": env_int("DB_CONN_MAX_AGE", 60),
        "CONN_HEALTH_CHECKS": True,   # verify a reused connection is still alive

        "OPTIONS": {
            # Never let one bad query hold a connection forever.
            "options": "-c statement_timeout=15000",
            "connect_timeout": 5,
        },
        "ATOMIC_REQUESTS": False,
        # Deliberately False. ATOMIC_REQUESTS wraps every request in one
        # transaction, which sounds safe and is actually a trap: it holds a
        # DB transaction open for the whole request including slow external
        # calls, and it makes it impossible to commit before publishing. We
        # take transactions explicitly, narrowly, where they belong.
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# --- cache / redis --------------------------------------------------------
REDIS_URL = env("REDIS_URL", "redis://localhost:6379")
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": f"{REDIS_URL}/0",
        "TIMEOUT": env_int("CACHE_TTL_SECONDS", 60),
    }
}

# --- auth -----------------------------------------------------------------
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
     "OPTIONS": {"min_length": 10}},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]
# Django hashes with PBKDF2 by default and will transparently upgrade a user's
# hash on their next login if you reorder this list. Argon2 is stronger; it
# needs the `argon2-cffi` package.
PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.PBKDF2PasswordHasher",
    "django.contrib.auth.hashers.PBKDF2SHA1PasswordHasher",
]

# --- DRF ------------------------------------------------------------------
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework_simplejwt.authentication.JWTAuthentication",
        *(("rest_framework.authentication.SessionAuthentication",) if DEBUG else ()),
    ),
    # Secure by default: locked unless a view opts out. The reverse default
    # (AllowAny, opt into security) means one forgotten decorator is a breach.
    "DEFAULT_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    "DEFAULT_FILTER_BACKENDS": (
        "django_filters.rest_framework.DjangoFilterBackend",
        "rest_framework.filters.OrderingFilter",
    ),
    # ALWAYS paginate a list endpoint. An unpaginated list is a table scan plus
    # a 50MB JSON response waiting for your data to grow.
    "DEFAULT_PAGINATION_CLASS": "orders.pagination.CursorPageNumberPagination",
    "PAGE_SIZE": 25,
    "DEFAULT_THROTTLE_CLASSES": (
        "rest_framework.throttling.UserRateThrottle",
        "rest_framework.throttling.AnonRateThrottle",
    ),
    "DEFAULT_THROTTLE_RATES": {
        "user": f"{env_int('RATE_LIMIT_PER_MINUTE', 120)}/min",
        "anon": "20/min",
    },
    "EXCEPTION_HANDLER": "orders.exceptions.api_exception_handler",
    "DEFAULT_RENDERER_CLASSES": (
        "rest_framework.renderers.JSONRenderer",
        *(("rest_framework.renderers.BrowsableAPIRenderer",) if DEBUG else ()),
    ),
}

SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(seconds=env_int("JWT_ACCESS_TTL_SECONDS", 900)),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=7),
    "ROTATE_REFRESH_TOKENS": True,
    "BLACKLIST_AFTER_ROTATION": False,
    "ALGORITHM": env("JWT_ALGORITHM", "HS256"),
    "SIGNING_KEY": env("JWT_SECRET", required=True),
    # Short access token + longer refresh token is the standard shape: a stolen
    # access token expires in minutes, and revocation only needs to handle the
    # refresh token. A 30-day access token cannot be revoked at all.
}

# --- celery ---------------------------------------------------------------
CELERY_BROKER_URL = f"{REDIS_URL}/1"
CELERY_RESULT_BACKEND = f"{REDIS_URL}/2"
CELERY_TASK_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT = ["json"]        # NEVER pickle: it executes arbitrary code
CELERY_TIMEZONE = "UTC"
CELERY_TASK_ACKS_LATE = True            # ack after the task runs, so a worker
                                        # crash re-queues the task instead of
                                        # losing it (requires idempotent tasks)
CELERY_TASK_REJECT_ON_WORKER_LOST = True
CELERY_WORKER_PREFETCH_MULTIPLIER = 1   # fair dispatch for long tasks
CELERY_TASK_TIME_LIMIT = 300            # hard kill
CELERY_TASK_SOFT_TIME_LIMIT = 240       # raises inside the task so it can clean up
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

# --- messaging ------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_ORDERS = env("KAFKA_TOPIC_ORDERS", "orders.events")
KAFKA_TOPIC_INVENTORY = env("KAFKA_TOPIC_INVENTORY", "inventory.events")
CONSUMER_GROUP = "orders-service"
OUTBOX_POLL_INTERVAL_SECONDS = env_int("OUTBOX_POLL_INTERVAL_SECONDS", 2)
OUTBOX_BATCH_SIZE = env_int("OUTBOX_BATCH_SIZE", 100)
RESERVATION_TTL_SECONDS = env_int("RESERVATION_TTL_SECONDS", 900)

# --- security -------------------------------------------------------------
# All no-ops behind a plain-HTTP local proxy, all essential in production.
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SECURE_SSL_REDIRECT = env_bool("SECURE_SSL_REDIRECT", not DEBUG)
SESSION_COOKIE_SECURE = not DEBUG
CSRF_COOKIE_SECURE = not DEBUG
SESSION_COOKIE_HTTPONLY = True          # JS cannot read it => XSS cannot steal it
SECURE_HSTS_SECONDS = 0 if DEBUG else 31_536_000
SECURE_HSTS_INCLUDE_SUBDOMAINS = not DEBUG
SECURE_CONTENT_TYPE_NOSNIFF = True
X_FRAME_OPTIONS = "DENY"

# --- i18n / static --------------------------------------------------------
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"      # store UTC, always. Render local in the client.
USE_I18N = True
USE_TZ = True
STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
}

# Logging is configured by eventflow_common.setup_logging(); tell Django not to
# install its own handlers on top and give us every line twice.
LOGGING_CONFIG = None
