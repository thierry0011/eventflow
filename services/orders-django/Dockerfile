# ---------------------------------------------------------------------------
# Multi-stage build.
#
# Stage 1 compiles wheels (needs gcc, headers, ~400MB of build tooling).
# Stage 2 copies only the finished wheels into a clean slim image.
#
# Result: a runtime image without a compiler in it. That is both ~300MB smaller
# AND materially more secure — an attacker who gets code execution in your
# container cannot build anything if there is nothing to build with.
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY services/orders-django/requirements.txt .
RUN pip wheel --wheel-dir /wheels -r requirements.txt


FROM python:3.12-slim AS runtime

# PYTHONDONTWRITEBYTECODE  no .pyc clutter in a read-only-ish container
# PYTHONUNBUFFERED         logs appear immediately instead of being buffered
#                          until the process dies — essential for container logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/libs \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /wheels /wheels
COPY services/orders-django/requirements.txt /tmp/requirements.txt
RUN pip install --no-index --find-links=/wheels -r /tmp/requirements.txt && rm -rf /wheels

# Never run as root. If the process is compromised, an unprivileged user limits
# what the attacker can reach — and it stops accidental writes to system paths.
RUN useradd --create-home --uid 10001 appuser

WORKDIR /app
COPY --chown=appuser:appuser libs /libs
COPY --chown=appuser:appuser services/orders-django /app
RUN pip install -e /libs

USER appuser
EXPOSE 8000

# Docker-level healthcheck; Kubernetes would use the same endpoint as a probe.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8000/health/live || exit 1

CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3"]
